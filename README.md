# AI Pay Observability

供 backend、IAM、支付、账务、结算和对账服务共用的日志、OpenTelemetry 追踪与任务观测库；各服务独立运行、落盘和管理审计数据库。

| 包 | 能力 |
| --- | --- |
| `logging` | `slog` JSON、访问/运行文件分离、轮转、Hertz 框架适配、统一配置 |
| `requestmeta` | Go context 中的 `request_id` / `trace_id`，不代表身份或权限 |
| `redact` | 结构化字段递归脱敏，不修改调用方数据 |
| `audit` | 审计事件契约、校验与脱敏，返回供服务存储适配器使用的记录 |
| `grpcobs` | gRPC 访问日志、可信身份接点、关联传播和 panic 恢复 |
| `tracing` | 显式 SDK、OTLP/gRPC 导出、W3C carrier 与有界刷新 |
| `hertzobs` / `httpobs` | 公网 HTTP 根 span、协议结果归一化、安全出站 HTTP |
| `asynqobs` | 入队与逐次执行 span、重试/超时/panic 日志和安全错误 |

指标导出尚未实现。按 docs 仓库的[监控指标与告警](https://github.com/aipay-dev/docs/blob/main/design/operations.md#监控指标与告警)设计，后续以 OpenTelemetry 指标经 OTLP 推送到 Collector，再进入 Prometheus 与 Grafana；资金域只主动推送，监控不作为资金控制的依据。

## 服务初始化

```go
options, level, err := logging.Load("ledger", os.Getenv)
if err != nil { return err }
logs, err := logging.Open(options, level)
if err != nil { return err }
defer logs.Close() // 实际入口应处理关闭错误，并在请求排空后关闭。
logs.Runtime.Info("service starting")
// 将 logs.Runtime / logs.Access 显式注入组件。
// Hertz 使用 hlog.SetLogger(logs.Framework)，在服务启动前设置。
```

统一读取 `AIPAY_LOG_DIR`（`./logs`）、`AIPAY_LOG_LEVEL`（`info`）、`AIPAY_INSTANCE_ID`（空则生成）、`AIPAY_LOG_MAX_SIZE_MB`（50）、`AIPAY_LOG_MAX_BACKUPS`（5）、`AIPAY_LOG_MAX_AGE_DAYS`（14）。`service` 由程序固定传入，不能取自请求；每个实例使用独立可写目录。

访问日志始终记录；运行日志按等级过滤。正常服务日志写入文件，目录无法打开则启动失败，运行中写入失败向 stderr 报告且不回显内容。时间统一为 UTC；备份清理在打开/轮转后异步执行，活动文件不按年龄删除。新目录/文件为 0700/0600，部署负责已有路径权限和持久卷。

## gRPC 接入

```go
observer, err := grpcobs.NewServer(logs.Access, logs.Runtime, verifyServiceIdentity)
if err != nil { return err }
server := grpc.NewServer(grpc.ChainUnaryInterceptor(observer.Unary(), authorize))
// 客户端在现有连接选项中追加：
grpc.WithChainUnaryInterceptor(grpcobs.UnaryClient())
```

`verifyServiceIdentity(ctx)` 必须根据已验证的 mTLS/服务凭据返回稳定服务名；不得从自报请求头取得。未提供验证器无法创建 observer。验证失败时拒绝请求，不采信其关联 ID；验证通过后才读取 `x-request-id` / `x-trace-id`。缺少全部关联信息时生成本地 ID；部分缺失、重复或非法值返回 InvalidArgument。业务权限、资源归属和私网监听仍由服务负责。

`grpcobs` 传递 request ID；存在有效 OpenTelemetry SpanContext 时，trace ID 以 SDK 为准，不依赖私有 protobuf SDK。现有 protobuf `RequestContext` 的字段若填充，应来自同一个 Go context；服务适配层不得让正文中的值覆盖已经建立的日志上下文。客户端覆盖旧关联 metadata 并保留其他字段，不修改调用者的 map。公网 HTTP 入口始终创建新根 span；worker 使用下述 carrier 恢复流程。无 SDK 上下文的旧调用仍保留 UUID trace ID 兼容行为。

访问日志每个入站 unary 调用一条，包含方法、可信调用服务、状态码及耗时；不记录正文、metadata 全量、错误原文或 panic 值。实际跨服务用 `InfoContext` / `ErrorContext` 记录运行日志才能保留关联 ID。gRPC span 由 servicekit 的官方 `otelgrpc` StatsHandler 创建（含流式调用），必须与经过 mTLS 验证的连接一起使用。日志拦截器本身不创建重复 RPC span。

## 审计接入

业务层面向 `audit.Recorder` 窄接口；服务自己的存储适配器调用 `audit.Prepare(ctx, service, event)`，映射返回的 `audit.Record` 到本地 sqlc 参数。关键业务变更与审计使用同一 `pgx.Tx`，任何错误都须向事务属主返回；DDL、SQL 和迁移不放在公共库中。backend 的审计适配器是现有实现示例。

事件需包含可信操作人、范围、稳定动作、对象和结果；`accepted` 只表示异步受理，最终结果另写事件。失败/拒绝事件在业务回滚后独立记录。审计只追加，不自动清理；写入器不能证明身份真实性，也不替代业务幂等。摘要限制 12 KiB，不保存完整对象、凭据或请求。结构化脱敏不能保证任意自由文本安全。

## 安装与验证

本库保持私有。调用方使用已发布标签依赖，独立构建设置 `GOPRIVATE=github.com/aipay-dev/*`，并通过自己的 GitHub/SSH 身份读取仓库；不要提交绝对路径 replace。工作区开发可将本仓库加入 `go.work`，交付前用 `GOWORK=off` 验证真实版本依赖。

CI 的 `AIPAY_MODULES_TOKEN` 只需本仓库 Contents 读取权限，通过临时 Git credential helper 使用，不能写入仓库或构建参数。该 secret 需要在消费本库的仓库单独配置，文档同步 token 保持独立。

运行 `make fmt check`。测试覆盖文件轮转/并发/写入错误、脱敏、审计校验、两台内存 gRPC 服务间的关联传递和拒绝/panic 路径；不代表真实业务服务、mTLS 部署或生产日志采集已接通。

## 追踪配置与兼容性

应用使用 `tracing.Load(os.Getenv)` 和 `tracing.Open(ctx, tracing.Options{Config: cfg, Service: service, Version: version, Instance: logs.InstanceID}, logs.Runtime)`，将 `Runtime.Provider()` 显式注入 HTTP、RPC、任务组件。没有全局 provider。日志自动使用当前 span 的 `trace_id`、`span_id`、`trace_sampled`，覆盖同名手填属性；`request_id` 仍是 UUID。接入 SDK 后旧 UUID trace ID 不再充当链路标识，消费者须使用有效 span context。

| `AIPAY_TRACE_` 后缀 | 默认值 / 含义 |
| --- | --- |
| `ENDPOINT` | 空：创建/传播有效上下文，不导出；设置 `http(s)://host:port` 后使用 OTLP/gRPC |
| `ALLOW_INSECURE` | `false`；非回环明文端点须显式允许 |
| `CA_FILE` / `CERT_FILE` / `KEY_FILE` | 空；HTTPS 使用 TLS 1.3，支持自定义 CA、成对客户端证书/私钥 |
| `SAMPLE_RATIO` | `1`；根 span 采样率 0–1，子 span 遵循父节点；未采样仍有有效 ID |
| `QUEUE_SIZE` / `BATCH_SIZE` | `2048` / `512` |
| `BATCH_INTERVAL` / `EXPORT_TIMEOUT` / `SHUTDOWN_TIMEOUT` | `1s` / `3s` / `3s` |
| `ENVIRONMENT` | `development` |

资源包含 `service.name`、版本、实例与环境。采集器不可用不影响启动、就绪和业务；缓冲满时丢弃 span，不阻塞请求。导出失败按连续故障记录一次安全警告，恢复时记录恢复事件，`ExportFailures()` 统计失败批次；没有磁盘补发，失败批次可能丢失。停机先排空调用、关闭依赖，再以进程剩余停机预算调用 `Runtime.Shutdown(ctx)`，最后关闭日志。刷新失败只作诊断。

`hertzobs.Middleware` 必须位于关联 ID、访问日志和恢复中间件之外；忽略公网 traceparent，按路由模板命名，并用调用方的 `Result` 判断 HTTP 200 中的业务失败。`httpobs.Transport` 只记录稳定操作名、方法和状态，不保存 URL、查询或正文；span 持续至响应体关闭/读完。默认删除出站 traceparent/tracestate/baggage；仅明确可信内网目标设置 `Propagate: true`，不要在可重定向到外部域名的客户端上启用。

## Asynq 与持久上下文

任务属主在本地事务中将 `asynqobs.NewMessage(ctx, type, payload)` 编码后与业务变更一起写入自己的 outbox。carrier 只含受限的 W3C traceparent/tracestate 与 request ID，禁止保存 baggage 或认证材料。派发器读已提交记录，调用 `Producer.Enqueue(freshContext, message, asynq.TaskID(stableID))`；恢复只带关联信息，不复用原请求的取消或截止时间。公共库不提供 outbox 表或业务事务。

producer span 表示入队结果；worker 每次尝试有独立 `span_id` / `execution_id`，重试保留 task ID 与原 trace。延期任务可设置 `Message.NewTrace=true`，执行建立新根并 link 到入队 span，日志保留 `origin_trace_id`。缺少 carrier 的独立任务建立新根；非法 metadata 返回 SkipRetry。访问日志包含队列、任务类型/ID、尝试次数、结果和耗时，不记录 payload 或错误原文；panic 只记录安全原因和堆栈，保留 `errors.Is` 重试信号，Redis LastErr 不含 handler 原文。

直接使用 Asynq 时同时设置 `Handler=observer.Middleware(handler)`、`ErrorHandler=observer`、`Logger=asynqobs.NewFrameworkLogger(runtimeLog)` 和 `BaseContext: func() context.Context { return observer.BaseContext(workerContext) }`；后者使框架自身的超时/租约错误也能关联到执行 span。框架日志只保留固定事件分类，不转储任意格式化参数。执行超时会结束观测记录；Go 无法强制终止忽略 context 的 handler，业务仍必须响应取消、使用数据库幂等并防止失效执行写入。连接和进程生命周期优先使用 servicekit/queue。

本库 race 测试验证真实 OTLP 接收、采样/资源、队列溢出、不可用采集器、有界刷新、上下文信任、HTTP 传播及任务异常。完整 PostgreSQL → Redis/Asynq → gRPC/HTTP → Collector/Jaeger 验证位于 servicekit；这些测试不代表支付或身份业务已经实现。
