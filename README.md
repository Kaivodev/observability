# Kaivo Observability

供 backend、IAM、支付、账务、结算和对账服务共用的日志、OpenTelemetry 追踪、指标与任务观测库。审计数据库由各服务管理。

| 包 | 能力 |
| --- | --- |
| `logging` | `slog` JSON、stdout、访问/运行文件分离与轮转、Hertz 框架适配、统一配置 |
| `requestmeta` | Go context 中的 `request_id` / `trace_id`，不代表身份或权限 |
| `redact` | 结构化字段递归脱敏，不修改调用方数据 |
| `audit` | 审计事件契约、校验与脱敏，返回供服务存储适配器使用的记录 |
| `grpcobs` | gRPC 访问日志、可信身份接点、关联传播和 panic 恢复 |
| `tracing` | 显式 SDK、OTLP/gRPC 导出、W3C carrier 与有界刷新 |
| `metrics` | 显式 MeterProvider、累积 OTLP/gRPC 指标、属性过滤与基数限制 |
| `hertzobs` / `httpobs` | 公网 HTTP 根 span、服务端耗时直方图、协议结果归一化、安全出站 HTTP |
| `asynqobs` | 入队与逐次执行 span、重试/超时/panic 日志和安全错误 |

## v0.4.0（待发布）

新增指标 provider 和 Hertz HTTP 指标中间件。升级时注意，K8s 内 `logging.Load` 的默认输出由文件改为 stdout。依赖文件采集的部署请显式设置 `AIPAY_LOG_OUTPUT=file`。直接构造 `logging.Options` 时，`Output` 的零值仍使用文件。原先按位置构造 `logging.Options` 的调用方需补上新字段，或改用字段名。

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

## 日志输出方式

`AIPAY_LOG_OUTPUT` 接受 `file` 或 `stdout`。未设置时，`KUBERNETES_SERVICE_HOST` 非空选 stdout，否则选 file。本机和 Compose 栈如需文件，应显式设置 `AIPAY_LOG_OUTPUT=file`。

- `stdout` 下，访问、运行和 Hertz 框架日志共用加锁的标准输出，每条 JSON 单次写入。框架日志归入 `log_type=runtime`。连续写入失败只向 stderr 报告一次，恢复后重新计数。本库校验轮转参数，跳过文件和目录操作。K8s 中由 kubelet 负责轮转。
- `file` 下，每个进程使用独立可写目录。`AIPAY_LOG_DIR` 默认 `./logs`，访问与运行日志分别落入 `access.log` 和 `runtime.log`。轮转键 `AIPAY_LOG_MAX_SIZE_MB`、`AIPAY_LOG_MAX_BACKUPS`、`AIPAY_LOG_MAX_AGE_DAYS` 默认分别为 50、5、14。新目录和文件权限为 0700 和 0600，备份在打开或轮转后异步清理。

两种模式共用 UTC 时间、`service`、`instance`、`log_type`、追踪关联和结构化脱敏。`AIPAY_LOG_LEVEL` 默认 `info`，仅过滤运行和框架日志。`AIPAY_INSTANCE_ID` 为空时自动生成。调用方应使用稳定的服务名和安全的日志消息，审计仍写属主数据库。

## 指标配置

`metrics.Load(os.Getenv)` 读取下表。空端点会创建可用的 SDK MeterProvider，调用方可以照常注册仪表并记录。此时没有 reader，也不会连接 Collector。有端点时使用 PeriodicReader 推送累积值，全程显式传递 provider。

| `AIPAY_METRICS_` 后缀 | 默认值 | 约束 |
| --- | --- | --- |
| `ENDPOINT` | 空 | 与追踪共用校验，支持 `http(s)://host:port`，禁止凭据、业务路径和查询串 |
| `ALLOW_INSECURE` | `false` | 非回环明文端点须显式允许 |
| `CA_FILE` / `CERT_FILE` / `KEY_FILE` | 空 | HTTPS 使用 TLS 1.3，可指定 CA，客户端证书与私钥成对配置 |
| `INTERVAL` | `15s` | 1s 到 5m，含两端 |
| `EXPORT_TIMEOUT` | `5s` | 正值，严格小于导出间隔 |
| `SHUTDOWN_TIMEOUT` | `3s` | 正值，同时受调用方剩余预算限制 |
| `ENVIRONMENT` | `development` | 部署环境，最多 64 字节 |
| `CARDINALITY_LIMIT` | `2000` | 每个仪表 100 到 100000 个属性组合，含溢出序列 |

仪表只使用设计列出的标签，取值来自固定枚举、路由模板或部署配置。SDK 视图将键转为小写，并将点和连字符转为下划线后匹配禁用项，涵盖业务与关联 ID、交易号、幂等键、网络地址、联系方式、URL、查询和错误原文。服务可用 `metrics.Forbidden(key)` 测试键名。为防止过滤掉的属性随 exemplar 导出，此 provider 关闭 exemplar。超限测量归入 `otel.metric.overflow=true`，Prometheus 标签名为 `otel_metric_overflow`。[完整禁用键规则](metrics/attributes.go)

Collector 离线时，业务照常启动并记录指标。失败批次计入 `ExportFailures()`，每段连续故障仅记一次安全警告，恢复时记一条。失败批次不重试，下一次导出仍是累积值。

### Hertz HTTP 指标

```go
cfg, err := metrics.Load(os.Getenv)
if err != nil { return err }
meters, err := metrics.Open(ctx, metrics.Options{
    Config: cfg, Service: service, Version: version, Instance: logs.InstanceID,
}, logs.Runtime)
if err != nil { return err }
// 注册到服务的停机流程，在请求排空后、日志关闭前调用 meters.Shutdown(drainCtx)。
observeHTTP, err := hertzobs.Metrics(meters.Provider(), "public_api")
if err != nil { return err }
h.Use(observeHTTP) // 注册在恢复中间件和路由之前。
```

`kaivo.http.server.duration` 以秒记录，使用 5 ms 至 10 s 的 11 个桶边界。标签为 `listener`、`http_route`、`http_method` 和字符串形式的 `http_status_code`。未匹配的路由记为 `unmatched`，非标准方法记为 `_OTHER`。每个监听器的名称由调用方固定配置。

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

调用方使用本库已发布的标签。工作区开发可将本仓库加入 `go.work`，交付前用 `GOWORK=off` 验证发布版本依赖，避免提交本地 replace。

运行 `make fmt check` 执行格式化、vet 和 race 测试。库内使用进程内 OTLP/gRPC 接收端验证导出、资源、属性过滤、基数溢出及故障恢复，并验证日志输出和 Hertz 指标。真实服务到 Collector、Prometheus 和 Loki 的链路由 servicekit、服务与部署仓库联验。

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

## 名称与兼容性

项目品牌统一为 **Kaivo**，GitHub 组织为 **Kaivodev**。Go module 已迁到 `github.com/Kaivodev/observability`，`GOPRIVATE` 使用同样大小写的 `github.com/Kaivodev/*`；协议/签名标识及现有配置键暂时保留原值，属于兼容接口；改名不修改历史账本或协议语义。范围与迁移规则见[命名约定](https://github.com/Kaivodev/docs/blob/main/design/naming.md)。
