# AI Pay Observability

供 backend、IAM、账务、结算和对账服务共用的 Go 日志库。版本化复用日志能力；各服务独立运行、落盘和管理审计数据库。

| 包 | 能力 |
| --- | --- |
| `logging` | `slog` JSON、访问/运行文件分离、轮转、Hertz 框架适配、统一配置 |
| `requestmeta` | Go context 中的 `request_id` / `trace_id`，不代表身份或权限 |
| `redact` | 结构化字段递归脱敏，不修改调用方数据 |
| `audit` | 审计事件契约、校验与脱敏，返回供服务存储适配器使用的记录 |
| `grpcobs` | unary gRPC 服务端访问日志、身份验证接点、关联传播和 panic 恢复 |

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

`grpcobs` 以传输 metadata 作为日志关联的来源，不依赖私有 protobuf SDK。现有 protobuf `RequestContext` 的字段若填充，应来自同一个 Go context；服务适配层不得让正文中的值覆盖已经建立的日志上下文。客户端覆盖旧关联 metadata 并保留其他字段，不修改调用者的 map。公网 HTTP 入口始终生成自己的 ID；worker/定时任务入口通过 `requestmeta.New` 建立流程上下文。

访问日志每个入站 unary 调用一条，包含方法、可信调用服务、状态码及耗时；不记录正文、metadata 全量、错误原文或 panic 值。实际跨服务用 `InfoContext` / `ErrorContext` 记录运行日志才能保留关联 ID。当前不提供流式 RPC、消息队列消费者或追踪后端；服务接通这些协议时再增加对应适配。

## 审计接入

业务层面向 `audit.Recorder` 窄接口；服务自己的存储适配器调用 `audit.Prepare(ctx, service, event)`，映射返回的 `audit.Record` 到本地 sqlc 参数。关键业务变更与审计使用同一 `pgx.Tx`，任何错误都须向事务属主返回；DDL、SQL 和迁移不放在公共库中。backend 的审计适配器是现有实现示例。

事件需包含可信操作人、范围、稳定动作、对象和结果；`accepted` 只表示异步受理，最终结果另写事件。失败/拒绝事件在业务回滚后独立记录。审计只追加，不自动清理；写入器不能证明身份真实性，也不替代业务幂等。摘要限制 12 KiB，不保存完整对象、凭据或请求。结构化脱敏不能保证任意自由文本安全。

## 安装与验证

本库保持私有。调用方使用已发布标签依赖，独立构建设置 `GOPRIVATE=github.com/aipay-dev/*`，并通过自己的 GitHub/SSH 身份读取仓库；不要提交绝对路径 replace。工作区开发可将本仓库加入 `go.work`，交付前用 `GOWORK=off` 验证真实版本依赖。

CI 的 `AIPAY_MODULES_TOKEN` 只需本仓库 Contents 读取权限，通过临时 Git credential helper 使用，不能写入仓库或构建参数。该 secret 需要在消费本库的仓库单独配置，文档同步 token 保持独立。

运行 `make fmt check`。测试覆盖文件轮转/并发/写入错误、脱敏、审计校验、两台内存 gRPC 服务间的关联传递和拒绝/panic 路径；不代表真实业务服务、mTLS 部署或生产日志采集已接通。
