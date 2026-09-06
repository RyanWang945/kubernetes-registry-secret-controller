# 日志与指标设计

## 1. 简介

本方案已接入生产与 E2E 入口，补齐 Controller 的生产可观测性。复用 `slog` JSON 日志、
controller-runtime Metrics Server 和 Kubernetes Event，接入集群已有日志与监控系统。
组件内不增加独立采集服务，也不改变凭据刷新和资源调谐的职责划分。

## 2. 需求分析

- 能区分 ACR 取证失败、Secret 分发失败、配置错误和所有权冲突，并定位具体资源。
- 分别观察内存凭据与已分发副本，避免“取证成功但业务仍使用过期凭据”的监控盲区。
- 正常等待、无变化调谐和退出不产生错误噪声；多副本切换不重复统计业务状态。
- 不输出 AK/SK、Token、Docker Auth 或完整配置；指标标签数量不随 Namespace 数量增长。
- 复用框架能力，以少量埋点、部署清单和测试完成接入。

## 3. 技术设计

### 3.1 日志与 Event

保留 JSON 日志输出到 stdout，由集群采集器负责收集与留存；增加 `--log-level`，
默认 `info`，排障时可使用 `debug`。统一字段为 `registry`、`namespace`、`name`、
`operation`、`reason` 和 `duration_seconds`，按场景提供必要字段。
运行参数生效日志记录 QPS/Burst 旧值与新值；Worker 差异记录实际/期望值和
`restart_required`，详见 [运行参数配置](./runtime-configuration.md)。

| 场景 | 处理方式 |
| --- | --- |
| 启动、配置生效、凭据刷新成功 | Info |
| 单个资源同步成功、无变化、等待 Secret | Debug |
| ACR 请求失败、构建失败、资源写入失败 | Error，由重试边界记录一次，底层返回带上下文的脱敏错误 |
| 配置无效、所有权冲突 | 记录错误；首次出现或状态变化时产生 Warning Event |
| 正常退出导致的取消 | 不记录业务错误 |

ServiceAccount Reconciler 将 `ErrManagedSecretNotReady` 转换为非错误的
`RequeueAfter` 指数退避，按 SA 从 100ms 起翻倍、最长 5s，成功后清零；debug 日志中的
`retry_after` 记录本次等待间隔。Secret Create 事件仍可提前唤醒，不修改已有稳定引用。
此项替代原方案中通过 Reconcile 错误表达正常依赖等待的行为。

Event 首版保留 `InvalidConfiguration` 和 `OwnershipConflict`，分别关联配置对象
和冲突 Secret，复用 Manager 的 EventRecorder。配置缺失使用日志和指标表达；
不为每次重试或每个 ServiceAccount 等待重复发送 Event。

### 3.2 指标

复用框架的 Reconcile 次数、错误和耗时、队列深度与等待时间、活跃 Worker、Leader、
REST 请求及 Go/进程指标。当前依赖的 `workqueue_retries_total` 包含正常延迟入队，
不能直接作为错误次数告警。

新增指标统一使用 `registry_secret_controller_` 前缀，下表省略该前缀：

| 名称 | 类型与标签 | 含义 |
| --- | --- | --- |
| `provider_requests_total` | Counter；`registry,result` | 实际 Provider 调用次数，result 为 success/error |
| `provider_request_duration_seconds` | Histogram；`result` | Provider 调用耗时，覆盖 15 秒请求超时范围 |
| `credential_expiration_timestamp_seconds` | Gauge；`registry` | 内存中匹配当前配置的凭据到期时间 |
| `secret_targets` | Gauge；`registry,state` | 各 Registry 的目标 Namespace 状态数量 |
| `oldest_secret_expiration_timestamp_seconds` | Gauge；`registry` | 目标受管副本中可解析的最早到期时间 |
| `config_valid` | Gauge | 最新配置有效为 1，无效或缺失为 0；不影响最后有效配置继续运行 |
| `observation_success` | Gauge | 业务状态采集成功为 1，缓存未同步或采集失败为 0 |
| `active` | Gauge | 当前进程正在运行 Leader 专属任务为 1，兼容关闭选主 |

`registry` 使用规范化 RegistryKey，不使用 Namespace、SA、Token、Hash、配置版本或
错误原文作标签。到期时间导出 Unix 秒，使用 `expiration_timestamp_seconds - time()`
计算剩余时间；没有凭据时不输出到期时间，由目标 pending 状态表达缺失。

`secret_targets` 对当前配置匹配且未终止的每个 Namespace/Registry 对只计一个状态，
按顺序判断：同名非受管为 conflict；缺失或删除中为 pending；Auth/State 结构损坏
为 invalid；凭据过期为 expired；与当前期望一致为 synced；其余为 pending。
旧版本或旧配置的完整凭据不能仅因与新期望不同就判为结构损坏。

### 3.3 接入与告警

新增 `internal/observability` 包，显式注册到 controller-runtime 的 `metrics.Registry`，
由启动入口创建并注入组件；测试使用独立 Registry。Provider 调用边界记录次数与
耗时，分发重试不计为 ACR 请求；Configuration Reconciler 更新配置有效状态。

Collector 从已同步的 Manager Cache、Config Store 和 Credential Store 快照汇总状态，
不在抓取时调用 ACR 或直连 API Server。分发状态以 Cache 观察到的 Secret 为准，
发布队列事件不代表分发完成。采集失败时输出 observation_success=0，省略无法可靠
计算的状态，不能伪报为零；固定状态输出零值，移除配置后不再输出对应 Registry。

两个 Pod 分别被抓取并保留 pod/instance 标签。Leader 负责输出凭据和分发状态 Gauge，
Follower 保留框架指标和 config_valid；告警同时筛选活动 Leader，关闭选主时按活动
实例处理。Readiness 继续表示加载过有效配置，Liveness 不依赖 ACR。

提供 metrics Service、可选 ServiceMonitor 和告警规则，默认每 30 秒抓取一次。
内部部署使用 ClusterIP，并通过已生效的 NetworkPolicy 限制采集来源；需要身份
认证时启用 controller-runtime 内置认证授权过滤器，配套 TLS 证书及双方 RBAC。
配置参数、清单与安装步骤见 [监控接入说明](../deploy/monitoring/README.md)。

| 告警条件 | 级别 |
| --- | --- |
| 全部实例不可抓取或持续无 Leader | Critical |
| 目标副本已过期，持续两个抓取周期 | Critical |
| 副本临期且仍未收敛 | Warning；临期阈值按分发耗时和重试预算配置 |
| 配置无效、采集失败、资源冲突/损坏/缺失持续超出收敛预算 | Warning |

错误率和队列指标用于排障，单次重试不直接告警。具体异常资源通过日志和 Event 定位。
指标规范参考 [Prometheus 埋点实践](https://prometheus.io/docs/practices/instrumentation/)，
端点接入参考 [Kubebuilder Metrics](https://book.kubebuilder.io/reference/metrics)。

## 4. 测试

- 单元测试：正常等待不增加调谐错误；实际 Provider 调用只计一次；错误只在边界记录；
  用测试凭据检查日志、Event 和指标无敏感值。
- 状态测试：覆盖正常、缺失、损坏、冲突、过期、旧版本及 Registry 删除；取证成功但
  分发失败时仍显示未收敛和旧副本到期时间；采集失败不能显示健康。
- 集成测试：实际 TLS `/metrics` 验证内置与业务指标、401/403/200；两个 Manager
  连接 envtest 验证切换后仅 Leader 输出业务状态；promtool 验证告警规则。
- 性能测试：5,000 Namespace、单 Registry 本地 Cache 汇总基准，Apple M4 上三轮
  p99 为 32.8～33.2 ms，每轮分配约 25.6 MiB；只访问内存，不涉及 ACR/API 请求。
- 目标集群验收：ServiceMonitor 发现、真实 RBAC、证书轮换、NetworkPolicy 与告警路由；
  验证采集对资源收敛的影响。这些尚未实测，本地基准不替代分发压测。
