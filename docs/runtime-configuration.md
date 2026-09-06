# 运行参数配置

生产与 E2E 入口默认使用 QPS 25、Burst 50，以及两类资源 Controller 各 8 个 Worker。
在现有 `registry-secret-controller-system/registry-secret-controller-config` 的 `data`
中增加以下可选字段，保留已有的 Namespace、ServiceAccount 和 Registry 配置：

```yaml
data:
  kubeAPIQPS: "25"
  kubeAPIBurst: "50"
  workers: "8"
```

| 字段 | 默认值 | 生效方式 |
| --- | --- | --- |
| `kubeAPIQPS` | 25 | 热更新；可表示为正 float32 的有限数值 |
| `kubeAPIBurst` | 50 | 热更新；正整数 |
| `workers` | 8 | 重启进程生效；正整数，分别用于 Namespace Secret 和 ServiceAccount Controller |

`workers` 表示每类 Controller 的并发数，默认合计 16 个资源 Worker；ACR 凭据调度
仍使用自己的 2 个 Worker。

## 配置优先级

有效 ConfigMap 字段 > 启动参数 > 内置默认值。

- 保留 `--kube-api-qps`、`--kube-api-burst` 作为启动值与缺省值。
- 未配置 `workers` 时，两类 Worker 分别使用已有的
  `--max-concurrent-namespace-reconciles` 和 `--max-concurrent-service-account-reconciles`。
- 删除单个字段回退到本进程的启动值；Worker 的回退同样需要重启。
- 空字符串、零、负数或其他非法值会拒绝整份配置，保留最后有效配置和实际限流值。
  删除整个 ConfigMap 也保留最后有效值。

## 启动与热更新

进程启动时以 10 秒超时直接读取固定 ConfigMap，完整校验后确定 Worker 数量，再创建
Manager。需要该 ConfigMap 的 `get` 权限。ConfigMap 不存在或无效时使用启动值，
Readiness 等待 Cache 中出现有效配置；其他读取错误中止本次启动，避免静默使用错误
的 Worker 数量。若 ConfigMap 在启动后才创建，其中的 Worker 配置也需要重启。

各副本的 Configuration Controller 都会接收更新。QPS/Burst 原位调整同一个令牌桶，
保留已消耗的额度；已预约等待的请求可能仍按原时间执行。生效日志包含旧值与新值。
Worker 变更记录实际数量、期望数量及 `restart_required`；启动日志和框架
`controller_runtime_max_concurrent_reconciles` 指标反映实际 Worker 数量。

动态限流作用于每个进程的业务 Kubernetes 客户端，Secret 与 ServiceAccount API
请求共享额度；缓存读取不消耗额度。Informer、发现、Event、指标认证和 Lease 客户端
继续使用独立限流，ACR 请求另行管理。因此这些配置不是整个进程或集群的总 API QPS 上限。

纯运行参数更新不会触发全量资源分发或重新取证；先前尚未完成的业务配置分发继续重试。
运行中修改 `workers` 后，应对实际 Controller Deployment 执行滚动重启；Follower
获得 Leader 身份时沿用启动时确定的 Worker 数量，不能代替进程重启。

## 验证

单元测试覆盖解析、优先级、字段回退、非法配置保留、令牌桶额度、并发更新与分发重试。
envtest 使用实际客户端验证两个副本热更新、低业务 QPS 下配置监听和 Lease 续约，
以及重启后两类 Worker 数量变化：

```sh
make test-race
make test-integration
```
