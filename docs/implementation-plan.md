# Kubernetes Registry Secret Controller 实现计划

本文档是实现进度的唯一检查清单。只有代码、测试和必要文档均完成并通过验证后，
对应项目才会标记为 `[x]`；尚未实现或未验证的项目保持 `[ ]`。

## 已完成里程碑：监听与调谐骨架

- [x] 初始化 Go Module、目录结构、构建入口和本地测试命令。
- [x] 实现固定 ConfigMap 的数据模型、严格解析、校验和规范化。
- [x] 实现 RegistryKey 合并、Domain 唯一性检查和确定性配置快照。
- [x] 实现线程安全的最后有效配置存储；无效更新和删除保留旧配置。
- [x] 使用 controller-runtime Manager 统一管理 Cache、Controller、选主、探针和优雅退出。
- [x] 将 Cache 限定为固定 ConfigMap、Namespace、ServiceAccount 和固定名称输出 Secret。
- [x] 让 Configuration Controller 在全部副本运行，并在有效配置后全量触发 Namespace 级 Reconcile。
- [x] 建立仅在 Leader 执行的 Namespace 级调谐骨架，接入标准限速队列、有界并发 Reconcile 和 Follower Cache warmup。
- [x] 提供可替换的 Namespace Syncer 接口。
- [x] 编写配置解析、规范化、配置存储和事件映射单元测试。
- [x] 编写 Manager envtest 集成测试，验证初始 List、持续 Watch、标准重试和优雅退出。
- [x] 通过 `gofmt`、`go test`、`go test -race`、`go vet` 和 envtest 验证。

## PR #1 Review 修订

- [x] 使用 `"*"` 作为 Namespace 和 ServiceAccount 的全部匹配通配符。
- [x] 增加可选 `excludeNamespace`，默认不排除任何 Namespace。
- [x] 将 `Snapshot` 重命名为含义明确的 `ConfigurationSnapshot`。
- [x] 将 Controller 的 `Options` 重命名为 `ControllerOptions`。
- [x] 同步设计文档和测试，并通过完整验证。
- [x] 将无状态配置解析改为 `config.Parse` 包级入口，不在 Controller 中持有 Parser。
- [x] 明确 Manager Cache 中固定名称资源和集群范围资源的作用域。
- [x] 回复全部 PR review comment，保留线程未 resolved 供复查。

## 设计修订：拆分资源调谐

- [x] 明确 Namespace Secret Controller 只管理固定名称 Secret，ServiceAccount Controller 按 Namespace/Name 独立调谐。
- [x] 明确提前创建 Secret 只能缩小 Pod 创建竞态；一期不提供 Admission 强保证。
- [x] 将现有 Namespace Syncer 和事件映射收窄为 Secret 调谐。
- [x] 新增 ServiceAccount Controller，单次 Reconcile 只处理一个 ServiceAccount。
- [x] 在有效配置变化和受管 Secret 创建时入队相关 ServiceAccount。
- [x] 为两个资源 Controller 分别配置有界并发、Leader Election 和 warmup。
- [x] 更新单元测试和 envtest，验证对象级 Key、事件 fan-out 和独立重试。
- [x] 实时 ServiceAccount Create 以优先级 100 入队，保留初始 List 和普通事件优先级；验证抢先出队、Key 去重与提升，并通过 Manager 监听及实际 Patch 的 envtest 回归。

## 已完成里程碑：凭证获取与调度

- [x] 定义 TokenProvider、Credential Store 和可注入 Clock。
- [x] 实现阿里云 ACR AK/SK `GetAuthorizationToken` Provider。
- [x] 实现以 RegistryKey 为 Key 的延迟/限速队列和两个 Credential Worker。
- [x] 实现 `expiresAt - 5m` 调度、陈旧任务检查和 5s～60s 失败退避。
- [x] 实现 Registry 配置新增、删除、AK/SK 轮换和 Domain 变化的差异动作。
- [x] 编写 Fake Provider/Fake Clock 调度与并发测试。

## 已完成里程碑：Secret 与 ServiceAccount 调谐

- [x] 实现 Docker Config 聚合、Registry State 和 `stateHash`。
- [x] 实现固定名称 `auto-patch-secret` 的创建、更新和所有权保护。
- [x] 实现 Pending Registry 保留旧 Auth/State 的合并语义。
- [x] 在独立 ServiceAccount Controller 中实现引用注入、去重和离开范围后的清理。
- [x] 将固定名称输出 Secret 注册到 Manager Cache，并映射到 Namespace 级 Reconcile。
- [x] 实现固定名称输出 Secret 的漂移修复。
- [x] 编写 Secret/ServiceAccount 独立调谐、冲突重试和部分 Namespace 失败测试。

## 已完成：ServiceAccount 的受管 Secret 校验

- [x] Secret Builder 尚未取得任何凭据时不创建空 Secret。
- [x] ServiceAccount 新增固定引用前校验 Secret 存在、未在删除、明确受管、Type 正确且 `.dockerconfigjson` 非空。
- [x] Secret 暂时不可用时不新增引用、不移除已有引用，并交由 Controller 非错误延迟重排。
- [x] Secret Create 和受管身份变化唤醒相关 ServiceAccount；普通 Token/Data Update 不扇出。
- [x] 所有权冲突由 Namespace Secret Controller 集中记录 Error 日志和 Warning Event。
- [x] 补充单元测试和 envtest，覆盖暂时不可用、稳定引用、事件唤醒及正常轮换不 Patch SA。

## Review 修复与可观测性

- [x] Secret 删除携带 UID/resourceVersion 前置条件，覆盖对象替换与所有权变化竞态。
- [x] 隔离 Pending Registry 的域名变化和损坏条目，保留可用旧条目并继续分发其他 Registry。
- [x] 统一 JSON 日志与级别，正常依赖等待和退出不产生业务错误噪声。
- [x] 接入配置无效/所有权冲突 Event、Provider 与分发业务指标，以及采集失败和 Leader 标记。
- [x] 提供监控 Service、NetworkPolicy、可选 ServiceMonitor、告警规则和 TLS/RBAC 配置。
- [x] 验证告警规则、TLS 端点、双 Manager 指标切换与 5,000 Namespace 采集基准。
- [ ] 在目标集群验收 ServiceMonitor、真实 RBAC/CNI/证书轮换、告警路由及收敛影响。

## 已完成：运行参数配置

- [x] 默认使用 QPS 25、Burst 50，以及两类资源 Controller 各 8 个 Worker。
- [x] ConfigMap 暴露 `kubeAPIQPS`、`kubeAPIBurst`、`workers`，支持启动参数回退和整份配置校验。
- [x] 两个副本均可原位热更新业务客户端限流，保持 ConfigMap 监听与 Lease 客户端独立。
- [x] 启动前读取 Worker 配置，运行中变更记录重启要求；重启后按新数量创建 Worker。
- [x] 纯运行参数变化不触发全量分发，并保留未完成的业务发布重试。
- [x] 通过解析、并发、回退、真实客户端限流与 Worker 重启的单元测试和 envtest。
- [x] 同步配置说明和性能测试默认值，保留 legacy profile 复现旧参数。

## 下一里程碑：恢复与高可用

- [ ] 实现从受管 Secret 按 RegistryKey 恢复最新有效凭证。
- [ ] 实现部分分发恢复、Hash 冲突处理和全量重新收敛。
- [x] 使用 Manager Leader Election 约束现有 Namespace 级写入路径。
- [ ] 实现 Leader Runtime recovery gate，并在恢复完成后开放凭证和资源写入。
- [ ] 使用两个 Pod 验证 Leader 切换、Follower warmup 和丢失 Lease 后进程退出。
- [x] 接入结构化日志、Manager 基础 Prometheus Metrics、Liveness 和配置 Readiness。
- [ ] 提供最小权限 RBAC、Deployment、ServiceAccount 和 ConfigMap 清单。
- [ ] 编写 Kind 端到端测试与部署、升级、卸载说明。
