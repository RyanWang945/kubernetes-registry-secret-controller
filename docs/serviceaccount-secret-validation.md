# ServiceAccount Patch 前的受管 Secret 校验

## 背景

Namespace Secret Controller 负责生成固定名称的镜像拉取 Secret，ServiceAccount
Controller 负责把该名称写入目标 ServiceAccount 的 `imagePullSecrets`。两个
Controller 通过 Kubernetes Secret 解耦，不能依赖调用顺序，也不能跨 Secret、
ServiceAccount 和 Pod 做原子事务。

ServiceAccount 引用的是稳定的 Secret 名称，不是某一版 Token。Token 轮换、配置
热更新、Cache 短暂延迟或受管 Secret 重建时，不应反复移除和添加该引用。已经创建的
Pod 也不会因为后续 Patch ServiceAccount 而自动更新 `imagePullSecrets`。

## 设计原则

职责边界如下：

- Secret Builder 不生成空的受管 Secret，Docker Auth 和 State 在一次 Kubernetes
  Create/Update 中原子写入；
- Namespace Secret Controller 负责 Secret 内容正确性、Token 更新、漂移修复和
  所有权冲突告警；
- ServiceAccount Controller 只判断固定名称 Secret 是否存在、未在删除、明确受管且
  具有最小合法结构；
- `stateHash` 只用于 Registry State 与 Docker Auth 的一致性校验和恢复，不参与
  ServiceAccount 引用判断；
- Token 是否临近过期或已经过期不改变 ServiceAccount 中稳定的 Secret 引用。

## 最小校验

ServiceAccount Syncer 每次准备新增固定引用前，通过 controller-runtime Cache 读取同
Namespace 的固定名称 Secret，并检查：

1. Secret 存在；
2. `deletionTimestamp` 为空；
3. 两个 Controller 身份标签均匹配；
4. Type 是 `kubernetes.io/dockerconfigjson`；
5. `.dockerconfigjson` 存在且非空。

该校验不解析完整 Docker Config，不重新计算 Hash，不调用 ACR，正常情况下也不直接
请求 API Server。它用于阻止引用明显不存在、非受管或结构不完整的 Secret；完整内容
仍由 Namespace Secret Controller 按期望状态修复。

## 调谐行为

- ServiceAccount 或 Namespace 不再匹配配置：移除固定引用，保留用户配置的其他
  `imagePullSecrets`；
- Secret 满足最小校验：确保固定引用恰好存在一次；
- Secret 不存在、正在删除或结构不完整：不新增引用，也不移除已经存在的固定引用，
  Syncer 返回 `ManagedSecretNotReady`，Controller 转换为 `RequeueAfter: 10s` 非错误重排；
- 同名 Secret 存在但不受管：不新增引用；已有固定引用时将其移除。所有权冲突由
  Namespace Secret Controller 记录 Error 日志并产生 Kubernetes Warning Event，
  不按 ServiceAccount 重复告警。

实时 ServiceAccount Create 以优先级 100 入队，先于普通配置分发，并按 Namespace/Name
去重；初始 List 和未变化的 Resync 保持低优先级，普通 Update/Delete 和配置分发使用
默认优先级。依赖等待和错误重试保留原调谐优先级，不抢占正在执行的任务，也不绕过
Secret 校验与 API 限流。

Secret Create 事件用于立即唤醒同 Namespace 中等待首次注入的目标 ServiceAccount；
延迟重排作为事件延迟或丢失时的兜底。Secret Update 只在受管身份发生变化时扇出
ServiceAccount，正常 Token 轮换和内容修复不触发 SA 全量调谐。Secret Delete 由
Namespace Secret Controller 负责重建，不主动摘除已有 SA 引用。

`NotFound`、删除中和结构暂不完整属于正常依赖等待，通过 Debug 日志和业务 Metrics
观察，不计入 Reconcile 错误，也不反复产生 Warning Event。Secret 创建
或更新失败的告警统一归属 Namespace Secret Controller。

## 一致性边界

Cache 可能短暂返回旧对象，本方案只保证最终收敛。保留稳定引用可以让 Secret 使用
同名对象完成重建或原地更新，而不会在短暂故障期间制造新的无引用 Pod。

异步 Controller 不能保证 Pod 一定晚于 Secret 和 ServiceAccount 就绪后创建。若业务
要求在 Secret 未准备好时拒绝 Pod，必须使用 Admission Webhook 或部署流程显式等待，
不由本 Controller 的 ServiceAccount Patch 承担。

## 测试

- Secret 不存在、正在删除、Type 错误或 `.dockerconfigjson` 为空时，不新增固定引用并
  返回依赖等待，由 Controller 非错误延迟重排；
- 上述暂时不可用场景不会移除 ServiceAccount 已有的固定引用；
- 受管且结构完整的 Secret 允许注入，并对重复引用去重；
- 同名非受管 Secret 阻止注入并清理固定引用，同时保留用户的其他引用；
- Secret Create 事件立即重新调谐等待注入的目标 ServiceAccount；
- Secret 受管身份发生变化时重新调谐，普通 Token/Data Update 不扇出
  ServiceAccount；
- Patch 冲突和 Secret 读取错误由 Controller 限速重试；
- 实时 SA Create 先于普通配置分发处理，同名不同 Namespace 的 SA 保持独立 Key，
  已排队的 Key 可提升优先级且不重复入队；初始 List 和普通事件不升级为实时 Create 优先级；
- 批量创建 ServiceAccount 时不产生逐个 ACR 请求，也不因正常 Token 轮换全量 Patch。
