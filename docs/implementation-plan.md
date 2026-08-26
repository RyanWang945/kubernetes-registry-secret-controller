# Kubernetes Registry Secret Controller 实现计划

本文档是实现进度的唯一检查清单。只有代码、测试和必要文档均完成并通过验证后，
对应项目才会标记为 `[x]`；尚未实现或未验证的项目保持 `[ ]`。

## 当前里程碑：监听与调谐骨架

- [x] 初始化 Go Module、目录结构、构建入口和本地测试命令。
- [x] 实现固定 ConfigMap 的数据模型、严格解析、校验和规范化。
- [x] 实现 RegistryKey 合并、Domain 唯一性检查和确定性配置快照。
- [x] 实现线程安全的最后有效配置存储；无效更新和删除保留旧配置。
- [x] 使用 SharedInformer 监听固定 ConfigMap、Namespace 和 ServiceAccount。
- [x] 将资源事件稳定映射到以 Namespace 为 Key 的 RateLimiting WorkQueue。
- [x] 实现两个 Resource Worker、有界重试、缓存同步和优雅退出。
- [x] 提供可替换的 Namespace Syncer 接口，为后续 Secret 调谐解耦。
- [x] 编写配置解析、规范化、配置存储和事件映射单元测试。
- [x] 编写 envtest 集成测试，验证真实 API Server 的初始 List 和持续 Watch。
- [x] 通过 `gofmt`、`go test`、`go test -race`、`go vet` 和 envtest 验证。

## 后续里程碑：凭证获取与调度

- [ ] 定义 TokenProvider、Credential Store 和可注入 Clock。
- [ ] 实现阿里云 ACR AK/SK `GetAuthorizationToken` Provider。
- [ ] 实现以 RegistryKey 为 Key 的延迟/限速队列和两个 Credential Worker。
- [ ] 实现 `expiresAt - 5m` 调度、陈旧任务检查和 5s～60s 失败退避。
- [ ] 实现 Registry 配置新增、删除、AK/SK 轮换和 Domain 变化的差异动作。
- [ ] 编写 Fake Provider/Fake Clock 调度与并发测试。

## 后续里程碑：Secret 与 ServiceAccount 调谐

- [ ] 实现 Docker Config 聚合、Registry State 和 `stateHash`。
- [ ] 实现固定名称 `auto-patch-secret` 的创建、更新和所有权保护。
- [ ] 实现 Pending Registry 保留旧 Auth/State 的合并语义。
- [ ] 实现目标 ServiceAccount 引用注入、去重和离开范围后的清理。
- [ ] 增加固定名称输出 Secret 的初始 List、持续 Watch 和漂移修复。
- [ ] 编写资源调谐、冲突重试和部分 Namespace 失败测试。

## 后续里程碑：恢复、高可用与可观测性

- [ ] 实现从受管 Secret 按 RegistryKey 恢复最新有效凭证。
- [ ] 实现部分分发恢复、Hash 冲突处理和全量重新收敛。
- [ ] 实现两个 Pod 副本的 Leader Election，只有 Leader 启动写入 Worker。
- [ ] 实现结构化日志、Kubernetes Event、Prometheus Metrics 和健康检查。
- [ ] 提供最小权限 RBAC、Deployment、ServiceAccount 和 ConfigMap 清单。
- [ ] 编写 Kind 端到端测试与部署、升级、卸载说明。
