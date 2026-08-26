# Kubernetes Registry Secret Controller 设计文档

> 状态：Draft / 设计已收敛
>
> 版本：v0.3
>
> 更新时间：2026-08-24

## 1. 背景

Kubernetes 拉取私有镜像时，需要在 Pod 所在 Namespace 中创建
kubernetes.io/dockerconfigjson 类型的 Secret，并通过 Pod 或
ServiceAccount 的 imagePullSecrets 引用该 Secret。

当一个集群需要访问多个阿里云 ACR 实例，并使用有过期时间的临时
Registry Token 时，手工管理会产生以下问题：

- Secret 不能跨 Namespace 使用，需要重复创建；
- 临时 Token 过期前需要及时刷新；
- 新 Namespace 和 ServiceAccount 容易漏配；
- Secret 被删除或修改后需要自动修复；
- 固定周期全量扫描会产生无效的 Kubernetes API 和 ACR API 请求；
- 多个 Controller 副本、并发刷新和部分分发失败会增加状态一致性难度。

本项目一期实现一个运行在集群中的 Controller。集群 Owner 在唯一的
ConfigMap 中配置目标 Namespace、ServiceAccount 和一个 ACR 实例数组。
每个实例节点包含 Region、Instance ID、AK/SK 和访问域名。Controller
使用 AK/SK 调用 ACR GetAuthorizationToken，获取临时 Registry
Username/Token，并将多个实例的凭据合并到每个目标 Namespace 的一个
Secret 中。

### 1.1 一期目标

1. 用户只维护一个固定名称的 ConfigMap；
2. 支持配置一个或多个 ACR 经济版或其他企业版实例；
3. 每个实例节点自包含 Region、Instance ID、AK/SK 和 Domains；
4. 每个目标 Namespace 只创建一个合并后的输出 Secret；
5. 自动为目标 ServiceAccount 注入输出 Secret；
6. 每个 Registry 在集群中只维护一个当前期望凭据和一个刷新任务；
7. 在 Token 过期前五分钟刷新，不进行固定周期全量取证；
8. 新 Token 只获取一次，再有界并发分发到全部目标 Namespace；
9. Controller 重启或 Leader 切换后从输出 Secret 恢复凭据和刷新计划；
10. 通过初始 List、持续 Watch、延迟队列和重试队列实现最终一致；
11. 支持两个 Controller Pod 副本和 Leader Election；
12. 提供日志、Event、Metrics、Readiness 和 Liveness。

### 1.2 一期不做

- ACR Personal Edition；
- RRSA、WorkerRole、STS AssumeRole 或其他云身份模式；
- 通过 Kubernetes SecretRef 保存 AK/SK；
- Provider、Policy、Binding 等 CRD；
- 其他云厂商或通用 Registry Provider；
- Namespace Label Selector；
- Mutating Admission Webhook；
- 修改已经创建的 Pod；
- Kubelet Image Credential Provider；
- 节点侧安装和 Kubelet 配置管理；
- 按 Namespace 或 ServiceAccount 获取不同的 Registry Token；
- 镜像代理、缓存、同步、加速、签名或漏洞扫描；
- 自动卸载清理流程。

## 2. 核心概念和系统不变量

### 2.1 RegistryKey

一个 Registry 实例的唯一身份是：

~~~text
RegistryKey = regionID + "/" + instanceID
~~~

例如：

~~~text
cn-hangzhou/cri-aaaaaaaa
~~~

RegistryKey 用于：

- 配置规范化和重复实例合并；
- Credential Store Key；
- Credential Queue Key；
- State Annotation 中的 Key；
- 日志、Event 和 Metrics 的实例标识。

AK/SK、Domain 和数组顺序不属于 RegistryKey。AK/SK 轮换或 Domain
变化表示同一 Registry 的配置发生变化，不表示出现了新 Registry。

### 2.2 一个 Registry 只有一个当前期望凭据

对任意 RegistryKey，Controller 在集群级只维护一份当前期望凭据：

~~~text
RegistryCredential
├── username
├── token
├── refreshedAt
├── expiresAt
└── stateHash
~~~

同一 Registry 配置的多个 Domain 共用这份凭据。所有目标 Namespace
中的输出 Secret 都是这份凭据的分发副本，不会按 Namespace 单独调用
ACR 获取 Token。

稳定状态下，各 Namespace 中同一 Registry 的 username、token、
refreshedAt、expiresAt 和 stateHash 完全相同。

分发过程中允许新旧凭据短暂共存。例如：

| Namespace | 凭据 | 过期时间 |
| --- | --- | --- |
| ns-a | T2 | 13:00 |
| ns-b | T2 | 13:00 |
| ns-c | T1 | 12:00 |

这表示 ns-c 尚未收敛，不表示该 Registry 存在多份独立的当前凭据。

### 2.3 三个成功阶段

一次 Registry 刷新分为三个阶段：

1. ACR API 返回有效 Token 并写入 Credential Store：内存刷新成功；
2. 至少一个目标 Secret 更新成功：新凭据已获得持久化副本；
3. 所有目标 Secret 更新成功：新凭据完成全量分发。

Provider 获取成功与 Namespace 分发成功是两个不同状态。部分 Namespace
写入失败只重试资源同步，不能重复调用 ACR。

## 3. 用户场景

### 3.1 首次安装

集群 Owner 安装两个 Controller Pod 副本并创建唯一 ConfigMap。Leader
校验配置、获取每个 Registry 的临时凭据、创建输出 Secret，并注入目标
ServiceAccount。

### 3.2 管理多个 Registry

Owner 在 registries 数组中添加实例。Controller 对数组进行规范化，以
RegistryKey 聚合实例，将所有有效 Registry 的 Domain 写入同一个
.dockerconfigjson.auths。

### 3.3 新增 Namespace

当新 Namespace 匹配 namespace 配置时，Controller：

1. 使用 Credential Store 中的当前凭据创建 auto-patch-secret；
2. 将该 Secret 注入目标 ServiceAccount；
3. 不重新调用 ACR；
4. 不创建新的 Registry 刷新任务。

### 3.4 新增 ServiceAccount

当新 ServiceAccount 匹配 serviceaccount 配置时，Controller 立即追加
auto-patch-secret 引用，不调用 ACR。

一期不修改 Pod。Kubernetes 只在 Pod 创建时从 ServiceAccount 复制
imagePullSecrets，因此 Pod 必须在 ServiceAccount 完成注入后创建。
已经创建且缺少引用的 Pod 需要重新创建。

### 3.5 自动刷新

每个 Registry 按自己的 expiresAt 建立一个延迟任务：

~~~text
refreshAt = expiresAt - 5m
~~~

到达 refreshAt 前不访问 ACR API。获取一次新 Token 后，将全部目标
Namespace 加入资源同步队列。

### 3.6 输出 Secret 被删除或修改

Controller 持续监听固定名称的输出 Secret：

- Secret 被删除时重新创建；
- Docker Auth 或 State 被修改时恢复期望内容；
- 同名但不归 Controller 管理的 Secret 不覆盖，并报告 OwnershipConflict；
- 正常运行时使用 Credential Store 修复，不重新调用 ACR。

### 3.7 部分分发后重启

如果 T2 只写入部分 Namespace 后 Leader 退出，新 Leader 按 RegistryKey
聚合所有有效候选，选择 refreshedAt 最新的 T2，恢复到 Credential
Store，再将全部目标 Namespace 入队，使旧副本收敛到 T2。

如果 T2 尚未成功写入任何 Namespace，新 Leader 只能恢复 T1。由于 T1
已经到达刷新时间，新 Leader 会立即重新调用 ACR。这种情况只增加一次
API 调用，不会写入错误状态。

### 3.8 配置热更新

- 新增 Registry：立即获取 Token；
- 删除 Registry：取消逻辑调度并从全部输出 Secret 删除对应 Auth；
- AK/SK 变化：立即重新获取该 Registry 的 Token；
- 只修改 Domain：复用当前 Token，重新渲染并分发输出 Secret；
- namespace 或 serviceaccount 变化：同步新增目标并清理离开范围的目标；
- 数组顺序或 Domain 顺序变化：规范化后无实际变化；
- 新配置无效：继续使用上一份有效配置，不执行清理；
- ConfigMap 暂时缺失：继续使用上一份有效配置，不执行清理。

### 3.9 卸载

一期没有 enabled 开关，也不把 ConfigMap 删除视为清理信号。直接卸载
Controller 后，业务 Namespace 中的 auto-patch-secret 和
ServiceAccount 引用继续保留，直到用户按文档手工清理。

## 4. 总体架构

~~~mermaid
flowchart LR
    Owner[Cluster Owner] --> Config[固定 ConfigMap]
    Config --> Loader[Config Loader]
    Loader --> Controller[Leader Controller]
    Controller --> TokenQueue[Credential Queue]
    TokenQueue --> ACR[ACR GetAuthorizationToken]
    ACR --> Store[Credential Store]
    Store --> SyncQueue[Namespace Sync Queue]
    SyncQueue --> SecretA[ns-a / auto-patch-secret]
    SyncQueue --> SecretB[ns-b / auto-patch-secret]
    SyncQueue --> SA[ServiceAccounts]
    SecretA --> Kubelet[Kubelet]
    SecretB --> Kubelet
    Kubelet --> Registry[ACR Registry]
~~~

内部模块：

- Config Loader：读取、校验、规范化并热更新 ConfigMap；
- Resource Watcher：监听 ConfigMap、Namespace、ServiceAccount 和输出 Secret；
- Credential Store：保存每个 RegistryKey 的当前期望凭据；
- Credential Scheduler：按 expiresAt 管理 Registry 延迟任务和失败重试；
- ACR Token Provider：使用实例节点内的 AK/SK 调用 ACR API；
- Secret Builder：生成完整 Docker Config 和 State；
- Resource Syncer：有界并发同步 Namespace、Secret 和 ServiceAccount；
- Observability：日志、Event、Metrics 和健康检查。

## 5. 配置设计

### 5.1 固定 ConfigMap

Controller 只读取以下 ConfigMap：

~~~yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: registry-secret-controller-config
  namespace: registry-secret-controller-system
data:
  namespace: "all"
  serviceaccount: "default"

  registries: |
    - regionID: cn-hangzhou
      instanceID: cri-aaaaaaaa
      accessKeyID: LTAIxxxxxxxx
      accessKeySecret: xxxxxxxxxxxxxxxx
      domains:
        - registry-a-vpc.cn-hangzhou.cr.aliyuncs.com
        - registry-a.cn-hangzhou.cr.aliyuncs.com

    - regionID: cn-shanghai
      instanceID: cri-bbbbbbbb
      accessKeyID: LTAIyyyyyyyy
      accessKeySecret: yyyyyyyyyyyyyyyy
      domains:
        - registry-b.cn-shanghai.cr.aliyuncs.com
~~~

ConfigMap 中没有 ControllerConfiguration、apiVersion、kind、provider、
identity、instanceType、enabled、beforeExpiry、jitter、secretName 或
Worker 数量等内部配置。

### 5.2 namespace

namespace 支持：

~~~yaml
namespace: "all"
~~~

或者英文逗号分隔的明确名称：

~~~yaml
namespace: "production,staging"
~~~

解析规则：

- 去除每项前后空格；
- 去重；
- all 不能和其他名称混用；
- 空值或非法 Namespace 名称使新配置无效；
- all 模式默认排除 kube-system、kube-public、kube-node-lease 和
  registry-secret-controller-system；
- Named 模式只处理明确列出的 Namespace。

一期不支持 Namespace Label Selector。

### 5.3 serviceaccount

serviceaccount 支持：

~~~yaml
serviceaccount: "default"
~~~

英文逗号分隔的多个名称：

~~~yaml
serviceaccount: "default,build"
~~~

或者：

~~~yaml
serviceaccount: "all"
~~~

解析、去重和 all 互斥规则与 namespace 相同。

### 5.4 Registry 节点

registries 是 RegistryConfig 数组：

~~~go
type RegistryConfig struct {
    RegionID        string   `yaml:"regionID"`
    InstanceID      string   `yaml:"instanceID"`
    AccessKeyID     string   `yaml:"accessKeyID"`
    AccessKeySecret string   `yaml:"accessKeySecret"`
    Domains         []string `yaml:"domains"`
}
~~~

每个节点必须满足：

- regionID、instanceID、accessKeyID、accessKeySecret 非空；
- instanceID 是 ACR 经济版或其他企业版实例 ID；
- domains 非空；
- Domain 不包含 scheme、路径、查询参数或用户信息；
- Domain 统一转成小写，去除末尾点，去重并排序；
- 同一个 Domain 不能属于不同 RegistryKey。

### 5.5 重复 Registry 合并

Config Loader 按 RegistryKey 分组：

- RegistryKey 相同且 AK/SK 相同：合并、去重并排序 Domains；
- RegistryKey 相同但 AK/SK 不同：拒绝整份新配置，报告
  ConflictingCredentialsForRegistry；
- 不同 RegistryKey 使用同一 Domain：拒绝整份新配置；
- 数组顺序和 Domain 顺序不影响规范化结果。

规范化后的运行时配置是：

~~~go
map[RegistryKey]RegistryConfig
~~~

### 5.6 固定运行参数

一期不在 ConfigMap 中暴露以下参数：

| 参数 | 固定值 |
| --- | --- |
| 输出 Secret 名称 | auto-patch-secret |
| 刷新提前量 | 5m |
| Jitter | 无 |
| Credential Worker | 2 |
| Resource Sync Worker | 2 |
| Controller Pod 副本 | 2 |
| Leader 写入者 | 1 |

## 6. ACR Token 获取

### 6.1 AK/SK 的作用

配置中的 AK/SK 只供 Controller 调用阿里云 ACR OpenAPI。Controller
调用 GetAuthorizationToken 获取临时 Username、Token 和 ExpireTime。
业务输出 Secret 中只保存临时 Registry 凭据，不保存 AK/SK。

AK/SK 对应的 RAM 身份至少需要：

- cr:GetAuthorizationToken；
- 目标仓库的 cr:PullRepository。

一期不需要 Push 权限，也不调用 ListInstanceEndpoint，因为访问 Domain
由用户明确配置。

### 6.2 内部接口

配置层只支持 ACR，但内部保留一个小接口以便 Fake Provider 测试：

~~~go
type RegistryKey struct {
    RegionID   string
    InstanceID string
}

type CredentialRequest struct {
    Key             RegistryKey
    AccessKeyID     string
    AccessKeySecret string
}

type Credential struct {
    Username  string
    Token     string
    ExpiresAt time.Time
}

type TokenProvider interface {
    GetCredential(ctx context.Context, req CredentialRequest) (Credential, error)
}
~~~

实现要求：

- ACR Endpoint 根据 regionID 构造；
- 每次刷新一个 RegistryKey 只调用一次 GetAuthorizationToken；
- RefreshedAt 由 Controller 在 API 成功返回并完成响应校验后使用当前时钟生成；
- 同一个 RegistryKey 通过 WorkQueue Key 去重和 Keyed Lock 防止并发取证；
- 外部请求必须设置超时；
- 校验返回值完整且 ExpiresAt 晚于当前时间和刷新安全窗口；
- SDK 调试日志默认关闭；
- 日志、Event、Metrics 和错误不得包含 AK/SK、Username 或 Token。

## 7. 输出 Secret 和状态

### 7.1 Secret 格式

每个目标 Namespace 只维护一个固定名称 Secret：

~~~yaml
apiVersion: v1
kind: Secret
metadata:
  name: auto-patch-secret
  namespace: production
  labels:
    app.kubernetes.io/name: kubernetes-registry-secret-controller
    app.kubernetes.io/managed-by: kubernetes-registry-secret-controller
  annotations:
    registry-secret-controller.io/state: |
      {
        "version": 1,
        "registries": {
          "cn-hangzhou/cri-aaaaaaaa": {
            "refreshedAt": "2026-08-24T11:00:00Z",
            "expiresAt": "2026-08-24T12:00:00Z",
            "stateHash": "sha256:aaa..."
          },
          "cn-shanghai/cri-bbbbbbbb": {
            "refreshedAt": "2026-08-24T11:10:00Z",
            "expiresAt": "2026-08-24T12:10:00Z",
            "stateHash": "sha256:bbb..."
          }
        }
      }
type: kubernetes.io/dockerconfigjson
data:
  .dockerconfigjson: <base64>
~~~

.dockerconfigjson 解码后：

~~~json
{
  "auths": {
    "registry-a-vpc.cn-hangzhou.cr.aliyuncs.com": {
      "username": "temporary-user-a",
      "password": "temporary-token-a",
      "auth": "base64(username:password)"
    },
    "registry-a.cn-hangzhou.cr.aliyuncs.com": {
      "username": "temporary-user-a",
      "password": "temporary-token-a",
      "auth": "base64(username:password)"
    },
    "registry-b.cn-shanghai.cr.aliyuncs.com": {
      "username": "temporary-user-b",
      "password": "temporary-token-b",
      "auth": "base64(username:password)"
    }
  }
}
~~~

同一 RegistryKey 的全部 Domain 必须使用同一 Username 和 Token。

### 7.2 State

State 按 RegistryKey 保存：

~~~go
type PersistedState struct {
    Version    int                      `json:"version"`
    Registries map[string]RegistryState `json:"registries"`
}

type RegistryState struct {
    RefreshedAt time.Time `json:"refreshedAt"`
    ExpiresAt   time.Time `json:"expiresAt"`
    StateHash   string    `json:"stateHash"`
}
~~~

不持久化：

- nextRefreshAt，因为可以从 ExpiresAt 重新计算；
- WorkQueue 延迟任务；
- 当前退避次数；
- 进程内 Lock；
- 分发中的 Namespace 列表。

### 7.3 stateHash

每个 Registry 只保存一个 stateHash，不再区分 configHash 和 authHash。
stateHash 使用 SHA-256 校验配置、状态和 Docker Auth 是否一致，不作为
安全签名。

Hash 输入使用确定性 Canonical JSON，包含：

- regionID；
- instanceID；
- accessKeyID；
- accessKeySecret；
- 规范化并排序后的 domains；
- 临时 username；
- 临时 token；
- refreshedAt；
- expiresAt。

规范化规则：

- JSON 字段名和顺序固定；
- Domain 已小写、去重并排序；
- 时间统一为 UTC RFC3339Nano；
- 不使用简单字符串无分隔拼接；
- auth 是 username:token 的派生字段，不重复参与 Hash，但恢复时必须校验。

State Annotation 和 .dockerconfigjson 在同一次 Kubernetes Secret
Create/Update 中提交。

## 8. Leader 启动与状态恢复

每次获得 Leader 身份时执行完整恢复，而不只在进程首次启动时执行。

### 8.1 初始 List

Leader List 集群中名称为 auto-patch-secret 且带受管标签的 Secret。
恢复候选可以来自当前或之前的目标 Namespace；资源分发和清理仍以当前
namespace 配置为准。Informer 随后持续 Watch 同一固定名称的 Secret。

### 8.2 按 Registry 恢复

恢复必须按 RegistryKey 独立进行，不能选择一个完整 Secret 作为全局
赢家。算法如下：

1. 加载并规范化当前 ConfigMap；
2. 读取全部候选输出 Secret；
3. 解析 State 和 .dockerconfigjson；
4. 按 RegistryKey 聚合候选；
5. 使用当前 RegistryConfig 和 Auth 重新计算 stateHash；
6. 校验候选包含该 Registry 的全部 Domain；
7. 校验各 Domain 的 Username/Token 一致且 auth 编码正确；
8. 丢弃 State 版本不支持、Hash 不匹配或已过期的候选；
9. 选择 refreshedAt 最新的有效候选；
10. 相同 refreshedAt 出现不同 stateHash 时，不任意选择，立即重新取证；
11. 将恢复结果写入 Credential Store；
12. 已到 refreshAt 或没有候选的 Registry 立即加入 Credential Queue；
13. 其他 Registry 通过 AddAfter 安排到 refreshAt；
14. 将全部目标 Namespace 加入 Resource Sync Queue。

仍在当前配置中但没有有效候选的 Registry 标记为 Pending，并立即取证。
Pending 不等同于已删除；在取得替代凭据前，资源同步不得从已有受管
Secret 中删除该 Registry 的旧 Auth 和 State。

不同 Registry 可以从不同 Namespace 的 Secret 恢复。例如 Registry A
从 ns-a 恢复，Registry B 从 ns-b 恢复，再由 Secret Builder 合并为最新
完整期望状态。

## 9. 凭据调度与刷新

### 9.1 延迟队列

凭据调度使用 client-go Typed RateLimiting/Delaying WorkQueue，不自行
实现最小堆或 Timer。

队列 Key 是 RegistryKey：

~~~text
cn-hangzhou/cri-aaaaaaaa
cn-shanghai/cri-bbbbbbbb
~~~

正常刷新使用：

~~~go
queue.AddAfter(key, time.Until(refreshAt))
~~~

Provider 失败使用：

~~~go
queue.AddRateLimited(key)
~~~

### 9.2 刷新流程

两个 Credential Worker 有界并发执行：

1. 从队列取得 RegistryKey；
2. 读取最新规范化配置和配置 Generation；
3. 判断 Registry 是否仍存在；
4. 重新计算并检查 refreshAt；
5. 陈旧或提前唤醒的任务重新安排，不调用 ACR；
6. 获取 Keyed Lock；
7. 使用该节点的 AK/SK 调用一次 GetAuthorizationToken；
8. 校验新凭据；
9. 原子更新 Credential Store，使新凭据成为当前期望凭据；
10. 为全部目标 Namespace 调用 Resource Sync Queue Add；
11. Forget 当前失败退避；
12. 按新 ExpiresAt 安排下一次刷新。

如果配置把刷新时间推迟，旧任务可以提前唤醒；Worker 二次检查后重新
AddAfter。Registry 被删除后，旧任务成为无操作。

### 9.3 失败退避

Provider 失败退避：

~~~text
5s → 10s → 20s → 40s → 最大 60s
~~~

规则：

- 保留 Credential Store 和输出 Secret 中的旧凭据；
- 旧凭据未过期时继续使用；
- 已过期时仍不主动删除 Secret，并持续报告不可用状态；
- Provider 成功后清除退避计数；
- 单个 Registry 失败不阻塞其他 Registry；
- 不存在固定五分钟或其他周期的全量 Registry 扫描。

## 10. Namespace 资源同步

### 10.1 Resource Sync Queue

队列 Key 是 Namespace 名称，使用两个 Resource Sync Worker。

一次同步读取 Credential Store 的最新完整快照，构建该 Namespace 的
完整期望 Secret。队列任务不携带某次 Token，避免排队期间旧任务覆盖
更新的凭据。

如果 Registry A 和 B 同时刷新，它们会添加相同 Namespace Key。
WorkQueue 对 Key 去重；Worker 执行时读取包含 A、B 最新状态的快照。

### 10.2 Secret 同步

对目标 Namespace：

1. 输出 Secret 不存在时创建；
2. 已存在且由本 Controller 管理时比较并更新；
3. 内容和 State 已符合期望时不写入；
4. 同名但不受管时不覆盖并报告 OwnershipConflict；
5. 使用 resourceVersion 冲突重试；
6. 单个 Namespace 失败时独立退避；
7. 分发失败不重新调用 ACR。

Secret Builder 必须区分以下两种情况：

- RegistryKey 已从有效配置删除：删除其 Auth 和 State；
- RegistryKey 仍在配置中但处于 Pending：已有受管 Secret 保留该
  Registry 当前的旧 Auth 和 State，新 Namespace 暂时不包含该 Registry。

因此，一个 Registry 暂时取证失败时，其他 Registry 的成功同步不会把
它的旧凭据从现有 Namespace 中顺带删除。新凭据取得后再统一替换并收敛。

当 Namespace 离开目标范围时，只删除明确受管的 auto-patch-secret，并
清理目标 ServiceAccount 中的保留引用。

### 10.3 ServiceAccount 同步

Secret 创建或更新成功后再处理 ServiceAccount：

- 保留其他名称的 imagePullSecrets；
- 确保 auto-patch-secret 引用只出现一次；
- auto-patch-secret 名称及其目标 SA 引用视为 Controller 保留资源；
- 使用 resourceVersion 冲突重试；
- ServiceAccount 离开目标范围时移除该固定引用；
- ServiceAccount 不存在时等待其创建事件，不创建 ServiceAccount；
- 不修改 Pod 自己显式设置的 imagePullSecrets。

### 10.4 全量监听和收敛

Controller 对固定名称 Secret 执行初始 List 和持续 Watch：

~~~text
metadata.name=auto-patch-secret
~~~

Watch 的目的包括：

- 发现删除和漂移；
- 发现某个 Namespace 仍持有旧 stateHash；
- 在 Leader 启动时提供恢复候选；
- 驱动各 Namespace 最终收敛。

Secret Watch 不为每个 Secret 创建刷新任务。若某个 Namespace 持有旧
expiresAt，只将该 Namespace 加入 Resource Sync Queue，不调用 ACR。

Controller 自己的 Secret Update 也会触发 Watch。Reconcile 发现
stateHash 和内容已一致后成为无操作，不形成写入循环。

## 11. 事件映射

| 事件 | 动作 |
| --- | --- |
| ConfigMap 更新 | 校验、规范化、比较新旧配置并执行差异动作 |
| ConfigMap 删除 | 继续使用最后一份有效配置，不清理 |
| Namespace 进入目标范围 | 同步 Secret 和 ServiceAccount |
| Namespace 离开目标范围 | 清理受管 Secret 和固定 SA 引用 |
| ServiceAccount 创建或变化 | 入队所在 Namespace |
| 输出 Secret 删除或漂移 | 入队所在 Namespace |
| 输出 Secret 已符合期望 | 无操作 |
| Registry 到达 refreshAt | 调用一次 ACR 并入队全部目标 Namespace |

Controller 不监听 Pod，也不通过固定周期扫描 Registry、Namespace 或
Secret。

## 12. 配置热更新细节

Config Loader 每次都先完整解析和规范化新配置，再原子替换当前配置。
不允许应用半份配置。

| 配置差异 | 动作 |
| --- | --- |
| 新增 RegistryKey | 立即入 Credential Queue |
| 删除 RegistryKey | 从 Store 删除，旧任务无操作，全部 Namespace 重同步 |
| AK/SK 变化 | 立即重新取证 |
| 只增加或删除 Domain | 复用当前 Token，重新计算 stateHash 并重同步 |
| Registry 数组顺序变化 | 无操作 |
| Domain 顺序变化 | 无操作 |
| namespace 新增目标 | 使用当前凭据同步，不取证 |
| namespace 删除目标 | 清理受管 Secret 和固定 SA 引用 |
| serviceaccount 新增目标 | 注入固定引用 |
| serviceaccount 删除目标 | 移除固定引用 |

启动时配置无效则 Readiness 失败且不运行 Worker。运行中收到无效配置或
ConfigMap 删除事件时，保留最后一份有效配置、Credential Store 和输出
Secret，并报告 InvalidConfiguration。

## 13. 并发、一致性与高可用

### 13.1 固定并发

- Deployment 固定两个 Pod 副本；
- Leader Election 保证只有一个写入者；
- Leader 内部运行两个 Credential Worker；
- Leader 内部运行两个 Resource Sync Worker；
- 同一个 RegistryKey 不并发取证；
- 同一个 Namespace 不并发同步；
- 不为每个 Namespace 无限制创建 Goroutine。

### 13.2 部分分发

Kubernetes 不支持跨 Namespace Secret 事务，因此允许短暂的新旧 Token
共存：

~~~text
ACR 返回 T2
  → Credential Store 切换到 T2
  → ns-a 写入成功
  → ns-b 写入成功
  → ns-c 写入失败并退避
  → 其他 Namespace 继续同步
~~~

新凭据的下一次刷新任务在 Provider 成功后即建立，不等待所有 Namespace
写入成功。资源分发和 Provider 调度使用独立队列。

### 13.3 Leader 切换

Follower 保持 ConfigMap 和 Informer Cache 更新，但不运行写入 Worker。
获得 Leader 身份后必须重新执行第 8 节的完整恢复流程，再启动调度和资源
同步。

如果多个 Secret 存在不同版本，按 RegistryKey 选择最新有效候选，不按
Namespace 多数表决，也不使用读到的第一个 Secret。

## 14. 故障处理

### 14.1 ACR API 失败

- 保留旧 Credential 和 Secret；
- 仍在配置中的 Pending Registry 不因其他 Registry 同步而被删除；
- 按 RegistryKey 独立指数退避；
- Event 和日志不包含 AK/SK 或 Token；
- 权限修复或 API 恢复后继续刷新；
- 已过期凭据通过 Event 和 Metrics 报告。

### 14.2 Kubernetes API 失败

- 只重试失败的 Namespace；
- 已成功 Namespace 不回滚；
- Credential Store 中的新 Token 保留；
- 不重复获取 Token；
- 使用 Kubernetes Client QPS/Burst 限流和 resourceVersion 冲突重试。

### 14.3 所有 Secret 写入失败后重启

如果新 Token 没有任何持久化副本，新 Leader 恢复旧 Token。旧 Token
已经到 refreshAt 时立即重新取证。这是允许的额外 API 调用。

### 14.4 所有权冲突

同名非受管 Secret 不覆盖、不删除。Controller 报告
OwnershipConflict，并等待 Secret 或配置变化后再次调谐。

## 15. 安全和边界

一期明确允许 AK/SK 直接存在 ConfigMap 中，这是为了简化部署和鉴权
流程。部署者必须限制 ConfigMap 的读写权限，并理解 ConfigMap 不提供
Secret 级别的保密语义。

仍需遵守：

- AK/SK、临时 Username、Token 和 Docker Auth 不写入日志、Event 或 Metrics；
- ACR RAM 身份只授予 GetAuthorizationToken 和所需仓库的 PullRepository；
- 输出 Secret 只包含临时 Registry 凭据；
- 推荐启用 Kubernetes Secret 静态加密；
- Controller 容器使用非 Root 和只读根文件系统；
- ACR API 使用 TLS 并校验证书；
- Controller ServiceAccount 使用满足功能所需的最小 RBAC；
- 目标 Namespace 中有 Secret 读取权限的主体可以读取临时 Token；
- stateHash 是完整性校验，不是防篡改签名。

## 16. 可观测性

建议指标：

- registry_secret_controller_provider_requests_total{region_id,instance_id,result}；
- registry_secret_controller_provider_request_duration_seconds{region_id,instance_id}；
- registry_secret_controller_credential_seconds_until_expiry{region_id,instance_id}；
- registry_secret_controller_resource_sync_total{result}；
- registry_secret_controller_resource_sync_duration_seconds；
- registry_secret_controller_out_of_sync_namespaces；
- registry_secret_controller_managed_namespaces；
- registry_secret_controller_queue_depth{queue}；
- registry_secret_controller_retries_total{queue,reason}。

建议 Event：

- CredentialRefreshed；
- ProviderUnavailable；
- CredentialExpired；
- SecretCreated；
- SecretRepaired；
- ServiceAccountInjected；
- OwnershipConflict；
- InvalidConfiguration；
- ConflictingCredentialsForRegistry。

Readiness 条件：

- 固定 ConfigMap 已加载且有效；
- Informer Cache 已同步；
- Follower 正常参与 Leader Election，或者 Leader 的 Worker 已启动；
- 单个 Registry 暂时失败不使整个 Pod NotReady。

Liveness 只检查进程和核心 Goroutine 是否存活，不依赖 ACR 或单个
Kubernetes 资源同步结果。

## 17. 测试

### 17.1 单元测试

配置：

- namespace 和 serviceaccount 的 all、逗号列表、去重和非法值；
- Registry 数组解析；
- RegistryKey 生成；
- 重复实例且 AK/SK 相同的 Domain 合并；
- 重复实例但 AK/SK 不同的冲突；
- 不同 Registry 使用同一 Domain 的冲突；
- Domain 规范化、去重和排序；
- 无效热更新保留上一份配置。

凭据和状态：

- 单 Registry 单 Domain；
- 单 Registry 多 Domain 共用凭据；
- 多 Registry 合并到一个 Docker Config；
- stateHash 确定性；
- 数组和 Domain 顺序不影响 stateHash；
- State 与 Docker Auth 绑定校验；
- State 版本不支持和 Hash 损坏；
- Registry 删除后准确删除对应 Auth。
- 一个 Registry 处于 Pending 时，其他 Registry 同步不删除其旧 Auth；

调度：

- refreshAt 等于 ExpiresAt 减五分钟；
- Fake Clock 下到期前不调用 Provider；
- 到期时一个 Registry 只调用一次 Provider；
- 三个 Registry 同时到期时最多两个并发；
- 相同 Registry 不并发刷新；
- 陈旧任务提前唤醒后重新安排；
- Registry 删除后旧任务无操作；
- Provider 失败按 5s 至 60s 退避；
- 不存在固定周期全量扫描。

资源同步：

- 一百个 Namespace 仍只获取一次 Registry Token；
- Namespace Queue 按 Key 去重；
- Worker 读取最新完整 Credential Store 快照；
- Secret 删除和漂移后恢复；
- 同名非受管 Secret 不覆盖；
- 保留 SA 的其他 imagePullSecrets；
- auto-patch-secret 引用不重复；
- SA 不存在时等待创建事件；
- Namespace 离开范围后清理受管资源。

恢复：

- 从单个 Secret 恢复；
- 从多个一致副本恢复；
- 从部分分发副本选择 refreshedAt 最新凭据；
- 只有一个 Namespace 持有新凭据时仍选择新凭据；
- 不按多数选择旧凭据；
- Registry A 和 B 从不同 Namespace 恢复；
- 相同 refreshedAt 但 stateHash 冲突时重新取证；
- 没有有效候选时重新取证；
- 新 Token 尚未持久化时重启并允许再次调用 Provider。

### 17.2 Envtest 集成测试

1. ConfigMap 创建后完成初始同步；
2. 新 Namespace 进入目标范围后创建 auto-patch-secret；
3. Namespace 离开范围后清理受管 Secret 和 SA 引用；
4. ServiceAccount 创建后注入引用；
5. Secret 删除或漂移后自动修复；
6. 同名非受管 Secret 返回所有权冲突；
7. 多 Registry 写入同一个 Secret；
8. AK/SK 更新只刷新对应 Registry；
9. Domain 更新复用当前 Token；
10. Registry 删除准确清理 Auth；
11. 无效热更新和 ConfigMap 删除保留上一份有效配置；
12. Provider 和 Kubernetes API 错误分别重试；
13. Pending Registry 的旧 Auth 在其他 Registry 更新时得到保留；
14. Leader 退出后新 Leader 恢复部分分发状态。

测试使用 Fake Provider 和 Fake Clock，不依赖真实等待时间。

### 17.3 端到端测试

在 Kind 或受控测试集群中：

- 部署两个 Controller Pod 并验证 Leader Election；
- 配置多个 ACR 测试实例；
- 创建多个 Namespace 和 ServiceAccount；
- 验证每个 Namespace 只有一个 auto-patch-secret；
- 验证 Secret 包含多个 Registry Domain；
- 创建 Pod 拉取私有测试镜像；
- 模拟 Token 轮换并验证新 Pod 拉取成功；
- 删除或修改 Secret 后验证恢复；
- 在部分 Namespace 分发成功时重启 Leader；
- 同时安排多个 Registry 到期并验证并发上限；
- 扩展 Namespace 数量并确认 Provider 调用次数不随 Namespace 增长。

真实 ACR 测试不得在日志或 CI Artifact 中输出 AK/SK 或临时 Token。

## 18. 验收标准

1. 用户只维护一个固定 ConfigMap；
2. 配置只包含 namespace、serviceaccount 和 Registry 实例数组；
3. 每个 Registry 节点包含 regionID、instanceID、AK/SK 和 domains；
4. RegistryKey 固定为 regionID + "/" + instanceID；
5. 重复实例使用相同 AK/SK 时合并 Domain，不同 AK/SK 时拒绝；
6. 每个目标 Namespace 只有一个 auto-patch-secret；
7. 一个 Registry 在集群中只有一份当前期望凭据和一个刷新任务；
8. 一百个 Namespace 对同一 Registry 只产生一次 Token 获取；
9. Token 在过期前五分钟刷新；
10. 两个 Credential Worker 和两个 Resource Worker 实现有界并发；
11. Controller 重启后能从最新有效 Secret 副本恢复；
12. 部分分发后不按多数回滚到旧凭据；
13. Secret 删除或漂移后自动修复；
14. ServiceAccount 保留其他引用并正确追加固定 Secret；
15. 不存在固定周期全量扫描；
16. 配置错误、Provider 错误、资源冲突和凭据过期均可观测；
17. 日志、Event 和 Metrics 不泄露 AK/SK 或临时 Token。

## 19. 发布与运维

一期发布物：

- Controller 容器镜像；
- Namespace、ServiceAccount、RBAC、Lease、Deployment 和 ConfigMap 模板；
- Kustomize 安装清单；
- 配置参考和最小示例；
- 指标、告警和故障排查说明；
- 手工卸载清理说明；
- SBOM、镜像摘要和版本变更记录。

Deployment 固定 replicas: 2，并启用 Leader Election。滚动升级期间只有
一个 Leader 执行取证和写入。

卸载步骤：

1. 停止业务变更；
2. 删除 Controller Deployment；
3. 按受管标签删除各 Namespace 中的 auto-patch-secret；
4. 从目标 ServiceAccount 移除 auto-patch-secret 引用；
5. 删除 RBAC、Lease、ConfigMap 和 Controller Namespace。

直接删除 ConfigMap 不触发清理。若不执行第 3、4 步，现有 Secret 和
ServiceAccount 引用将继续保留。

## 20. 参考资料

- [Kubernetes：Pull an Image from a Private Registry](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/)
- [Kubernetes：Images / Using a private registry](https://kubernetes.io/docs/concepts/containers/images/)
- [Kubernetes：ServiceAccount API](https://kubernetes.io/docs/reference/kubernetes-api/core-resources/service-account-v1/)
- [阿里云 ACR：GetAuthorizationToken](https://help.aliyun.com/en/acr/developer-reference/api-cr-2018-12-01-getauthorizationtoken)
