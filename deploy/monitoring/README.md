# 日志与监控接入

应用输出 JSON 到 stdout，默认 `--log-level=info`，排障时改为 `debug`。
复用现有集群日志采集器。指标包括框架自带指标和 [业务指标](../../docs/observability.md)。

## 内部网络接入

1. Controller Pod 添加标签 `app.kubernetes.io/name: kubernetes-registry-secret-controller`，
   保留默认 `--metrics-bind-address=:8080`。这里的资源不会创建 Controller Deployment。
2. 按实际环境修改 NetworkPolicy 中的监控 Namespace、Prometheus Pod 标签，并确认 CNI
   执行 NetworkPolicy；检查其他允许访问相同 Pod 的策略。然后应用 `kubectl apply -k deploy/monitoring`。
3. 已安装 Prometheus Operator 时，应用 `servicemonitor.yaml`；确保 Prometheus 的
   ServiceMonitor Namespace/标签选择器能发现它。抓取以每个 Pod endpoint 为单位，
   job 固定为 `registry-secret-controller`，保留 instance 标签。
4. Operator 用户应用 `prometheusrule.yaml`，确认其 Rule 选择器可以发现该资源；普通
   Prometheus 将 `rules.yaml` 加入 `rule_files`，并配置等价的 Kubernetes endpoint 发现。

规则默认一个集群一套 Controller。120 秒临期阈值、5 分钟收敛预算需要按目标规模
调整。`active` 标记实际运行 Leader-only Runnable 的进程，支持关闭选主；规则还
检查 `up`，避免停止抓取的旧 Leader 残留样本触发业务告警。

## 可选 TLS 与认证

在生产入口增加 `--metrics-secure=true --metrics-cert-dir=/etc/metrics-tls`，将包含
`tls.crt`、`tls.key` 的证书 Secret 挂载到该目录。证书需覆盖 metrics Service DNS，
不能跳过服务端证书校验。端口仍可使用 8080。

应用 `secure/rbac.yaml`，按实际 Controller ServiceAccount 调整绑定；将其中的
metrics-reader ClusterRole 绑定给实际执行抓取的 Prometheus ServiceAccount。
应用 `secure/servicemonitor-patch.yaml` 补丁，并在 ServiceMonitor 所在 Namespace
提供 `registry-secret-controller-metrics-ca` Secret 的 `ca.crt`。认证使用 Kubernetes
TokenReview/SubjectAccessReview，无需额外 sidecar。

```sh
kubectl patch servicemonitor registry-secret-controller -n registry-secret-controller-system --type=merge --patch-file=deploy/monitoring/secure/servicemonitor-patch.yaml
```

该补丁通过 bearerTokenFile 使用 Prometheus Pod 已挂载且自动轮换的 ServiceAccount
Token，需允许 ServiceMonitor 文件引用。若平台禁止此引用，改用平台管理并轮换的
Token Secret，配置 `authorization.credentials`，不要使用未经轮换的一次性 Token。

## 验证

修改规则时同步更新 `rules.yaml` 与 `prometheusrule.yaml` 的 spec；单元测试检查两者一致。

```sh
promtool check rules deploy/monitoring/rules.yaml
promtool test rules deploy/monitoring/rules.test.yaml
GOCACHE="$PWD/.cache/go-build" go test -race ./...
GOCACHE="$PWD/.cache/go-build" go test -tags=integration ./internal/controller ./internal/observability
GOCACHE="$PWD/.cache/go-build" go test ./internal/observability -run '^$' -bench=Collect5000 -benchmem
```

在目标集群检查 ServiceMonitor target、TLS/认证、允许和拒绝的访问来源、告警路由。
采集微基准只验证本地 Cache 汇总成本，不替代目标集群的资源收敛压测。
