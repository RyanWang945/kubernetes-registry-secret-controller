# 固定参数下的 NS / SA 规模实验

## 1. 固定项与变量

本组实验只增加目标资源数量。业务 client 固定 QPS=100、burst=200，Namespace Secret controller 和 ServiceAccount controller 各 16 个 worker。一个 registry；每个目标 NS 都只有 default、workload 两个 SA。测试 50、200、1000、2500、5000 个已有 NS，每档独立运行三轮。

每轮先准备已有 NS、SA 和测试权限，清除上一轮 Secret 和托管引用，再启动同一镜像的单副本 controller。初始化完成后等待 2 秒，触发凭证刷新。准备、重置和资源清理都不计入刷新时间。

刷新期间额外创建一个 NS，其中同样只有两个 SA。没有向已有 NS 额外插入第三个 SA。曲线横轴是注入前的目标 NS 数量，注入后实际多出 1 个 NS 和 2 个 SA。

## 2. 环境

| 项目 | 本轮实际配置 |
| --- | --- |
| 宿主机 | Mac mini M4 |
| 虚拟化 | Lima / VZ / ARM64 |
| 控制面 VM | 2 vCPU、4 GiB |
| 两个 worker VM | 每台 2 vCPU、3 GiB |
| Kubernetes | K3s v1.36.3+k3s1，SQLite/Kine |
| K3s Namespace controller 并发 | concurrent-namespace-syncs=30 |
| 被测 controller 位置 | 固定在 lima-k8s-worker-1 |
| 被测容器上限 | CPU 1500m、内存 1 GiB |
| 压测驱动 client | QPS=200、burst=400；与被测组件的限流独立 |
| 驱动准备并发 | 25 |

博客原文中的三台 2C/2GB 是此前环境描述；本轮按上述实际 VM 配置记录，不将两批结果混作同环境对照。组件与观测程序的 Go 源码哈希、二进制哈希和环境记录保存在 [provenance.json](provenance.json)。

## 3. 计时口径

鉴权 provider 和刷新调度时钟使用 mock。token TTL 为 1 小时，测试刷新提前量为 10 分钟，推进 50 分钟触发真实 scheduler。Kubernetes API、informer、工作队列、controller 和资源写入均为真实执行。

三个指标分别为：

1. **整轮 Secret 刷新时间**：驱动开始发起时钟推进请求，到观测到所有已有目标 Secret 的凭证指纹均更新。包含推进请求及正常调度链路的开销。
2. **新 NS 的 Secret 创建时间**：新 NS 的 Create 请求成功返回，到观测到其中的托管 Secret 已包含本轮新凭证。
3. **新 NS 的两个 SA 完成时间**：同样从 NS 创建成功返回起算，到观测到 default、workload 两个 SA 都引用托管 Secret。它是 NS 创建后的端到端准备时间，包含 SA、测试 RoleBinding 的准备、监听、排队和重试，不是单次 HTTP PATCH 请求耗时。

时间差由同一个宿主机进程记录。分别记录 Secret 和 SA 完成时间，避免把 Secret 就绪当成两个 SA 均已完成。

注入门槛是观测到至少 25% 的已有 Secret 完成刷新，同时整轮刷新尚未结束。驱动每 10 毫秒检查一次；小规模刷新很快，因此实际注入进度可能超过 25%，原始 JSON 和 CSV 记录了实际比例。生成图表时还核验 NS 创建请求发出及成功返回均早于整轮刷新结束。

## 4. 校验与统计

正式实验运行于北京时间 2026-09-06 23:37 至 2026-09-07 00:18，共 15 轮，均通过最终状态校验。

| 已有 NS | 已有 SA | 整轮刷新中位数（秒） | 新 Secret 就绪中位数（毫秒） | 两个 SA 完成中位数（毫秒） |
| ---: | ---: | ---: | ---: | ---: |
| 50 | 100 | 0.0435 | 45.8 | 49.1 |
| 200 | 400 | 0.1837 | 129.6 | 133.3 |
| 1000 | 2000 | 7.7568 | 156.7 | 328.3 |
| 2500 | 5000 | 22.2137 | 158.9 | 332.4 |
| 5000 | 10000 | 46.3116 | 156.9 | 330.3 |

每轮检查全部目标 Secret、两个 SA 的最终状态，以及注入 NS 的凭证指纹。普通刷新期间已有目标 SA 的非预期更新数必须为 0；新 NS 不得先暴露旧一代凭证。运行前后核对镜像和参数相同。

曲线上的点是三轮中位数，误差线是三轮最小值至最大值。它们是本机这些实验的观察范围，不是生产流量的 p99 延迟或服务等级保证。

测试通过在新 NS 内创建 RoleBinding 授予写权限，权限传播和给自动创建的默认 SA 补标签可能触发短暂的 403 或乐观锁冲突。原始日志保留这些重试，不将“最后状态正确”表述成“过程中没有错误”。

极短的测试可能在 metrics-server 采集到新 Pod 前结束，这时汇总 CSV 将对应 CPU、内存值留空，不将缺少样本写成零资源消耗。三个图都直接使用完成时间，不依赖 metrics-server 采样。

本实验没有验证真实 ACR 接口延迟、真实镜像拉取或 Pod 启动时序。

## 5. 文件与复现

- [measurements.csv](measurements.csv)：逐轮结果，包含固定参数、三项耗时、实际注入进度和原始结果 SHA-256。
- [raw/](raw/)：各轮原始汇总 JSON。
- [medians.json](medians.json)：用于正文和图表的三轮中位数。
- [provenance.json](provenance.json)：源码、镜像和环境证据。
- 同目录 PNG / SVG：从以上结果生成的三个图。

压测参数位于 [component-blog.yaml](../../../test/performance/profiles/component-blog.yaml)。先按仓库的 E2E Dockerfile 构建当前 cmd/e2e-controller，将镜像导入本地 K3s，并创建 test/performance/manifests.yaml 中的测试资源。镜像名与单副本固定 worker 节点应与 provenance.json 一致。

```sh
GOCACHE="$PWD/.cache/go-build" go build -o .cache/blog-benchmark/perf-driver ./cmd/perf-driver
python3 test/performance/run_blog_series.py scale --qps=100 --burst=200 --workers=16 --repeats=3
python3 -m venv .cache/blog-venv
.cache/blog-venv/bin/pip install matplotlib==3.9.4
.cache/blog-venv/bin/python test/performance/plot_blog_series.py
```

绘图使用 matplotlib。运行脚本会操作明确命名的本地测试 Deployment 和 blog-* 测试数据集，原始运行日志写入 test/performance/results/blog-20260906。归档在本目录的结果用于保留这次实际测量，不以重新运行后的文件替代。

实验之间先移除注入的测试 NS。整批清理在测量之外进行；针对 K3s 较慢的 NS 回收，仅处理本轮标签匹配的测试资源，在遍历所有可删除的 namespaced 资源、确认无残留后，才完成空 NS 的删除。没有修改 K3s 并发参数来加速测量。
