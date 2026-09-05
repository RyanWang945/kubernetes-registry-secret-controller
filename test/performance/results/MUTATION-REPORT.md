# Expiry refresh concurrent-onboarding report

Date: 2026-09-04 (Asia/Shanghai)

Result: **PASS for correctness. Creating one target ServiceAccount and one preselected Namespace while 1,000 existing Secrets were refreshing did not measurably slow the existing fan-out. The ServiceAccount patched in 13.7 ms after Create returned. The new Namespace was not lost and received only the current credential generation, but it waited 28.95 seconds behind the existing Namespace queue before becoming usable.**

Follow-up: the live Namespace priority change is implemented and validated in
[`PRIORITY-REPORT.md`](PRIORITY-REPORT.md); its same-process old/new comparison
reduced this queue-tail latency from 30.059 seconds to 0.350 seconds.

## Fixed environment

- Three Lima ARM64 VMs, K3s `v1.36.3+k3s1`, SQLite/Kine datastore.
- K3s process PID `620576`, started at 16:59 CST.
- `/etc/rancher/k3s/config.yaml` mtime stayed `2026-09-04 16:58:58.273372756 +0800` before and after both runs.
- Kube controller manager: `--concurrent-namespace-syncs=30`.
- Test controller: one replica, QPS 25, burst 50, Namespace workers 8, ServiceAccount workers 8.
- Mock token TTL 1 hour, refresh-before 10 minutes, fake clock advanced 50 minutes.
- Local controller image digest: `sha256:56f6a9a43dede28b44509f20272e64823be072861ac307da41a717e8127bb62c`.

Both result files captured the live Deployment image and full argument list before the test and verified that they were unchanged afterward. The K3s PID and configuration mtime were also unchanged between the control and injection runs.

## Method

The control and injection runs used the same 1,000-Namespace dataset. The control dataset was retained, then the injection run reset the managed Secrets and ServiceAccount references while the controller was scaled to zero. Setup duration is therefore not comparable and is excluded from the impact result.

In the injection run, the driver waited until exactly 250 of 1,000 existing Secrets had changed from generation 1 to generation 2, then concurrently:

1. created `late-workload` in an already-refreshed Namespace;
2. created one additional Namespace that had been included in the configuration before controller startup;
3. created the new Namespace's `default` and `workload` ServiceAccounts and its test-only writer RoleBinding.

Informer events supplied completion timestamps; the driver did not add polling traffic. The new Namespace counted as ready only after its managed Secret matched the observed generation-2 fingerprint and both ServiceAccounts referenced it. Any observed usable Secret with another fingerprint would fail the run.

## Results

| Measurement | Control | Concurrent injection | Difference |
|---|---:|---:|---:|
| Existing 1,000 Secret expiry fan-out | 36.648404 s | 36.654573 s | +0.006169 s (+0.0168%) |
| Existing Secret rate | 27.2863/s | 27.2817/s | -0.0046/s |
| Existing expiry p50 | 17.395365 s | 17.394501 s | -0.000864 s |
| Existing expiry p99 | 36.266805 s | 36.277293 s | +0.010488 s |
| Unexpected updates to the original 2,000 SAs | 0 | 0 | 0 |
| Expiry controller CPU peak | 47m | 48m | +1m |
| Expiry controller memory peak | 27.31 MiB | 28.88 MiB | +1.57 MiB |

The 6.2 ms bulk-fan-out difference is far below the one-second progress sampling interval and normal local-cluster jitter. This single paired run shows no measurable throughput impact from the three additional ServiceAccount patches and one additional Secret create.

The clean control's 36.648-second expiry result is also within 18.3 ms (0.05%) of the earlier 1,000-Namespace observation under the same high component profile. This supports repeatability of that expiry path under the current K3s configuration; it is not compared causally with the original baseline because the cluster settings differed.

### Injected ServiceAccount

- Trigger: 250/1,000 existing Secrets complete, 7.741 seconds after expiry started.
- Namespace: `krsc-perf-late1000-00004`, already on generation 2.
- Create request start to patched observation: 26.34 ms.
- Create response to patched observation: **13.71 ms**.
- No error was logged for `late-workload`.

### Injected Namespace

- Namespace Create response to generation-2 Secret: **28.944 seconds**.
- Namespace Create response to Secret plus both patched ServiceAccounts: **28.947 seconds**.
- Usable stale credential fingerprints observed: **0**.
- Expected/observed patched ServiceAccounts: **2/2**.
- The new Secret appeared 38.98 ms after the last of the original 1,000 Secrets; the two ServiceAccounts were ready 41.78 ms after that original sweep completed.

This timing indicates queue ordering rather than lost reconciliation: the new Namespace was enqueued during the fan-out but was serviced at the tail of the existing Namespace backlog. At this load and QPS, onboarding latency was effectively the remaining refresh duration. A 5,000-Namespace latency should not be claimed from this run; it needs a separate measurement if that exact SLO matters.

## Problems and interpretation

1. **New Namespace priority:** correctness is good, but a Namespace created during a large expiry sweep can wait behind the sweep. If new-Namespace readiness has a tight SLO, the scheduler fan-out and live Namespace events should use differentiated priority or separate queues.
2. **Expected dependency waits are still ERROR logs:** while the new Namespace waited for its Secret, its two ServiceAccounts produced 26 `managed Secret is not ready` reconciliation errors before the Secret create event resolved them. There were no Forbidden, Unauthorized, panic, OOM, timeout, deadline, or conflict messages. Treating this dependency as a normal wait remains the recommended fix.
3. **Historical comparison was confounded:** audit showed K3s was restarted at 16:59 with `concurrent-namespace-syncs=30`, before the high-profile runs at 17:01 but after the earlier baseline. The mixed-parameter report was removed; those raw observations are not used for causal conclusions.
4. **SQLite Namespace teardown remains slow:** the old 5,000-Namespace dataset took tens of minutes to finalize even with 30 namespace workers. Teardown is outside the Secret convergence timer, but background teardown must be zero before performance comparisons.

## Verification and artifacts

- `go test ./...`: pass.
- `go vet ./...`: pass.
- `go test -race ./...`: pass.
- Both benchmark summaries: `success: true`.
- Both controller logs contain the startup marker and are complete.
- All 1,001 test Namespaces completed normal finalization; the test controller Namespace and RBAC were removed afterward. All three K3s nodes remained Ready.
- `late-control-1000-summary.json`: control result and live profile.
- `late-inject-1000-summary.json`: injection timings and correctness checks.
- `mutation-summary.csv`: compact control/injection comparison.
- Matching `*-expiry-progress.csv`, `*-initial-progress.csv`, `*-resources.csv`, and `*-controller.log` files contain raw observations.
