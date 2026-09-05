# Live Namespace priority report

Date: 2026-09-05 (Asia/Shanghai)

Result: **PASS. Giving live Namespace Create events priority 100 reduced current-generation Secret latency during a 1,000-Namespace expiry fan-out from 30.059 seconds to 0.350 seconds in a same-process old/new comparison. Full readiness, including both ServiceAccounts, fell from 30.064 seconds to 0.352 seconds. The 85.9x latency improvement did not produce a measurable bulk-refresh penalty in this single paired run.**

## Change under test

The Namespace Secret controller now distinguishes these queue classes:

| Event class | Priority |
|---|---:|
| Live Namespace Create observed after informer startup | 100 |
| Expiry/configuration fan-out, actual Namespace update/delete, Secret repair | 0 |
| Namespace informer initial list or unchanged resync | -100 |

The implementation uses controller-runtime's built-in priority queue and explicitly enables it for the Namespace controller. Queue keys remain de-duplicated. If a live Create arrives for a Namespace already waiting at priority 0, that existing key is promoted to 100 rather than duplicated. Update, delete, generic fan-out, retry, and startup-list behavior are otherwise unchanged. A non-priority-queue fallback still enqueues normally.

Priority is non-preemptive: it does not interrupt one of the eight reconciles already running, and it does not bypass Kubernetes client rate limiting. It selects the new Namespace before the next normal-priority backlog item becomes available to a worker.

## Fixed environment

- Three Lima ARM64 VMs; K3s `v1.36.3+k3s1`; SQLite/Kine datastore.
- The controlled old/new injection runs used the same K3s process, PID `912`, started `2026-09-05 11:14:29 CST`.
- `/etc/rancher/k3s/config.yaml`: mode `0600`, `root:root`, mtime `2026-09-04 16:58:58.273372756 +0800`.
- Kube controller manager: `--concurrent-namespace-syncs=30`.
- Component profile in every run: one replica, QPS 25, burst 50, eight Namespace workers, eight ServiceAccount workers.
- Mock token TTL one hour, refresh-before ten minutes, fake clock advanced 50 minutes.
- New image: `perf-priority`, digest `sha256:2e5ce54b077b525e9579d96e530e977a98c7265ff3b0c7884bdfe63d947e7967`.
- Old image: `perf-tuned`, digest `sha256:56f6a9a43dede28b44509f20272e64823be072861ac307da41a717e8127bb62c`.

The Deployment was scaled to zero for every dataset reset and image change. Every summary captured the live image and full argument list before its run and verified that they remained stable through final-state verification.

## Method

All cases used the reusable `priority1000` dataset: 1,000 existing Namespaces and 2,000 existing target ServiceAccounts. Initial convergence completed before the fake clock triggered generation 2.

For an injection case, the driver waited until exactly 250 of 1,000 existing Secrets had reached generation 2, then concurrently:

1. created `late-workload` in an already-refreshed Namespace;
2. created one additional Namespace that was already present in the controller configuration;
3. created that Namespace's writer RoleBinding plus `default` and `workload` ServiceAccounts.

The driver measured readiness from informer observations and then verified objects directly through the API. The injected Namespace passed only if its usable Secret matched the generation-2 fingerprint and both ServiceAccounts referenced it. Any usable stale fingerprint failed the run.

Three runs were made:

1. new image without injection, to measure its clean fan-out;
2. new image with injection;
3. old image with injection, after deleting only the prior injected resources and resetting the same dataset.

The old and new injection runs therefore share the same running K3s process, VM allocation, cluster concurrency, component arguments, dataset, and 25% trigger. The order was not randomized and each cell has one observation, so sub-second differences should be treated as local-cluster jitter rather than a precise causal estimate.

## Results

### Same-process old/new injection comparison

| Measurement | Old FIFO behavior | New priority behavior | Difference |
|---|---:|---:|---:|
| Existing 1,000-Secret expiry fan-out | 38.043443 s | 38.079518 s | +0.036075 s (+0.0948%) |
| Existing Secret rate | 26.2857/s | 26.2608/s | -0.0249/s |
| New Namespace Create response to current Secret | 30.058818 s | **0.349789 s** | -29.709029 s (-98.836%; 85.93x faster) |
| New Namespace Create response to Secret plus two SAs | 30.063523 s | **0.351615 s** | -29.711908 s (-98.830%; 85.50x faster) |
| New SA in an already-refreshed Namespace | 10.703 ms | 7.424 ms | -3.279 ms |
| Usable stale credential fingerprints | 0 | 0 | unchanged |
| Patched SAs in new Namespace | 2/2 | 2/2 | unchanged |
| Unexpected updates to original 2,000 SAs during expiry | 0 | 0 | unchanged |

The old run reproduced the queue-tail diagnosis under the current K3s process: its new Secret appeared only as the original fan-out was completing. In the new run, the Secret appeared between the 250 and 273 existing-Secret progress observations, while more than 700 normal-priority items remained.

### New-image injection impact

| Measurement | New control | New injection | Difference |
|---|---:|---:|---:|
| Existing 1,000-Secret expiry fan-out | 38.040673 s | 38.079518 s | +0.038845 s (+0.1021%) |
| Existing Secret rate | 26.2877/s | 26.2608/s | -0.0268/s |
| Existing expiry p50 | 18.047249 s | 18.086377 s | +0.039128 s |
| Existing expiry p99 | 37.645425 s | 37.682075 s | +0.036650 s |
| Unexpected existing-SA updates | 0 | 0 | 0 |
| Controller CPU peak | 64m | 61m | -3m |
| Controller memory peak | 27.77 MiB | 29.46 MiB | +1.69 MiB |

The roughly 39 ms fan-out difference is below the one-second progress sampling interval and is not evidence of a throughput regression. The old and new injection fan-outs also differed by only 36 ms.

### New Namespace timing detail

- Expiry began at `03:26:22.389260275Z`.
- Injection triggered at exactly 250/1,000, `7.644` seconds into expiry.
- Namespace Create returned at `03:26:30.039018112Z`.
- Its RoleBinding and two ServiceAccounts were ready at `03:26:30.060618499Z`.
- Its generation-2 Secret was observed at `03:26:30.388806994Z`.
- Both ServiceAccounts were observed ready at `03:26:30.390633368Z`.

The measured 350 ms is therefore not pure queue residence: approximately 22 ms was prerequisite creation and 330 ms elapsed from prerequisites to full readiness. Worker occupancy, the QPS/burst limiter, API-server latency, informer propagation, and ServiceAccount retries all remain in the path.

## Problems and limits

1. **Expected dependency waits are still logged as errors.** The two injected ServiceAccounts retried 14 times while their Secret was unavailable, versus 26 times with FIFO behavior. The priority change shortens but does not remove this race. Treating `managed Secret is not ready` as a normal dependency wait remains a separate recommended change.
2. **Two transient Secret Create races occurred in the new injection run.** Two normal Namespaces returned `AlreadyExists` after a cache-stale duplicate reconcile attempted Create. Rate-limited retries converged, final verification passed, and neither involved the injected Namespace. This existing cache-consistency race should preferably treat `AlreadyExists` as an immediate re-read/reconcile instead of emitting an ERROR.
3. **Initial convergence emits noisy expected errors.** The new injection log contained 4,199 `managed Secret is not ready` errors; the control had 4,412. They arise because ServiceAccount reconciliation begins before all Secrets exist. They are not expiry failures and should not be used as a throughput metric.
4. **No fatal failure was observed.** The three logs contained no panic, fatal, Forbidden, Unauthorized, deadline, timeout, OOM, or stale-credential event.
5. **Fairness is not proven under a Namespace storm.** Strict priority can delay expiry work if live Namespace Creates arrive continuously. This test injected one Namespace, not a sustained burst. If both an onboarding SLO and a hard refresh-completion SLO are required, add a burst test and consider weighted fairness or separate worker capacity.
6. **Scope is intentionally narrow.** Only a live Kubernetes Namespace Create is elevated. Adding an already-existing Namespace to the ConfigMap still arrives through the normal priority-0 configuration fan-out.
7. **Scale boundary.** This validates one injection during a 1,000-Namespace refresh. It does not claim a 5,000-Namespace onboarding latency; that requires a dedicated run if it is an acceptance criterion.

## Verification and artifacts

- `go test ./...`: pass.
- controller envtest initial List and continuous Watch integration test: pass.
- `go vet ./...`: pass.
- `go test -race ./...`: pass.
- `git diff --check`: pass.
- All three benchmark summaries: `success: true`; controller configuration stable.
- `priority-control-1000-*`: new-image control raw data.
- `priority-inject-1000-*`: new-image injection raw data.
- `priority-old-inject-1000-*`: same-process old-image injection raw data.
- `priority-summary.csv`: compact comparison.
