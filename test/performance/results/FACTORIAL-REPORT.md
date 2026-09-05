# Component and cluster concurrency factorial report

Date: 2026-09-04 (Asia/Shanghai)

Result: **PASS. Raising the component capacity profile reduced 1,000-Namespace cold-start convergence by about 80% and expiry refresh by about 81% at both cluster settings. Raising K3s `concurrent-namespace-syncs` from 10 to 30 produced no measured improvement for either path. Enabling both was materially the same as increasing the component profile alone.**

## Controlled comparison

This supersedes the removed mixed-parameter report. The test crossed two factors on one retained, pre-warmed workload:

| Factor | Default | High |
|---|---|---|
| Component capacity profile | client QPS 5, burst 10, Namespace workers 2, ServiceAccount workers 2 | client QPS 25, burst 50, Namespace workers 8, ServiceAccount workers 8 |
| K3s Namespace controller | `concurrent-namespace-syncs=10` | `concurrent-namespace-syncs=30` |

The workload was fixed at 1,000 existing Namespaces and 2,000 target ServiceAccounts. Every measured run reset the same objects while the controller was scaled to zero. Dataset preparation was excluded, and a separate high-profile warm-up run was excluded from the analysis. No Namespace was terminating during a measured phase.

The mock credential TTL was one hour, refresh-before was ten minutes, and the fake clock advanced by fifty minutes. This exercised the real scheduler, informer queues, reconciles, and Kubernetes writes without waiting for wall-clock expiry.

The four runs were ordered default/10, high/10, high/30, default/30. Component order was reversed inside the second K3s block to reduce simple warm-order bias. There is one observation per cell, so small differences cannot be assigned statistical significance.

## Results

| Component | KCM Namespace concurrency | All Secrets created | Initial total: all SA patches | Expiry: all Secrets updated | Expiry rate | Expiry p99 | Controller peak CPU | Controller peak memory |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| default | 10 | 193.960 s | 386.777 s | 190.892 s | 5.239/s | 188.969 s | 44m | 30.73 MiB |
| high | 10 | 38.256 s | 76.826 s | 36.652 s | 27.283/s | 36.273 s | 65m | 29.38 MiB |
| default | 30 | 194.013 s | 387.607 s | 191.660 s | 5.218/s | 189.728 s | 41m | 30.05 MiB |
| high | 30 | 39.357 s | 78.087 s | 36.803 s | 27.172/s | 36.416 s | 81m | 30.05 MiB |

### Main effects

| Comparison | Initial total | Expiry refresh |
|---|---:|---:|
| Raise component profile, KCM=10 | 5.03x faster; 80.14% less time | 5.21x faster; 80.80% less time |
| Raise component profile, KCM=30 | 4.96x faster; 79.85% less time | 5.21x faster; 80.80% less time |
| Raise KCM 10→30, component default | 0.830 s slower (+0.21%) | 0.768 s slower (+0.40%) |
| Raise KCM 10→30, component high | 1.261 s slower (+1.64%) | 0.151 s slower (+0.41%) |

The difference-in-differences interaction was +0.431 seconds for initial convergence and -0.617 seconds for expiry. Both are tiny compared with the approximately 310-second initial and 154-second expiry improvements from the component profile, and a single observation per cell cannot distinguish them from run-to-run noise.

## Interpretation

The useful tuning is in the component profile. Its steady Secret write rate rose from about 5.2/s to 27.2/s. The profile changes QPS, burst, and both worker counts together, so this experiment attributes the gain to that **bundle**, not to reconcile workers alone. The rate tracking the REST client limit strongly suggests QPS/burst is the dominant part, but isolating that claim would require another controlled factor.

K3s `concurrent-namespace-syncs` controls work performed by the Kubernetes Namespace controller. It is relevant mainly to Namespace lifecycle and finalization; it is not on this controller's steady-Namespace Secret create/update path. The nearly overlapping 10/30 results are therefore expected. The slow, batched deletion observed after the benchmark is a separate reason to test this K3s flag with a dedicated Namespace teardown benchmark, not a reason to credit it for Secret refresh performance.

For this workload, “both high” should not be presented as faster than “component high only”: expiry differed by only 0.151 seconds and was slightly slower. Keep KCM=30 only for a separately demonstrated Namespace lifecycle requirement.

## Correctness and diagnostics

- All four summaries report `success: true`; final verification found 1,000 current Secrets and 2,000 correctly patched ServiceAccounts in every cell.
- Expiry caused zero ServiceAccount updates in all four cells, confirming the fixed Secret name avoided unnecessary patches.
- Every summary captured the live image and full argument list, then verified they were unchanged at completion.
- K3s PID/config remained unchanged inside each two-run block. Startup logs explicitly showed KCM values 10 and 30 for their respective blocks.
- Metrics collection had zero errors, controller logs included their startup marker, and no panic, fatal, authorization, timeout, or OOM signal was found.
- The logs contained 5,501 / 3,975 / 4,181 / 5,356 ERROR lines by cell. Every one was the known transient `managed Secret is not ready` dependency wait; this remains noisy behavior that should be downgraded or handled as a normal wait.

## Limits

- One run per cell is enough to reject a large KCM benefit here, but not to estimate sub-percent effects. Replicated and randomized blocks would be needed for confidence intervals.
- The environment was three ARM64 Lima VMs running K3s `v1.36.3+k3s1` with SQLite/Kine, not a production etcd control plane.
- Namespace creation and finalization were deliberately outside the measured phases.
- The provider and clock were deterministic mocks; registry API latency, throttling, and failures were not measured.
- This 1,000-Namespace factorial isolates configuration effects cheaply. Exact 5,000-Namespace SLO claims still require a controlled 5,000-Namespace confirmation run.

## Artifacts

- `factorial-summary.csv`: compact four-cell comparison.
- `factorial-*-summary.json`: complete timings, percentiles, live profiles, environment notes, and diagnostics.
- `factorial-*-initial-progress.csv` and `factorial-*-expiry-progress.csv`: one-second progress observations.
- `factorial-*-resources.csv`: five-second controller and node resource samples.
- `factorial-*-controller.log`: complete controller logs.
- `factorial-warmup-*`: excluded warm-up evidence only.

The earlier `tuned-*` JSON/CSV/log files and `tuning-summary.csv` are retained only as audit evidence and are not inputs to this report.
