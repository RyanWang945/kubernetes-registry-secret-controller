# Kubernetes registry Secret controller performance report

Date: 2026-09-04 (Asia/Shanghai)

Result: **PASS for correctness and stability at 5000 namespaces / 10000 target ServiceAccounts, with a high-impact refresh-latency risk at the current default client rate limit.**

The former tuned comparison is excluded because both component and K3s settings changed between runs. Its raw observations remain available only as audit evidence; the controlled 2x2 comparison in `FACTORIAL-REPORT.md` supersedes it.

## Scope and method

The test ran five sizes: 50, 200, 1000, 2500, and 5000 namespaces. Every namespace contained two target ServiceAccounts (`default` and `workload`). Setup time was excluded from controller convergence time.

Initial convergence started with the controller Deployment at zero replicas and with no managed Secret references on the target ServiceAccounts. The timer started immediately before scaling to one replica. Completion required:

- one usable `auto-patch-secret` in every target namespace;
- both target ServiceAccounts in every namespace referencing that Secret;
- final API verification of all expected objects.

Expiry refresh used the E2E-only fake clock. The mock token TTL was 1 hour and refresh-before was 10 minutes. Advancing the scheduler clock by 50 minutes caused the real scheduler to acquire token generation 2 and execute the normal namespace publication and Kubernetes Secret Update path. Completion required every Secret fingerprint to differ from its generation-1 baseline. ServiceAccount resource versions were watched to detect unnecessary patches.

Progress was measured with long-running Kubernetes informers, not repeated full-object polling. Raw progress was sampled once per second. Metrics Server resource samples were taken every five seconds from the 200-namespace run onward.

## Environment

- Three Lima ARM64 VMs running K3s `v1.36.3+k3s1`.
- Control plane: 2 vCPU, 4 GiB RAM, SQLite datastore.
- Two workers: 2 vCPU and 3 GiB RAM each.
- One controller replica on a worker; leader election enabled.
- Namespace and ServiceAccount reconcile concurrency: 2 each.
- Controller REST configuration unchanged: client-go defaults, QPS 5 and burst 10.
- Test image built locally from this working tree as a static Linux/ARM64 binary in a scratch image and imported into both worker containerd stores. No remote controller image was used. Local image ID: `sha256:8848346e8fad07bdeb5f6ce2d91082e43ac282a40b5ee84bf108a7c75d9e4648`.
- RBAC was read-only cluster-wide. Secret and ServiceAccount writes were granted only through RoleBindings inside the generated test namespaces.

## Results

| Namespaces | Target SAs | All Secrets created | Initial total (all SA patches) | Expiry: all Secrets updated | Steady expiry throughput | Controller peak CPU | Controller peak memory |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 50 | 100 | 9.28 s | 18.97 s | 7.76 s | 6.44/s¹ | n/a | n/a |
| 200 | 400 | 38.63 s | 77.27 s | 36.72 s | 5.45/s | 30m | 15.1 MiB |
| 1000 | 2000 | 193.32 s | 386.50 s | 191.23 s | 5.23/s | 47m | 30.6 MiB |
| 2500 | 5000 | 482.95 s | 965.72 s | 480.79 s | 5.20/s | 70m | 57.3 MiB |
| 5000 | 10000 | 966.40 s (16.11 min) | 1931.84 s (32.20 min) | 963.47 s (16.06 min) | 5.19/s | 127m | 97.8 MiB |

¹ The 50-object result benefits materially from the REST client's initial burst allowance and is not the steady-state rate.

At 5000 namespaces:

| Path | p50 | p90 | p95 | p99 | maximum |
|---|---:|---:|---:|---:|---:|
| Initial Secret create | 483.82 s | 869.89 s | 918.15 s | 956.75 s | 966.40 s |
| Initial SA patch | 966.58 s | 1738.79 s | 1835.31 s | 1912.54 s | 1931.84 s |
| Expiry Secret update | 480.88 s | 866.96 s | 915.22 s | 953.83 s | 963.47 s |

Linear regressions across all five sizes were:

- initial full convergence: `0.386388 s × namespaces - 0.122 s`, R² `0.999999947`;
- initial Secret completion: `0.193309 s × namespaces - 0.177 s`, R² `0.999999805`;
- expiry Secret update: `0.193071 s × namespaces - 1.879 s`, R² `0.999999996`.

This is effectively linear scaling. The asymptotic object rate is about 5.17–5.23 writes/s. CPU did not saturate: the controller peaked at 127 millicores and 97.8 MiB at the largest size. During measured phases, the control-plane node peaked at about 696 millicores and 1682 MiB. No controller restart occurred and all Metrics API samples succeeded.

## Correctness checks

- All five sizes completed successfully.
- The 5000-namespace final verification found all 5000 usable managed Secrets and all 10000 target ServiceAccounts referencing the fixed Secret name.
- All 5000 Secrets changed credential fingerprint after the scheduled expiry refresh.
- Expiry caused **zero** ServiceAccount updates at every size; the Secret name reference remained stable.
- No permission error, API timeout, panic, OOM, token error, or non-retry controller error appeared in captured logs.

## Problems found

### 1. Refresh fan-out exceeds the production refresh window at large scale

This is the primary risk. Updating 5000 Secrets took 963.47 seconds (16.06 minutes), while the production scheduler default refresh-before is 5 minutes. At the measured rate, only about 1563 of 5000 Secrets had been updated after five minutes; roughly 69% would still contain the old credential when it expired. Even this test's 10-minute refresh window is shorter than the measured fan-out.

The limiting factor in this baseline was the unchanged client-go QPS 5 / burst 10 configuration, not CPU or memory. The binary now exposes REST QPS/Burst and reconcile concurrency as runtime tuning parameters; `FACTORIAL-REPORT.md` records their controlled retest.

The bounded component settings have now been added and tested. Independently, make refresh-before greater than measured p99 fan-out plus safety margin; it should not remain below the worst-case propagation time.

### 2. Normal Secret dependency waiting is logged as an error storm

On cold start, ServiceAccounts are reconciled before their namespace Secret exists. `ErrManagedSecretNotReady` is returned as a reconciliation error, causing rate-limited retries and ERROR logs. The complete 2500-namespace log contained 17606 such ERROR lines. The retained 5000-namespace log contained 17344, but its startup marker was absent because kubelet had rotated earlier output, so that count is only a lower bound. Every captured ERROR was this expected condition. The harness now records whether the startup marker is present so truncated logs are not treated as complete.

The managed Secret create/update watch already enqueues ServiceAccounts in that namespace. A steadier design is to treat “Secret not ready yet” as a normal wait (or a non-error delayed requeue), preserve any existing reference, and let the Secret event trigger the patch. That removes false alarms and retry/log amplification.

### 3. Namespace teardown is slow on this local SQLite K3s cluster

Deleting the 50-namespace smoke dataset through normal Kubernetes finalization took 133.47 seconds, much longer than its measured business phases. The larger test therefore reused one incrementally grown dataset and reset Secret/SA state between runs. Normal deletion for the final 5000 namespaces was submitted after verification; no finalizer was force-removed. This is an environment/cleanup limitation, not controller convergence time.

### 4. One derived harness field was initially wrong and was corrected

The first implementation divided initial Secret count by the full initial phase duration (which waits for twice as many SA patches), under-reporting initial Secret throughput by half. Event times, percentiles, phase durations, and raw CSV were unaffected. The driver now divides each resource count by that resource's own maximum completion time, has a regression test for it, and the saved summary JSON files were corrected.

## Limits of this test

- The credential provider was deterministic and local; Alibaba Cloud API latency and failure behavior were not measured.
- One registry and one active controller leader were used.
- The controller ran with current defaults; no tuned-QPS comparison was included.
- Node metrics include K3s and system work, so node peaks are cluster-level observations rather than controller-only attribution.
- Namespace creation and teardown are deliberately excluded from convergence measurements.

## Artifacts

- `summary.csv`: aggregate curve data.
- `nNNNN-summary.json`: full percentiles, resource peaks, and diagnostics for each size.
- `nNNNN-initial-progress.csv` and `nNNNN-expiry-progress.csv`: one-second progress series.
- `nNNNN-resources.csv`: five-second controller and node CPU/memory samples (200 namespaces and above).
- `nNNNN-controller.log`: captured controller logs (200 namespaces and above).
