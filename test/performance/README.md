# Local K3s performance harness

This harness measures cold-start Secret creation, ServiceAccount `imagePullSecrets` patching, and scheduled credential-expiry Secret updates at large namespace counts.

The E2E controller is a test-only binary. It refuses to start without `--allow-mock-provider`. When `--mock-clock-control-address` is set, its scheduler and token provider share a controllable fake clock; Kubernetes watches, queues, reconciles, and writes remain real.

The driver records progress from informers, verifies final objects through the API, samples Metrics Server, and captures controller logs. Use `--dataset-id`, `--reuse-dataset`, and `--retain-dataset` to grow one dataset between size tiers without measuring Kubernetes namespace garbage collection.

The deployment manifest grants only cluster-wide reads. Mutating access is bound separately inside each generated test namespace and disappears with that namespace.

The base manifest and `profiles/component-default.yaml` explicitly use the new production defaults: QPS 25, burst 50, and eight workers in each resource controller. `profiles/component-high.yaml` retains the same values for compatibility. `profiles/component-legacy.yaml` preserves the historical QPS 5, burst 10, and two-worker profile used in earlier reports; those reports have not been rerun with this change.

For controlled comparisons, omit `kubeAPIQPS`, `kubeAPIBurst`, and `workers` from the ConfigMap before restarting the Deployment, then keep tuning unchanged throughout the run. The profile recorder reads Deployment arguments and cannot establish which hot overrides or restart-pending worker settings a live process has applied. Runtime semantics are documented in [runtime configuration](../../docs/runtime-configuration.md).

Use `--inject-during-expiry` to exercise onboarding while the expiry queue is busy. At `--injection-percent` (25 by default), the driver creates `late-workload` in an already-refreshed Namespace and creates one additional preselected Namespace. The result records time-to-patch for the new ServiceAccount, time-to-current-generation Secret and ServiceAccounts for the new Namespace, and verifies that the new Namespace never completes with the stale credential generation. `--environment-note` stores external cluster settings that cannot be discovered from the Deployment, such as K3s `concurrent-namespace-syncs`.

For a clean impact comparison, run a control and injection case against the same retained dataset and do not change control-plane settings between them. The summary reads the controller image and arguments from the live Deployment instead of assuming a profile.

```sh
go run ./cmd/perf-driver --run-id=late-control-1000 --dataset-id=late1000 --namespaces=1000 --retain-dataset --environment-note='<fixed cluster settings>'
go run ./cmd/perf-driver --run-id=late-inject-1000 --dataset-id=late1000 --namespaces=1000 --reuse-dataset --retain-dataset --inject-during-expiry --injection-percent=25 --environment-note='<same fixed cluster settings>'
```

For a controlled tuning comparison, use the same workload for all four cells of a 2x2 matrix: component legacy/high crossed with K3s `concurrent-namespace-syncs` 10/30. Keep namespace creation and deletion outside the measured phases, and verify the K3s PID/config plus the live Deployment arguments for every cell. The earlier mixed-parameter artifacts are retained only as raw audit evidence and are excluded from this comparison.

```sh
kubectl patch deployment registry-secret-controller-perf \
  -n registry-secret-controller-system --type=strategic \
  --patch-file=test/performance/profiles/component-legacy.yaml

kubectl patch deployment registry-secret-controller-perf \
  -n registry-secret-controller-system --type=strategic \
  --patch-file=test/performance/profiles/component-high.yaml
```

See `results/FACTORIAL-REPORT.md` for the controlled component/cluster comparison, `results/REPORT.md` for the original default-profile scale curve, `results/MUTATION-REPORT.md` for the FIFO concurrent-onboarding diagnosis, and `results/PRIORITY-REPORT.md` for the same-process old/new Namespace-priority validation.
