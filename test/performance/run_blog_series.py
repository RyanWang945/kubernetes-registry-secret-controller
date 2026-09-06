#!/usr/bin/env python3
"""Run reproducible, sequential local-K3s benchmarks for the blog figures.

Build .cache/blog-benchmark/perf-driver and deploy the current e2e-controller
first. Every target Namespace has exactly default + workload ServiceAccounts.
The driver and controller have independent QPS limits; this script only tunes
the controller. Dataset setup/reset/teardown is outside measured phases.
"""

import argparse
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONTEXT = "lima-local-k8s"
SYSTEM_NS = "registry-secret-controller-system"
DEPLOYMENT = "registry-secret-controller-perf"
KUBECTL = ["kubectl", "--context", CONTEXT]
DRIVER = ROOT / ".cache/blog-benchmark/perf-driver"
OUTPUT = ROOT / "test/performance/results/blog-20260906"
ENVIRONMENT = (
    "Mac mini M4; Lima ARM64; K3s v1.36.3+k3s1 SQLite/Kine; "
    "control-plane 2CPU/4GiB, two workers 2CPU/3GiB each; "
    "concurrent-namespace-syncs=30; controller pinned to lima-k8s-worker-1; "
    "each Namespace has default+workload; only one new Namespace injected at "
    "25% expiry progress; mock TTL 1h, refresh-before 10m, fake clock +50m"
)


def execute(command, capture=False):
    return subprocess.run(command, cwd=ROOT, check=True, text=True,
                          stdout=subprocess.PIPE if capture else None).stdout


def set_profile(qps, burst, workers):
    args = ["--allow-mock-provider", "--leader-elect=true", "--mock-token-ttl=1h",
            "--credential-refresh-before=10m", "--mock-clock-control-address=:8082",
            f"--kube-api-qps={qps}", f"--kube-api-burst={burst}",
            f"--max-concurrent-namespace-reconciles={workers}",
            f"--max-concurrent-service-account-reconciles={workers}"]
    patch = {"spec": {"template": {"spec": {"containers": [{"name": "controller", "args": args}]}}}}
    execute(KUBECTL + ["-n", SYSTEM_NS, "patch", "deployment", DEPLOYMENT,
                      "--type=strategic", "--patch", json.dumps(patch)])


def remove_injected_namespace(dataset):
    # Exact name, not a broad Namespace deletion. Dataset labels are also
    # checked by the driver before any existing workload is reused.
    execute(KUBECTL + ["delete", "namespace", f"krsc-perf-{dataset}-late",
                      "--ignore-not-found", "--wait=true", "--timeout=5m"])


def benchmark(run_id, dataset, count):
    command = [str(DRIVER), "--context=" + CONTEXT, "--run-id=" + run_id,
               "--dataset-id=" + dataset, f"--namespaces={count}",
               "--reuse-dataset", "--retain-dataset", "--inject-during-expiry",
               "--inject-namespace-only", "--injection-percent=25",
               "--client-qps=200", "--client-burst=400",
               "--initial-timeout=15m", "--expiry-timeout=10m",
               "--injection-timeout=2m", "--output-dir=" + str(OUTPUT),
               "--environment-note=" + ENVIRONMENT]
    (OUTPUT / "active-run.json").write_text(json.dumps({"runID": run_id, "command": command}, indent=2))
    print(f"RUN {run_id} namespaces={count}", flush=True)
    with (OUTPUT / (run_id + "-driver.log")).open("w") as log:
        process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            log.write(line)
            log.flush()
            print(line, end="", flush=True)
        if process.wait() != 0:
            raise RuntimeError(f"benchmark failed: {run_id}; inspect retained data and logs")
    summary = json.loads((OUTPUT / (run_id + "-summary.json")).read_text())
    mutation = summary["concurrentMutation"]
    assert summary["success"] and summary["controllerProfile"]["configurationStable"]
    assert summary["serviceAccountCount"] == count * 2
    assert summary["expiry"]["secretCompletion"]["count"] == count
    assert summary["expiry"]["unexpectedServiceAccountUpdates"] == 0
    assert mutation["success"] and mutation["namespaceOnly"]
    assert mutation["secretsCompleteAtTrigger"] < count
    assert mutation["lateNamespace"]["observedPatchedServiceAccounts"] == 2
    assert mutation["lateNamespace"]["staleCredentialFingerprints"] == 0
    print("RESULT " + json.dumps({
        "runID": run_id, "refreshSeconds": summary["expiry"]["durationSeconds"],
        "newSecretSeconds": mutation["lateNamespace"]["secretLatencySeconds"],
        "twoSAsSeconds": mutation["lateNamespace"]["serviceAccountsLatencySeconds"],
        "peakCPUm": summary["resources"]["controllerPeak"]["cpuMillicores"],
    }), flush=True)
    remove_injected_namespace(dataset)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["scale", "cleanup"])
    parser.add_argument("--qps", type=int, default=100)
    parser.add_argument("--burst", type=int, default=200)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--sizes", type=int, nargs="+", default=[50, 200, 1000, 2500, 5000])
    parser.add_argument("--dataset", default=None)
    options = parser.parse_args()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    nodes = json.loads(execute(KUBECTL + ["get", "nodes", "-o", "json"], capture=True))
    expected = {"lima-k8s-control-plane", "lima-k8s-worker-1", "lima-k8s-worker-2"}
    assert {node["metadata"]["name"] for node in nodes["items"]} == expected
    dataset = options.dataset or "blog-scale-260906"
    assert dataset.startswith("blog-")
    if options.mode == "cleanup":
        execute([str(DRIVER), "--context=" + CONTEXT, "--run-id=" + dataset,
                 "--dataset-id=" + dataset, "--cleanup-only", "--cleanup-timeout=30m"])
    else:
        assert options.sizes == sorted(options.sizes)
        set_profile(options.qps, options.burst, options.workers)
        for count in options.sizes:
            for repeat in range(1, options.repeats + 1):
                benchmark(f"blog-scale-n{count:04d}-r{repeat}", dataset, count)


if __name__ == "__main__":
    main()
