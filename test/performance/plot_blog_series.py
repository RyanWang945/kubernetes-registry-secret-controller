#!/usr/bin/env python3
"""Validate measured runs, archive small raw summaries, and render blog charts."""

import csv
import datetime
import hashlib
import json
import os
import shutil
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "test/performance/results/blog-20260906"
DEST = ROOT / "docs/benchmarks/2026-09-06"
os.environ.setdefault("MPLCONFIGDIR", str(ROOT / ".cache/matplotlib"))
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np

FONT = Path("/System/Library/Fonts/Supplemental/Arial Unicode.ttf")
if FONT.exists():
    font_manager.fontManager.addfont(str(FONT))
    plt.rcParams["font.family"] = font_manager.FontProperties(fname=str(FONT)).get_name()
plt.rcParams.update({
    "font.size": 11, "axes.titlesize": 17, "axes.labelsize": 12,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.edgecolor": "#ccd4de", "text.color": "#233047",
    "axes.labelcolor": "#233047", "xtick.color": "#46556b",
    "ytick.color": "#46556b", "figure.facecolor": "white",
    "axes.unicode_minus": False, "svg.fonttype": "path",
})


def get_row(path):
    value = json.loads(path.read_text())
    count = value["namespaceCount"]
    mutation = value["concurrentMutation"]
    new_ns = mutation["lateNamespace"]
    assert value["success"] and mutation["success"]
    assert value["serviceAccountCount"] == count * 2
    assert value["controllerProfile"]["configurationStable"]
    assert value["expiry"]["secretCompletion"]["count"] == count
    assert value["expiry"]["unexpectedServiceAccountUpdates"] == 0
    assert mutation["namespaceOnly"] and mutation["secretsCompleteAtTrigger"] < count
    assert mutation["secretsCompleteAtTrigger"] >= count * .25
    assert new_ns["staleCredentialFingerprints"] == 0
    assert new_ns["expectedServiceAccountCount"] == new_ns["observedPatchedServiceAccounts"] == 2
    def timestamp(text):
        base, fraction = text.rstrip("Z").split(".")
        return datetime.datetime.fromisoformat(base + "." + fraction.ljust(6, "0")[:6])
    assert timestamp(new_ns["createRequestStartedAt"]) < timestamp(value["expiry"]["completedAt"])
    assert timestamp(new_ns["createdAt"]) < timestamp(value["expiry"]["completedAt"])
    arguments = value["controllerProfile"]["arguments"]
    def arg(name):
        return next(item.split("=", 1)[1] for item in arguments if item.startswith("--" + name + "="))
    return {
        "run": value["runID"], "namespaces": count, "serviceaccounts": 2 * count,
        "qps": float(arg("kube-api-qps")), "burst": int(arg("kube-api-burst")),
        "namespace_workers": int(arg("max-concurrent-namespace-reconciles")),
        "sa_workers": int(arg("max-concurrent-service-account-reconciles")),
        "refresh_seconds": value["expiry"]["durationSeconds"],
        "refresh_p99_seconds": value["expiry"]["secretCompletion"]["p99Seconds"],
        "new_secret_ms": new_ns["secretLatencySeconds"] * 1000,
        "two_sa_patched_ms": new_ns["serviceAccountsLatencySeconds"] * 1000,
        "initial_seconds": value["initial"]["durationSeconds"],
        # Very short runs may finish before metrics-server observes the Pod.
        "controller_peak_cpu_m": value["resources"]["controllerPeak"]["cpuMillicores"] if value["resources"]["controllerPeak"]["memoryMiB"] else None,
        "controller_peak_memory_mib": value["resources"]["controllerPeak"]["memoryMiB"] or None,
        "actual_injection_percent": mutation["secretsCompleteAtTrigger"] / count * 100,
        "summary_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def save(fig, name):
    fig.savefig(DEST / (name + ".png"), dpi=180, facecolor="white")
    fig.savefig(DEST / (name + ".svg"), facecolor="white")
    plt.close(fig)


def format_value(value):
    return f"{value:.2f}" if value < 10 else f"{value:.1f}"


def scale_chart(rows, metric, title, ylabel, name, color):
    sizes = sorted({row["namespaces"] for row in rows})
    groups = [[row[metric] for row in rows if row["namespaces"] == size] for size in sizes]
    assert all(len(group) == 3 for group in groups), "Need three measured runs at every size"
    medians = np.array([statistics.median(group) for group in groups])
    lower = medians - np.array([min(group) for group in groups])
    upper = np.array([max(group) for group in groups]) - medians
    fig, ax = plt.subplots(figsize=(10.5, 5.4))
    fig.subplots_adjust(left=.10, right=.97, bottom=.23, top=.79)
    fig.text(.10, .94, title, fontsize=18, fontweight="bold")
    ax.errorbar(sizes, medians, yerr=[lower, upper], color=color, marker="o",
                markersize=7, linewidth=2.2, capsize=5, elinewidth=1.5, zorder=3)
    for i, (x, y) in enumerate(zip(sizes, medians)):
        dy = 13 if i != 1 else (32 if metric == "refresh_seconds" else -22)
        ax.annotate(format_value(y), (x, y), xytext=((0 if i > 1 else 5), dy),
                    textcoords="offset points", ha="center" if i > 1 else "left",
                    fontsize=10, color=color, fontweight="bold")
    ax.set_xticks(sizes)
    ax.tick_params(axis="x", labelrotation=45)
    ax.set_xlabel("已有 NS 数量（每个 NS 2 个 SA）")
    ax.set_ylabel(ylabel)
    ax.set_xlim(-120, max(sizes) * 1.045)
    ax.set_ylim(0, max(max(group) for group in groups) * 1.27)
    ax.grid(axis="y", color="#e8edf2", linewidth=.8)
    profile = rows[0]
    origin = "从触发刷新起算" if metric == "refresh_seconds" else "从新 NS 创建成功起算"
    fig.text(.10, .88, f'QPS {profile["qps"]:g} · burst {profile["burst"]} · NS/SA 各 {profile["namespace_workers"]} workers · {origin}',
             fontsize=10, color="#607086")
    fig.text(.10, .035, "每档实测 3 轮；圆点为中位数，误差线为最小值—最大值。观测到至少 25% 已刷新后发起新 NS 创建。",
             fontsize=9, color="#607086")
    save(fig, name)


def main():
    DEST.mkdir(parents=True, exist_ok=True)
    raw_dest = DEST / "raw"
    raw_dest.mkdir(exist_ok=True)
    paths = sorted(SOURCE.glob("blog-scale-*-summary.json"))
    rows = [get_row(path) for path in paths]
    series = rows
    assert len(series) == 15
    assert {row["namespaces"] for row in series} == {50, 200, 1000, 2500, 5000}
    assert {(row["qps"], row["burst"], row["namespace_workers"], row["sa_workers"]) for row in series} == {(100, 200, 16, 16)}
    assert len({json.loads(path.read_text())["controllerProfile"]["image"] for path in paths}) == 1
    for path in paths:
        shutil.copyfile(path, raw_dest / path.name)
    with (DEST / "measurements.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    scale_chart(series, "refresh_seconds", "NS 增多，一轮 Secret 刷新需要多久？", "整轮刷新耗时（秒）",
                "01-refresh-scaling", "#087f72")
    scale_chart(series, "new_secret_ms", "批量刷新期间，新 NS 的 Secret 多快就绪？", "新 Secret 就绪耗时（毫秒）",
                "02-new-namespace-secret", "#327fa8")
    scale_chart(series, "two_sa_patched_ms", "批量刷新期间，两个 SA 多快全部 patch 完成？", "两个 SA 完成耗时（毫秒）",
                "03-new-namespace-serviceaccounts", "#9162a7")
    medians = []
    for count in sorted({row["namespaces"] for row in series}):
        group = [row for row in series if row["namespaces"] == count]
        medians.append({"namespaces": count, **{key: statistics.median(row[key] for row in group)
                        for key in ["refresh_seconds", "new_secret_ms", "two_sa_patched_ms"]}})
    (DEST / "medians.json").write_text(json.dumps(medians, indent=2) + "\n")
    print(json.dumps(medians, indent=2))


if __name__ == "__main__":
    main()
