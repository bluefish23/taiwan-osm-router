#!/usr/bin/env python3
"""plot_accuracy.py — 模型準確率對比圖（讀 results/*.json，不重跑訓練）。

左：T-GCN 交通預測 MAE 對比（4 模型 × 3 視野，越低越好）
右：GCN 節點分類準確率（表二資料集，附文獻水準參考線）

用法：python gcn/plot_accuracy.py → results/accuracy_comparison.png
"""
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import rcParams

RESULTS = Path(__file__).resolve().parent / "results"
rcParams["font.family"] = "Microsoft JhengHei"
rcParams["axes.unicode_minus"] = False


def load(name):
    return json.loads((RESULTS / name).read_text(encoding="utf-8"))


def main():
    tgcn = load("traffic_tgcn.json")
    gru = load("traffic_gru.json")

    horizons = ["@5min", "@15min", "@30min"]
    hlabels = ["5 分鐘", "15 分鐘", "30 分鐘"]
    models = {
        "T-GCN（本模型）": (tgcn["test_metrics"], "#2C7FB8"),
        "GRU（無 GCN）":   (gru["test_metrics"],  "#7FCDBB"),
        "naive（延續上一刻）": (tgcn["baseline_naive"], "#FDBB84"),
        "歷史平均 HA":     (tgcn["baseline_historical_average"], "#D9D9D9"),
    }

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5.6),
                                   gridspec_kw={"width_ratios": [1.7, 1]})

    # ── 左：交通預測 MAE 對比 ──
    x = np.arange(len(horizons))
    w = 0.2
    for i, (name, (m, color)) in enumerate(models.items()):
        vals = [m["MAE" + h] for h in horizons]
        bars = ax1.bar(x + (i - 1.5) * w, vals, w, label=name,
                       color=color, edgecolor="white", linewidth=0.6)
        for b, v in zip(bars, vals):
            ax1.text(b.get_x() + b.get_width() / 2, v + 0.05, f"{v:.2f}",
                     ha="center", va="bottom", fontsize=8.5,
                     fontweight="bold" if i == 0 else "normal")
    ax1.set_xticks(x)
    ax1.set_xticklabels(hlabels)
    ax1.set_ylabel("MAE (km/h)　← 越低越準")
    ax1.set_title("交通車速預測", fontsize=14, fontweight="bold")
    ax1.legend(fontsize=10, framealpha=0.9)
    ax1.grid(axis="y", alpha=0.3)
    ax1.set_ylim(0, 5.4)

    # ── 右：GCN 節點分類準確率 ──
    pubmed = load("benchmark_pubmed_L2_lr0.01.json")
    arxiv = load("benchmark_ogbn-arxiv_L3_lr0.01.json")
    names = ["Pubmed\n(19.7K 節點)", "ogbn-arxiv\n(169K 節點)"]
    acc = [pubmed["test_accuracy"] * 100, arxiv["test_accuracy"] * 100]
    ref = [79.0, 71.7]   # 文獻水準
    xx = np.arange(len(names))
    b1 = ax2.bar(xx - 0.2, acc, 0.4, label="本實作", color="#2C7FB8", edgecolor="white")
    b2 = ax2.bar(xx + 0.2, ref, 0.4, label="文獻水準", color="#BDBDBD", edgecolor="white")
    for bars in (b1, b2):
        for b in bars:
            ax2.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.7,
                     f"{b.get_height():.1f}%", ha="center", va="bottom",
                     fontsize=10, fontweight="bold")
    ax2.set_xticks(xx)
    ax2.set_xticklabels(names)
    ax2.set_ylabel("Test Accuracy (%)　← 越高越好")
    ax2.set_title("GCN 節點分類", fontsize=14, fontweight="bold")
    ax2.legend(fontsize=10)
    ax2.grid(axis="y", alpha=0.3)
    ax2.set_ylim(0, 100)

    fig.suptitle("模型準確率總覽", fontsize=17, fontweight="bold", y=0.99)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    out = RESULTS / "accuracy_comparison.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"saved -> {out}")


if __name__ == "__main__":
    main()
