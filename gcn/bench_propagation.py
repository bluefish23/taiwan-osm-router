"""Phase 2 benchmark：全圖重算 vs 增量更新 vs 直接相依（計畫書 p.9 實驗）。

在表二資料集上，模擬「單一節點狀態更新」情境，比較三種方法：
  A. full      —— 全圖冪迭代重算（傳統 GCN/圖處理計算模式，p.1 的問題）
  B. incremental —— 局部殘差推送，只更新受影響傳播鏈上的節點（p.2 子圖更新）
  C. direct    —— 核心節點直接相依索引，查表一步套用影響（p.4–5）
量測：耗時、實際觸碰（更新）的節點數、與 A 的誤差。
另輸出子圖分割統計（核心節點數、子圖大小分佈）與
「理想平行時間」= 各子圖工作量最大值（對應計畫書多核心平行論述）。

用法：python gcn/bench_propagation.py --dataset pubmed --updates 20
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np

from datasets import load
from incremental_pagerank import (DirectDependencyIndex, LocalPushPageRank,
                                  pagerank_full)
from subgraph_partition import find_core_nodes, partition

RESULTS_DIR = Path(__file__).parent / "results"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="pubmed")
    ap.add_argument("--updates", type=int, default=20, help="模擬的活動節點更新次數")
    ap.add_argument("--delta", type=float, default=0.01)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    adj, *_ = load(args.dataset)
    n = adj.shape[0]
    rng = np.random.default_rng(args.seed)

    # --- 子圖分割統計 ---
    t0 = time.perf_counter()
    cores = find_core_nodes(adj)
    subgraphs, owner = partition(adj)
    t_part = time.perf_counter() - t0
    sizes = np.array([len(v) for v in subgraphs.values()])
    print(f"{args.dataset}: {n:,} nodes / {adj.nnz:,} edges")
    print(f"核心節點: {len(cores):,} ({len(cores)/n:.1%})  分割耗時 {t_part:.2f}s")
    print(f"子圖大小: max={sizes.max():,} median={int(np.median(sizes))} mean={sizes.mean():.1f}")

    # --- 初始化三種方法 ---
    t0 = time.perf_counter()
    r_full, _ = pagerank_full(adj)
    t_init_full = time.perf_counter() - t0

    t0 = time.perf_counter()
    lp = LocalPushPageRank(adj)
    t_init_lp = time.perf_counter() - t0
    print(f"初始 PageRank: full {t_init_full:.2f}s / local-push {t_init_lp:.2f}s "
          f"(L1 err {np.abs(lp.r - r_full).sum():.1e})")

    ddi = DirectDependencyIndex(adj, cores)

    # 活動節點：一半隨機、一半從核心節點挑（計畫書情境是核心節點狀態更新）
    core_picks = rng.choice(cores, size=args.updates // 2, replace=False)
    rand_picks = rng.choice(n, size=args.updates - len(core_picks), replace=False)
    active_nodes = np.concatenate([core_picks, rand_picks])
    core_set = set(cores.tolist())

    rows = []
    s = np.ones(n) / n
    for v in active_nodes:
        v = int(v)
        # A. 全圖重算
        s[v] += args.delta
        t0 = time.perf_counter()
        r_a, touched_a = pagerank_full(adj, personalization=s * n)
        t_a = time.perf_counter() - t0

        # B. 增量（局部推送）
        before = lp.touched
        t0 = time.perf_counter()
        lp.update_source(v, args.delta)
        t_b = time.perf_counter() - t0
        touched_b = lp.touched - before
        err_b = np.abs(lp.r / lp.r.sum() - r_a / r_a.sum()).sum()

        # C. 直接相依（僅核心節點適用；首查建索引 flag N→A，之後查表即用）
        if v in ddi.flags:
            t0 = time.perf_counter()
            _ = ddi.apply_update(lp.r, v, args.delta)      # 首次：建索引
            t_c_first = time.perf_counter() - t0
            t0 = time.perf_counter()
            _ = ddi.apply_update(lp.r, v, args.delta)      # 快取命中（flag=A）
            t_c_cached = time.perf_counter() - t0
        else:
            t_c_first = t_c_cached = None

        rows.append({"node": v, "is_core": v in core_set,
                     "t_full": t_a, "touched_full": touched_a,
                     "t_incr": t_b, "touched_incr": touched_b, "err_incr": err_b,
                     "t_direct_first": t_c_first, "t_direct_cached": t_c_cached})

    t_full_m = float(np.mean([r["t_full"] for r in rows]))
    t_incr_m = float(np.mean([r["t_incr"] for r in rows]))
    directs = [r["t_direct_first"] for r in rows if r["t_direct_first"] is not None]
    cached = [r["t_direct_cached"] for r in rows if r["t_direct_cached"] is not None]
    result = {
        "dataset": args.dataset,
        "nodes": n, "edges": int(adj.nnz),
        "num_cores": int(len(cores)),
        "partition_seconds": round(t_part, 3),
        "subgraph_size_max": int(sizes.max()),
        "subgraph_size_median": int(np.median(sizes)),
        "updates": len(rows),
        "mean_time_full_recompute": round(t_full_m, 5),
        "mean_time_incremental": round(t_incr_m, 5),
        "mean_time_direct_first_N": round(float(np.mean(directs)), 6) if directs else None,
        "mean_time_direct_cached_A": round(float(np.mean(cached)), 6) if cached else None,
        "speedup_incremental_vs_full": round(t_full_m / t_incr_m, 1),
        "mean_touched_full": int(np.mean([r["touched_full"] for r in rows])),
        "mean_touched_incremental": int(np.mean([r["touched_incr"] for r in rows])),
        "max_err_incremental": float(np.max([r["err_incr"] for r in rows])),
        "index_computed_entries": ddi.compute_count,
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))

    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"propagation_{args.dataset}.json"
    out.write_text(json.dumps({"summary": result, "rows": rows}, indent=2,
                              ensure_ascii=False, default=float), encoding="utf-8")
    print(f"saved -> {out}")


if __name__ == "__main__":
    main()
