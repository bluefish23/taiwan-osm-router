#!/usr/bin/env python3
"""
benchmark_recompute.py — 全量掃描 vs 增量預測 效能對比

1. 全量掃描：無 WHERE 條件 UPDATE 全部 7.6M 邊 + 逐事件套用
2. 增量預測：apply_event_incremental()，只改記憶體中受影響的邊
"""
import math
import random
import time
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib import rcParams

from osm_router import OSMRouter, _conn, INCIDENT_MULT

DB = "taiwan_osm.db"
STEP = 50
MAX_EVENTS = 500

TW_BOUNDS = {"lat": (22.0, 25.3), "lon": (120.2, 121.8)}
EVENT_TYPES = ["accident", "construction", "congestion", "closure"]


def random_coord():
    return random.uniform(*TW_BOUNDS["lat"]), random.uniform(*TW_BOUNDS["lon"])


def true_full_scan_recompute(db_path, events):
    with _conn(db_path) as conn:
        cur = conn.cursor()
        cur.execute("UPDATE osm_edges SET dynamic_mult=1.0, closure_flag=0, risk_score=0.0")
        conn.commit()
        for ev in events:
            elat = float(ev["lat"] or 0)
            elon = float(ev["lon"] or 0)
            erad = float(ev["radius_km"] or 0.5)
            sev = float(ev["severity"] or 1)
            etype = ev["event_type"]
            mult = INCIDENT_MULT.get(etype, 1.25)
            dlat = erad / 110.574
            dlon = erad / (111.320 * math.cos(math.radians(elat)))
            bbox = (elat - dlat, elat + dlat, elon - dlon, elon + dlon)
            if etype in ("closure", "landslide_closure"):
                cur.execute(
                    "UPDATE osm_edges SET closure_flag=1, dynamic_mult=999, "
                    "risk_score=risk_score+100 "
                    "WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ?", bbox)
            else:
                ra = {"accident": 8, "construction": 5, "congestion": 3,
                      "manual": 2}.get(etype, 2) * sev
                cur.execute(
                    "UPDATE osm_edges SET dynamic_mult=MAX(dynamic_mult,?), "
                    "risk_score=risk_score+? "
                    "WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ?",
                    (sev * mult, ra) + bbox)
        conn.commit()


def main():
    print("載入路由引擎...", flush=True)
    r = OSMRouter(DB)
    r.init_dynamic_schema()
    r.ensure_component_ids(verbose=False)
    r.load_graph(verbose=True)

    r.clear_events()
    print(f"\n開始 benchmark: 0 ~ {MAX_EVENTS} 事件，每 {STEP} 個測一次\n")
    print(f"{'事件數':>6} | {'全量掃描':>12} | {'增量預測':>10}")
    print("-" * 42)

    counts = []
    full_scan_times = []
    inc_times_avg = []
    event_ids = []

    for target in range(0, MAX_EVENTS + 1, STEP):
        while len(event_ids) < target:
            lat, lon = random_coord()
            etype = random.choice(EVENT_TYPES)
            eid = r.add_event(etype, lat=lat, lon=lon, description=f"bench_{len(event_ids)}")
            r.apply_event_incremental(eid)
            event_ids.append(eid)

        active_events = r.list_events()

        t0 = time.perf_counter()
        true_full_scan_recompute(DB, active_events)
        t_full_scan = time.perf_counter() - t0

        inc_samples = []
        for _ in range(5):
            lat, lon = random_coord()
            etype = random.choice(EVENT_TYPES)
            tmp_eid = r.add_event(etype, lat=lat, lon=lon, description="bench_inc_tmp")
            t0 = time.perf_counter()
            r.apply_event_incremental(tmp_eid)
            t_inc = time.perf_counter() - t0
            inc_samples.append(t_inc * 1000)
            r.remove_event(tmp_eid)
        avg_inc = sum(inc_samples) / len(inc_samples)

        counts.append(target)
        full_scan_times.append(round(t_full_scan * 1000, 1))
        inc_times_avg.append(round(avg_inc, 2))

        fs_label = f"{t_full_scan:.1f}s" if t_full_scan >= 1 else f"{t_full_scan*1000:.0f}ms"
        print(f"  {target:>4d}   | {fs_label:>12} | {avg_inc:>8.1f} ms")
        sys.stdout.flush()

    r.clear_events()

    # ── 畫圖 ──
    rcParams["font.family"] = "Microsoft JhengHei"
    rcParams["axes.unicode_minus"] = False

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))
    fig.suptitle("全量掃描 vs 增量預測 — 事件數量 vs 耗時", fontsize=20, fontweight="bold", y=0.97)

    color1 = "#E74C3C"
    ax1.bar(counts, full_scan_times, width=35, color=color1, edgecolor="#C0392B", linewidth=1.2, alpha=0.85)
    for x, y in zip(counts, full_scan_times):
        label = f"{y/1000:.1f}s" if y >= 1000 else f"{y:.0f}ms"
        ax1.text(x, y + max(full_scan_times) * 0.02, label, ha="center", va="bottom", fontsize=9, fontweight="bold")
    ax1.set_xlabel("事件數量", fontsize=13)
    ax1.set_ylabel("耗時", fontsize=13)
    ax1.set_title("全量掃描（UPDATE 全部 7.6M 邊 + 逐事件套用）", fontsize=14, fontweight="bold", color="#C0392B")
    ax1.set_xticks(counts)
    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.1f}s" if x >= 1000 else f"{x:.0f}ms"))
    ax1.grid(axis="y", alpha=0.3)
    ax1.set_xlim(-30, MAX_EVENTS + 30)

    color2 = "#2ECC71"
    ax2.bar(counts, inc_times_avg, width=35, color=color2, edgecolor="#27AE60", linewidth=1.2, alpha=0.85)
    for x, y in zip(counts, inc_times_avg):
        ax2.text(x, y + max(inc_times_avg) * 0.03, f"{y:.1f}ms", ha="center", va="bottom", fontsize=9, fontweight="bold")
    ax2.set_xlabel("事件數量", fontsize=13)
    ax2.set_ylabel("耗時", fontsize=13)
    ax2.set_title("增量預測（apply_event_incremental）", fontsize=14, fontweight="bold", color="#27AE60")
    ax2.set_xticks(counts)
    ax2.grid(axis="y", alpha=0.3)
    ax2.set_xlim(-30, MAX_EVENTS + 30)

    if full_scan_times[-1] > 0 and inc_times_avg[-1] > 0:
        speedup = full_scan_times[-1] / inc_times_avg[-1]
        fig.text(0.5, 0.005,
                 f"{MAX_EVENTS} 個事件：全量掃描 {full_scan_times[-1]/1000:.1f}s vs "
                 f"增量預測 {inc_times_avg[-1]:.1f}ms → 加速約 {speedup:.0f} 倍",
                 ha="center", fontsize=14, fontweight="bold",
                 bbox=dict(boxstyle="round,pad=0.5", facecolor="#FFFFCC", edgecolor="#CCCC00"))

    plt.tight_layout(rect=[0, 0.05, 1, 0.93])
    out = "benchmark_chart.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"\n圖表已儲存: {out}")
    plt.close()


if __name__ == "__main__":
    main()
