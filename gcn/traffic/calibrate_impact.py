"""以真實事故量測「事件衝擊」，校準三因子模型參數（BASE_SEVERITY / BASE_RADIUS）。

背景：手動事件的三因子模型（ω/κ/δ）參數原為文獻概念 + 自訂值；
Phase 6 下載的 123 件國道一號事故 + 同期 5 分鐘車速矩陣，正好能量測真實值：

每件事故量測（相對「事故前 60~30 分鐘」的基準速）：
1. 幅度：事故期間命中路段的最低速 → speed_ratio = baseline / v_min
   系統語意：sev × INCIDENT_MULT = 時間乘數 = speed_ratio →
   隱含 severity = ratio / 1.80（INCIDENT_MULT['accident']，見 osm_router.py:51）
2. 空間：上游 0~4 跳路段的 ratio 衰減曲線 → 回堵延伸公里數（ratio ≥ 1.15 視為受影響）
3. 時間：命中路段速度回到基準 90% 所需分鐘數

用法：python gcn/traffic/calibrate_impact.py
輸出：results/impact_calibration.json、results/impact_calibration.png
"""
from __future__ import annotations

import json
import sys
from datetime import timedelta
from pathlib import Path

import numpy as np

_GCN_DIR = str(Path(__file__).resolve().parent.parent)
if _GCN_DIR not in sys.path:
    sys.path.insert(0, _GCN_DIR)
from traffic.anomaly_eval import DATA, EVENTS_CSV, RESULTS_DIR, load_accidents, section_spans  # noqa: E402

ACCIDENT_MULT = 1.80        # osm_router.INCIDENT_MULT['accident']（避免 import 整個路由引擎）
CURRENT_SEVERITY = 0.80     # osm_router.BASE_SEVERITY['accident']
CURRENT_RADIUS = 0.8        # osm_router.BASE_RADIUS['accident']（km，ω=1 時 ×2）
BASELINE_WIN = (12, 6)      # 事故前 60~30 分鐘（步）
IMPACT_RATIO = 1.15         # ratio 超過此值視為受影響
RECOVERY = 0.90             # 速度回到基準 90% 視為恢復
MAX_HOPS = 4


def upstream_chain(spans, hit_idx: int) -> list[int]:
    """回傳 [hit, 上游1跳, 上游2跳, ...]（S 向=較小里程、N 向=較大里程）。"""
    d = spans[hit_idx][0]
    same = sorted((i for i, sp in enumerate(spans) if sp[0] == d),
                  key=lambda i: spans[i][1])
    pos = same.index(hit_idx)
    if d == "S":
        ups = same[max(0, pos - MAX_HOPS):pos][::-1]
    else:
        ups = same[pos + 1:pos + 1 + MAX_HOPS]
    return [hit_idx] + ups


def main() -> None:
    data = np.load(DATA, allow_pickle=True)
    speed, sections = data["speed"], data["sections"]
    timestamps = data["timestamps"].astype("datetime64[ns]")
    spans = section_spans(sections)
    sec_len = {i: hi - lo for i, (_, lo, hi) in enumerate(spans)}

    accidents = load_accidents(EVENTS_CSV)
    per_hop_ratios: list[list[float]] = [[] for _ in range(MAX_HOPS + 1)]
    extents_km, durations_min, records = [], [], []
    skipped = 0

    for a in accidents:
        onset = np.datetime64(a["onset"])
        end = np.datetime64(a["cleared"] + timedelta(minutes=15))
        t_on = int(np.searchsorted(timestamps, onset))
        t_end = int(np.searchsorted(timestamps, end))
        if t_on < BASELINE_WIN[0] or t_on >= len(timestamps) - 1:
            skipped += 1
            continue
        hits = [i for i, (d, lo, hi) in enumerate(spans)
                if d == a["direction"] and lo <= a["km"] <= hi]
        if not hits:
            skipped += 1
            continue
        chain = upstream_chain(spans, hits[0])
        t_end = max(t_end, t_on + 2)

        ratios = []
        for h, sec in enumerate(chain):
            base = float(speed[t_on - BASELINE_WIN[0]:t_on - BASELINE_WIN[1], sec].mean())
            v_min = float(speed[t_on:t_end, sec].min())
            r = base / max(v_min, 5.0) if base > 5.0 else 1.0
            ratios.append(min(r, 10.0))                     # 車速異常值截頂
            per_hop_ratios[h].append(ratios[-1])

        extent = 0.0
        for h, sec in enumerate(chain):
            if ratios[h] >= IMPACT_RATIO:
                extent += sec_len[sec] if h > 0 else sec_len[sec] / 2   # 命中段算一半（事故點居中）
            else:
                break
        extents_km.append(extent)

        # 恢復時間：命中路段最低點之後，速度連續 2 步 ≥ 90% 基準
        sec0 = chain[0]
        base0 = float(speed[t_on - BASELINE_WIN[0]:t_on - BASELINE_WIN[1], sec0].mean())
        seg = speed[t_on:min(t_on + 36, len(timestamps)), sec0]         # 最多追 3 小時
        t_min = int(np.argmin(seg[:max(t_end - t_on, 1)]))
        dur = None
        for t in range(t_min + 1, len(seg) - 1):
            if seg[t] >= RECOVERY * base0 and seg[t + 1] >= RECOVERY * base0:
                dur = (t + 1) * 5
                break
        if dur is not None:
            durations_min.append(dur)

        records.append({"event_id": a["event_id"], "km": a["km"],
                        "direction": a["direction"],
                        "ratio_hop0": round(ratios[0], 3),
                        "extent_km": round(extent, 2),
                        "duration_min": dur})

    n = len(records)
    med_hop = [round(float(np.median(r)), 3) if r else None for r in per_hop_ratios]
    p75_hop = [round(float(np.percentile(r, 75)), 3) if r else None for r in per_hop_ratios]
    med_ratio = med_hop[0]
    implied_sev = round(med_ratio / ACCIDENT_MULT, 3)
    implied_sev_p75 = round(p75_hop[0] / ACCIDENT_MULT, 3)
    med_extent = round(float(np.median(extents_km)), 2)
    p75_extent = round(float(np.percentile(extents_km, 75)), 2)
    med_dur = round(float(np.median(durations_min)), 1) if durations_min else None

    result = {
        "n_accidents_measured": n,
        "n_skipped": skipped,
        "median_speed_ratio_by_hop": med_hop,
        "p75_speed_ratio_by_hop": p75_hop,
        "queue_extent_km": {"median": med_extent, "p75": p75_extent},
        "recovery_minutes": {"median": med_dur,
                             "n_recovered": len(durations_min)},
        "calibration": {
            "current": {"severity": CURRENT_SEVERITY, "radius_km": CURRENT_RADIUS,
                        "implied_speed_ratio": round(CURRENT_SEVERITY * ACCIDENT_MULT, 2)},
            "empirical": {"median_speed_ratio": med_ratio,
                          "implied_severity_median": implied_sev,
                          "implied_severity_p75": implied_sev_p75,
                          "freeway_queue_km_median": med_extent,
                          "freeway_queue_km_p75": p75_extent},
        },
        "note": "隱含 severity = ratio/INCIDENT_MULT(1.8)；extent 以 ratio>=1.15 的連續上游路段累計",
    }
    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / "impact_calibration.json"
    out.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(result, indent=2, ensure_ascii=True))   # console 用 ASCII（Windows cp950 安全）
    print(f"saved -> {out}")

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.5))
    hops = list(range(MAX_HOPS + 1))
    axes[0].plot(hops, med_hop, "o-", label="median")
    axes[0].plot(hops, p75_hop, "s--", label="p75")
    axes[0].axhline(IMPACT_RATIO, color="gray", ls=":", label=f"impact threshold ({IMPACT_RATIO})")
    axes[0].set_xlabel("upstream hop (sections)")
    axes[0].set_ylabel("speed ratio (baseline / min)")
    axes[0].set_title("Impact decay along upstream")
    axes[0].legend(); axes[0].grid(alpha=0.3)
    axes[1].hist(extents_km, bins=20, color="#2C7FB8", edgecolor="white")
    axes[1].axvline(med_extent, color="red", ls="--", label=f"median {med_extent} km")
    axes[1].set_xlabel("queue extent (km)")
    axes[1].set_title("Queue extent distribution")
    axes[1].legend(); axes[1].grid(alpha=0.3)
    axes[2].hist(durations_min, bins=20, color="#41AB5D", edgecolor="white")
    if med_dur:
        axes[2].axvline(med_dur, color="red", ls="--", label=f"median {med_dur} min")
    axes[2].set_xlabel("recovery time (min)")
    axes[2].set_title("Recovery time distribution")
    axes[2].legend(); axes[2].grid(alpha=0.3)
    fig.suptitle(f"Accident impact calibration (n={n}, 2026-05-26~30 freeway No.1)")
    fig.tight_layout()
    png = RESULTS_DIR / "impact_calibration.png"
    fig.savefig(png, dpi=150)
    print(f"plot -> {png}")


if __name__ == "__main__":
    main()
