"""異常偵測 ground truth 評估：預測殘差 vs 高公局事故通報（REPORT §6 缺口）。

方法：
1. 載入 T-GCN checkpoint，重建測試集 5 分鐘預測（與 train_traffic 同切分）
2. 殘差 z = (pred - actual) 標準化；z > 閾值 → 標記異常（實際車速低於預測 = 非預期減速）
3. Ground truth：download_events.py 之國道一號事故，依 方向 + 里程 對應門架路段，
   影響窗 = [通報前 tol_before, 最後出現後 tol_after]，並向上游擴散 n_upstream 個路段（回堵）
4. 掃描閾值算 point-precision / event-recall / F1（事件偵測文獻慣例：
   precision 以標記點計、recall 以事故事件計）

用法：python gcn/traffic/anomaly_eval.py
輸出：results/anomaly_eval.json、results/anomaly_pr_curve.png
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import torch

_GCN_DIR = str(Path(__file__).resolve().parent.parent)
if _GCN_DIR not in sys.path:
    sys.path.insert(0, _GCN_DIR)
from gcn_layer import normalize_adjacency, sparse_to_torch          # noqa: E402
from traffic.tgcn_model import TGCN                                 # noqa: E402
from traffic.train_traffic import IN_STEPS, HORIZONS, build_adjacency, make_windows  # noqa: E402

DATA = Path(__file__).resolve().parent.parent / "data" / "m05a" / "m05a_matrix.npz"
EVENTS_CSV = Path(__file__).resolve().parent.parent / "data" / "m05a" / "events_accidents.csv"
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
CKPT = RESULTS_DIR / "traffic_tgcn.ckpt"

THRESHOLDS = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0]


def section_spans(sections: np.ndarray):
    """'01F0339S-01F0376S' → (dir, km_lo, km_hi)。門架 ID 第 4-7 碼為里程×10。"""
    spans = []
    for s in sections:
        frm, to = s.split("-")
        km_a, km_b = int(frm[3:7]) / 10.0, int(to[3:7]) / 10.0
        spans.append((frm[-1], min(km_a, km_b), max(km_a, km_b)))
    return spans


def load_accidents(path: Path) -> list[dict]:
    """讀 events_accidents.csv → [{onset: datetime, cleared: datetime, direction, km}]"""
    out = []
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            onset = datetime.fromisoformat(row["effective"]).replace(tzinfo=None)
            cleared = datetime.strptime(row["last_sample"], "%Y%m%d %H%M")
            out.append({"event_id": row["event_id"], "onset": onset,
                        "cleared": max(cleared, onset),
                        "direction": row["direction"], "km": float(row["km"])})
    return out


def match_sections(acc: dict, spans, n_upstream: int) -> list[int]:
    """事故 → 命中路段索引（含上游回堵擴散）。

    S 向里程遞增、N 向遞減 → 上游（回堵方向）= S 向較小里程、N 向較大里程的路段。
    """
    hit = [i for i, (d, lo, hi) in enumerate(spans)
           if d == acc["direction"] and lo <= acc["km"] <= hi]
    if not hit or n_upstream == 0:
        return hit
    same_dir = sorted((i for i, sp in enumerate(spans) if sp[0] == acc["direction"]),
                      key=lambda i: spans[i][1])
    result = set(hit)
    for h in hit:
        pos = same_dir.index(h)
        if acc["direction"] == "S":
            result.update(same_dir[max(0, pos - n_upstream):pos])
        else:
            result.update(same_dir[pos + 1:pos + 1 + n_upstream])
    return sorted(result)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tol-before", type=int, default=10, help="事故通報前容忍（分鐘）")
    ap.add_argument("--tol-after", type=int, default=30, help="事故清除後容忍（分鐘，回堵消散）")
    ap.add_argument("--upstream", type=int, default=2, help="上游回堵擴散路段數")
    args = ap.parse_args()

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    data = np.load(DATA, allow_pickle=True)
    speed, volume, sections = data["speed"], data["volume"], data["sections"]
    timestamps = data["timestamps"].astype("datetime64[ns]")
    T, N = speed.shape

    ck = torch.load(CKPT, map_location=device, weights_only=False)
    norm = ck["norm"]
    feats = np.stack([(speed - norm["sp_mean"]) / norm["sp_std"],
                      (volume - norm["vo_mean"]) / norm["vo_std"]], axis=-1).astype(np.float32)

    t_val_end = int(T * 0.8)
    idx_all = np.arange(IN_STEPS, T - max(HORIZONS) + 1)
    idx_test = idx_all[idx_all >= t_val_end]
    x_test, y_test = make_windows(feats, idx_test)

    a_hat = sparse_to_torch(normalize_adjacency(build_adjacency(sections)), device)
    model = TGCN(num_features=2, hidden=ck["hidden"], num_horizons=len(HORIZONS)).to(device)
    model.load_state_dict(ck["state_dict"])
    model.eval()
    preds = []
    with torch.no_grad():
        for i in range(0, len(x_test), 64):
            xb = torch.from_numpy(x_test[i:i + 64]).to(device)
            preds.append(model(a_hat, xb).cpu().numpy())
    test_pred = np.concatenate(preds)

    # 有向殘差：pred - actual > 0 = 實際比預測慢（事故特徵）
    resid = test_pred[..., 0] - y_test[..., 0]                     # (S, N)
    z = (resid - resid.mean()) / resid.std()
    test_times = timestamps[idx_test]                              # horizon=1 目標時刻

    # ground truth 遮罩 (S, N)
    accidents = load_accidents(EVENTS_CSV)
    spans = section_spans(sections)
    t0, t1 = test_times[0], test_times[-1]
    in_test = [a for a in accidents
               if np.datetime64(a["cleared"]) >= t0 and np.datetime64(a["onset"]) <= t1]
    truth = np.zeros(z.shape, dtype=bool)
    acc_cells: list[tuple] = []                                    # 每事故的 (id, 時間遮罩, 路段)
    unmatched = 0
    for a in in_test:
        secs = match_sections(a, spans, args.upstream)
        if not secs:
            unmatched += 1
            continue
        w0 = np.datetime64(a["onset"] - timedelta(minutes=args.tol_before))
        w1 = np.datetime64(a["cleared"] + timedelta(minutes=args.tol_after))
        tmask = (test_times >= w0) & (test_times <= w1)
        if not tmask.any():
            unmatched += 1
            continue
        truth[np.ix_(tmask, secs)] = True
        acc_cells.append((a["event_id"], tmask, secs))

    coverage = float(truth.mean())                                 # 隨機基準 precision
    curve = []
    for thr in THRESHOLDS:
        flag = z > thr
        n_flag = int(flag.sum())
        tp = int((flag & truth).sum())
        precision = tp / n_flag if n_flag else 0.0
        detected = sum(1 for _, tm, sc in acc_cells if flag[np.ix_(tm, sc)].any())
        recall = detected / len(acc_cells) if acc_cells else 0.0
        f1 = (2 * precision * recall / (precision + recall)
              if precision + recall > 0 else 0.0)
        curve.append({"threshold": thr, "flagged": n_flag,
                      "point_precision": round(precision, 4),
                      "event_recall": round(recall, 4),
                      "f1": round(f1, 4),
                      "detected_events": detected})
        print(f"z>{thr:.1f}: flagged={n_flag:5d}  precision={precision:.3f}  "
              f"recall={recall:.3f} ({detected}/{len(acc_cells)})  f1={f1:.3f}")

    best = max(curve, key=lambda c: c["f1"])
    result = {
        "config": vars(args),
        "test_period": [str(t0), str(t1)],
        "accidents_total": len(accidents),
        "accidents_in_test": len(in_test),
        "accidents_matched": len(acc_cells),
        "accidents_unmatched": unmatched,
        "truth_coverage": round(coverage, 5),
        "note": "point_precision=標記點命中率（隨機基準=truth_coverage）；event_recall=事故被偵測比例",
        "curve": curve,
        "best_by_f1": best,
    }
    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / "anomaly_eval.json"
    out.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\ncoverage(random baseline)={coverage:.4f}  best_f1={best['f1']} @z>{best['threshold']}")
    print(f"saved -> {out}")

    # PR 曲線
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(7, 5))
    rs = [c["event_recall"] for c in curve]
    ps = [c["point_precision"] for c in curve]
    ax.plot(rs, ps, "o-", color="#2C7FB8")
    for c in curve:
        ax.annotate(f"z>{c['threshold']}", (c["event_recall"], c["point_precision"]),
                    textcoords="offset points", xytext=(6, 4), fontsize=8)
    ax.axhline(coverage, color="gray", ls="--", lw=1,
               label=f"random baseline ({coverage:.3f})")
    ax.set_xlabel("event recall")
    ax.set_ylabel("point precision")
    ax.set_title("Anomaly detection vs accident ground truth (test set)")
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    png = RESULTS_DIR / "anomaly_pr_curve.png"
    fig.savefig(png, dpi=150)
    print(f"plot -> {png}")


if __name__ == "__main__":
    main()
