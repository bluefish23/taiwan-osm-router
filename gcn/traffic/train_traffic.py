"""Phase 4：GCN+GRU 交通車速預測（計畫書 p.7–8 實驗章節）。

資料：高公局 M05A 國道一號門架路段 5 分鐘平均速率（download_m05a.py 產出）。
圖：節點 = 門架路段，路段 A 的迄門架 == 路段 B 的起門架 → A–B 建邊。
任務：以過去 12 步（60 分鐘）預測未來 1/3/6 步（5/15/30 分鐘）車速。
指標：MAE、RMSE（計畫書 p.8）；baseline：naive、歷史平均（HA）、無 GCN 的 GRU。
異常偵測：測試集上預測殘差 z-score > 3 標記為異常（p.8 「交通流量突發波動」）。

用法：python gcn/traffic/train_traffic.py --epochs 50
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import scipy.sparse as sp
import torch
import torch.nn.functional as F

_GCN_DIR = str(Path(__file__).resolve().parent.parent)
if _GCN_DIR not in sys.path:
    sys.path.insert(0, _GCN_DIR)
from gcn_layer import normalize_adjacency, sparse_to_torch  # noqa: E402
from traffic.tgcn_model import NodeGRU, TGCN                # noqa: E402

DATA = Path(__file__).resolve().parent.parent / "data" / "m05a" / "m05a_matrix.npz"
RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"

IN_STEPS = 12
HORIZONS = [1, 3, 6]          # 5 / 15 / 30 分鐘
STEPS_PER_DAY = 288
STEPS_PER_WEEK = STEPS_PER_DAY * 7


def build_adjacency(sections: np.ndarray) -> sp.coo_matrix:
    """路段 A(from-to) 的 to == 路段 B 的 from → 建邊（沿行車方向的鏈）。"""
    n = len(sections)
    frm = np.array([s.split("-")[0] for s in sections])
    to = np.array([s.split("-")[1] for s in sections])
    row, col = [], []
    for i in range(n):
        nxt = np.where(frm == to[i])[0]
        for j in nxt:
            if i != j:
                row.append(i)
                col.append(j)
    return sp.coo_matrix((np.ones(len(row)), (row, col)), shape=(n, n))


def make_windows(x: np.ndarray, idx: np.ndarray):
    """x: (T, N, F) 正規化特徵。回傳 (X: (S,12,N,F), Y: (S,N,H)) 的索引式生成。"""
    xs, ys = [], []
    for t in idx:
        xs.append(x[t - IN_STEPS:t])
        ys.append(np.stack([x[t + h - 1, :, 0] for h in HORIZONS], axis=-1))
    return np.stack(xs), np.stack(ys)


def evaluate(pred: np.ndarray, true: np.ndarray, mean: float, std: float) -> dict:
    """反正規化後計算各 horizon 的 MAE / RMSE（單位 km/h）。"""
    p = pred * std + mean
    t = true * std + mean
    out = {}
    for k, h in enumerate(HORIZONS):
        out[f"MAE@{h*5}min"] = round(float(np.abs(p[..., k] - t[..., k]).mean()), 3)
        out[f"RMSE@{h*5}min"] = round(float(np.sqrt(((p[..., k] - t[..., k]) ** 2).mean())), 3)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--epochs", type=int, default=50)      # 計畫書 p.8
    ap.add_argument("--lr", type=float, default=0.001)     # 計畫書 p.8
    ap.add_argument("--hidden", type=int, default=64)      # 計畫書 p.8
    ap.add_argument("--batch", type=int, default=32)
    ap.add_argument("--patience", type=int, default=8)
    ap.add_argument("--model", choices=["tgcn", "gru"], default="tgcn")
    ap.add_argument("--data", default=str(DATA), help="矩陣路徑（預設 M05A，VD 用 data/vd/vd_matrix.npz）")
    ap.add_argument("--tag", default="", help="輸出檔名後綴（如 _vd），避免覆蓋 M05A 結果")
    args = ap.parse_args()

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    data = np.load(args.data, allow_pickle=True)
    speed, volume = data["speed"], data["volume"]
    sections = data["sections"]
    T, N = speed.shape
    print(f"data: {T} steps x {N} sections, model={args.model}, device={device}")

    # 圖：VD 矩陣已預建 edges（按里程連鏈）；M05A 則現場以門架配對建圖
    if "edges" in data.files:
        import scipy.sparse as _sp
        e = data["edges"]
        adj = _sp.coo_matrix((np.ones(e.shape[1]), (e[0], e[1])), shape=(N, N))
    else:
        adj = build_adjacency(sections)
    print(f"graph: {adj.nnz} edges")
    a_hat = sparse_to_torch(normalize_adjacency(adj), device)

    # 特徵 (T, N, 2)：z-score 正規化的 speed 與 volume（統計量只用訓練段）
    t_train_end = int(T * 0.7)
    t_val_end = int(T * 0.8)
    sp_mean, sp_std = speed[:t_train_end].mean(), max(speed[:t_train_end].std(), 1e-6)
    vo_mean, vo_std = volume[:t_train_end].mean(), max(volume[:t_train_end].std(), 1e-6)
    feats = np.stack([(speed - sp_mean) / sp_std,
                      (volume - vo_mean) / vo_std], axis=-1).astype(np.float32)

    max_h = max(HORIZONS)
    idx_all = np.arange(IN_STEPS, T - max_h + 1)
    idx_train = idx_all[idx_all < t_train_end]
    idx_val = idx_all[(idx_all >= t_train_end) & (idx_all < t_val_end)]
    idx_test = idx_all[idx_all >= t_val_end]
    print(f"windows: train {len(idx_train)} / val {len(idx_val)} / test {len(idx_test)}")

    # 三個切分的視窗都只建一次（訓練迴圈內僅做索引切片）
    x_train, y_train = make_windows(feats, idx_train)
    x_val, y_val = make_windows(feats, idx_val)
    x_test, y_test = make_windows(feats, idx_test)

    model_cls = TGCN if args.model == "tgcn" else NodeGRU
    model = model_cls(num_features=2, hidden=args.hidden,
                      num_horizons=len(HORIZONS)).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=args.lr)

    def run_eval(x, y, bs=64):
        model.eval()
        preds = []
        with torch.no_grad():
            for i in range(0, len(x), bs):
                xb = torch.from_numpy(x[i:i + bs]).to(device)
                preds.append(model(a_hat, xb).cpu().numpy())
        return np.concatenate(preds)

    best_val, best_state, no_improve = np.inf, None, 0
    t0 = time.perf_counter()
    for epoch in range(1, args.epochs + 1):
        model.train()
        perm = np.random.default_rng(epoch).permutation(len(x_train))
        losses = []
        for i in range(0, len(perm), args.batch):
            sel = perm[i:i + args.batch]
            xb = torch.from_numpy(x_train[sel]).to(device)
            yb = torch.from_numpy(y_train[sel]).to(device)
            opt.zero_grad()
            loss = F.l1_loss(model(a_hat, xb), yb)
            loss.backward()
            opt.step()
            losses.append(loss.item())
        val_pred = run_eval(x_val, y_val)
        val_mae = float(np.abs(val_pred - y_val).mean())
        if val_mae < best_val - 1e-5:
            best_val, no_improve = val_mae, 0
            best_state = {k: v.detach().clone() for k, v in model.state_dict().items()}
        else:
            no_improve += 1
        print(f"epoch {epoch:2d}  train_l1 {np.mean(losses):.4f}  val_l1 {val_mae:.4f}")
        if no_improve >= args.patience:
            print(f"early stopping at epoch {epoch}")
            break
    train_time = time.perf_counter() - t0

    model.load_state_dict(best_state)
    test_pred = run_eval(x_test, y_test)
    metrics = evaluate(test_pred, y_test, sp_mean, sp_std)

    # --- baselines ---
    # naive：以最後觀測值當所有 horizon 的預測
    naive = np.repeat(x_test[:, -1, :, 0:1], len(HORIZONS), axis=-1)
    m_naive = evaluate(naive, y_test, sp_mean, sp_std)
    # 歷史平均 HA：訓練段依「一週中的時間槽」平均
    slot = (np.arange(T) % STEPS_PER_WEEK)
    ha_table = np.zeros((STEPS_PER_WEEK, N), dtype=np.float32)
    for s0 in range(STEPS_PER_WEEK):
        m = (slot[:t_train_end] == s0)
        ha_table[s0] = feats[:t_train_end][m, :, 0].mean(axis=0) if m.any() else 0.0
    ha = np.stack([ha_table[(idx_test + h - 1) % STEPS_PER_WEEK] for h in HORIZONS],
                  axis=-1)
    m_ha = evaluate(ha, y_test, sp_mean, sp_std)

    # --- 異常偵測（p.8）：殘差 z-score > 3 ---
    resid = (test_pred[..., 0] - y_test[..., 0])
    z = (resid - resid.mean()) / resid.std()
    anomalies = np.argwhere(np.abs(z) > 3)
    anomaly_summary = {
        "count": int(len(anomalies)),
        "rate": round(float(len(anomalies)) / z.size, 5),
        "note": "殘差 z>3 之 (時間窗, 路段)；precision/recall 需事件標記（未來以 TDX 事故資料對齊）",
    }

    # 存 checkpoint 供線上推論（predict_service.py）使用
    ckpt_path = RESULTS_DIR / f"traffic_{args.model}{args.tag}.ckpt"
    RESULTS_DIR.mkdir(exist_ok=True)
    torch.save({
        "model": args.model,
        "state_dict": best_state,
        "hidden": args.hidden,
        "in_steps": IN_STEPS,
        "horizons": HORIZONS,
        "sections": sections.tolist(),
        "norm": {"sp_mean": float(sp_mean), "sp_std": float(sp_std),
                 "vo_mean": float(vo_mean), "vo_std": float(vo_std)},
    }, ckpt_path)
    print(f"checkpoint -> {ckpt_path}")

    result = {
        "model": args.model,
        "config": vars(args) | {"in_steps": IN_STEPS, "horizons": HORIZONS,
                                "device": str(device)},
        "graph": {"nodes": int(N), "edges": int(adj.nnz)},
        "train_seconds": round(train_time, 1),
        "test_metrics": metrics,
        "baseline_naive": m_naive,
        "baseline_historical_average": m_ha,
        "anomaly_detection": anomaly_summary,
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))
    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"traffic_{args.model}{args.tag}.json"
    out.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"saved -> {out}")

    # 預測 vs 實際示意圖（挑測試段一個路段、一天）
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    node = int(np.argmax(speed[:t_train_end].std(axis=0)))   # 變化最大的路段
    day = min(STEPS_PER_DAY, len(idx_test))
    tt = np.arange(day)
    fig, ax = plt.subplots(figsize=(11, 4))
    ax.plot(tt, y_test[:day, node, 0] * sp_std + sp_mean, label="actual")
    ax.plot(tt, test_pred[:day, node, 0] * sp_std + sp_mean, label="predicted (5min)")
    ax.set_xlabel("5-min step (test day 1)")
    ax.set_ylabel("speed (km/h)")
    ax.set_title(f"section {sections[node]} ({args.model})")
    ax.legend(); ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(RESULTS_DIR / f"traffic_pred_{args.model}{args.tag}.png", dpi=150)
    print(f"plot -> {RESULTS_DIR / f'traffic_pred_{args.model}{args.tag}.png'}")


if __name__ == "__main__":
    main()
