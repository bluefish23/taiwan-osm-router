# -*- coding: utf-8 -*-
"""交通預測章節的視覺圖：用訓練好的 T-GCN 與真實資料畫出 AI 看到什麼、預測得如何。

只讀：讀 gcn/data/m05a 的車速矩陣、gcn/results/traffic_tgcn.ckpt 與事故清單，
並唯讀查詢 taiwan_osm.db 畫底圖。不寫入任何資料。輸出 event_figures/pred_*.png。

    python make_prediction_figures.py
"""
import os
import sqlite3
import sys
from datetime import timedelta

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import torch
from matplotlib.collections import LineCollection

GCN = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gcn")
sys.path.insert(0, GCN)
from gcn_layer import normalize_adjacency, sparse_to_torch                       # noqa: E402
from traffic.anomaly_eval import (EVENTS_CSV, load_accidents, match_sections,    # noqa: E402
                                  section_spans)
from traffic.predict_service import load_gantry_positions                        # noqa: E402
from traffic.tgcn_model import TGCN                                              # noqa: E402
from traffic.train_traffic import HORIZONS, IN_STEPS, build_adjacency, make_windows  # noqa: E402

DB = "taiwan_osm.db"
OUT = "event_figures"
DATA = os.path.join(GCN, "data", "m05a", "m05a_matrix.npz")
CKPT = os.path.join(GCN, "results", "traffic_tgcn.ckpt")
DARK, ACC, INK, MUTED = "#2E5369", "#C2571A", "#1F2A2E", "#6B7C82"
GREY = "#9AA9AE"
EXAMPLE_SECTION = "01F0293N-01F0256N"       # 國道一號北上（台中路段），測試期間有明顯尖峰壅塞

plt.rcParams["font.sans-serif"] = ["Microsoft JhengHei", "PingFang TC", "Noto Sans TC"]
plt.rcParams["axes.unicode_minus"] = False


def predict_test():
    """在測試段（資料最後 20%）跑一次模型。回傳預測與實際 (S, N, H)、對應的目標時刻。"""
    z = np.load(DATA, allow_pickle=True)
    speed, volume, sections = z["speed"], z["volume"], z["sections"]
    times = z["timestamps"].astype("datetime64[ns]")
    T = speed.shape[0]
    ck = torch.load(CKPT, map_location="cpu", weights_only=False)
    norm = ck["norm"]
    feats = np.stack([(speed - norm["sp_mean"]) / norm["sp_std"],
                      (volume - norm["vo_mean"]) / norm["vo_std"]], axis=-1).astype(np.float32)
    idx_all = np.arange(IN_STEPS, T - max(HORIZONS) + 1)
    idx = idx_all[idx_all >= int(T * 0.8)]
    x, y = make_windows(feats, idx)
    dev = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = TGCN(num_features=2, hidden=ck["hidden"], num_horizons=len(HORIZONS)).to(dev)
    model.load_state_dict(ck["state_dict"])
    model.eval()
    a_hat = sparse_to_torch(normalize_adjacency(build_adjacency(sections)), dev)
    out = []
    with torch.no_grad():
        for i in range(0, len(x), 64):
            out.append(model(a_hat, torch.from_numpy(x[i:i + 64]).to(dev)).cpu().numpy())
    pred = np.concatenate(out) * norm["sp_std"] + norm["sp_mean"]
    true = y * norm["sp_std"] + norm["sp_mean"]
    return speed, sections, times, idx, pred, true


def fig_network(speed, sections, times):
    """378 個國道路段：AI 學習的路網，顏色是某個平日傍晚的實際車速。"""
    pos = load_gantry_positions()
    T = speed.shape[0]
    test = np.arange(int(T * 0.8), T)
    slow = (speed[test] < 60).sum(axis=1)
    t = test[int(np.argmax(slow))]                         # 測試期間最多路段變慢的時刻
    stamp = str(times[t].astype("datetime64[m]")).replace("T", " ")

    fig, ax = plt.subplots(figsize=(5.2, 6.4), dpi=200)
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    rows = con.execute("SELECT lat_a, lon_a, lat_b, lon_b FROM osm_edges "
                       "WHERE highway IN ('motorway','trunk','primary')").fetchall()
    con.close()
    ax.add_collection(LineCollection([[(r[1], r[0]), (r[3], r[2])] for r in rows],
                                     colors="#E1E7E9", linewidths=0.4, zorder=1))
    segs, vals = [], []
    for i, s in enumerate(sections):
        a, b = str(s).split("-")
        if a in pos and b in pos:
            segs.append([(pos[a][1], pos[a][0]), (pos[b][1], pos[b][0])])
            vals.append(speed[t, i])
    cmap = plt.get_cmap("RdYlGn")
    lc = LineCollection(segs, array=np.array(vals), cmap=cmap, linewidths=2.6, zorder=3,
                        capstyle="round")
    lc.set_clim(20, 100)
    ax.add_collection(lc)
    pts = np.array([p for sgm in segs for p in sgm])
    ax.scatter(pts[:, 0], pts[:, 1], s=2.5, color=DARK, zorder=4, linewidths=0)
    ax.set_xlim(119.95, 122.05)
    ax.set_ylim(21.95, 25.35)
    ax.set_aspect(1 / np.cos(np.radians(23.6)))
    ax.set_xticks([]); ax.set_yticks([])
    for sp in ax.spines.values():
        sp.set_color("#DCE4E6")
    cb = fig.colorbar(lc, ax=ax, fraction=0.04, pad=0.02)
    cb.set_label("車速（公里/小時）", fontsize=10, color=INK)
    cb.ax.tick_params(labelsize=9)
    ax.text(0.03, 0.03, f"{stamp} 的實際車速", transform=ax.transAxes, fontsize=10,
            color=INK, bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#DCE4E6"))
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "pred_1_國道路網.png"), facecolor="white")
    plt.close(fig)
    print("  pred_1：", stamp, f"，{len(segs)} 段有座標")


def fig_forecast(sections, times, idx, pred, true):
    """一個路段、測試期間一整天：30 分鐘前的預測 vs 實際車速，另畫「直接沿用現在的車速」。"""
    n = list(map(str, sections)).index(EXAMPLE_SECTION)
    h = 2                                                   # HORIZONS[2] = 6 步 = 30 分鐘
    tgt = idx + HORIZONS[h] - 1                             # 預測目標時刻
    tt = times[tgt]
    day = tt.astype("datetime64[D]")
    pick = day == np.unique(day)[1]                         # 測試期間第一個完整日
    actual = true[pick, n, h]
    ahead = pred[pick, n, h]
    naive = true[pick, n, 0]                                # 第 t 步的實際值 = 預測當下看到的最新車速
    hrs = (tt[pick] - tt[pick][0].astype("datetime64[D]")) / np.timedelta64(1, "h")

    fig, ax = plt.subplots(figsize=(11.5, 4.3), dpi=200)
    ax.plot(hrs, actual, color=GREY, lw=2.4, label="實際車速")
    ax.plot(hrs, naive, color=DARK, lw=1.4, ls="--", label="只看現在：沿用當下車速")
    ax.plot(hrs, ahead, color=ACC, lw=2.0, label="AI：30 分鐘前就做的預測")
    ax.set_xlim(0, 24)
    ax.set_xticks(range(0, 25, 3))
    ax.set_xticklabels([f"{x}:00" for x in range(0, 25, 3)], fontsize=11)
    ax.tick_params(axis="y", labelsize=11)
    ax.set_ylabel("車速（公里/小時）", fontsize=12, color=INK)
    ax.grid(alpha=0.3)
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
    ax.legend(fontsize=11, loc="lower left", frameon=True, framealpha=0.95)
    date = str(np.unique(day)[1])
    ax.set_title(f"國道一號北上・台中路段　{date}", fontsize=12, color=MUTED, loc="left")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "pred_2_預測與實際.png"), facecolor="white")
    plt.close(fig)
    mae = lambda a, b: float(np.abs(a - b).mean())          # noqa: E731
    print(f"  pred_2：{date}，30 分鐘預測誤差 AI {mae(ahead, actual):.2f}、"
          f"沿用現在 {mae(naive, actual):.2f} km/h")


def fig_anomaly(sections, times, idx, pred, true):
    """一件真實事故：實際車速突然掉到預測以下，落差超過門檻即自動標記。"""
    resid = pred[..., 0] - true[..., 0]
    z = (resid - resid.mean()) / resid.std()
    tt = times[idx]                                         # horizon 1 的目標時刻
    # 事故清單只有國道一號；其他國道的門架編號格式不同，給一個不會被對到的方向
    spans = [section_spans([s])[0] if str(s).startswith("01F") else ("X", 0.0, 0.0)
             for s in sections]
    best = None
    for a in load_accidents(EVENTS_CSV):
        secs = match_sections(a, spans, 0)
        if not secs:
            continue
        w0 = np.datetime64(a["onset"] - timedelta(minutes=10))
        w1 = np.datetime64(a["cleared"] + timedelta(minutes=30))
        tm = (tt >= w0) & (tt <= w1)
        if tm.sum() < 3:
            continue
        n = secs[0]
        drop = float(np.median(true[~tm, n, 0][-200:]) - true[tm, n, 0].min())
        hit = bool((z[tm, n] > 3).any())
        if hit and (best is None or drop > best[0]):
            best = (drop, a, n)
    _, a, n = best
    on = np.datetime64(a["onset"])
    span = (tt >= on - np.timedelta64(120, "m")) & (tt <= on + np.timedelta64(150, "m"))
    mins = (tt[span] - on) / np.timedelta64(1, "m")
    flag = z[span, n] > 3

    fig, ax = plt.subplots(figsize=(11.5, 4.3), dpi=200)
    cl = (np.datetime64(a["cleared"]) - on) / np.timedelta64(1, "m")
    ax.axvspan(0, max(cl, 5), color=ACC, alpha=0.10, lw=0)
    ax.axvline(0, color=ACC, lw=1.4, ls="--")
    ax.text(2, 0.97, "事故通報", transform=ax.get_xaxis_transform(), color=ACC, fontsize=12,
            va="top")
    ax.plot(mins, pred[span, n, 0], color=DARK, lw=2.0, label="AI 預測的車速（照路況該有的速度）")
    ax.plot(mins, true[span, n, 0], color=GREY, lw=2.4, label="實際車速")
    ax.scatter(mins[flag], true[span, n, 0][flag], s=70, color="#D7301F", zorder=5,
               label="落差過大：自動標記異常")
    ax.set_xlabel("距離事故通報（分鐘）", fontsize=12, color=INK)
    ax.set_ylabel("車速（公里/小時）", fontsize=12, color=INK)
    ax.tick_params(labelsize=11)
    ax.grid(alpha=0.3)
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
    ax.legend(fontsize=11, loc="lower left", frameon=True, framealpha=0.95)
    d = "南下" if a["direction"] == "S" else "北上"
    ax.set_title(f"國道一號{d} {a['km']:.0f} 公里處　{str(on.astype('datetime64[m]')).replace('T', ' ')}",
                 fontsize=12, color=MUTED, loc="left")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "pred_3_異常偵測.png"), facecolor="white")
    plt.close(fig)
    print(f"  pred_3：{a['event_id']}，{sections[n]}，標記 {int(flag.sum())} 點")


def main():
    os.makedirs(OUT, exist_ok=True)
    speed, sections, times, idx, pred, true = predict_test()
    mae = np.abs(pred - true).mean(axis=(0, 1))
    print("測試段 MAE（應與 traffic_tgcn.json 相同）：", np.round(mae, 3))
    fig_network(speed, sections, times)
    fig_forecast(sections, times, idx, pred, true)
    fig_anomaly(sections, times, idx, pred, true)


if __name__ == "__main__":
    main()
