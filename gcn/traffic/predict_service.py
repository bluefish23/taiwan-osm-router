"""預測服務：把 T-GCN 車速預測轉成路由系統的 dynamic_events（Phase 5 整合）。

設計原則：完全重用原系統機制、不動任何既有程式——
- 預測壅塞以 `source='prediction'` 寫入既有的 dynamic_events 表，
  與手動事件（manual）、即時事件（realtime）互不干擾：
  realtime_sync 每輪只清 source='realtime'，本服務每輪只清 source='prediction'。
- 事件座標 = 門架路段兩端座標的中點（高公局 ETag 靜態資料）。
- 套用到路網成本沿用原本流程（POST /dynamic/recompute 或 apply_event_incremental）。

demo 為「重播模式」：從已下載的 M05A 矩陣取一段 60 分鐘視窗當作「現在」，
預測未來 5/15/30 分鐘。之後接上即時 ETag 資料流即可轉為線上模式。

用法：
    python gcn/traffic/predict_service.py --db taiwan_osm.db            # 用資料末端當現在
    python gcn/traffic/predict_service.py --db taiwan_osm.db --at 5500  # 指定時間索引
"""
from __future__ import annotations

import argparse
import gzip
import json
import sqlite3
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

GCN_DIR = Path(__file__).resolve().parent.parent
DATA = GCN_DIR / "data" / "m05a" / "m05a_matrix.npz"
ETAG_XML = GCN_DIR / "data" / "etag" / "ETag.xml.gz"
ETAG_URL = ("https://tisvcloud.freeway.gov.tw/history/motc20/ETag/"
            "20260530/ETag_0000.xml.gz")
CKPT = GCN_DIR / "results" / "traffic_tgcn.ckpt"

# 壅塞判定與 realtime_sync 的 VD 邏輯一致：速度 < 自由流 70% 才建事件，
# severity = 1 - speed/free_speed（同 realtime_sync.sync_traffic_speed）
CONGESTION_RATIO = 0.70
EVENT_RADIUS_KM = 1.0

# 靜態資源（門架座標、模型）於行程內只載一次
_GANTRY_POS: dict[str, tuple[float, float]] | None = None
_PREDICTOR: "TrafficPredictor | None" = None


def load_gantry_positions() -> dict[str, tuple[float, float]]:
    """門架 ID → (lat, lon)，來源：高公局 ETag 靜態資料（行程內快取）。"""
    global _GANTRY_POS
    if _GANTRY_POS is not None:
        return _GANTRY_POS
    if not ETAG_XML.exists():
        from traffic.download_m05a import fetch_tisvcloud
        ETAG_XML.parent.mkdir(parents=True, exist_ok=True)
        fetch_tisvcloud(ETAG_URL, ETAG_XML)
    ns = "{http://traffic.transportdata.tw/standard/traffic/schema/}"
    pos = {}
    with gzip.open(ETAG_XML, "rt", encoding="utf-8") as f:
        root = ET.parse(f).getroot()
    for etag in root.iter(f"{ns}ETag"):
        gid = etag.findtext(f"{ns}ETagGantryID")
        lat = etag.findtext(f"{ns}PositionLat")
        lon = etag.findtext(f"{ns}PositionLon")
        if gid and lat and lon:
            pos[gid] = (float(lat), float(lon))
    _GANTRY_POS = pos
    return pos


def clear_predictions(db_path: str) -> int:
    """清除所有預測事件（source='prediction'）。manual/realtime 不受影響。"""
    with sqlite3.connect(db_path, timeout=30) as conn:
        n = conn.execute(
            "DELETE FROM dynamic_events WHERE source='prediction'").rowcount
        conn.commit()
    return n


class TrafficPredictor:
    """載入訓練好的 T-GCN checkpoint 做車速推論。"""

    def __init__(self, ckpt_path: Path = CKPT):
        import sys
        if str(GCN_DIR) not in sys.path:
            sys.path.insert(0, str(GCN_DIR))
        import torch
        from gcn_layer import normalize_adjacency, sparse_to_torch
        from traffic.tgcn_model import NodeGRU, TGCN
        from traffic.train_traffic import build_adjacency

        self.torch = torch
        ckpt = torch.load(ckpt_path, map_location="cpu", weights_only=False)
        self.sections: list[str] = ckpt["sections"]
        self.norm = ckpt["norm"]
        self.horizons: list[int] = ckpt["horizons"]
        self.in_steps: int = ckpt["in_steps"]

        adj = build_adjacency(np.array(self.sections))
        self.a_hat = sparse_to_torch(normalize_adjacency(adj), torch.device("cpu"))
        cls = TGCN if ckpt["model"] == "tgcn" else NodeGRU
        self.model = cls(num_features=2, hidden=ckpt["hidden"],
                         num_horizons=len(self.horizons))
        self.model.load_state_dict(ckpt["state_dict"])
        self.model.eval()

    def predict(self, speed_win: np.ndarray, volume_win: np.ndarray) -> np.ndarray:
        """輸入最近 in_steps 步的 (T, N) 車速/流量，回傳 (N, H) 預測車速 km/h。"""
        n = self.norm
        x = np.stack([(speed_win - n["sp_mean"]) / n["sp_std"],
                      (volume_win - n["vo_mean"]) / n["vo_std"]],
                     axis=-1).astype(np.float32)[None]          # (1, T, N, 2)
        with self.torch.no_grad():
            out = self.model(self.a_hat, self.torch.from_numpy(x))
        return out.squeeze(0).numpy() * n["sp_std"] + n["sp_mean"]


def run_prediction(db_path: str, at_index: int | None = None,
                   horizon_pick: int = -1) -> dict:
    """跑一輪預測並把壅塞事件寫入 DB。回傳摘要 dict。

    horizon_pick：用哪個預測視野建事件（-1 = 最遠，即 30 分鐘）。
    """
    global _PREDICTOR
    data = np.load(DATA, allow_pickle=True)
    speed, volume = data["speed"], data["volume"]
    sections = data["sections"]
    if _PREDICTOR is None:
        _PREDICTOR = TrafficPredictor()
    predictor = _PREDICTOR
    if list(sections) != predictor.sections:
        raise RuntimeError("m05a 矩陣與 checkpoint 的路段清單不一致，請重新訓練")

    t_end = at_index if at_index is not None else speed.shape[0]
    t0 = t_end - predictor.in_steps
    if t0 < 0:
        raise ValueError("資料不足一個輸入視窗")
    pred = predictor.predict(speed[t0:t_end], volume[t0:t_end])   # (N, H)
    pred_speed = pred[:, horizon_pick]
    horizon_min = predictor.horizons[horizon_pick] * 5

    # 自由流速：各路段車速 85 百分位（與 realtime_sync 的 VD 壅塞判定邏輯一致）
    freeflow = np.percentile(speed, 85, axis=0)
    ratio = pred_speed / np.maximum(freeflow, 1.0)
    congested = np.where(ratio < CONGESTION_RATIO)[0]

    gantry_pos = load_gantry_positions()
    # 時間戳格式與 osm_router.utc_now 一致，維持 ORDER BY created_at 的正確排序
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows, skipped = [], 0
    for i in congested:
        frm, to = str(sections[i]).split("-")
        if frm not in gantry_pos or to not in gantry_pos:
            skipped += 1
            continue
        (lat1, lon1), (lat2, lon2) = gantry_pos[frm], gantry_pos[to]
        # severity 公式同 realtime_sync（ratio<0.7 保證 severity>=0.3）
        severity = float(min(1.0, max(0.0, 1.0 - ratio[i])))
        rows.append((
            f"pred_{uuid.uuid4().hex[:12]}", "congestion", severity, "motorway",
            (lat1 + lat2) / 2, (lon1 + lon2) / 2, EVENT_RADIUS_KM,
            f"[預測+{horizon_min}分] {sections[i]} 預測 {pred_speed[i]:.0f}km/h "
            f"(自由流 {freeflow[i]:.0f})", 1, now, "prediction",
        ))

    clear_predictions(db_path)
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.executemany(
            "INSERT INTO dynamic_events VALUES(?,?,?,?,?,?,?,?,?,?,?)", rows)
        conn.commit()

    return {
        "ok": True,
        "horizon_min": horizon_min,
        "sections_total": int(len(sections)),
        "predicted_congested": int(len(congested)),
        "events_created": len(rows),
        "skipped_no_gantry_pos": skipped,
        "mean_predicted_speed": round(float(pred_speed.mean()), 1),
        "note": "事件已寫入（source='prediction'），呼叫 /dynamic/recompute 套用至路網成本",
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--at", type=int, default=None,
                    help="M05A 時間索引（demo 重播用；預設 = 資料末端）")
    args = ap.parse_args()
    print(json.dumps(run_prediction(args.db, args.at), indent=2, ensure_ascii=False))
