"""即時預測：從高公局 ETag 即時配對資料建滾動視窗，餵 T-GCN 做線上預測。

取代 predict_service 的重播模式——不再從歷史矩陣切片，而是每次抓最新的
ETagPairLive（門架配對即時車速，與 M05A 同格式），維護最近 60 分鐘車速，
預測 30 分鐘後壅塞。事件寫入沿用 predict_service.write_congestion_events。

冷啟動：一次抓過去 12 個 5 分鐘檔填滿視窗，立即可預測（不需枯等一小時）。

即時源：https://tisvcloud.freeway.gov.tw/history/motc20/ETag/{YYYYMMDD}/ETagPairLive_{HHMM}.xml.gz
"""
from __future__ import annotations

import gzip
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

GCN_DIR = Path(__file__).resolve().parent.parent
if str(GCN_DIR) not in sys.path:
    sys.path.insert(0, str(GCN_DIR))
from traffic.predict_service import (DATA, TrafficPredictor,           # noqa: E402
                                     get_predictor, write_congestion_events)
from traffic.download_m05a import fetch_tisvcloud                      # noqa: E402

ETAG_LIVE = ("https://tisvcloud.freeway.gov.tw/history/motc20/ETag/"
             "{day}/ETagPairLive_{hhmm}.xml.gz")
LAG_MIN = 10          # 即時檔有數分鐘延遲，視窗結束時刻退 10 分鐘較保險
TMP = GCN_DIR / "data" / "etag" / "_livetmp"

_FREEFLOW: np.ndarray | None = None
_VOLUME: np.ndarray | None = None


def _predictor() -> TrafficPredictor:
    # 與重播模式共用推論器：模型更新換上新版本後自動改用（model_update / model_registry）
    return get_predictor()


def _volume_baseline() -> np.ndarray:
    """各路段的典型 5 分鐘交通量（中位數），即時檔缺該路段時的補值。"""
    global _VOLUME
    if _VOLUME is None:
        _VOLUME = np.median(np.load(DATA, allow_pickle=True)["volume"], axis=0).astype(np.float32)
    return _VOLUME


def _freeflow_baseline() -> np.ndarray:
    """自由流速（85 百分位）——對齊 checkpoint 路段順序，即時模式的壅塞基準。"""
    global _FREEFLOW
    if _FREEFLOW is None:
        data = np.load(DATA, allow_pickle=True)
        if list(data["sections"]) != _predictor().sections:
            raise RuntimeError("m05a 矩陣與 checkpoint 路段不一致，請重新訓練")
        _FREEFLOW = np.percentile(data["speed"], 85, axis=0).astype(np.float32)
    return _FREEFLOW


def parse_pair_live_flows(raw: bytes) -> dict[str, tuple[float, float]]:
    """ETagPairLive → {配對ID: (流量加權平均車速, 總交通量)}。

    與訓練資料 M05A 的前處理相同：各車種以 VehicleCount 加權平均車速、交通量加總。
    """
    raw = re.sub(rb'\sxmlns="[^"]+"', b"", raw, count=1)          # 移除預設 namespace
    try:
        root = ET.fromstring(raw)
    except Exception:
        return {}
    out = {}
    for pair in root.iter("ETagPairLive"):
        pid = pair.findtext("ETagPairID") or ""
        num = den = 0.0
        for flow in pair.iter("Flow"):
            v = float(flow.findtext("VehicleCount") or 0)
            s = float(flow.findtext("SpaceMeanSpeed") or 0)
            if v > 0 and s > 0:
                num += v * s
                den += v
        if den > 0:
            out[pid] = (num / den, den)
    return out


def parse_pair_live(raw: bytes) -> dict[str, float]:
    """ETagPairLive → {配對ID: 流量加權平均車速}（各車種以 VehicleCount 加權）。"""
    return {pid: spd for pid, (spd, _) in parse_pair_live_flows(raw).items()}


def _fetch_pair_live(t: datetime) -> dict[str, tuple[float, float]] | None:
    TMP.mkdir(parents=True, exist_ok=True)
    dest = TMP / f"pair_{t:%Y%m%d_%H%M}.xml.gz"
    try:
        fetch_tisvcloud(ETAG_LIVE.format(day=f"{t:%Y%m%d}", hhmm=f"{t:%H%M}"), dest)
        parsed = parse_pair_live_flows(gzip.decompress(dest.read_bytes()))
        dest.unlink(missing_ok=True)
        return parsed or None
    except Exception:
        if dest.exists():
            dest.unlink(missing_ok=True)
        return None


def build_live_window(sections, n_steps: int):
    """抓最近 n_steps 個 5 分鐘 ETag 即時檔，對齊 sections 建 (T,N) 車速視窗。

    缺的路段補自由流速與典型交通量；缺的時間步以前一步前向填補。
    回傳 (speed, volume, stamps, got)。volume 是實際交通量，與訓練資料同單位
    （每 5 分鐘車輛數）——模型的第二個特徵就是這個值，不能用佔位數字。
    """
    ff = _freeflow_baseline()
    vol0 = _volume_baseline()
    N = len(sections)
    sec_idx = {s: i for i, s in enumerate(sections)}
    tw_now = datetime.now(timezone.utc) + timedelta(hours=8)       # 台灣時間
    end = tw_now.replace(second=0, microsecond=0)
    end -= timedelta(minutes=end.minute % 5 + LAG_MIN)            # 對齊 5 分鐘格 + 退延遲

    speed = np.tile(ff, (n_steps, 1)).astype(np.float32)
    volume = np.tile(vol0, (n_steps, 1)).astype(np.float32)
    stamps, got = [], 0
    prev = None
    for k in range(n_steps):
        t = end - timedelta(minutes=5 * (n_steps - 1 - k))
        stamps.append(f"{t:%m/%d %H:%M}")
        parsed = _fetch_pair_live(t)
        if parsed:
            got += 1
            for pid, (spd, vol) in parsed.items():
                j = sec_idx.get(pid)
                if j is not None:
                    speed[k, j] = spd
                    volume[k, j] = vol
            prev = (speed[k].copy(), volume[k].copy())
        elif prev is not None:
            speed[k], volume[k] = prev                             # 前向填補缺檔
    return speed, volume, stamps, got


def run_live_prediction(db_path: str, horizon_pick: int = -1) -> dict:
    """抓即時資料 → 預測 → 寫入 predicted_congestion 事件。回傳摘要（含 event_ids）。"""
    p = _predictor()
    ff = _freeflow_baseline()
    speed, volume, stamps, got = build_live_window(p.sections, p.in_steps)
    if got == 0:
        raise RuntimeError("無法取得任何即時 ETag 資料（來源延遲或網路問題）")
    pred = p.predict(speed, volume)[:, horizon_pick]
    horizon_min = p.horizons[horizon_pick] * 5
    result = write_congestion_events(db_path, pred, ff, np.array(p.sections), horizon_min)
    result.update(mode="live", window=[stamps[0], stamps[-1]],
                  fetched_steps=f"{got}/{p.in_steps}")
    return result


if __name__ == "__main__":
    import argparse
    import json
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    print(json.dumps(run_live_prediction(ap.parse_args().db), indent=2, ensure_ascii=False))
