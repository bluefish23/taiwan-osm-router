"""交通預測的歷史資料庫：每天一個檔，持續累積新資料供模型重新訓練。

存放 gcn/data/history/YYYYMMDD.npz：
    speed, volume  (288, N) float32，缺值為 NaN（尚未補值，保留原始缺漏）
    sections       (N,) 路段清單——一律對齊模型的路段順序
    source         'seed'（拆自原始訓練矩陣）或 'm05a'（高公局每日歷史檔）

資料來源與原始訓練資料相同（高公局 TDCS M05A，download_m05a.parse_day），
前處理一致：各車種以交通量加權平均車速、交通量加總。

    python gcn/traffic/traffic_history.py --seed               # 由原始訓練矩陣建立前 21 天
    python gcn/traffic/traffic_history.py --import 20260531 20260620
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

GCN_DIR = Path(__file__).resolve().parent.parent
if str(GCN_DIR) not in sys.path:
    sys.path.insert(0, str(GCN_DIR))
from traffic.download_m05a import DATA_DIR as M05A_DIR                  # noqa: E402
from traffic.download_m05a import daterange, download_day, parse_day   # noqa: E402

HISTORY_DIR = GCN_DIR / "data" / "history"
SEED_MATRIX = GCN_DIR / "data" / "m05a" / "m05a_matrix.npz"
STEPS_PER_DAY = 288
MIN_COVERAGE = 0.90      # 一天裡有觀測的格子比例低於此值，整天不拿來訓練
_FMT = "%Y%m%d"


def day_path(day: str) -> Path:
    return HISTORY_DIR / f"{day}.npz"


def download_m05a_path(day: str) -> Path:
    """download_m05a.download_day 存放原始壓縮檔的位置。"""
    return M05A_DIR / "raw" / f"M05A_{day}.tar.gz"


def missing_days(until: str) -> list[str]:
    """從第一天到 until 之間還沒有資料檔的日子（含中間因下載失敗留下的缺口）。"""
    days = available_days()
    if not days:
        return [until]
    return [d for d in daterange(days[0], until) if not day_path(d).exists()]


def available_days() -> list[str]:
    return sorted(p.stem for p in HISTORY_DIR.glob("*.npz") if p.stem.isdigit())


def _save_day(day: str, speed, volume, sections, source: str) -> None:
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    tmp = HISTORY_DIR / f"{day}.tmp.npz"
    np.savez_compressed(tmp, speed=speed.astype(np.float32), volume=volume.astype(np.float32),
                        sections=np.asarray(sections), source=np.array(source))
    tmp.replace(day_path(day))


def seed_from_matrix(matrix: Path = SEED_MATRIX) -> int:
    """把原始訓練矩陣拆成每日檔（已存在的日子不覆寫）。回傳新建的天數。"""
    z = np.load(matrix, allow_pickle=True)
    stamps = pd.to_datetime(z["timestamps"])
    days = stamps.strftime(_FMT)
    made = 0
    for day in sorted(set(days)):
        rows = np.where(days == day)[0]
        if len(rows) != STEPS_PER_DAY or day_path(day).exists():
            continue
        _save_day(day, z["speed"][rows], z["volume"][rows], z["sections"], "seed")
        made += 1
    return made


def import_m05a_day(day: str, sections, keep_raw: bool = False) -> dict:
    """下載並解析高公局 M05A 一天的資料，對齊路段後存檔。回傳覆蓋率。

    原始壓縮檔解析完即刪除（每日約 3.5 MB，持續累積會很可觀）；
    下載前就已存在的原始檔（原始訓練資料）一律保留。
    """
    raw = download_m05a_path(day)
    existed = raw.exists()
    long = parse_day(download_day(day))
    if not keep_raw and not existed:
        raw.unlink(missing_ok=True)
    long["time"] = pd.to_datetime(long["time"])
    grid = pd.date_range(datetime.strptime(day, _FMT), periods=STEPS_PER_DAY, freq="5min")
    cols = list(sections)
    speed = (long.pivot_table(index="time", columns="section", values="speed")
             .reindex(index=grid, columns=cols).to_numpy(dtype=np.float32))
    volume = (long.pivot_table(index="time", columns="section", values="volume")
              .reindex(index=grid, columns=cols).to_numpy(dtype=np.float32))
    _save_day(day, speed, volume, cols, "m05a")
    return {"day": day, "coverage": round(float(np.isfinite(speed).mean()), 4)}


def _fill_day(speed: np.ndarray, volume: np.ndarray, prev: np.ndarray | None):
    """同一路段的缺值以前後時刻補；整天沒有資料的路段沿用前一天同時段（與原始矩陣的補值一致）。"""
    out = []
    for arr, prev_arr in ((speed, None if prev is None else prev[0]),
                          (volume, None if prev is None else prev[1])):
        df = pd.DataFrame(arr).ffill().bfill()
        a = df.to_numpy(dtype=np.float32)
        if prev_arr is not None:
            empty = np.isnan(a).all(axis=0)
            a[:, empty] = prev_arr[:, empty]
        out.append(a)
    return out[0], out[1]


def load_range(sections, first: str, last: str):
    """讀 first～last 的連續資料（每 5 分鐘一格）。

    回傳 speed, volume (T, N)、timestamps (T,) datetime64[ns]、day_of (T,) 日期字串。
    沒有檔案或覆蓋率不足的日子整天為 NaN，建訓練視窗時會自動跳過。
    """
    cols = list(map(str, sections))
    speeds, vols, stamps, labels = [], [], [], []
    prev = None
    for day in daterange(first, last):
        grid = pd.date_range(datetime.strptime(day, _FMT), periods=STEPS_PER_DAY, freq="5min")
        sp = np.full((STEPS_PER_DAY, len(cols)), np.nan, dtype=np.float32)
        vo = sp.copy()
        p = day_path(day)
        if p.exists():
            z = np.load(p, allow_pickle=True)
            src = list(map(str, z["sections"]))
            if src == cols:
                raw_sp, raw_vo = z["speed"], z["volume"]
            else:                                          # 路段清單不同：依名稱對齊
                pos = {s: i for i, s in enumerate(src)}
                take = np.array([pos.get(s, -1) for s in cols])
                raw_sp = np.where(take >= 0, z["speed"][:, take], np.nan)
                raw_vo = np.where(take >= 0, z["volume"][:, take], np.nan)
            if np.isfinite(raw_sp).mean() >= MIN_COVERAGE:
                sp, vo = _fill_day(raw_sp, raw_vo, prev)
                prev = (sp, vo)
        speeds.append(sp)
        vols.append(vo)
        stamps.append(grid.to_numpy())
        labels += [day] * STEPS_PER_DAY
    return (np.concatenate(speeds), np.concatenate(vols), np.concatenate(stamps),
            np.array(labels))


def next_day(day: str) -> str:
    return (datetime.strptime(day, _FMT) + timedelta(days=1)).strftime(_FMT)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", action="store_true", help="由原始訓練矩陣建立每日檔")
    ap.add_argument("--import", nargs=2, metavar=("START", "END"), dest="span",
                    help="下載並匯入 M05A 日期範圍（YYYYMMDD）")
    args = ap.parse_args()
    sections = np.load(SEED_MATRIX, allow_pickle=True)["sections"]
    if args.seed:
        print(f"seed：新建 {seed_from_matrix()} 天")
    if args.span:
        for day in daterange(*args.span):
            try:
                print(import_m05a_day(day, sections))
            except Exception as e:                          # 缺天就跳過
                print(f"skip {day}: {e}")
    print(f"歷史資料：{len(available_days())} 天")


if __name__ == "__main__":
    main()
