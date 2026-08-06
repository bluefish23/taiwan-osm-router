"""下載並前處理高公局 TDCS M05A 歷史資料（每 5 分鐘各門架路段平均速率）。

資料來源：https://tisvcloud.freeway.gov.tw/history/TDCS/M05A/M05A_YYYYMMDD.tar.gz
每個 tar.gz 內含當日 288 個 5 分鐘 CSV，欄位（無標頭）：
    TimeInterval, GantryFrom, GantryTo, VehicleType, SpaceMeanSpeed, Volume

處理流程：
1. 下載指定日期範圍的 tar.gz
2. 篩選指定國道門架（ID 前綴，預設含國1/國3/國5/國1高架/國3甲）
3. 各車種以交通量加權平均出該路段的平均速率
4. 輸出 (T × N) 速率矩陣 + 路段清單 -> data/m05a/m05a_matrix.npz

用法：python gcn/traffic/download_m05a.py --start 20260510 --end 20260530
"""
from __future__ import annotations

import argparse
import io
import subprocess
import tarfile
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

BASE_URL = "https://tisvcloud.freeway.gov.tw/history/TDCS/M05A"
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "m05a"
# 國道門架前綴（M05A 原始資料本就含多條國道）：
#   01F 國道一號主線、03F 國道三號、05F 國道五號、01H 國道一號高架、03A 國道三甲
DEFAULT_PREFIXES = ("01F", "03F", "05F", "01H", "03A")


def daterange(start: str, end: str):
    d0 = datetime.strptime(start, "%Y%m%d")
    d1 = datetime.strptime(end, "%Y%m%d")
    while d0 <= d1:
        yield d0.strftime("%Y%m%d")
        d0 += timedelta(days=1)


def fetch_tisvcloud(url: str, dest: Path) -> None:
    """下載 tisvcloud 檔案。其憑證缺 Subject Key Identifier，
    Python ssl 會拒絕，統一改走 curl（predict_service 也共用此函式）。"""
    subprocess.run(["curl", "-sf", "-o", str(dest), url], check=True)


def download_day(day: str) -> Path:
    dest = DATA_DIR / "raw" / f"M05A_{day}.tar.gz"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.exists():
        url = f"{BASE_URL}/M05A_{day}.tar.gz"
        print(f"downloading {url}")
        fetch_tisvcloud(url, dest)
    return dest


def parse_day(tar_path: Path, prefixes: tuple[str, ...] = DEFAULT_PREFIXES) -> pd.DataFrame:
    """讀取一天的 tar.gz，回傳 volume 加權平均速率的長表。只保留起訖門架同屬指定國道者。"""
    frames = []
    with tarfile.open(tar_path, "r:gz") as tar:
        for member in tar.getmembers():
            if not member.name.endswith(".csv"):
                continue
            f = tar.extractfile(member)
            if f is None:
                continue
            df = pd.read_csv(io.BytesIO(f.read()), header=None,
                             names=["time", "from", "to", "vtype", "speed", "volume"])
            df = df[df["from"].str.startswith(prefixes)
                    & df["to"].str.startswith(prefixes)]
            if df.empty:
                continue
            # speed=0 且 volume=0 代表無車，不列入平均
            df = df[(df["volume"] > 0) & (df["speed"] > 0)]
            frames.append(df)
    day = pd.concat(frames, ignore_index=True)
    day["section"] = day["from"] + "-" + day["to"]
    day["sv"] = day["speed"] * day["volume"]
    agg = day.groupby(["time", "section"]).agg(sv=("sv", "sum"),
                                               volume=("volume", "sum"))
    agg["speed"] = agg["sv"] / agg["volume"]
    return agg.reset_index()[["time", "section", "speed", "volume"]]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--prefixes", default=",".join(DEFAULT_PREFIXES),
                    help="逗號分隔的國道門架前綴，如 01F,03F,05F（預設全部國道）")
    args = ap.parse_args()
    prefixes = tuple(p.strip() for p in args.prefixes.split(",") if p.strip())
    print(f"門架前綴：{prefixes}")

    all_days = []
    for day in daterange(args.start, args.end):
        try:
            tar_path = download_day(day)
            all_days.append(parse_day(tar_path, prefixes))
            print(f"  parsed {day}: {len(all_days[-1]):,} rows")
        except Exception as e:                       # 缺天就跳過
            print(f"  skip {day}: {e}")

    long = pd.concat(all_days, ignore_index=True)
    long["time"] = pd.to_datetime(long["time"])

    # 只保留观测覆蓋率 > 95% 的路段，缺值前向填補
    speed = long.pivot_table(index="time", columns="section", values="speed")
    speed = speed.sort_index()
    coverage = speed.notna().mean()
    keep = coverage[coverage > 0.95].index
    speed = speed[keep].ffill().bfill()
    volume = (long.pivot_table(index="time", columns="section", values="volume")
              .sort_index()[keep].ffill().bfill())

    out = DATA_DIR / "m05a_matrix.npz"
    np.savez_compressed(
        out,
        speed=speed.to_numpy(dtype=np.float32),
        volume=volume.to_numpy(dtype=np.float32),
        timestamps=speed.index.astype("int64").to_numpy(),
        sections=np.array(speed.columns.tolist()),
    )
    print(f"matrix: {speed.shape[0]} timesteps x {speed.shape[1]} sections")
    print(f"saved -> {out}")


if __name__ == "__main__":
    main()
