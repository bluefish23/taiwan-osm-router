"""下載高公局 VD 車輛偵測器即時資料，聚合成 5 分鐘車速矩陣（全國道 8 條）。

M05A 只涵蓋有 ETC 門架的 5 條國道；VD 偵測器涵蓋全部 8 條（含國2/4/6/8/10）。
資料來源：https://tisvcloud.freeway.gov.tw/history/motc20/VD/YYYYMMDD/VDLive_HHMM.xml.gz
每分鐘一檔；本工具以 --interval 分鐘取樣（預設 5，對齊 M05A 粒度）。

VDLive 結構：每個 VD → LinkFlows/LinkFlow/Lanes/Lane/Vehicles/Vehicle(Volume, Speed)
主線 VDID 格式：VD-N{道}-{方向NSEW}-{里程}-M-{型}（M=主線，排除匝道 O/R）

處理：各 VD 以車種流量加權平均車速 → (T × N) 矩陣。
建圖：同國道同方向的主線 VD 按里程排序，相鄰兩點連邊（沿行車方向）。

輸出：data/vd/vd_matrix.npz（speed, volume, timestamps, sections=VDID, edges）
用法：python gcn/traffic/download_vd.py --start 20260510 --end 20260530
"""
from __future__ import annotations

import argparse
import gzip
import re
import sys
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from itertools import groupby
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from download_m05a import daterange, fetch_tisvcloud  # noqa: E402

BASE_URL = "https://tisvcloud.freeway.gov.tw/history/motc20/VD"
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "vd"
RAW_DIR = DATA_DIR / "raw"

# 主線 VDID：VD-N1-N-86.120-M-LOOP → (國道, 方向, 里程)
MAINLINE = re.compile(r"^VD-(N\d+)-([NSEW])-([\d.]+)-M-")


def download_one(day: str, hhmm: str) -> Path | None:
    dest = RAW_DIR / day / f"VDLive_{hhmm}.xml.gz"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    try:
        fetch_tisvcloud(f"{BASE_URL}/{day}/VDLive_{hhmm}.xml.gz", dest)
        return dest if dest.stat().st_size > 0 else None
    except Exception:
        if dest.exists():
            dest.unlink()
        return None


def parse_vdlive(path: Path) -> dict[str, tuple[float, float]]:
    """回傳 {vdid: (流量加權平均車速, 總流量)}，僅主線、Status=0（正常）。"""
    try:
        raw = gzip.open(path, "rb").read()
        raw = re.sub(rb'\sxmlns="[^"]+"', b"", raw, count=1)   # 移除預設 namespace，ET 才找得到裸標籤
        root = ET.fromstring(raw)
    except Exception:
        return {}
    out = {}
    for live in root.iter("VDLive"):
        vdid = live.findtext("VDID") or ""
        if not MAINLINE.match(vdid):
            continue
        if (live.findtext("Status") or "1") != "0":
            continue
        num = den = 0.0                              # sum(vol×speed), sum(vol)
        for veh in live.iter("Vehicle"):
            v = float(veh.findtext("Volume") or 0)
            s = float(veh.findtext("Speed") or 0)
            if v > 0 and s > 0:
                num += v * s
                den += v
        if den > 0:
            out[vdid] = (num / den, den)             # (加權速度, 總流量)
    return out


def build_edges(vdids: list[str]) -> np.ndarray:
    """同國道同方向的主線 VD 按里程排序，相鄰連邊。回傳 (2, E) 的 row/col。"""
    parsed = []
    for idx, v in enumerate(vdids):
        m = MAINLINE.match(v)
        parsed.append((m.group(1), m.group(2), float(m.group(3)), idx))
    row, col = [], []
    parsed.sort(key=lambda x: (x[0], x[1], x[2]))
    for _, grp in groupby(parsed, key=lambda x: (x[0], x[1])):
        chain = list(grp)
        for a, b in zip(chain, chain[1:]):
            row.append(a[3]); col.append(b[3])       # 上游 → 下游
    return np.array([row, col], dtype=np.int64)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--interval", type=int, default=5, help="取樣間隔（分鐘）")
    ap.add_argument("--workers", type=int, default=12)
    args = ap.parse_args()

    stamps = [f"{h:02d}{m:02d}" for h in range(24) for m in range(0, 60, args.interval)]
    tasks = [(day, s) for day in daterange(args.start, args.end) for s in stamps]
    print(f"下載 {len(tasks)} 檔 VDLive（{args.start}~{args.end}, 每 {args.interval} 分）", flush=True)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        paths = list(pool.map(lambda t: download_one(*t), tasks))
    ok = [p for p in paths if p is not None]
    print(f"下載/快取: {len(ok)} / {len(tasks)}", flush=True)

    def parse_with_ts(p: Path):
        ts = f"{p.parent.name} {p.name.split('_')[1].split('.')[0]}"   # 'YYYYMMDD HHMM'
        return ts, parse_vdlive(p)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(parse_with_ts, sorted(ok)))
    print(f"解析完成 {len(results)} 檔", flush=True)

    records = []
    for ts, d in results:
        t = pd.to_datetime(ts, format="%Y%m%d %H%M")
        for vdid, (spd, vol) in d.items():
            records.append((t, vdid, spd, vol))
    long = pd.DataFrame(records, columns=["time", "vdid", "speed", "volume"])

    speed = long.pivot_table(index="time", columns="vdid", values="speed").sort_index()
    coverage = speed.notna().mean()
    keep = coverage[coverage > 0.95].index
    speed = speed[keep].ffill().bfill()
    volume = (long.pivot_table(index="time", columns="vdid", values="volume")
              .sort_index()[keep].ffill().bfill())
    vdids = speed.columns.tolist()
    edges = build_edges(vdids)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out = DATA_DIR / "vd_matrix.npz"
    np.savez_compressed(
        out,
        speed=speed.to_numpy(dtype=np.float32),
        volume=volume.to_numpy(dtype=np.float32),
        timestamps=speed.index.astype("int64").to_numpy(),
        sections=np.array(vdids),
        edges=edges,
    )
    print(f"matrix: {speed.shape[0]} timesteps x {speed.shape[1]} VD, edges={edges.shape[1]}")
    print(f"saved -> {out}")


if __name__ == "__main__":
    main()
