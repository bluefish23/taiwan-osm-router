"""下載高公局 LiveEvents 歷史事件資料，萃取國道一號交通事故（異常偵測 ground truth）。

資料來源：https://tisvcloud.freeway.gov.tw/history/motc20/LiveEvents/YYYYMMDD/LiveEventList_HHmm.xml.gz
每分鐘一檔；本工具以 --interval 分鐘取樣（事故在資料流中通常持續數十分鐘，
5 分鐘取樣足以捕捉起訖），依 EventID 去重並記錄 first_seen / last_seen。

欄位（XML, UTF-8）：EventID, EventTitle, EventType(1=交通事故), EffectiveTime,
Positions POINT(lon lat), Location/FreeExpressHighway: Road, Direction(北向/南向), StartKM "41K+500"

輸出：data/m05a/events_accidents.csv
用法：python gcn/traffic/download_events.py --start 20260526 --end 20260530
"""
from __future__ import annotations

import argparse
import csv
import gzip
import re
import sys
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from download_m05a import daterange, fetch_tisvcloud  # noqa: E402

BASE_URL = "https://tisvcloud.freeway.gov.tw/history/motc20/LiveEvents"
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "m05a"
RAW_DIR = DATA_DIR / "raw_events"

EVENT_TYPE_ACCIDENT = "1"       # MOTC LiveEvent: 1=交通事故
TARGET_ROAD = "國道一號"
DIR_MAP = {"北向": "N", "南向": "S"}   # 對應門架 ID 尾碼


def _km_to_float(km_text: str) -> float | None:
    """'41K+500' → 41.5；解析失敗回 None。"""
    m = re.match(r"(\d+)K\+(\d+)", km_text or "")
    if not m:
        return None
    return int(m.group(1)) + int(m.group(2)) / 1000.0


def download_one(day: str, hhmm: str) -> Path | None:
    dest = RAW_DIR / day / f"LiveEventList_{hhmm}.xml.gz"
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        return dest
    try:
        fetch_tisvcloud(f"{BASE_URL}/{day}/LiveEventList_{hhmm}.xml.gz", dest)
        return dest
    except Exception:
        if dest.exists():
            dest.unlink()          # curl 失敗仍可能留空檔
        return None


def parse_file(path: Path) -> list[dict]:
    """萃取國道一號交通事故。回傳 [{event_id, effective, direction, km, ...}]"""
    out = []
    try:
        root = ET.fromstring(gzip.open(path, "rb").read())
    except Exception:
        return out
    for ev in root.iter("LiveEvent"):
        if (ev.findtext("EventType") or "") != EVENT_TYPE_ACCIDENT:
            continue
        loc = ev.find("Location/FreeExpressHighway")
        if loc is None or (loc.findtext("Road") or "") != TARGET_ROAD:
            continue
        direction = DIR_MAP.get(loc.findtext("Direction") or "")
        km = _km_to_float(loc.findtext("StartKM") or "")
        if direction is None or km is None:
            continue
        pm = re.match(r"POINT\(([\d.]+) ([\d.]+)\)", ev.findtext("Positions") or "")
        out.append({
            "event_id": ev.findtext("EventID") or "",
            "effective": ev.findtext("EffectiveTime") or "",
            "direction": direction,
            "km": km,
            "lon": float(pm.group(1)) if pm else None,
            "lat": float(pm.group(2)) if pm else None,
            "step": ev.findtext("EventStep") or "",
            "last_update": ev.findtext("LastUpdateTime") or "",
        })
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--interval", type=int, default=5, help="取樣間隔（分鐘）")
    ap.add_argument("--workers", type=int, default=6)
    args = ap.parse_args()

    stamps = [f"{h:02d}{m:02d}" for h in range(24) for m in range(0, 60, args.interval)]
    tasks = [(day, s) for day in daterange(args.start, args.end) for s in stamps]
    print(f"downloading {len(tasks)} samples "
          f"({args.start}~{args.end}, every {args.interval} min)", flush=True)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        paths = list(pool.map(lambda t: download_one(*t), tasks))
    ok = [p for p in paths if p is not None]
    print(f"downloaded/cached: {len(ok)} / {len(tasks)}", flush=True)

    # 依 EventID 彙整：onset = EffectiveTime，last_seen = 最後出現的取樣時間
    events: dict[str, dict] = {}
    for p in sorted(ok):
        hhmm = p.name.split("_")[1].split(".")[0]               # 'LiveEventList_0145.xml.gz' → '0145'
        sample_ts = f"{p.parent.name} {hhmm}"                   # 'YYYYMMDD HHmm'
        for rec in parse_file(p):
            eid = rec["event_id"]
            if eid not in events:
                events[eid] = rec | {"first_sample": sample_ts, "last_sample": sample_ts}
            else:
                events[eid]["last_sample"] = sample_ts
                events[eid]["step"] = rec["step"]

    out = DATA_DIR / "events_accidents.csv"
    cols = ["event_id", "effective", "direction", "km", "lat", "lon",
            "step", "last_update", "first_sample", "last_sample"]
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for rec in sorted(events.values(), key=lambda r: r["effective"]):
            w.writerow(rec)
    print(f"accidents on {TARGET_ROAD}: {len(events)}")
    print(f"saved -> {out}")


if __name__ == "__main__":
    main()
