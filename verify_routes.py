#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
verify_routes.py — 路線回歸比對（需要伺服器已啟動）

用途：任何會動到路徑搜尋的改動，都必須先存基準、改完再比對，
證明「行為等價的優化真的等價」「會改變行為的改動改了多少」。

使用方式
--------
    # 1. 改動前存基準
    python verify_routes.py --baseline base.json

    # 2. 改動後比對
    python verify_routes.py --compare base.json

    # 階段 2（路口收縮）比對：
    #   PARTITION_MODE=0 啟動 → --baseline base_plain.json
    #   PARTITION_MODE=1 重啟 → --compare base_plain.json --tol-min 100

取樣的 50 組起訖點由固定亂數種子產生，每次執行完全相同。
本工具只送 POST /route（唯讀），不建立事件、不寫資料庫。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import statistics
import sys
import time
import urllib.error
import urllib.request

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

API = "http://127.0.0.1:8000"
SEED = 20260910          # 固定種子：改了這個數字取樣就不同，既有基準檔即失效

# 錨點城市（lat, lon）。取樣時在其周邊加抖動，順便測到 nearest_node。
CITIES = [
    ("台北", 25.047, 121.517), ("板橋", 25.011, 121.459),
    ("桃園", 24.993, 121.301), ("新竹", 24.804, 120.969),
    ("苗栗", 24.560, 120.821), ("台中", 24.147, 120.673),
    ("彰化", 24.081, 120.541), ("南投", 23.913, 120.685),
    ("嘉義", 23.480, 120.449), ("台南", 22.999, 120.213),
    ("高雄", 22.627, 120.301), ("基隆", 25.128, 121.741),
    ("宜蘭", 24.757, 121.753), ("花蓮", 23.977, 121.604),
]

# 一定會被測到的固定組合（涵蓋 measure_routes.py 的量測點，便於交叉對照）
CANONICAL = [
    ("台北", "板橋"), ("台北", "桃園"), ("台北", "新竹"),
    ("台北", "台中"), ("台北", "嘉義"), ("台北", "高雄"),
    ("台中", "台南"), ("新竹", "台中"), ("基隆", "宜蘭"), ("台南", "高雄"),
]

JITTER = 0.02            # 約 2 km；讓起訖點落在不同節點上
N_PAIRS = 50


def haversine_km(la, loa, lb, lob):
    r = 6371.0088
    p1, p2 = math.radians(la), math.radians(lb)
    dp, dl = math.radians(lb - la), math.radians(lob - loa)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(max(0.0, a)))


def build_pairs() -> list[dict]:
    """產生 N_PAIRS 組起訖點。固定種子，每次結果相同。"""
    rng = random.Random(SEED)
    by_name = {n: (la, lo) for n, la, lo in CITIES}
    pairs, seen = [], set()

    def add(a_name, b_name):
        key = (a_name, b_name)
        if key in seen:
            return
        seen.add(key)
        ala, alo = by_name[a_name]
        bla, blo = by_name[b_name]
        s = (round(ala + rng.uniform(-JITTER, JITTER), 6),
             round(alo + rng.uniform(-JITTER, JITTER), 6))
        e = (round(bla + rng.uniform(-JITTER, JITTER), 6),
             round(blo + rng.uniform(-JITTER, JITTER), 6))
        pairs.append({"label": a_name + "→" + b_name, "start": s, "end": e,
                      "direct_km": round(haversine_km(s[0], s[1], e[0], e[1]), 2)})

    for a, b in CANONICAL:
        add(a, b)
    names = [n for n, _, _ in CITIES]
    while len(pairs) < N_PAIRS:
        a, b = rng.sample(names, 2)
        add(a, b)
    return pairs


def call_route(start, end, mode, alternatives, timeout=600):
    body = json.dumps({
        "start_lat": start[0], "start_lon": start[1],
        "end_lat": end[0], "end_lon": end[1],
        "mode": mode, "alternatives": alternatives,
    }).encode()
    req = urllib.request.Request(API + "/route", data=body,
                                 headers={"Content-Type": "application/json"})
    t0 = time.perf_counter()
    try:
        d = json.load(urllib.request.urlopen(req, timeout=timeout))
        edges = d["edges"]
        return {
            "ok": True,
            "seconds": round(time.perf_counter() - t0, 3),
            "total_km": d["total_km"], "total_min": d["total_min"],
            "num_edges": d["num_edges"],
            # 邊序列完整存太大，存雜湊即可判斷是否逐段相同。
            # 必須用 hashlib——內建 hash() 對字串是每個行程隨機化的，
            # 基準與比對是兩個不同行程，用 hash() 會永遠對不上。
            "edge_sig": hashlib.sha1(
                "|".join(e["edge_id"] for e in edges).encode()).hexdigest()[:16],
            "first_edge": edges[0]["edge_id"] if edges else None,
            "last_edge": edges[-1]["edge_id"] if edges else None,
            "start_node": d["start_node"]["node_id"],
            "end_node": d["end_node"]["node_id"],
        }
    except urllib.error.HTTPError as e:
        try:
            detail = json.loads(e.read().decode()).get("detail", "")
        except Exception:
            detail = str(e)
        return {"ok": False, "seconds": round(time.perf_counter() - t0, 3),
                "error": detail[:90]}
    except Exception as e:
        return {"ok": False, "seconds": round(time.perf_counter() - t0, 3),
                "error": type(e).__name__ + ": " + str(e)}


def server_info():
    try:
        s = json.load(urllib.request.urlopen(API + "/stats", timeout=10))
        return {"graph_nodes": s.get("graph_nodes"), "graph_edges": s.get("graph_edges"),
                "active_events": s.get("active_events"),
                "active_weather": s.get("active_weather")}
    except Exception:
        return None


def run(mode, alternatives) -> dict:
    info = server_info()
    if info is None:
        print("連不上 " + API + " —— 請先啟動伺服器：")
        print("    python -m uvicorn osm_api:app --host 127.0.0.1 --port 8000")
        sys.exit(1)
    print(f"伺服器：{info['graph_nodes']:,} 節點 / {info['graph_edges']:,} 邊"
          f"　活動事件 {info['active_events']}　活動天氣 {info['active_weather']}")

    pairs = build_pairs()
    alt_label = "開" if alternatives else "關"
    print(f"取樣 {len(pairs)} 組（種子 {SEED}）　模式 {mode}　備選 {alt_label}\n")
    rows, t_all = [], time.perf_counter()
    for i, p in enumerate(pairs, 1):
        r = call_route(p["start"], p["end"], mode, alternatives)
        row = dict(p)
        row.update(r)
        rows.append(row)
        flag = "ok  " if r["ok"] else "FAIL"
        if r["ok"]:
            extra = (f"{r['total_km']:8.2f} km {r['total_min']:7.1f} 分 "
                     f"{r['num_edges']:6,} 邊")
        else:
            extra = "  " + r["error"][:44]
        print(f"  [{i:2}/{len(pairs)}] {p['label']:<12}{p['direct_km']:>7.1f}k "
              f"{flag} {r['seconds']:>6.2f}s {extra}")
        sys.stdout.flush()
    print(f"\n完成，總耗時 {time.perf_counter() - t_all:.0f} 秒")
    return {"seed": SEED, "mode": mode, "alternatives": alternatives,
            "server": info, "rows": rows}


def pct(a, b):
    """b 相對 a 的變化百分比。"""
    return (b - a) / a * 100 if a else 0.0


def compare(base: dict, cur: dict, tol_km: float, tol_min: float) -> int:
    if base["seed"] != cur["seed"]:
        print(f"!! 種子不同（基準 {base['seed']} vs 現在 {cur['seed']}），無法比對")
        return 2
    bmap = {r["label"]: r for r in base["rows"]}
    regress, changed, identical = [], [], 0
    dk, dm = [], []

    for c in cur["rows"]:
        b = bmap.get(c["label"])
        if b is None:
            continue
        if b["ok"] and not c["ok"]:
            regress.append((c["label"], "原本成功，現在失敗：" + c.get("error", "")[:50]))
            continue
        if not b["ok"] and c["ok"]:
            changed.append((c["label"], "原本失敗，現在成功　←改善"))
            continue
        if not b["ok"] and not c["ok"]:
            identical += 1
            continue
        pk = pct(b["total_km"], c["total_km"])
        pm = pct(b["total_min"], c["total_min"])
        dk.append(abs(pk))
        dm.append(abs(pm))
        same_path = b["edge_sig"] == c["edge_sig"]
        path_label = "相同" if same_path else "不同"
        if same_path and abs(pk) < 1e-9 and abs(pm) < 1e-9:
            identical += 1
        elif abs(pk) > tol_km or abs(pm) > tol_min:
            regress.append((c["label"],
                            f"距離 {pk:+.2f}%　時間 {pm:+.2f}%　路徑{path_label}"))
        else:
            changed.append((c["label"],
                            f"距離 {pk:+.3f}%　時間 {pm:+.3f}%　路徑{path_label}"))

    n = len(cur["rows"])
    print("\n" + "=" * 74)
    print("  比對結果")
    print("=" * 74)
    print(f"  完全一致　　{identical:>3} / {n}")
    print(f"  容差內變動　{len(changed):>3} / {n}")
    print(f"  超出容差　　{len(regress):>3} / {n}    "
          f"（容差：距離 {tol_km}%　時間 {tol_min}%）")

    if dk:
        p95k = sorted(dk)[max(0, int(len(dk) * .95) - 1)]
        p95m = sorted(dm)[max(0, int(len(dm) * .95) - 1)]
        print(f"\n  距離差異 |%|　中位數 {statistics.median(dk):.3f}"
              f"　p95 {p95k:.3f}　最大 {max(dk):.3f}")
        print(f"  時間差異 |%|　中位數 {statistics.median(dm):.3f}"
              f"　p95 {p95m:.3f}　最大 {max(dm):.3f}")

    bt = sum(r["seconds"] for r in base["rows"])
    ct = sum(r["seconds"] for r in cur["rows"])
    print(f"\n  總耗時　基準 {bt:.1f}s → 現在 {ct:.1f}s　({pct(bt, ct):+.1f}%)")

    if changed:
        print(f"\n  容差內的變動（{len(changed)} 筆）")
        for lab, msg in changed[:12]:
            print(f"    {lab:<14}{msg}")
        if len(changed) > 12:
            print(f"    …另有 {len(changed) - 12} 筆")

    if regress:
        print(f"\n  !! 超出容差（{len(regress)} 筆）——逐筆列出")
        for lab, msg in regress:
            print(f"    {lab:<14}{msg}")
        return 1

    print("\n  通過。")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="路線回歸比對（需伺服器）")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--baseline", metavar="FILE", help="跑一輪並存成基準檔")
    g.add_argument("--compare", metavar="FILE", help="跑一輪並與基準檔比對")
    ap.add_argument("--mode", default="balanced",
                    choices=["fastest", "balanced", "safest"])
    ap.add_argument("--alternatives", action="store_true",
                    help="請求備選路線（預設關閉，比對用不到且慢 4 倍）")
    ap.add_argument("--tol-km", type=float, default=0.0,
                    help="距離容差百分比（預設 0 = 要求完全一致）")
    ap.add_argument("--tol-min", type=float, default=0.0,
                    help="時間容差百分比（預設 0 = 要求完全一致）")
    ap.add_argument("--out", metavar="FILE", help="--compare 時另存本次結果")
    args = ap.parse_args()

    cur = run(args.mode, args.alternatives)

    if args.baseline:
        with open(args.baseline, "w", encoding="utf-8") as f:
            json.dump(cur, f, ensure_ascii=False, indent=1)
        ok = sum(1 for r in cur["rows"] if r["ok"])
        print(f"\n基準已存：{args.baseline}　成功 {ok}/{len(cur['rows'])} 條")
        return 0

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(cur, f, ensure_ascii=False, indent=1)
    with open(args.compare, encoding="utf-8") as f:
        base = json.load(f)
    return compare(base, cur, args.tol_km, args.tol_min)


if __name__ == "__main__":
    sys.exit(main())
