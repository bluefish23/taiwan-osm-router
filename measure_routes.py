#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
measure_routes.py — 路徑搜尋成本量測（需要伺服器已啟動）

analyze_complexity.py 量的是資料庫那一側（唯讀，不需伺服器）；
這一支量的是路由那一側，必須先啟動伺服器：

    python -m uvicorn osm_api:app --host 127.0.0.1 --port 8000

量兩件事：
  1. 距離 vs 搜尋耗時（單條路線）
  2. alternatives=true 的真實成本
     ——前端 index.html:552 永遠帶 alternatives:true，而後端
       route_alternatives() 是「跑 n 次完整 A*，每次把前面走過的邊加價」，
       所以使用者實際等待的時間並不是單條路線的耗時。

本腳本只送 POST /route（唯讀操作，不建立事件、不寫 DB）。

用法：
    python measure_routes.py                # 全部
    python measure_routes.py --max-km 200   # 跳過會失敗的長程
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
import urllib.error
import urllib.request

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

API = "http://127.0.0.1:8000"

# (標籤, 起點 lat/lon, 終點 lat/lon)。沿用先前分析用的六個量測點。
ROUTES = [
    ("台北 → 板橋", (25.047, 121.517), (25.011, 121.459)),
    ("台北 → 桃園", (25.047, 121.517), (24.993, 121.301)),
    ("台北 → 新竹", (25.047, 121.517), (24.804, 120.969)),
    ("台北 → 台中", (25.047, 121.517), (24.147, 120.673)),
    ("台北 → 嘉義", (25.047, 121.517), (23.480, 120.449)),
    ("台北 → 高雄", (25.047, 121.517), (22.627, 120.301)),
]


def haversine_km(la, loa, lb, lob):
    r = 6371.0088
    p1, p2 = math.radians(la), math.radians(lb)
    dp, dl = math.radians(lb - la), math.radians(lob - loa)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(max(0.0, a)))


def call_route(start, end, mode="balanced", alternatives=False, timeout=600):
    """回傳 (耗時秒, 結果 dict 或 None, 錯誤訊息或 None)。"""
    body = json.dumps({
        "start_lat": start[0], "start_lon": start[1],
        "end_lat": end[0], "end_lon": end[1],
        "mode": mode, "alternatives": alternatives,
    }).encode()
    req = urllib.request.Request(
        f"{API}/route", data=body, headers={"Content-Type": "application/json"})
    t0 = time.perf_counter()
    try:
        data = json.load(urllib.request.urlopen(req, timeout=timeout))
        return time.perf_counter() - t0, data, None
    except urllib.error.HTTPError as e:
        elapsed = time.perf_counter() - t0
        try:
            detail = json.loads(e.read().decode()).get("detail", str(e))
        except Exception:
            detail = str(e)
        return elapsed, None, detail
    except Exception as e:                       # 連線失敗、逾時
        return time.perf_counter() - t0, None, f"{type(e).__name__}: {e}"


def check_server():
    try:
        s = json.load(urllib.request.urlopen(f"{API}/stats", timeout=10))
        print(f"伺服器就緒：{s['graph_nodes']:,} 節點 / {s['graph_edges']:,} 邊"
              f"　活動事件 {s.get('active_events', '?')}"
              f"　活動天氣 {s.get('active_weather', '?')}")
        return True
    except Exception as e:
        print(f"連不上 {API} —— 請先啟動伺服器（{type(e).__name__}）")
        print("    python -m uvicorn osm_api:app --host 127.0.0.1 --port 8000")
        return False


def main() -> int:
    ap = argparse.ArgumentParser(description="路徑搜尋成本量測（需伺服器）")
    ap.add_argument("--max-km", type=float, default=1e9,
                    help="跳過直線距離超過此值的路線")
    ap.add_argument("--mode", default="balanced",
                    choices=["fastest", "balanced", "safest"])
    args = ap.parse_args()

    print("=" * 78)
    print("  路徑搜尋成本量測")
    print("=" * 78)
    if not check_server():
        return 1

    print(f"\n模式 {args.mode}　·　單次量測，未取多次平均\n")
    print(f"  {'路線':<14}{'直線':>8}{'單條':>10}{'含備選':>10}"
          f"{'備選數':>8}{'倍數':>8}  結果")
    print("  " + "-" * 74)

    rows = []
    for label, start, end in ROUTES:
        direct = haversine_km(start[0], start[1], end[0], end[1])
        if direct > args.max_km:
            continue

        # ① 單條路線
        t_one, one, err_one = call_route(start, end, args.mode, alternatives=False)
        # ② 含備選（前端實際送的請求）
        t_alt, alt, err_alt = call_route(start, end, args.mode, alternatives=True)

        n_alt = len(alt.get("alternatives", [])) + 1 if alt else 0
        ratio = (t_alt / t_one) if (one and alt and t_one > 0) else None

        if one and alt:
            status = "成功"
        elif not one and not alt:
            status = f"兩者皆失敗：{(err_one or '')[:26]}"
        else:
            status = "單條成功／含備選失敗" if one else "單條失敗／含備選成功"

        print(f"  {label:<14}{direct:>7.0f}k{t_one:>9.2f}s{t_alt:>9.2f}s"
              f"{n_alt:>8}{(f'{ratio:.1f}x' if ratio else '—'):>8}  {status}")
        sys.stdout.flush()

        rows.append({"label": label, "direct_km": round(direct, 1),
                     "single_s": round(t_one, 2), "with_alt_s": round(t_alt, 2),
                     "n_routes": n_alt,
                     "ratio": round(ratio, 2) if ratio else None,
                     "single_ok": one is not None, "alt_ok": alt is not None,
                     "error": err_one or err_alt})

    ok = [r for r in rows if r["ratio"]]
    if ok:
        avg = sum(r["ratio"] for r in ok) / len(ok)
        print(f"\n  含備選 / 單條 的耗時比：平均 {avg:.1f}x"
              f"（{len(ok)} 條成功的路線）")
        worst = max(ok, key=lambda r: r["with_alt_s"])
        print(f"  最慢一筆：{worst['label']} {worst['with_alt_s']:.1f} 秒"
              f"（單條 {worst['single_s']:.1f} 秒）")

    print("\n說明：前端 index.html:552 永遠送 alternatives:true，")
    print("      所以「含備選」那一欄才是使用者實際等待的時間。")
    print("      後端 route_alternatives() 會跑 n 次完整 A*，")
    print("      每次把前次走過的邊乘上 1.6 的懲罰再重搜。")

    print("\n" + json.dumps(rows, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
