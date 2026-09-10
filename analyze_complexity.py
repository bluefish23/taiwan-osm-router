#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyze_complexity.py — 現行動態路網演算法的時間複雜度量測（唯讀）

背景：教授質疑「事件更新後的計算是指數級，事件多了會算不完」，要求把地圖切割
以節省計算成本。本腳本量測現行演算法的實際成本，用來回答這個質疑。

結論（見輸出）：不是指數，是「線性、但常數等於全國路網」——每次更新的成本
與改動大小無關，只與整張圖大小綁定。這正是計畫書 p.1 描述的問題。

本腳本**完全唯讀**：DB 一律以 file:...?mode=ro 開啟，不執行任何 INSERT/UPDATE/
DELETE，也不修改 osm_router.py / osm_api.py 的任何行為。可安全在正式 DB 上重跑。

用法：
    python analyze_complexity.py                 # 全部六組量測
    python analyze_complexity.py --skip-scan     # 跳過最慢的全表掃描（第 4 組）
    python analyze_complexity.py --db other.db
"""
from __future__ import annotations

import argparse
import collections
import math
import sqlite3
import sys
import time

from osm_router import MAJOR_HW          # 只讀常數，不動既有邏輯

# 主控台在 Windows 預設 cp950，中文與框線字元會炸掉
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

DEG_LAT_KM = 110.574                     # 與 osm_router 的換算常數一致
DEG_LON_KM = 111.320

# 事件情境：(標籤, lat, lon, 半徑km)。座標取三大都會 + 一個山區點，
# 半徑沿用 osm_router.BASE_RADIUS 的量級（accident 0.8、closure 1.5）。
EVENT_CASES = [
    ("台北 accident r=1km", 25.047, 121.517, 1.0),
    ("台中 accident r=1km", 24.147, 120.673, 1.0),
    ("高雄 accident r=1km", 22.627, 120.301, 1.0),
    ("山區 closure  r=2km", 24.500, 121.200, 2.0),
    ("天氣 r=30km", 24.147, 120.673, 30.0),
    ("天氣 r=50km", 24.147, 120.673, 50.0),
]

PROBE_LAT, PROBE_LON = 24.147, 120.673   # _predict_impact 拆解用的取樣點（台中）

# 本機開機後第一次全表掃描的實測秒數（2.5 GB DB）。腳本重跑時 OS 頁面快取
# 通常已暖，量到的會是 1~2 秒，故保留此參考值對照，避免把暖快取誤報為冷快取。
COLD_SCAN_REFERENCE_S = 34.4


def bbox(lat: float, lon: float, radius_km: float) -> tuple[float, float, float, float]:
    """與 osm_router._predict_impact / recompute_dynamic_cost 相同的 bbox 換算。"""
    dlat = radius_km / DEG_LAT_KM
    dlon = radius_km / (DEG_LON_KM * math.cos(math.radians(lat)))
    return lat - dlat, lat + dlat, lon - dlon, lon + dlon


def connect(db_path: str) -> sqlite3.Connection:
    """唯讀連線。mode=ro 讓任何寫入嘗試直接報錯，確保腳本不可能污染正式 DB。"""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA cache_size=-262144")     # 256 MB，讓量測不被預設快取綁住
    return conn


def head(n: int, title: str) -> None:
    print(f"\n{'=' * 74}\n[{n}] {title}\n{'=' * 74}")


# ── 1. 資料規模 ────────────────────────────────────────────────────
def measure_scale(conn: sqlite3.Connection) -> dict:
    head(1, "資料規模")
    edges = conn.execute("SELECT COUNT(*) FROM osm_edges").fetchone()[0]
    nodes = conn.execute("SELECT COUNT(*) FROM osm_nodes").fetchone()[0]
    avg_km, min_km, max_km = conn.execute(
        "SELECT AVG(length_km), MIN(length_km), MAX(length_km) FROM osm_edges"
    ).fetchone()

    ph = ",".join("?" * len(MAJOR_HW))
    main_edges = conn.execute(
        f"SELECT COUNT(*) FROM osm_edges WHERE highway IN ({ph}) AND component_id=0",
        MAJOR_HW,
    ).fetchone()[0]

    print(f"  DB 總邊數  N          = {edges:,}")
    print(f"  DB 總節點數 V         = {nodes:,}")
    print(f"  記憶體主幹圖邊數 N_mem = {main_edges:,}")
    print(f"  平均邊長              = {avg_km * 1000:.0f} m"
          f"   (最短 {min_km * 1000:.1f} m / 最長 {max_km:.1f} km)")
    print("\n  → 平均 34 公尺一條邊：圖的粒度遠細於「一條路」，")
    print("    搜尋與更新都在沒有語意的粒度上進行。")
    return {"edges": edges, "nodes": nodes, "main_edges": main_edges}


# ── 2. 索引與查詢計畫 ──────────────────────────────────────────────
def measure_query_plan(conn: sqlite3.Connection) -> None:
    head(2, "索引與查詢計畫 — 證明空間索引只用得到一半")
    print("  osm_edges 上的索引：")
    for name, sql in conn.execute(
        "SELECT name, sql FROM sqlite_master "
        "WHERE type='index' AND tbl_name='osm_edges' ORDER BY name"
    ):
        print(f"    {name:<28}{sql or '(自動索引)'}")

    lo_la, hi_la, lo_lo, hi_lo = bbox(PROBE_LAT, PROBE_LON, 1.0)
    print("\n  事件 bbox 查詢的計畫（recompute / apply_event_incremental 都用這個形狀）：")
    for row in conn.execute(
        "EXPLAIN QUERY PLAN SELECT edge_id FROM osm_edges "
        "WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ?",
        (lo_la, hi_la, lo_lo, hi_lo),
    ):
        print(f"    {row[-1]}")
    print("    ↑ 只有 lat_a 進得了索引範圍，lon_a 退化成逐筆過濾")

    print("\n  全圖重算「清除」步驟的計畫：")
    for row in conn.execute(
        "EXPLAIN QUERY PLAN SELECT edge_id FROM osm_edges "
        "WHERE dynamic_mult!=1.0 OR closure_flag!=0 OR risk_score!=0.0"
    ):
        print(f"    {row[-1]}")
    print("    ↑ SCAN = 全表掃描，沒有任何索引可用")


# ── 3. 緯度帶浪費比 ────────────────────────────────────────────────
def measure_band_waste(conn: sqlite3.Connection) -> list[dict]:
    head(3, "緯度帶浪費比 — 每個事件實際掃了多少、真正用到多少")
    print(f"  {'情境':<22}{'掃描筆數':>12}{'實際命中':>11}{'浪費':>9}{'耗時':>9}")
    print(f"  {'-' * 63}")
    rows = []
    for label, lat, lon, radius in EVENT_CASES:
        lo_la, hi_la, lo_lo, hi_lo = bbox(lat, lon, radius)
        t0 = time.perf_counter()
        hit = conn.execute(
            "SELECT COUNT(*) FROM osm_edges "
            "WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ?",
            (lo_la, hi_la, lo_lo, hi_lo),
        ).fetchone()[0]
        elapsed = time.perf_counter() - t0
        # 索引實際走訪的筆數 = 純 lat 範圍能命中的列數（lon 只是事後過濾）
        scanned = conn.execute(
            "SELECT COUNT(*) FROM osm_edges WHERE lat_a BETWEEN ? AND ?",
            (lo_la, hi_la),
        ).fetchone()[0]
        waste = f"{scanned / hit:.1f}x" if hit else "全白工"
        print(f"  {label:<22}{scanned:>12,}{hit:>11,}{waste:>9}{elapsed * 1000:>8.0f}ms")
        rows.append({"label": label, "scanned": scanned, "hit": hit})

    print("\n  → 事件半徑越小，浪費越嚴重：索引把「橫貫台灣的整條緯度帶」都走過一遍，")
    print("    成本與事件的實際影響範圍無關。")
    return rows


# ── 4. 全表掃描成本 ────────────────────────────────────────────────
def measure_full_scan(conn: sqlite3.Connection) -> float:
    head(4, "全表掃描成本 — 全圖重算的 Θ(N) 地板")
    print("  recompute_dynamic_cost() 一次會做兩趟無索引全表掃描：")
    print("    (a) 清除    UPDATE ... WHERE dynamic_mult!=1.0 OR closure_flag!=0 OR risk_score!=0.0")
    print("    (b) 讀回    SELECT   ... WHERE <同上>   (_apply_dynamic_to_graph)")
    print("\n  量測中...", flush=True)

    t0 = time.perf_counter()
    n = conn.execute(
        "SELECT COUNT(*) FROM osm_edges "
        "WHERE dynamic_mult!=1.0 OR closure_flag!=0 OR risk_score!=0.0"
    ).fetchone()[0]
    first = time.perf_counter() - t0

    t0 = time.perf_counter()
    conn.execute(
        "SELECT COUNT(*) FROM osm_edges "
        "WHERE dynamic_mult!=1.0 OR closure_flag!=0 OR risk_score!=0.0"
    ).fetchone()
    second = time.perf_counter() - t0

    ref = COLD_SCAN_REFERENCE_S
    print(f"\n  第一趟掃描   = {first:6.2f} s      命中 {n:,} 列")
    print(f"  第二趟掃描   = {second:6.2f} s")
    print(f"  全圖重算地板 = {first * 2:6.2f} s  （兩趟，事件數 = 0 時也要付）")
    print("\n  ※ 這兩個數字取決於 OS 頁面快取。2.5 GB 的 DB 若已被快取會量到 1~2 秒；")
    print(f"    真正冷快取（開機後第一次）本機實測 {ref:.1f} s/趟，即地板約 {ref * 2:.0f} 秒。")
    print("    要重現冷快取需重開機或清 standby list。")
    print("\n  → 不論冷暖，這個成本與「改了幾個事件」完全無關。事件 0 個也是這個價。")
    return first


# ── 5. _predict_impact 拆解 ────────────────────────────────────────
def measure_predict_impact(conn: sqlite3.Connection) -> None:
    head(5, "apply_event_incremental 逐步拆解（單一事件的成本組成）")
    lat, lon = PROBE_LAT, PROBE_LON
    total = 0.0

    # κ：1 km 內的邊數 → 替代道路密度
    lo_la, hi_la, lo_lo, hi_lo = bbox(lat, lon, 1.0)
    t0 = time.perf_counter()
    k_edges = conn.execute(
        "SELECT COUNT(*) FROM osm_edges "
        "WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ?",
        (lo_la, hi_la, lo_lo, hi_lo),
    ).fetchone()[0]
    t_kappa = time.perf_counter() - t0
    total += t_kappa

    # ω：200 m 內優先權最高的道路類型 → 道路重要性
    lo2_la, hi2_la, lo2_lo, hi2_lo = bbox(lat, lon, 0.2)
    t0 = time.perf_counter()
    conn.execute(
        "SELECT highway FROM osm_edges "
        "WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ? "
        "ORDER BY CASE highway WHEN 'motorway' THEN 1 WHEN 'trunk' THEN 2 "
        "WHEN 'primary' THEN 3 WHEN 'secondary' THEN 4 ELSE 5 END LIMIT 1",
        (lo2_la, hi2_la, lo2_lo, hi2_lo),
    ).fetchone()
    t_omega = time.perf_counter() - t0
    total += t_omega

    # δ：nearest_node 的第一圈（±0.005°）。每個候選節點各發一次相關子查詢。
    t0 = time.perf_counter()
    cands = conn.execute(
        "SELECT n.node_id, n.lat, n.lon, "
        "  (SELECT e.highway FROM osm_edges e WHERE e.from_node=n.node_id LIMIT 1) AS hw "
        "FROM osm_nodes n "
        "WHERE n.lat BETWEEN ? AND ? AND n.lon BETWEEN ? AND ?",
        (lat - 0.005, lat + 0.005, lon - 0.005, lon + 0.005),
    ).fetchall()
    t_delta = time.perf_counter() - t0
    total += t_delta

    # 取得要改的 edge_id 清單
    t0 = time.perf_counter()
    edge_ids = conn.execute(
        "SELECT edge_id FROM osm_edges "
        "WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ?",
        (lo_la, hi_la, lo_lo, hi_lo),
    ).fetchall()
    t_select = time.perf_counter() - t0
    total += t_select

    print(f"  取樣點：台中 ({lat}, {lon})，accident r=1km\n")
    print(f"  {'步驟':<34}{'複雜度':<14}{'耗時':>9}")
    print(f"  {'-' * 57}")
    print(f"  {'κ 替代道路密度 (1km COUNT)':<34}{'Θ(B)':<14}{t_kappa * 1000:>8.1f}ms")
    print(f"  {'ω 道路重要性 (200m probe)':<34}{'Θ(B′)':<14}{t_omega * 1000:>8.1f}ms")
    print(f"  {'δ 節點度數 (nearest_node)':<34}{'Θ(P)+P 次子查詢':<14}{t_delta * 1000:>8.1f}ms")
    print(f"  {'取得受影響 edge_id':<34}{'Θ(B)':<14}{t_select * 1000:>8.1f}ms")
    print(f"  {'-' * 57}")
    print(f"  {'DB 部分合計':<34}{'Θ(B)':<14}{total * 1000:>8.1f}ms")
    print(f"\n  δ 的候選節點數 P = {len(cands):,}（每個都發一次 osm_edges 子查詢）")
    print(f"  受影響邊數 k     = {len(edge_ids):,}（記憶體套用是 Θ(k)，約 2ms）")
    print(f"  1km 內總邊數     = {k_edges:,}（κ 的分子）")
    print("\n  → 單一事件 ≈ Θ(B + k)，E 個事件 ≈ Θ(E·(B+k))：線性，不是指數。")
    print("    但 B 綁在全國路網上，所以常數很大。最大單項是 nearest_node。")


# ── 6. 核心節點定義對比 ────────────────────────────────────────────
def measure_core_definition(conn: sqlite3.Connection) -> None:
    head(6, "計畫書「核心節點 = out-degree > 1」在路網上的實測")
    ph = ",".join("?" * len(MAJOR_HW))
    sql = (f"SELECT from_node, to_node FROM osm_edges "
           f"WHERE highway IN ({ph}) AND component_id=0")

    out_deg: collections.Counter = collections.Counter()
    undirected: dict[int, set[int]] = collections.defaultdict(set)
    directed: set[tuple[int, int]] = set()

    t0 = time.perf_counter()
    for fn, tn in conn.execute(sql, MAJOR_HW):
        out_deg[fn] += 1
        undirected[fn].add(tn)
        undirected[tn].add(fn)
        directed.add((fn, tn))
    load_s = time.perf_counter() - t0

    reciprocal = sum(1 for a, b in directed if (b, a) in directed)
    print(f"  主幹路網載入完成（{load_s:.1f}s）：{len(directed):,} 條有向邊")
    print(f"  其中互為反向的邊：{reciprocal:,} "
          f"({reciprocal / max(len(directed), 1):.1%}) → 路網以雙向存放\n")

    # (A) 計畫書原文規則
    n_src = len(out_deg)
    cores = sum(1 for d in out_deg.values() if d > 1)
    print("  (A) 計畫書 p.2 原文：核心節點 = out-degree > 1")
    print(f"      核心節點  {cores:>9,} / {n_src:,}  = {cores / max(n_src, 1):.1%}")
    print(f"      平均子圖大小 {n_src / max(cores, 1):.1f} 個節點  ← 等於沒切")
    print("      原因：雙向存放讓路中間的形狀點 out-degree 天生就是 2")

    # (B) 路網的自然對應
    hist = collections.Counter(len(v) for v in undirected.values())
    n_all = len(undirected)
    junctions = sum(c for d, c in hist.items() if d >= 3)
    shape_pts = hist.get(2, 0)
    print("\n  (B) 路網的自然對應：無向度數 >= 3（真正的路口）")
    print(f"      {'無向度數':<12}{'節點數':>12}{'佔比':>9}")
    for d in sorted(hist)[:7]:
        print(f"      {d:<12}{hist[d]:>12,}{hist[d] / n_all:>8.1%}")
    print(f"\n      形狀點 (deg=2)  {shape_pts:>9,}  ({shape_pts / n_all:.1%})")
    print(f"      路口   (deg>=3) {junctions:>9,}  ({junctions / n_all:.1%})")

    n_edges = conn.execute(
        f"SELECT COUNT(*) FROM osm_edges WHERE highway IN ({ph}) AND component_id=0",
        MAJOR_HW,
    ).fetchone()[0]
    print(f"\n  → 以路口切段：{n_all:,} 節點壓成 {junctions:,}"
          f"（{n_all / max(junctions, 1):.0f}x）")
    print(f"                 {n_edges:,} 邊壓成約 {junctions:,} 條路段邊"
          f"（{n_edges / max(junctions, 1):.0f}x）")
    print("    每個子圖有自然語意 = 兩個路口之間的一條路段。")
    print("\n  ※ 兩案並陳，選哪一個需與教授確認。")


def summarise(scale: dict, cold_scan: float | None) -> None:
    head(0, "總結")
    print("  路徑                          複雜度                        隨事件數")
    print("  " + "-" * 72)
    print("  A 新增事件 (增量套用)         Θ(B + k) / 事件               線性")
    print("  B 全圖重算                    2·Θ(N) + Θ(ΣB) + Θ(ΣN_w)     線性，但有 Θ(N) 地板")
    print("  C A* 搜尋                     O(N_mem log N_mem)            常數惡化")
    print("  D /route 回應組裝             Θ(E · L)                      乘積成長 ←唯一")
    print()
    print("  結論：不是指數。是「線性、但常數等於全國路網」——")
    print(f"        每次更新都與 N={scale['edges']:,} 條邊綁在一起，")
    print("        而不是與「這次改了什麼」綁在一起。")
    if cold_scan is not None:
        print(f"        全圖重算的地板 ≈ {cold_scan * 2:.0f} 秒（本次執行，快取已暖）／"
              f"{COLD_SCAN_REFERENCE_S * 2:.0f} 秒（冷快取），事件 0 個也要付。")
    print()
    print("  這正是計畫書 p.1 指出的問題：節點狀態沿傳播鏈更新，")
    print("  成本與圖大小綁定 —— 也正是子圖分割要解決的。")


def main() -> None:
    ap = argparse.ArgumentParser(description="動態路網演算法時間複雜度量測（唯讀）")
    ap.add_argument("--db", default="taiwan_osm.db", help="SQLite 路網資料庫")
    ap.add_argument("--skip-scan", action="store_true",
                    help="跳過第 4 組全表掃描（最慢，冷快取約 35 秒）")
    args = ap.parse_args()

    print("=" * 74)
    print("  現行動態路網演算法 — 時間複雜度量測")
    print(f"  DB: {args.db}   （唯讀連線，不會寫入任何資料）")
    print("=" * 74)

    conn = connect(args.db)
    try:
        scale = measure_scale(conn)
        measure_query_plan(conn)
        measure_band_waste(conn)
        cold = None if args.skip_scan else measure_full_scan(conn)
        measure_predict_impact(conn)
        measure_core_definition(conn)
        summarise(scale, cold)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
