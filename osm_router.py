#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
osm_router.py — 台灣 OSM 動態路由引擎

核心設計：
  - 啟動時預載主幹路網（motorway→tertiary）到記憶體
  - 短程（<15 km）另外查局部全類型路網
  - A*（haversine 啟發式）搜尋
  - 動態因子（天氣/事件）以乘數套用在邊成本
  - OSM 原生 node_id 做交叉路口 → 道路類型天然互通
"""
from __future__ import annotations

import collections
import logging
import threading
import gc
import heapq
import itertools
import math
import sqlite3
import uuid
from collections import namedtuple
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

Edge = namedtuple('Edge', ['to', 'eid', 'hw', 'name', 'dist', 'spd', 'cost', 'risk', 'la', 'loa', 'lb', 'lob'])

HIGHWAY_SPEEDS: dict[str, float] = {
    "motorway":110,"motorway_link":70,"trunk":90,"trunk_link":60,
    "primary":60,"primary_link":45,"secondary":50,"secondary_link":40,
    "tertiary":40,"tertiary_link":30,"unclassified":30,"residential":30,
    "living_street":10,"service":20,"road":30,
}

MAJOR_HW = (
    "motorway","motorway_link","trunk","trunk_link",
    "primary","primary_link","secondary","secondary_link",
    "tertiary","tertiary_link",
)
LOCAL_HW = MAJOR_HW + ("unclassified","residential","living_street","road")

MODE_WEIGHTS = {
    "fastest":  {"time":1.0,"risk":0.10},
    "balanced": {"time":1.0,"risk":0.40},
    "safest":   {"time":1.0,"risk":0.90},
}
INCIDENT_MULT = {
    "accident":1.80,"construction":1.55,"closure":999.0,"congestion":1.35,"manual":1.25,
    "landslide_closure":999.0,"landslide_high":3.0,"landslide_warning":1.5,
    # T-GCN 預測壅塞：severity = (自由流速/預測速)/3.5，故 sev×3.5 = 實際時間乘數
    "predicted_congestion":3.5,
}
RAIN_M,WIND_M,VIS_M,WARN_M = 0.40,0.20,0.35,0.30
VALID_HIGHWAYS = {"motorway","motorway_link","trunk","trunk_link","primary","primary_link",
    "secondary","secondary_link","tertiary","tertiary_link","unclassified","residential",
    "living_street","service","road"}
MIN_SPEED = 3.0

ROAD_CRITICALITY = {
    "motorway":1.0,"motorway_link":0.9,"trunk":0.85,"trunk_link":0.7,
    "primary":0.65,"primary_link":0.55,"secondary":0.45,"secondary_link":0.35,
    "tertiary":0.30,"tertiary_link":0.25,"residential":0.15,"service":0.10,
    "unclassified":0.20,"living_street":0.10,"road":0.20,
}
BASE_RADIUS = {
    "accident":0.8,"construction":0.5,"congestion":1.0,"closure":1.5,
    "landslide_warning":1.0,"landslide_high":1.5,"landslide_closure":2.0,"manual":0.5,
}
RISK_ADDITION = {"accident":8,"construction":5,"congestion":3,"manual":2,
                 "landslide_warning":6,"landslide_high":10,
                 "predicted_congestion":2}   # 預測壅塞以時間為主，risk 佔比低
BASE_SEVERITY = {
    "accident":0.80,"construction":0.50,"closure":1.00,"congestion":0.60,
    "manual":0.50,"landslide_warning":0.60,"landslide_high":0.85,"landslide_closure":1.00,
}
MAX_SPEED = 120.0
SHORT_DIST_KM = 15.0
_CTR = itertools.count()

NIGHT_RISK = {
    (0,4): 0.25, (4,6): 0.15, (6,7): 0.05,
    (7,17): 0.0, (17,19): 0.05, (19,24): 0.15,
}

_TW_UTC_OFFSET = timedelta(hours=8)

def _tw_hour() -> int:
    return (datetime.now(timezone.utc) + _TW_UTC_OFFSET).hour

def _night_risk_score() -> float:
    h = _tw_hour()
    for (lo, hi), risk in NIGHT_RISK.items():
        if lo <= h < hi:
            return risk
    return 0.0

TIME_SPEED_MULT: dict[str, dict[tuple[int,int], float]] = {
    "motorway":     {(0,6):1.0,(6,7):0.95,(7,9):0.85,(9,17):0.92,(17,19):0.85,(19,22):0.95,(22,24):1.0},
    "motorway_link":{(0,6):1.0,(6,7):0.95,(7,9):0.85,(9,17):0.92,(17,19):0.85,(19,22):0.95,(22,24):1.0},
    "trunk":        {(0,6):1.0,(6,7):0.92,(7,9):0.80,(9,17):0.88,(17,19):0.80,(19,22):0.93,(22,24):1.0},
    "trunk_link":   {(0,6):1.0,(6,7):0.92,(7,9):0.80,(9,17):0.88,(17,19):0.80,(19,22):0.93,(22,24):1.0},
    "primary":      {(0,6):1.0,(6,7):0.88,(7,9):0.70,(9,17):0.82,(17,19):0.70,(19,22):0.90,(22,24):1.0},
    "primary_link": {(0,6):1.0,(6,7):0.88,(7,9):0.70,(9,17):0.82,(17,19):0.70,(19,22):0.90,(22,24):1.0},
    "secondary":    {(0,6):1.0,(6,7):0.85,(7,9):0.65,(9,17):0.78,(17,19):0.65,(19,22):0.88,(22,24):1.0},
    "secondary_link":{(0,6):1.0,(6,7):0.85,(7,9):0.65,(9,17):0.78,(17,19):0.65,(19,22):0.88,(22,24):1.0},
    "tertiary":     {(0,6):1.0,(6,7):0.82,(7,9):0.62,(9,17):0.75,(17,19):0.62,(19,22):0.85,(22,24):1.0},
    "tertiary_link":{(0,6):1.0,(6,7):0.82,(7,9):0.62,(9,17):0.75,(17,19):0.62,(19,22):0.85,(22,24):1.0},
}
_DEFAULT_TIME_MULT = {(0,6):1.0,(6,7):0.85,(7,9):0.60,(9,17):0.75,(17,19):0.60,(19,22):0.88,(22,24):1.0}

def _time_speed_factor(highway: str) -> float:
    h = _tw_hour()
    table = TIME_SPEED_MULT.get(highway, _DEFAULT_TIME_MULT)
    for (lo, hi), m in table.items():
        if lo <= h < hi:
            return m
    return 1.0

TURN_PENALTY_MIN = {
    "straight": 0.0, "slight": 0.05, "turn": 0.17, "sharp": 0.25, "uturn": 0.42,
}
SIGNAL_DELAY_MIN = 0.33


def utc_now(): return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def haversine_km(la,loa,lb,lob):
    R=6371.0088; p1,p2=math.radians(la),math.radians(lb)
    dp,dl=math.radians(lb-la),math.radians(lob-loa)
    a=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2*R*math.asin(math.sqrt(max(0.0,a)))

def _bearing(la,loa,lb,lob):
    dlon=math.radians(lob-loa)
    y=math.sin(dlon)*math.cos(math.radians(lb))
    x=math.cos(math.radians(la))*math.sin(math.radians(lb))-math.sin(math.radians(la))*math.cos(math.radians(lb))*math.cos(dlon)
    return math.degrees(math.atan2(y,x))%360

def _turn_type(angle_diff: float) -> str:
    a = abs(angle_diff)
    if a < 20: return "straight"
    if a < 60: return "slight"
    if a < 130: return "turn"
    if a < 170: return "sharp"
    return "uturn"

_log = logging.getLogger(__name__)

def _conn(db_path):
    c=sqlite3.connect(db_path,timeout=30,check_same_thread=False)
    c.row_factory=sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.execute("PRAGMA cache_size=-65536")
    return c


class RouteResult:
    def __init__(self,nodes,edges,total_km,total_min,total_cost,mode):
        self.nodes=nodes; self.edges=edges
        self.total_km=total_km; self.total_min=total_min
        self.total_cost=total_cost; self.mode=mode


class OSMRouter:
    def __init__(self, db_path: str):
        if not Path(db_path).exists():
            raise FileNotFoundError(f"找不到 DB：{db_path}")
        self.db_path = db_path
        self._graph: Dict[int,List[dict]] = {}
        self._ncoords: Dict[int,Tuple[float,float]] = {}
        self._graph_loaded = False
        self._graph_node_count = 0
        self._graph_edge_count = 0
        self._osm_node_count = 0
        self._osm_edge_count = 0
        self._edge_index: Dict[str, Tuple[int, int]] = {}
        self._modified_edges: set = set()
        self._signal_nodes: set = set()
        self._base_graph_nodes: frozenset = frozenset()
        self._local_cells: set = set()   # 已合併局部圖的網格快取
        self._graph_lock = threading.RLock()

    def init_dynamic_schema(self):
        with _conn(self.db_path) as conn:
            cur=conn.cursor()
            existing={r["name"] for r in cur.execute("PRAGMA table_info(osm_edges)").fetchall()}
            for col,ddl in {"dynamic_mult":"REAL DEFAULT 1.0","closure_flag":"INTEGER DEFAULT 0",
                            "risk_score":"REAL DEFAULT 0.0"}.items():
                if col not in existing:
                    cur.execute(f"ALTER TABLE osm_edges ADD COLUMN {col} {ddl}")
            cur.execute("""CREATE TABLE IF NOT EXISTS dynamic_events(
                event_id TEXT PRIMARY KEY,event_type TEXT,severity REAL,highway TEXT,
                lat REAL,lon REAL,radius_km REAL DEFAULT 0.5,
                description TEXT,is_active INTEGER DEFAULT 1,created_at TEXT,
                source TEXT DEFAULT 'manual')""")
            cur.execute("""CREATE TABLE IF NOT EXISTS dynamic_weather(
                weather_id TEXT PRIMARY KEY,lat REAL,lon REAL,radius_km REAL DEFAULT 50,
                rain_level REAL DEFAULT 0,wind_level REAL DEFAULT 0,
                visibility_level REAL DEFAULT 0,warning_level REAL DEFAULT 0,
                is_active INTEGER DEFAULT 1,updated_at TEXT,
                source TEXT DEFAULT 'manual')""")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_edges_latlon ON osm_edges(lat_a, lon_a)")
            conn.commit()

    def ensure_component_ids(self, verbose=True):
        if self._has_component_id():
            return
        import time; t0=time.time()
        if verbose: print("計算連通分量（一次性）...", flush=True)
        with _conn(self.db_path) as conn:
            conn.execute("ALTER TABLE osm_edges ADD COLUMN component_id INTEGER DEFAULT -1")
            conn.commit()
            hw_ph=",".join("?"*len(MAJOR_HW))
            adj: dict[int, set[int]] = {}
            for r in conn.execute(f"SELECT from_node,to_node FROM osm_edges WHERE highway IN ({hw_ph})", MAJOR_HW):
                fn, tn = r["from_node"], r["to_node"]
                adj.setdefault(fn, set()).add(tn)
                adj.setdefault(tn, set()).add(fn)
            best_cc: set[int] = set()
            visited: set[int] = set()
            comp_id = 0
            components: list[tuple[int, set[int]]] = []
            for seed in adj:
                if seed in visited: continue
                queue = collections.deque([seed])
                cc: set[int] = set()
                while queue:
                    n = queue.popleft()
                    if n in cc: continue
                    cc.add(n)
                    for nb in adj.get(n, ()):
                        if nb not in cc: queue.append(nb)
                visited |= cc
                components.append((comp_id, cc))
                if len(cc) > len(best_cc):
                    best_cc = cc
                    best_id = comp_id
                comp_id += 1
            del adj, visited
            for cid, cc in components:
                final_id = 0 if cid == best_id else cid
                nodes = list(cc)
                for i in range(0, len(nodes), 500):
                    batch = nodes[i:i+500]
                    ph = ",".join("?" * len(batch))
                    conn.execute(f"UPDATE osm_edges SET component_id=? WHERE from_node IN ({ph})", [final_id] + batch)
            conn.commit()
            conn.execute("CREATE INDEX IF NOT EXISTS idx_comp ON osm_edges(component_id)")
            conn.commit()
        if verbose:
            print(f"  {len(components)} 分量，最大 {len(best_cc):,} 節點  ({time.time()-t0:.1f}s)", flush=True)

    @staticmethod
    def _build_edge(r):
        fn, tn = r["from_node"], r["to_node"]
        dist = float(r["length_km"] or 0)
        spd = max(float(r["speed_kmh"] or 30) / max(float(r["mult"] or 1), 0.01), MIN_SPEED)
        la, loa = float(r["lat_a"] or 0), float(r["lon_a"] or 0)
        lb, lob = float(r["lat_b"] or 0), float(r["lon_b"] or 0)
        raw_spd = float(r["speed_kmh"] or 30)
        rec = Edge(tn, r["edge_id"], r["highway"], r["name"] or "",
                   dist, raw_spd, (dist / spd) * 60, float(r["risk"] or 0),
                   la, loa, lb, lob)
        return fn, tn, la, loa, lb, lob, rec

    def load_graph(self, verbose=True) -> int:
        import time; t0=time.time()
        if verbose: print("載入主幹路網...",flush=True)
        gc.collect()
        hw_ph=",".join("?"*len(MAJOR_HW))
        graph = collections.defaultdict(list)
        ncoords = {}
        edge_count = 0
        comp_filter = self._has_component_id()
        comp_clause = " AND component_id=0" if comp_filter else ""
        with _conn(self.db_path) as conn:
            cur=conn.execute(f"""SELECT from_node,to_node,edge_id,highway,name,
                length_km,speed_kmh,lat_a,lon_a,lat_b,lon_b,
                COALESCE(dynamic_mult,1.0) AS mult,
                COALESCE(closure_flag,0) AS closed,
                COALESCE(risk_score,0) AS risk
                FROM osm_edges WHERE highway IN ({hw_ph}){comp_clause}""",MAJOR_HW)
            for r in cur:
                edge_count += 1
                if int(r["closed"] or 0): continue
                fn, tn, la, loa, lb, lob, rec = self._build_edge(r)
                graph[fn].append(rec)
                ncoords[fn] = (la, loa); ncoords[tn] = (lb, lob)

        edge_index = {}
        for fn, edges in graph.items():
            for i, e in enumerate(edges):
                edge_index[e.eid] = (fn, i)

        self._graph = graph
        self._ncoords = ncoords
        self._edge_index = edge_index
        self._modified_edges = set()
        kept_edges = sum(len(v) for v in graph.values())
        self._graph_node_count = len(graph)
        self._graph_edge_count = kept_edges
        # 載入時的節點快照：nearest_node 以此篩選，確保起終點解析
        # 不受路由時的端點局部圖合併影響（見 nearest_node 註解）
        self._base_graph_nodes = frozenset(graph.keys())
        self._graph_loaded = True
        self._load_signal_nodes()
        gc.collect()
        import threading
        def _count():
            with _conn(self.db_path) as conn:
                self._osm_node_count=conn.execute("SELECT COUNT(*) FROM osm_nodes").fetchone()[0]
                self._osm_edge_count=conn.execute("SELECT COUNT(*) FROM osm_edges").fetchone()[0]
        threading.Thread(target=_count, daemon=True).start()
        if verbose: print(f"  {len(graph):,} 節點，{kept_edges:,} 邊，{len(self._signal_nodes):,} 號誌  ({time.time()-t0:.1f}s)",flush=True)
        return kept_edges

    def _load_signal_nodes(self):
        try:
            with _conn(self.db_path) as conn:
                cols = {r["name"] for r in conn.execute("PRAGMA table_info(osm_nodes)").fetchall()}
                if "is_signal" in cols:
                    rows = conn.execute("SELECT node_id FROM osm_nodes WHERE is_signal=1").fetchall()
                    self._signal_nodes = {r[0] for r in rows}
                    return
        except Exception:
            pass
        self._signal_nodes = set()

    def _has_component_id(self) -> bool:
        with _conn(self.db_path) as conn:
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(osm_edges)").fetchall()}
            return "component_id" in cols

    def _load_local_graph(self, bbox, pad=0.025):
        min_lat, min_lon, max_lat, max_lon = bbox
        hw_ph = ",".join("?" * len(LOCAL_HW))
        params = LOCAL_HW + (min_lat - pad, max_lat + pad, min_lon - pad, max_lon + pad)
        with _conn(self.db_path) as conn:
            rows = conn.execute(f"""SELECT from_node,to_node,edge_id,highway,name,
                length_km,speed_kmh,lat_a,lon_a,lat_b,lon_b,
                COALESCE(dynamic_mult,1.0) AS mult,
                COALESCE(closure_flag,0) AS closed,
                COALESCE(risk_score,0) AS risk
                FROM osm_edges WHERE highway IN ({hw_ph})
                AND lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ?""", params).fetchall()
        local = collections.defaultdict(list)
        for r in rows:
            if int(r["closed"] or 0): continue
            fn, tn, la, loa, lb, lob, rec = self._build_edge(r)
            local[fn].append(rec)
            self._ncoords.setdefault(fn, (la, loa))
            self._ncoords.setdefault(tn, (lb, lob))
        return local, None

    _MOTORWAY_HW = {"motorway", "motorway_link", "trunk", "trunk_link"}

    def nearest_node(self, lat, lon, prefer_major=True) -> dict:
        dlat = dlon = 0.005
        with _conn(self.db_path) as conn:
            cur = conn.cursor()
            for _ in range(8):
                rows = cur.execute(
                    "SELECT n.node_id, n.lat, n.lon, "
                    "  (SELECT e.highway FROM osm_edges e "
                    "   WHERE e.from_node=n.node_id LIMIT 1) AS hw "
                    "FROM osm_nodes n "
                    "WHERE n.lat BETWEEN ? AND ? AND n.lon BETWEEN ? AND ?",
                    (lat - dlat, lat + dlat, lon - dlon, lon + dlon),
                ).fetchall()
                if prefer_major and self._graph_loaded:
                    # 用「載入時的主幹圖節點快照」篩選，而非目前的 self._graph——
                    # 路由時端點局部圖會被合併進 self._graph，若以其成員篩選，
                    # 第一次與後續呼叫會解析出不同的起終點節點（路線不一致）
                    base_nodes = self._base_graph_nodes or self._graph
                    in_graph = [r for r in rows if r["node_id"] in base_nodes]
                    non_hw = [r for r in in_graph if r["hw"] not in self._MOTORWAY_HW]
                    pool = non_hw if non_hw else in_graph
                    if pool:
                        b = min(pool, key=lambda r: haversine_km(lat, lon, r["lat"], r["lon"]))
                        return {"node_id": b["node_id"], "lat": float(b["lat"]), "lon": float(b["lon"]),
                                "distance_km": haversine_km(lat, lon, b["lat"], b["lon"])}
                elif rows:
                    b = min(rows, key=lambda r: haversine_km(lat, lon, r["lat"], r["lon"]))
                    return {"node_id": b["node_id"], "lat": float(b["lat"]), "lon": float(b["lon"]),
                            "distance_km": haversine_km(lat, lon, b["lat"], b["lon"])}
                dlat *= 2; dlon *= 2
        raise ValueError(f"找不到 ({lat},{lon}) 附近節點")

    def route(self, start_node, end_node, mode="balanced") -> RouteResult:
        self._graph_lock.acquire()
        try:
            return self._route_inner(start_node, end_node, mode, None)
        finally:
            self._graph_lock.release()

    def route_alternatives(self, start_node, end_node, mode="balanced", n=2) -> list:
        self._graph_lock.acquire()
        try:
            results = []
            penalty_edges: set = set()
            for i in range(n):
                try:
                    r = self._route_inner(start_node, end_node, mode, penalty_edges)
                    results.append(r)
                    for e in r.edges:
                        penalty_edges.add(e["edge_id"])
                except ValueError:
                    break
            return results
        finally:
            self._graph_lock.release()

    def _route_inner(self, start_node, end_node, mode="balanced", penalty_edges: set = None) -> RouteResult:
        if not self._graph_loaded:
            raise RuntimeError("請先呼叫 load_graph()")
        graph = self._graph

        anchor_coords = []
        for nid in (start_node, end_node):
            c = self._ncoords.get(nid)
            if c is None:
                with _conn(self.db_path) as conn:
                    r = conn.execute("SELECT lat, lon FROM osm_nodes WHERE node_id=?", (nid,)).fetchone()
                if r is None:
                    raise ValueError(f"節點 {nid} 不存在於路網中。")
                c = (float(r["lat"]), float(r["lon"]))
                self._ncoords[nid] = c
            anchor_coords.append(c)
        (s_la, s_lo), (e_la, e_lo) = anchor_coords
        direct_km = haversine_km(s_la, s_lo, e_la, e_lo)

        LOCAL_PAD = 0.02
        CELL = 0.01   # 局部圖快取網格（~1.1km）；同格端點不重複載 DB
        for anchor_la, anchor_lo in ((s_la, s_lo), (e_la, e_lo)):
            cell = (round(anchor_la / CELL), round(anchor_lo / CELL))
            if cell in self._local_cells:
                continue
            # 以格心為中心、外加一格 padding，保證格內任一點都有原本
            # LOCAL_PAD 範圍的完整覆蓋（快取不縮小覆蓋保證）
            cla, clo = cell[0] * CELL, cell[1] * CELL
            bbox = (cla - LOCAL_PAD - CELL, clo - LOCAL_PAD - CELL,
                    cla + LOCAL_PAD + CELL, clo + LOCAL_PAD + CELL)
            local, _ = self._load_local_graph(bbox, pad=0.005)
            for fn, edges in local.items():
                if fn not in graph:
                    graph[fn] = edges
                    for idx, e in enumerate(edges):
                        self._edge_index.setdefault(e.eid, (fn, idx))
                else:
                    existing_eids = {e.eid for e in graph[fn]}
                    for e in edges:
                        if e.eid not in existing_eids:
                            graph[fn].append(e)
                            self._edge_index.setdefault(e.eid, (fn, len(graph[fn]) - 1))
            self._local_cells.add(cell)

        w = MODE_WEIGHTS.get(mode, MODE_WEIGHTS["balanced"])
        night_r = _night_risk_score()
        sig_nodes = self._signal_nodes

        def h(nid):
            la, lo = self._ncoords.get(nid, (e_la, e_lo))
            return (haversine_km(la, lo, e_la, e_lo) / MAX_SPEED) * 60

        heap = [(h(start_node), next(_CTR), start_node, 0.0)]
        best = {start_node: 0.0}
        came_from: dict = {}
        max_iters = min(len(graph) * 3, 2_000_000)
        iters = 0

        while heap:
            iters += 1
            if iters > max_iters:
                raise ValueError(f"路徑搜尋超時（{iters} 步），請縮短距離或改用不同模式。")
            f, _, node, g = heapq.heappop(heap)
            if node == end_node:
                edges = []
                tot_km = tot_min = 0.0
                cur = end_node
                while cur in came_from:
                    prev, e = came_from[cur]
                    tsf = _time_speed_factor(e.hw)
                    seg_min = e.cost / tsf   # 含事件速度乘數（與搜尋成本一致）
                    edges.append({
                        "edge_id": e.eid, "highway": e.hw, "name": e.name,
                        "length_km": e.dist, "speed_kmh": e.spd,
                        "travel_min": seg_min, "risk_score": e.risk,
                        "from_node": prev, "to_node": cur,
                        "lat_a": e.la, "lon_a": e.loa,
                        "lat_b": e.lb, "lon_b": e.lob,
                    })
                    tot_km += e.dist
                    tot_min += seg_min
                    cur = prev
                edges.reverse()
                nodes = [start_node] + [ed["to_node"] for ed in edges]
                return RouteResult(nodes, edges, tot_km, tot_min, g, mode)
            if g > best.get(node, float("inf")):
                continue

            prev_entry = came_from.get(node)
            prev_bearing = None
            if prev_entry is not None:
                pe = prev_entry[1]
                prev_bearing = _bearing(pe.la, pe.loa, pe.lb, pe.lob)

            for e in graph.get(node, []):
                nxt = e.to
                tsf = _time_speed_factor(e.hw)
                # e.cost 已含事件速度乘數（dynamic_mult 折進時間，見
                # _apply_dynamic_to_graph）；除以時段係數得當下有效時間。
                # 中性狀態 e.cost=(dist/spd)*60，與舊公式完全等價。
                adj_cost = e.cost / tsf
                risk = e.risk + night_r
                ec2 = adj_cost * (w["time"] + risk * w["risk"])
                if ec2 >= 1e8:
                    continue

                if prev_bearing is not None:
                    cur_bearing = _bearing(e.la, e.loa, e.lb, e.lob)
                    diff = (cur_bearing - prev_bearing + 180) % 360 - 180
                    tp = TURN_PENALTY_MIN.get(_turn_type(diff), 0.0)
                    ec2 += tp

                if nxt in sig_nodes and e.hw not in ("motorway","motorway_link","trunk","trunk_link"):
                    ec2 += SIGNAL_DELAY_MIN

                if penalty_edges and e.eid in penalty_edges:
                    ec2 *= 1.6

                ng = g + ec2
                if ng >= best.get(nxt, float("inf")):
                    continue
                best[nxt] = ng
                came_from[nxt] = (node, e)
                self._ncoords.setdefault(nxt, (e.lb, e.lob))
                heapq.heappush(heap, (ng + h(nxt), next(_CTR), nxt, ng))

        raise ValueError(f"找不到路徑（{start_node}→{end_node}，直線{direct_km:.1f}km）。"
                         "請確認座標在台灣道路範圍內。")

    # CRUD
    def add_event(self,event_type,severity=None,lat=0,lon=0,radius_km=None,highway=None,description=""):
        if severity is None:
            severity = BASE_SEVERITY.get(event_type, 0.50)
        eid=f"evt_{uuid.uuid4().hex[:12]}"
        with _conn(self.db_path) as conn:
            conn.execute("INSERT INTO dynamic_events VALUES(?,?,?,?,?,?,?,?,1,?,?)",
                (eid,event_type,float(severity),highway,float(lat),float(lon),float(radius_km or 0),description,utc_now(),"manual"))
            conn.commit()
        return eid

    def remove_event(self,event_id):
        with _conn(self.db_path) as conn:
            conn.execute("UPDATE dynamic_events SET is_active=0 WHERE event_id=?", (event_id,))
            conn.commit()

    def clear_events(self):
        with _conn(self.db_path) as conn:
            conn.execute("DELETE FROM dynamic_events")
            conn.execute("DELETE FROM dynamic_weather")
            conn.commit()

    def add_weather(self,lat,lon,radius_km=50.0,rain=0,wind=0,visibility=0,warning=0):
        wid=f"wx_{uuid.uuid4().hex[:12]}"
        with _conn(self.db_path) as conn:
            conn.execute("INSERT INTO dynamic_weather VALUES(?,?,?,?,?,?,?,?,1,?,?)",
                (wid,float(lat),float(lon),float(radius_km),float(rain),float(wind),float(visibility),float(warning),utc_now(),"manual"))
            conn.commit()
        return wid

    def list_events(self):
        try:
            with _conn(self.db_path) as conn:
                return [dict(r) for r in conn.execute("SELECT * FROM dynamic_events WHERE is_active=1 ORDER BY created_at DESC").fetchall()]
        except Exception as e:
            _log.warning("list_events failed: %s", e)
            return []

    def recompute_dynamic_cost(self, mode="balanced"):
        with _conn(self.db_path) as conn:
            cur=conn.cursor()
            cur.execute("UPDATE osm_edges SET dynamic_mult=1.0,closure_flag=0,risk_score=0.0 WHERE dynamic_mult!=1.0 OR closure_flag!=0 OR risk_score!=0.0")
            conn.commit()
            for ev in cur.execute("SELECT * FROM dynamic_events WHERE is_active=1").fetchall():
                elat,elon,erad=float(ev["lat"] or 0),float(ev["lon"] or 0),float(ev["radius_km"] or 0.5)
                sev=float(ev["severity"] or 1); etype=ev["event_type"]
                mult=INCIDENT_MULT.get(etype,1.25)
                dlat=erad/110.574; dlon=erad/(111.320*math.cos(math.radians(elat)))
                hw=ev["highway"] if ev["highway"] and ev["highway"] in VALID_HIGHWAYS else None
                bbox=(elat-dlat,elat+dlat,elon-dlon,elon+dlon)
                if etype in ("closure", "landslide_closure"):
                    sql="UPDATE osm_edges SET closure_flag=1,dynamic_mult=999,risk_score=risk_score+100 WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ?"
                    params=bbox
                    if hw: sql+=" AND highway=?"; params=bbox+(hw,)
                    cur.execute(sql,params)
                else:
                    ra={"accident":8,"construction":5,"congestion":3,"manual":2,
                        "landslide_warning":6,"landslide_high":10}.get(etype,2)*sev
                    sql="UPDATE osm_edges SET dynamic_mult=MAX(dynamic_mult,?),risk_score=risk_score+? WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ?"
                    params=(sev*mult,ra)+bbox
                    if hw: sql+=" AND highway=?"; params=params+(hw,)
                    cur.execute(sql,params)
            for wx in cur.execute("SELECT * FROM dynamic_weather WHERE is_active=1").fetchall():
                wlat,wlon,wrad=float(wx["lat"] or 0),float(wx["lon"] or 0),float(wx["radius_km"] or 50)
                dlat=wrad/110.574; dlon=wrad/(111.320*math.cos(math.radians(wlat)))
                rain,wind=float(wx["rain_level"] or 0),float(wx["wind_level"] or 0)
                vis,warn=float(wx["visibility_level"] or 0),float(wx["warning_level"] or 0)
                wm=1.0+RAIN_M*max(0,min(1,rain))+WIND_M*max(0,min(1,wind))+VIS_M*max(0,min(1,vis))+WARN_M*max(0,min(1,warn))
                rw=0.6*rain+0.35*wind+0.6*vis+0.5*warn
                cur.execute("UPDATE osm_edges SET dynamic_mult=MAX(dynamic_mult,?),risk_score=risk_score+? WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ?",
                    (wm,rw,wlat-dlat,wlat+dlat,wlon-dlon,wlon+dlon))
            conn.commit()
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        self._apply_dynamic_to_graph()

    def _apply_dynamic_to_graph(self):
      with self._graph_lock:
        for eid in self._modified_edges:
            loc = self._edge_index.get(eid)
            if loc is None: continue
            fn, idx = loc
            e = self._graph[fn][idx]
            orig_cost = (e.dist / max(e.spd, MIN_SPEED)) * 60
            self._graph[fn][idx] = e._replace(cost=orig_cost, risk=0.0)
        self._modified_edges.clear()
        with _conn(self.db_path) as conn:
            rows = conn.execute(
                "SELECT edge_id, COALESCE(dynamic_mult,1.0) AS mult, "
                "COALESCE(closure_flag,0) AS closed, COALESCE(risk_score,0) AS risk "
                "FROM osm_edges WHERE dynamic_mult!=1.0 OR closure_flag!=0 OR risk_score!=0.0"
            ).fetchall()
        for r in rows:
            eid = r["edge_id"]
            loc = self._edge_index.get(eid)
            if loc is None: continue
            fn, idx = loc
            e = self._graph[fn][idx]
            if int(r["closed"] or 0):
                self._graph[fn][idx] = e._replace(cost=1e8, risk=100.0)
            else:
                mult = float(r["mult"] or 1.0)
                spd = max(e.spd / max(mult, 0.01), MIN_SPEED)
                new_cost = (e.dist / spd) * 60
                self._graph[fn][idx] = e._replace(cost=new_cost, risk=float(r["risk"] or 0))
            self._modified_edges.add(eid)

    def _predict_impact(self, lat, lon, event_type, severity, highway=None):
        omega = ROAD_CRITICALITY.get(highway, 0.30) if highway else 0.30
        dlat = 1.0 / 110.574
        dlon = 1.0 / (111.320 * math.cos(math.radians(lat)))
        with _conn(self.db_path) as conn:
            n_edges = conn.execute(
                "SELECT COUNT(*) FROM osm_edges "
                "WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ?",
                (lat - dlat, lat + dlat, lon - dlon, lon + dlon)
            ).fetchone()[0]
        kappa = min(1.0, n_edges / 200.0)
        if not highway:
            dlat2 = 0.2 / 110.574
            dlon2 = 0.2 / (111.320 * math.cos(math.radians(lat)))
            with _conn(self.db_path) as conn:
                row = conn.execute(
                    "SELECT highway FROM osm_edges "
                    "WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ? "
                    "ORDER BY CASE highway WHEN 'motorway' THEN 1 WHEN 'trunk' THEN 2 "
                    "WHEN 'primary' THEN 3 WHEN 'secondary' THEN 4 ELSE 5 END LIMIT 1",
                    (lat - dlat2, lat + dlat2, lon - dlon2, lon + dlon2)
                ).fetchone()
            if row:
                omega = ROAD_CRITICALITY.get(row[0], 0.30)
        try:
            nd = self.nearest_node(lat, lon)
            node_deg = len(self._graph.get(nd["node_id"], []))
        except Exception:
            node_deg = 4
        delta = min(1.0, node_deg / 8.0)
        r_base = BASE_RADIUS.get(event_type, 0.5)
        predicted_r = r_base * (1 + omega) * (1 - 0.5 * kappa)
        predicted_s = severity * (1 + omega * (1 - kappa) * (1 - delta))
        return predicted_r, predicted_s, {
            "omega": round(omega, 3), "kappa": round(kappa, 3),
            "delta": round(delta, 3), "n_edges_1km": n_edges,
            "node_degree": node_deg,
        }

    def apply_event_incremental(self, event_id):
        with _conn(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            ev = conn.execute(
                "SELECT * FROM dynamic_events WHERE event_id=? AND is_active=1",
                (event_id,)
            ).fetchone()
            if not ev:
                return {"applied": 0}
            elat, elon = float(ev["lat"] or 0), float(ev["lon"] or 0)
            sev = float(ev["severity"] or 1)
            etype = ev["event_type"]
            hw_filter = ev["highway"] if ev["highway"] and ev["highway"] in VALID_HIGHWAYS else None
            pred_r, pred_s, factors = self._predict_impact(elat, elon, etype, sev, hw_filter)
            mult = INCIDENT_MULT.get(etype, 1.25)
            dlat = pred_r / 110.574
            dlon = pred_r / (111.320 * math.cos(math.radians(elat)))
            bbox = (elat - dlat, elat + dlat, elon - dlon, elon + dlon)
            sql = ("SELECT edge_id FROM osm_edges "
                   "WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ?")
            params = list(bbox)
            if hw_filter:
                sql += " AND highway=?"
                params.append(hw_filter)
            edges = conn.execute(sql, params).fetchall()
            conn.execute("UPDATE dynamic_events SET severity=?, radius_km=? WHERE event_id=?",
                         (round(pred_s, 4), round(pred_r, 4), event_id))
            conn.commit()
        applied = 0
        is_closure = etype in ("closure", "landslide_closure")
        with self._graph_lock:
            for r in edges:
                eid = r["edge_id"]
                loc = self._edge_index.get(eid)
                if loc is None:
                    continue
                fn, idx = loc
                e = self._graph[fn][idx]
                if is_closure:
                    self._graph[fn][idx] = e._replace(cost=1e8, risk=100.0)
                else:
                    ra = RISK_ADDITION.get(etype, 2) * pred_s
                    new_mult = pred_s * mult
                    orig_cost = (e.dist / max(e.spd, MIN_SPEED)) * 60
                    current_mult = e.cost / orig_cost if orig_cost > 0 else 1.0
                    final_mult = max(current_mult, new_mult)
                    new_spd = max(e.spd / max(final_mult, 0.01), MIN_SPEED)
                    self._graph[fn][idx] = e._replace(
                        cost=(e.dist / new_spd) * 60, risk=e.risk + ra)
                self._modified_edges.add(eid)
                applied += 1
        return {
            "applied": applied, "event_id": event_id, "event_type": etype,
            "predicted_radius_km": round(pred_r, 3),
            "predicted_severity": round(pred_s, 3),
            "factors": factors,
        }

    def stats(self):
        s={"db_name":Path(self.db_path).name,"graph_loaded":self._graph_loaded,
           "graph_nodes":self._graph_node_count,
           "graph_edges":self._graph_edge_count,
           "osm_nodes":self._osm_node_count,
           "osm_edges":self._osm_edge_count}
        with _conn(self.db_path) as conn:
            cur=conn.cursor()
            try:
                s["active_events"]=cur.execute("SELECT COUNT(*) FROM dynamic_events WHERE is_active=1").fetchone()[0]
                s["active_weather"]=cur.execute("SELECT COUNT(*) FROM dynamic_weather WHERE is_active=1").fetchone()[0]
            except Exception as e: _log.warning("stats query failed: %s", e)
        return s

    @staticmethod
    def to_geojson(edges):
        features=[]
        for e in edges:
            la,loa=e.get("lat_a",0),e.get("lon_a",0)
            lb,lob=e.get("lat_b",0),e.get("lon_b",0)
            if not all([la,loa,lb,lob]): continue
            features.append({"type":"Feature",
                "geometry":{"type":"LineString","coordinates":[[loa,la],[lob,lb]]},
                "properties":{k:e.get(k) for k in ["edge_id","highway","name","length_km","speed_kmh","travel_min","risk_score"]}})
        return {"type":"FeatureCollection","features":features}

    @staticmethod
    def analysis_text(result, sn, en, events):
        from collections import Counter
        hw=Counter(e.get("highway","?") for e in result.edges)
        nr = _night_risk_score()
        tw_h = _tw_hour()
        time_label = "深夜" if tw_h<4 else "凌晨" if tw_h<6 else "早尖峰" if tw_h<9 else "日間" if tw_h<17 else "晚尖峰" if tw_h<19 else "夜間"
        lines=["="*52,"台灣 OSM 動態路徑分析報告","="*52,
               f"模式：{result.mode}",
               f"起點：{sn.get('node_id')} ({sn.get('lat',0):.5f},{sn.get('lon',0):.5f})",
               f"終點：{en.get('node_id')} ({en.get('lat',0):.5f},{en.get('lon',0):.5f})",
               f"總距離：{result.total_km:.2f} km",
               f"預估時間：{result.total_min:.0f} 分鐘 ({result.total_min/60:.1f} hr)",
               f"路段數：{len(result.edges)}",
               f"道路類型：{dict(hw.most_common(6))}",
               f"時段：{time_label} ({tw_h}:00 UTC+8)，夜間風險 +{nr:.0%}",
               "─"*52]
        prev=None; km=mn=0; segs=[]
        for e in result.edges:
            nm=e.get("name") or e.get("highway") or "—"
            if nm==prev: km+=e.get("length_km",0); mn+=e.get("travel_min",0)
            else:
                if prev: segs.append((prev,km,mn))
                prev=nm; km=e.get("length_km",0); mn=e.get("travel_min",0)
        if prev: segs.append((prev,km,mn))
        for i,(nm,km,mn) in enumerate(segs,1):
            lines.append(f"{i:03d}. {nm[:42]:<44}{km:.2f}km  {mn:.1f}min")
        lines+=["","沿途事件","─"*52]
        if not events: lines.append("無")
        else:
            for ev in events: lines.append(f"  [{ev.get('event_type')}] {ev.get('description','')}")
        return "\n".join(lines)
