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
import heapq
import itertools
import math
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
}
RAIN_M,WIND_M,VIS_M,WARN_M = 0.40,0.20,0.35,0.30
MIN_SPEED = 3.0
MAX_SPEED = 120.0
SHORT_DIST_KM = 15.0
_CTR = itertools.count()


def utc_now(): return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def haversine_km(la,loa,lb,lob):
    R=6371.0088; p1,p2=math.radians(la),math.radians(lb)
    dp,dl=math.radians(lb-la),math.radians(lob-loa)
    a=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2*R*math.asin(math.sqrt(max(0.0,a)))

def _conn(db_path):
    c=sqlite3.connect(db_path,check_same_thread=False)
    c.row_factory=sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
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
                description TEXT,is_active INTEGER DEFAULT 1,created_at TEXT)""")
            cur.execute("""CREATE TABLE IF NOT EXISTS dynamic_weather(
                weather_id TEXT PRIMARY KEY,lat REAL,lon REAL,radius_km REAL DEFAULT 50,
                rain_level REAL DEFAULT 0,wind_level REAL DEFAULT 0,
                visibility_level REAL DEFAULT 0,warning_level REAL DEFAULT 0,
                is_active INTEGER DEFAULT 1,updated_at TEXT)""")
            conn.commit()

    @staticmethod
    def _build_edge(r):
        fn, tn = r["from_node"], r["to_node"]
        dist = float(r["length_km"] or 0)
        spd = max(float(r["speed_kmh"] or 30) / max(float(r["mult"] or 1), 0.01), MIN_SPEED)
        la, loa = float(r["lat_a"] or 0), float(r["lon_a"] or 0)
        lb, lob = float(r["lat_b"] or 0), float(r["lon_b"] or 0)
        rec = {"to": tn, "eid": r["edge_id"], "hw": r["highway"], "name": r["name"] or "",
               "dist": dist, "spd": float(r["speed_kmh"] or 30), "cost": (dist / spd) * 60,
               "risk": float(r["risk"] or 0), "la": la, "loa": loa, "lb": lb, "lob": lob}
        rev = {"to": fn, "eid": r["edge_id"], "hw": r["highway"], "name": r["name"] or "",
               "dist": dist, "spd": float(r["speed_kmh"] or 30), "cost": (dist / spd) * 60,
               "risk": float(r["risk"] or 0), "la": lb, "loa": lob, "lb": la, "lob": loa}
        return fn, tn, la, loa, lb, lob, rec, rev

    def load_graph(self, verbose=True) -> int:
        import time; t0=time.time()
        if verbose: print("載入主幹路網...",flush=True)
        hw_ph=",".join("?"*len(MAJOR_HW))
        with _conn(self.db_path) as conn:
            rows=conn.execute(f"""SELECT from_node,to_node,edge_id,highway,name,
                length_km,speed_kmh,lat_a,lon_a,lat_b,lon_b,
                COALESCE(dynamic_mult,1.0) AS mult,
                COALESCE(closure_flag,0) AS closed,
                COALESCE(risk_score,0) AS risk
                FROM osm_edges WHERE highway IN ({hw_ph})""",MAJOR_HW).fetchall()

        graph = collections.defaultdict(list)
        ncoords = {}
        for r in rows:
            if int(r["closed"] or 0): continue
            fn, tn, la, loa, lb, lob, rec, _ = self._build_edge(r)
            graph[fn].append(rec)
            ncoords[fn] = (la, loa); ncoords[tn] = (lb, lob)

        self._graph = graph
        self._ncoords = ncoords; self._graph_loaded = True
        if verbose: print(f"  {len(graph):,} 節點，{len(rows):,} 邊  ({time.time()-t0:.1f}s)",flush=True)
        return len(rows)

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
            fn, tn, la, loa, lb, lob, rec, _ = self._build_edge(r)
            local[fn].append(rec)
            self._ncoords.setdefault(fn, (la, loa))
            self._ncoords.setdefault(tn, (lb, lob))
        return local, None

    def nearest_node(self, lat, lon, prefer_major=True) -> dict:
        dlat = dlon = 0.005
        with _conn(self.db_path) as conn:
            cur = conn.cursor()
            for _ in range(8):
                rows = cur.execute(
                    "SELECT node_id,lat,lon FROM osm_nodes "
                    "WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?",
                    (lat - dlat, lat + dlat, lon - dlon, lon + dlon),
                ).fetchall()
                if prefer_major and self._graph_loaded:
                    valid = [r for r in rows if r["node_id"] in self._graph]
                    if valid:
                        b = min(valid, key=lambda r: haversine_km(lat, lon, r["lat"], r["lon"]))
                        return {"node_id": b["node_id"], "lat": float(b["lat"]), "lon": float(b["lon"]),
                                "distance_km": haversine_km(lat, lon, b["lat"], b["lon"])}
                elif rows:
                    b = min(rows, key=lambda r: haversine_km(lat, lon, r["lat"], r["lon"]))
                    return {"node_id": b["node_id"], "lat": float(b["lat"]), "lon": float(b["lon"]),
                            "distance_km": haversine_km(lat, lon, b["lat"], b["lon"])}
                dlat *= 2; dlon *= 2
        raise ValueError(f"找不到 ({lat},{lon}) 附近節點")

    def route(self, start_node, end_node, mode="balanced") -> RouteResult:
        if not self._graph_loaded:
            raise RuntimeError("請先呼叫 load_graph()")
        sc = self._ncoords.get(start_node)
        ec = self._ncoords.get(end_node)
        if sc is None or ec is None:
            raise ValueError("起點或終點不在主幹路網中。請嘗試在主要道路旁的座標。")
        s_la, s_lo = sc
        e_la, e_lo = ec
        direct_km = haversine_km(s_la, s_lo, e_la, e_lo)
        if direct_km <= SHORT_DIST_KM:
            bbox = (min(s_la, e_la), min(s_lo, e_lo), max(s_la, e_la), max(s_lo, e_lo))
            graph, _ = self._load_local_graph(bbox, pad=0.03)
        else:
            graph = self._graph
        w = MODE_WEIGHTS.get(mode, MODE_WEIGHTS["balanced"])

        def h(nid):
            la, lo = self._ncoords.get(nid, (e_la, e_lo))
            return (haversine_km(la, lo, e_la, e_lo) / MAX_SPEED) * 60

        heap = [(h(start_node), next(_CTR), start_node, 0.0)]
        best = {start_node: 0.0}
        came_from: dict = {}

        while heap:
            f, _, node, g = heapq.heappop(heap)
            if node == end_node:
                edges = []
                tot_km = tot_min = 0.0
                cur = end_node
                while cur in came_from:
                    prev, e = came_from[cur]
                    seg_min = (e["dist"] / max(e["spd"], MIN_SPEED)) * 60
                    edges.append({
                        "edge_id": e["eid"], "highway": e["hw"], "name": e["name"],
                        "length_km": e["dist"], "speed_kmh": e["spd"],
                        "travel_min": seg_min, "risk_score": e["risk"],
                        "from_node": prev, "to_node": cur,
                        "lat_a": e["la"], "lon_a": e["loa"],
                        "lat_b": e["lb"], "lon_b": e["lob"],
                    })
                    tot_km += e["dist"]
                    tot_min += seg_min
                    cur = prev
                edges.reverse()
                nodes = [start_node] + [ed["to_node"] for ed in edges]
                return RouteResult(nodes, edges, tot_km, tot_min, g, mode)
            if g > best.get(node, float("inf")):
                continue
            for e in graph.get(node, []):
                nxt = e["to"]
                ec2 = e["cost"] * w["time"] + e["risk"] * w["risk"] * 60
                if ec2 >= 1e8:
                    continue
                ng = g + ec2
                if ng >= best.get(nxt, float("inf")):
                    continue
                best[nxt] = ng
                came_from[nxt] = (node, e)
                self._ncoords.setdefault(nxt, (e["lb"], e["lob"]))
                heapq.heappush(heap, (ng + h(nxt), next(_CTR), nxt, ng))

        raise ValueError(f"找不到路徑（{start_node}→{end_node}，直線{direct_km:.1f}km）。"
                         "請確認座標在台灣道路範圍內。")

    # CRUD
    def add_event(self,event_type,severity,lat,lon,radius_km=0.5,highway=None,description=""):
        eid=f"evt_{uuid.uuid4().hex[:12]}"
        with _conn(self.db_path) as conn:
            conn.execute("INSERT INTO dynamic_events VALUES(?,?,?,?,?,?,?,?,1,?)",
                (eid,event_type,float(severity),highway,float(lat),float(lon),float(radius_km),description,utc_now()))
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
            conn.execute("INSERT INTO dynamic_weather VALUES(?,?,?,?,?,?,?,?,1,?)",
                (wid,float(lat),float(lon),float(radius_km),float(rain),float(wind),float(visibility),float(warning),utc_now()))
            conn.commit()
        return wid

    def list_events(self):
        try:
            with _conn(self.db_path) as conn:
                return [dict(r) for r in conn.execute("SELECT * FROM dynamic_events WHERE is_active=1 ORDER BY created_at DESC").fetchall()]
        except: return []

    def recompute_dynamic_cost(self, mode="balanced"):
        with _conn(self.db_path) as conn:
            cur=conn.cursor()
            cur.execute("UPDATE osm_edges SET dynamic_mult=1.0,closure_flag=0,risk_score=0.0")
            conn.commit()
            for ev in cur.execute("SELECT * FROM dynamic_events WHERE is_active=1").fetchall():
                elat,elon,erad=float(ev["lat"] or 0),float(ev["lon"] or 0),float(ev["radius_km"] or 0.5)
                sev=float(ev["severity"] or 1); etype=ev["event_type"]
                mult=INCIDENT_MULT.get(etype,1.25)
                dlat=erad/110.574; dlon=erad/(111.320*math.cos(math.radians(elat)))
                hwc=f"AND highway='{ev['highway']}'" if ev["highway"] else ""
                if etype=="closure":
                    cur.execute(f"UPDATE osm_edges SET closure_flag=1,dynamic_mult=999,risk_score=risk_score+100 WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ? {hwc}",
                        (elat-dlat,elat+dlat,elon-dlon,elon+dlon))
                else:
                    ra={"accident":8,"construction":5,"congestion":3,"manual":2}.get(etype,2)*sev
                    cur.execute(f"UPDATE osm_edges SET dynamic_mult=MAX(dynamic_mult,?),risk_score=risk_score+? WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ? {hwc}",
                        (sev*mult,ra,elat-dlat,elat+dlat,elon-dlon,elon+dlon))
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
        self.load_graph(verbose=False)

    def stats(self):
        with _conn(self.db_path) as conn:
            cur=conn.cursor()
            s={"db_path":self.db_path,"graph_loaded":self._graph_loaded,
               "graph_nodes":len(self._graph),
               "graph_edges":sum(len(v) for v in self._graph.values()),
               "osm_nodes":cur.execute("SELECT COUNT(*) FROM osm_nodes").fetchone()[0],
               "osm_edges":cur.execute("SELECT COUNT(*) FROM osm_edges").fetchone()[0]}
            try:
                s["active_events"]=cur.execute("SELECT COUNT(*) FROM dynamic_events WHERE is_active=1").fetchone()[0]
                s["active_weather"]=cur.execute("SELECT COUNT(*) FROM dynamic_weather WHERE is_active=1").fetchone()[0]
            except: pass
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
        lines=["="*52,"台灣 OSM 動態路徑分析報告","="*52,
               f"模式：{result.mode}",
               f"起點：{sn.get('node_id')} ({sn.get('lat',0):.5f},{sn.get('lon',0):.5f})",
               f"終點：{en.get('node_id')} ({en.get('lat',0):.5f},{en.get('lon',0):.5f})",
               f"總距離：{result.total_km:.2f} km",
               f"預估時間：{result.total_min:.0f} 分鐘 ({result.total_min/60:.1f} hr)",
               f"路段數：{len(result.edges)}",
               f"道路類型：{dict(hw.most_common(6))}","─"*52]
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
