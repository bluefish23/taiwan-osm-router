#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
osm_api.py  啟動: uvicorn osm_api:app --host 127.0.0.1 --port 8000
環境變數: DB_PATH, TDX_CLIENT_ID, TDX_CLIENT_SECRET, CWB_API_KEY, AUTO_SYNC_INTERVAL
"""
from __future__ import annotations
import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import Optional
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from osm_router import OSMRouter, haversine_km

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")

DB_PATH = os.getenv("DB_PATH", "taiwan_osm.db")
_router: Optional[OSMRouter] = None
_syncer = None

@asynccontextmanager
async def lifespan(app):
    global _router, _syncer
    db = os.getenv("DB_PATH", "taiwan_osm.db")
    print(f"[startup] DB={db}")
    _router = OSMRouter(db)
    _router.init_dynamic_schema()
    _router.load_graph(verbose=True)

    interval = int(os.getenv("AUTO_SYNC_INTERVAL", "300"))
    tdx_id = os.getenv("TDX_CLIENT_ID", "")
    tdx_secret = os.getenv("TDX_CLIENT_SECRET", "")
    cwa_key = os.getenv("CWB_API_KEY", "")

    if interval > 0 and (tdx_id or cwa_key):
        from realtime_sync import RealtimeSyncer, TDXClient, CWAClient
        tdx = TDXClient(tdx_id, tdx_secret) if tdx_id and tdx_secret else None
        cwa = CWAClient(cwa_key) if cwa_key else None
        _syncer = RealtimeSyncer(_router, tdx, cwa, interval=interval)
        _syncer.start()
        print(f"[startup] realtime sync started (interval={interval}s)")
    else:
        print("[startup] realtime sync disabled (AUTO_SYNC_INTERVAL=0 or no API keys)")

    print("[startup] 就緒")
    yield

    if _syncer:
        _syncer.stop()
        print("[shutdown] realtime sync stopped")

app = FastAPI(title="Taiwan OSM Dynamic Router", version="3.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=False,allow_methods=["*"],allow_headers=["*"])

def get_r():
    if _router is None: raise HTTPException(503,"路由器尚未初始化")
    return _router

class EventReq(BaseModel):
    event_type: str = Field(pattern="^(accident|construction|closure|congestion|manual)$")
    severity: float = Field(ge=0,le=1)
    lat: float; lon: float; radius_km: float=0.5
    highway: Optional[str]=None; description: str=""

class WeatherReq(BaseModel):
    lat: float; lon: float; radius_km: float=50.0
    rain_level: float=Field(default=0,ge=0,le=1)
    wind_level: float=Field(default=0,ge=0,le=1)
    visibility_level: float=Field(default=0,ge=0,le=1)
    warning_level: float=Field(default=0,ge=0,le=1)

class RecomputeReq(BaseModel):
    mode: str=Field(default="balanced",pattern="^(fastest|balanced|safest)$")

class RouteReq(BaseModel):
    start_lat: float; start_lon: float
    end_lat: float;   end_lon: float
    mode: str=Field(default="balanced",pattern="^(fastest|balanced|safest)$")
    start_node: Optional[int]=None; end_node: Optional[int]=None

def chk(lat,lon,name=""):
    if not(21.5<=lat<=26.5 and 118<=lon<=122.5):
        raise HTTPException(400,f"{name}({lat:.4f},{lon:.4f}) 超出台灣範圍")

@app.get("/")
def root():
    return {"service":"Taiwan OSM Dynamic Router","version":"3.0.0",
            "graph_loaded":_router is not None and _router._graph_loaded,
            "sync_enabled": _syncer is not None}

@app.get("/stats")
def stats():
    s = get_r().stats()
    if _syncer:
        s["sync"] = _syncer.status()
    return s

@app.get("/sync/status")
def sync_status():
    if _syncer is None:
        return {"enabled": False, "message": "Realtime sync not configured"}
    return {"enabled": True, **_syncer.status()}

@app.post("/sync/trigger")
def sync_trigger():
    if _syncer is None:
        raise HTTPException(400, "Realtime sync not configured")
    threading.Thread(target=_syncer._run_one_sync, daemon=True).start()
    return {"ok": True, "message": "Sync triggered"}

@app.get("/nearest")
def nearest(lat:float,lon:float):
    chk(lat,lon)
    try: return get_r().nearest_node(lat,lon)
    except Exception as e: raise HTTPException(500,str(e))

@app.get("/events")
def list_events(): return get_r().list_events()

@app.post("/events")
def add_event(req:EventReq):
    chk(req.lat,req.lon,"事件")
    eid=get_r().add_event(req.event_type,req.severity,req.lat,req.lon,
                           req.radius_km,req.highway,req.description)
    return {"ok":True,"event_id":eid,"note":"請呼叫 /dynamic/recompute 套用"}

@app.delete("/events/{event_id}")
def del_event(event_id:str):
    get_r().remove_event(event_id); return {"ok":True,"deleted":event_id}

@app.delete("/events")
def clear_events():
    get_r().clear_events(); return {"ok":True}

@app.get("/weather")
def list_weather():
    try:
        with __import__('sqlite3').connect(get_r().db_path,timeout=10) as conn:
            conn.row_factory = __import__('sqlite3').Row
            return [dict(r) for r in conn.execute("SELECT * FROM dynamic_weather WHERE is_active=1").fetchall()]
    except: return []

@app.post("/weather")
def add_weather(req:WeatherReq):
    chk(req.lat,req.lon,"天氣")
    wid=get_r().add_weather(req.lat,req.lon,req.radius_km,req.rain_level,
                              req.wind_level,req.visibility_level,req.warning_level)
    return {"ok":True,"weather_id":wid,"note":"請呼叫 /dynamic/recompute 套用"}

@app.post("/dynamic/recompute")
def recompute(req:RecomputeReq):
    try:
        get_r().recompute_dynamic_cost(mode=req.mode)
        return {"ok":True,"mode":req.mode,"stats":get_r().stats()}
    except Exception as e: raise HTTPException(500,str(e))

@app.post("/route")
def route(req:RouteReq):
    chk(req.start_lat,req.start_lon,"起點"); chk(req.end_lat,req.end_lon,"終點")
    r=get_r()
    try:
        sn=({"node_id":req.start_node,"lat":req.start_lat,"lon":req.start_lon}
            if req.start_node else r.nearest_node(req.start_lat,req.start_lon))
        en=({"node_id":req.end_node,"lat":req.end_lat,"lon":req.end_lon}
            if req.end_node else r.nearest_node(req.end_lat,req.end_lon))
        result=r.route(sn["node_id"],en["node_id"],mode=req.mode)
        geojson=OSMRouter.to_geojson(result.edges)
        matched=[]
        for ev in r.list_events():
            elat,elon,erad=float(ev.get("lat",0)),float(ev.get("lon",0)),float(ev.get("radius_km",0.5))
            for e in result.edges:
                if haversine_km(elat,elon,e.get("lat_a",0),e.get("lon_a",0))<=erad*1.5:
                    matched.append(ev); break
        return {"ok":True,"mode":req.mode,"start_node":sn,"end_node":en,
                "total_km":round(result.total_km,4),"total_min":round(result.total_min,2),
                "total_cost":round(result.total_cost,4),"num_edges":len(result.edges),
                "edges":result.edges,"geojson":geojson,"matched_events":matched,
                "analysis_text":OSMRouter.analysis_text(result,sn,en,matched)}
    except HTTPException: raise
    except ValueError as e: raise HTTPException(400,str(e))
    except Exception as e: raise HTTPException(500,str(e))
