#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
osm_api.py  啟動: uvicorn osm_api:app --host 127.0.0.1 --port 8000
環境變數: DB_PATH, TDX_CLIENT_ID, TDX_CLIENT_SECRET, CWB_API_KEY, AUTO_SYNC_INTERVAL
"""
from __future__ import annotations
import asyncio
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
import secrets
from fastapi import FastAPI, Depends, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
import httpx
from osm_router import OSMRouter, haversine_km

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")

DB_PATH = os.getenv("DB_PATH", "taiwan_osm.db")
_router: Optional[OSMRouter] = None
_syncer = None
_recompute_status: dict = {"running": False, "last_ok": None, "last_error": None}

@asynccontextmanager
async def lifespan(app):
    global _router, _syncer
    db = os.getenv("DB_PATH", "taiwan_osm.db")
    print(f"[startup] DB={db}")
    try:
        _router = OSMRouter(db)
        _router.init_dynamic_schema()
        _router.ensure_component_ids()
        _router.load_graph(verbose=True)

        print("[startup] TDX/CWA sync disabled (demo mode — manual events only)")

        print("[startup] 就緒")
    except FileNotFoundError:
        print(f"[startup] DB not found at {db} — running without router (upload DB then restart)")
    yield

    if _syncer:
        _syncer.stop()
        print("[shutdown] realtime sync stopped")

app = FastAPI(title="Taiwan OSM Dynamic Router", version="3.0.0", lifespan=lifespan)
_ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(CORSMiddleware,allow_origins=_ALLOWED_ORIGINS,allow_credentials=False,
                   allow_methods=["GET","POST","DELETE"],allow_headers=["Content-Type","Authorization"])

_STATIC_DIR = Path(__file__).parent
_TGOS_KEY = os.getenv("TGOS_API_KEY", "")
_ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
_geocode_cache: dict[str, tuple[float, list]] = {}
_cache_lock = threading.Lock()
_log = logging.getLogger(__name__)

def require_admin(request: Request):
    client = request.client.host if request.client else ""
    if client in ("127.0.0.1", "::1", "localhost"):
        return
    auth = request.headers.get("authorization", "")
    token = auth.replace("Bearer ", "") if auth.startswith("Bearer ") else ""
    if not token:
        token = request.query_params.get("token", "")
    if not _ADMIN_TOKEN:
        return
    if not secrets.compare_digest(token, _ADMIN_TOKEN):
        raise HTTPException(403, "需要管理員權限")

@app.get("/app", response_class=FileResponse)
def serve_user():
    return FileResponse(_STATIC_DIR / "index.html", media_type="text/html")

@app.get("/admin", response_class=FileResponse)
def serve_admin(_=Depends(require_admin)):
    return FileResponse(_STATIC_DIR / "admin.html", media_type="text/html")

@app.get("/geocode")
async def geocode(q: str = Query(..., min_length=1, max_length=200)):
    now = time.time()
    with _cache_lock:
        if q in _geocode_cache:
            ts, results = _geocode_cache[q]
            if now - ts < 60:
                return results

    results = []
    async with httpx.AsyncClient(timeout=8) as client:
        tasks = []
        # Nominatim
        tasks.append(client.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": q, "format": "json", "limit": "5",
                    "viewbox": "118,26.5,122.5,21.5", "bounded": "1",
                    "accept-language": "zh-TW"},
            headers={"User-Agent": "TaiwanOSMRouter/1.0"}
        ))
        # TGOS
        if _TGOS_KEY:
            tasks.append(client.get(
                "https://api.tgos.tw/TGOS_API/tgos_addr_to_coord",
                params={"oAPPId": _TGOS_KEY, "oAddress": q, "oSRS": "EPSG:4326", "oFuzzyType": "2"}
            ))

        responses = await asyncio.gather(*tasks, return_exceptions=True)

        # Parse Nominatim
        nom_resp = responses[0]
        if not isinstance(nom_resp, Exception) and nom_resp.status_code == 200:
            for item in nom_resp.json()[:5]:
                results.append({
                    "name": item.get("display_name", "")[:80],
                    "lat": float(item["lat"]),
                    "lon": float(item["lon"]),
                    "source": "nominatim"
                })

        # Parse TGOS
        if _TGOS_KEY and len(responses) > 1:
            tgos_resp = responses[1]
            if not isinstance(tgos_resp, Exception) and tgos_resp.status_code == 200:
                try:
                    data = tgos_resp.json()
                    if isinstance(data, dict) and data.get("AddressList"):
                        for addr in data["AddressList"][:3]:
                            x = addr.get("X") or addr.get("x")
                            y = addr.get("Y") or addr.get("y")
                            if x and y:
                                results.insert(0, {
                                    "name": addr.get("FULL_ADDR", q),
                                    "lat": float(y),
                                    "lon": float(x),
                                    "source": "tgos"
                                })
                except Exception:
                    pass

    with _cache_lock:
        _geocode_cache[q] = (now, results)
        if len(_geocode_cache) > 500:
            oldest = min(_geocode_cache, key=lambda k: _geocode_cache[k][0])
            del _geocode_cache[oldest]

    return results

def get_r():
    if _router is None: raise HTTPException(503,"路由器尚未初始化")
    return _router

class EventReq(BaseModel):
    event_type: str = Field(pattern="^(accident|construction|closure|congestion|manual)$")
    lat: float; lon: float
    description: str=""

class WeatherReq(BaseModel):
    lat: float; lon: float
    radius_km: float=Field(default=30.0,ge=1,le=200)
    severity: float=Field(default=0.5,ge=0,le=1)

class RecomputeReq(BaseModel):
    mode: str=Field(default="balanced",pattern="^(fastest|balanced|safest)$")

class RouteReq(BaseModel):
    start_lat: float; start_lon: float
    end_lat: float;   end_lon: float
    mode: str=Field(default="balanced",pattern="^(fastest|balanced|safest)$")
    start_node: Optional[int]=None; end_node: Optional[int]=None
    alternatives: bool=False

def chk(lat,lon,name=""):
    if not(21.5<=lat<=26.5 and 118<=lon<=122.5):
        raise HTTPException(400,f"{name}({lat:.4f},{lon:.4f}) 超出台灣範圍")

@app.get("/")
def root():
    html = Path(__file__).parent / "index.html"
    if html.exists():
        return FileResponse(html, media_type="text/html")
    return {"service":"Taiwan OSM Dynamic Router","version":"3.0.0",
            "graph_loaded":_router is not None and _router._graph_loaded,
            "sync_enabled": _syncer is not None}

@app.get("/status")
def status():
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
def sync_trigger(_=Depends(require_admin)):
    if _syncer is None:
        raise HTTPException(400, "Realtime sync not configured")
    threading.Thread(target=_syncer._run_one_sync, daemon=True).start()
    return {"ok": True, "message": "Sync triggered"}

@app.get("/nearest")
def nearest(lat:float,lon:float):
    chk(lat,lon)
    try: return get_r().nearest_node(lat,lon)
    except Exception as e:
        _log.exception("nearest failed")
        raise HTTPException(500,"查詢最近節點失敗")

@app.get("/events")
def list_events(): return get_r().list_events()

@app.post("/events")
def add_event(req:EventReq, _=Depends(require_admin)):
    chk(req.lat,req.lon,"事件")
    eid=get_r().add_event(req.event_type,lat=req.lat,lon=req.lon,
                           description=req.description)
    result=get_r().apply_event_incremental(eid)
    return {"ok":True,"event_id":eid,"incremental":result,
            "note":"已即時套用（增量預測更新）"}

@app.post("/events/apply/{event_id}")
def apply_single_event(event_id:str, _=Depends(require_admin)):
    result=get_r().apply_event_incremental(event_id)
    return {"ok":True,**result}

@app.delete("/events/{event_id}")
def del_event(event_id:str, _=Depends(require_admin)):
    get_r().remove_event(event_id); return {"ok":True,"deleted":event_id}

@app.delete("/events")
def clear_events(_=Depends(require_admin)):
    get_r().clear_events(); return {"ok":True}

@app.get("/weather")
def list_weather():
    try:
        with __import__('sqlite3').connect(get_r().db_path,timeout=10) as conn:
            conn.row_factory = __import__('sqlite3').Row
            return [dict(r) for r in conn.execute("SELECT * FROM dynamic_weather WHERE is_active=1").fetchall()]
    except Exception as e:
        _log.warning("list_weather failed: %s", e)
        return []

@app.post("/weather")
def add_weather(req:WeatherReq, _=Depends(require_admin)):
    chk(req.lat,req.lon,"天氣")
    s=req.severity
    wid=get_r().add_weather(req.lat,req.lon,req.radius_km,
                              rain=s*0.8,wind=s*0.4,visibility=s*0.7,warning=s*0.6)
    return {"ok":True,"weather_id":wid,"severity":s,"radius_km":req.radius_km,
            "note":"請呼叫 /dynamic/recompute 套用"}

def _do_recompute(mode: str):
    try:
        get_r().recompute_dynamic_cost(mode=mode)
        _recompute_status.update(running=False, last_ok=time.time(), last_error=None)
        _log.info("recompute done (mode=%s)", mode)
    except Exception as e:
        _recompute_status.update(running=False, last_error=str(e))
        _log.exception("recompute failed")

@app.post("/dynamic/recompute")
def recompute(req:RecomputeReq, _=Depends(require_admin)):
    if _recompute_status["running"]:
        return {"ok":True,"message":"重算已在執行中，請稍候"}
    _recompute_status["running"] = True
    _recompute_status["last_error"] = None
    threading.Thread(target=_do_recompute, args=(req.mode,), daemon=True).start()
    return {"ok":True,"mode":req.mode,"message":"重算已開始（背景執行）"}

@app.get("/dynamic/recompute/status")
def recompute_status():
    return _recompute_status

@app.post("/benchmark/recompute")
def benchmark_recompute(_=Depends(require_admin)):
    import time as _time
    temp_eid=get_r().add_event("congestion",1.0,25.05,121.52,0.5,description="benchmark_temp")
    t0=_time.perf_counter()
    inc_result=get_r().apply_event_incremental(temp_eid)
    t_inc=_time.perf_counter()-t0
    get_r().remove_event(temp_eid)
    t0=_time.perf_counter()
    get_r().recompute_dynamic_cost(mode="balanced")
    t_full=_time.perf_counter()-t0
    return {"incremental_ms":round(t_inc*1000,2),
            "full_recompute_ms":round(t_full*1000,2),
            "speedup":round(t_full/max(t_inc,0.0001),1),
            "incremental_edges":inc_result.get("applied",0),
            "prediction_factors":inc_result.get("factors",{})}

# ------------------------------------------------------------------
# GCN 預測整合（Phase 5，僅新增——不影響既有事件/天氣/路由機制）
# 預測事件以 source='prediction' 存於 dynamic_events，
# 與 manual / realtime 事件互不干擾；套用成本沿用既有 /dynamic/recompute。
# ------------------------------------------------------------------
_predict_status: dict = {"running": False, "last_ok": None, "last_error": None,
                          "last_result": None}

def _gcn_path():
    import sys
    p = str(Path(__file__).parent / "gcn")
    if p not in sys.path:
        sys.path.insert(0, p)

def _do_predict(at_index: Optional[int]):
    try:
        _gcn_path()
        from traffic.predict_service import run_prediction
        result = run_prediction(get_r().db_path, at_index)
        # 預測事件立即增量套用（重用原系統 apply_event_incremental），
        # 事件一產生路由馬上反應，不需等待全圖 recompute
        t0 = time.perf_counter()
        applied_edges = 0
        for eid in result.get("event_ids", []):
            applied_edges += get_r().apply_event_incremental(eid).get("applied", 0)
        result["incremental_apply_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        result["incremental_applied_edges"] = applied_edges
        result["note"] = "事件已寫入並立即增量套用；清除還原時才需 /dynamic/recompute"
        _predict_status.update(running=False, last_ok=time.time(),
                               last_error=None, last_result=result)
        _log.info("prediction done: %s events, %s edges applied in %sms",
                  result.get("events_created"), applied_edges,
                  result.get("incremental_apply_ms"))
    except Exception as e:
        _predict_status.update(running=False, last_error=str(e))
        _log.exception("prediction failed")

@app.post("/predict/run")
def predict_run(at_index: Optional[int] = None, _=Depends(require_admin)):
    """跑一輪 T-GCN 車速預測，把預測壅塞寫入事件表（背景執行）。"""
    if _predict_status["running"]:
        return {"ok": True, "message": "預測已在執行中，請稍候"}
    _predict_status["running"] = True
    _predict_status["last_error"] = None
    threading.Thread(target=_do_predict, args=(at_index,), daemon=True).start()
    return {"ok": True, "message": "預測已開始（背景執行），完成即自動增量套用至路網"}

@app.get("/predict/status")
def predict_status():
    return _predict_status

@app.get("/predict/congestion")
def predict_congestion():
    """列出目前的預測壅塞事件（source='prediction'）。"""
    return [ev for ev in get_r().list_events() if ev.get("source") == "prediction"]

@app.delete("/predict/events")
def clear_prediction_events(_=Depends(require_admin)):
    _gcn_path()
    from traffic.predict_service import clear_predictions
    n = clear_predictions(get_r().db_path)
    return {"ok": True, "deleted": n,
            "note": "如事件先前已套用成本，請呼叫 /dynamic/recompute 重置"}

@app.post("/route")
def route(req:RouteReq):
    chk(req.start_lat,req.start_lon,"起點"); chk(req.end_lat,req.end_lon,"終點")
    r=get_r()
    try:
        if req.start_node and req.start_node not in r._ncoords:
            raise HTTPException(400,"start_node 不存在於路網中")
        if req.end_node and req.end_node not in r._ncoords:
            raise HTTPException(400,"end_node 不存在於路網中")
        sn=({"node_id":req.start_node,"lat":req.start_lat,"lon":req.start_lon}
            if req.start_node else r.nearest_node(req.start_lat,req.start_lon))
        en=({"node_id":req.end_node,"lat":req.end_lat,"lon":req.end_lon}
            if req.end_node else r.nearest_node(req.end_lat,req.end_lon))

        def _build_response(result):
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

        if req.alternatives:
            results=r.route_alternatives(sn["node_id"],en["node_id"],mode=req.mode,n=3)
            if not results: raise ValueError("找不到路徑")
            primary=_build_response(results[0])
            primary["alternatives"]=[_build_response(alt) for alt in results[1:]]
            return primary
        else:
            result=r.route(sn["node_id"],en["node_id"],mode=req.mode)
            return _build_response(result)
    except HTTPException: raise
    except ValueError as e: raise HTTPException(400,str(e))
    except Exception as e:
        _log.exception("route failed")
        raise HTTPException(500,"路徑計算失敗")
