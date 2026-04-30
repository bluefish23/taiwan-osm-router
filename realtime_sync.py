#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
realtime_sync.py — TDX 即時車速/路況 + CWA 氣象資料同步引擎

背景執行緒定期擷取外部 API，轉換為 dynamic_events/weather 並重算路網成本。
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

logger = logging.getLogger("realtime_sync")

TDX_AUTH_URL = "https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token"
TDX_BASE = "https://tdx.transportdata.tw"
TDX_NEWS = "/api/basic/v2/Road/Traffic/Live/News/Highway"

TDX_VD_CITIES = [
    "Taipei", "NewTaipei", "Taoyuan", "Taichung",
    "Tainan", "Kaohsiung", "Keelung", "Hsinchu",
    "HsinchuCounty", "MiaoliCounty", "ChanghuaCounty",
    "YunlinCounty", "ChiayiCounty", "Chiayi",
    "PingtungCounty", "YilanCounty", "HualienCounty",
    "TaitungCounty", "NantouCounty",
]
TDX_VD_STATIC_TPL = "/api/basic/v2/Road/Traffic/VD/City/{city}"
TDX_VD_LIVE_TPL = "/api/basic/v2/Road/Traffic/Live/VD/City/{city}"

ROAD_CLASS_SPEED = {0: 100, 1: 80, 2: 60, 3: 50, 4: 40, 5: 30}

CWA_BASE = "https://opendata.cwa.gov.tw/api/v1/rest/datastore"
CWA_WEATHER_STATION = "O-A0001-001"
CWA_RAIN_STATION = "O-A0002-001"

NEWS_CATEGORY_MAP = {
    1: "construction",
    2: "accident",
    3: "closure",
    4: "congestion",
}
NEWS_SEVERITY = {
    "closure": 1.0,
    "accident": 0.8,
    "construction": 0.6,
    "congestion": 0.5,
}


def _utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _conn(db_path):
    c = sqlite3.connect(db_path, check_same_thread=False)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    return c


class TDXClient:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self._session = requests.Session()
        self._token: Optional[str] = None
        self._token_expiry: float = 0
        self._lock = threading.Lock()

    def _ensure_token(self):
        with self._lock:
            if time.time() < self._token_expiry - 60:
                return
            try:
                resp = self._session.post(TDX_AUTH_URL, data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                }, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                self._token = data["access_token"]
                self._token_expiry = time.time() + data.get("expires_in", 86400)
                logger.info("TDX token refreshed, expires_in=%s", data.get("expires_in"))
            except Exception as e:
                logger.error("TDX token refresh failed: %s", e)

    def get(self, path: str) -> Optional[dict]:
        self._ensure_token()
        if not self._token:
            return None
        url = f"{TDX_BASE}{path}"
        params = {"$format": "JSON"}
        try:
            resp = self._session.get(url, headers={
                "Authorization": f"Bearer {self._token}",
                "Accept-Encoding": "gzip",
            }, params=params, timeout=(10, 30))
            if resp.status_code == 401:
                self._token_expiry = 0
                self._ensure_token()
                resp = self._session.get(url, headers={
                    "Authorization": f"Bearer {self._token}",
                }, params=params, timeout=(10, 30))
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error("TDX GET %s failed: %s", path, e)
            return None


class CWAClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self._session = requests.Session()
        self._session.verify = False
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def get(self, dataset_id: str) -> Optional[dict]:
        url = f"{CWA_BASE}/{dataset_id}"
        try:
            resp = self._session.get(url, params={
                "Authorization": self.api_key,
            }, timeout=(10, 60))
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error("CWA GET %s failed: %s", dataset_id, e)
            return None


def _ensure_source_columns(db_path: str):
    with _conn(db_path) as conn:
        for table in ("dynamic_events", "dynamic_weather"):
            cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            if "source" not in cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN source TEXT DEFAULT 'manual'")
        conn.commit()


class RealtimeSyncer:
    def __init__(self, router, tdx: Optional[TDXClient], cwa: Optional[CWAClient],
                 interval: int = 300):
        self.router = router
        self.tdx = tdx
        self.cwa = cwa
        self.interval = interval
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._sync_lock = threading.Lock()
        self._status = {
            "running": False,
            "last_sync_time": None,
            "last_sync_ok": None,
            "last_error": None,
            "last_sync_duration_s": None,
            "tdx_speed_count": 0,
            "tdx_incident_count": 0,
            "cwa_station_count": 0,
            "next_sync_time": None,
        }
        self._status_lock = threading.Lock()
        self._vd_positions: dict[str, tuple[float, float, int]] = {}
        _ensure_source_columns(router.db_path)

    def start(self):
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._sync_loop, daemon=True)
        self._thread.start()
        with self._status_lock:
            self._status["running"] = True

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
        with self._status_lock:
            self._status["running"] = False

    def status(self) -> dict:
        with self._status_lock:
            s = dict(self._status)
        if self._thread:
            s["thread_alive"] = self._thread.is_alive()
        return s

    def _sync_loop(self):
        self._run_one_sync()
        while not self._stop_event.is_set():
            with self._status_lock:
                self._status["next_sync_time"] = (
                    datetime.now(timezone.utc) + timedelta(seconds=self.interval)
                ).isoformat()
            self._stop_event.wait(timeout=self.interval)
            if self._stop_event.is_set():
                break
            self._run_one_sync()

    def _run_one_sync(self):
        if not self._sync_lock.acquire(blocking=False):
            logger.warning("Sync already in progress, skipping")
            return
        try:
            self._do_sync()
        finally:
            self._sync_lock.release()

    def _do_sync(self):
        t0 = time.time()
        errors = []
        counts = {"speed": 0, "incident": 0, "weather": 0}

        self._clear_realtime_data()

        if self.tdx:
            try:
                counts["speed"] = self.sync_traffic_speed()
            except Exception as e:
                errors.append(f"traffic_speed: {e}")
                logger.exception("sync_traffic_speed failed")
            try:
                counts["incident"] = self.sync_incidents()
            except Exception as e:
                errors.append(f"incidents: {e}")
                logger.exception("sync_incidents failed")

        if self.cwa:
            try:
                counts["weather"] = self.sync_weather()
            except Exception as e:
                errors.append(f"weather: {e}")
                logger.exception("sync_weather failed")

        try:
            self.router.recompute_dynamic_cost()
        except Exception as e:
            errors.append(f"recompute: {e}")
            logger.exception("recompute_dynamic_cost failed")

        elapsed = time.time() - t0
        with self._status_lock:
            self._status.update({
                "last_sync_time": datetime.now(timezone.utc).isoformat(),
                "last_sync_duration_s": round(elapsed, 1),
                "last_sync_ok": len(errors) == 0,
                "last_error": "; ".join(errors) if errors else None,
                "tdx_speed_count": counts["speed"],
                "tdx_incident_count": counts["incident"],
                "cwa_station_count": counts["weather"],
            })
        logger.info("Sync done in %.1fs: speed=%d incident=%d weather=%d errors=%d",
                     elapsed, counts["speed"], counts["incident"], counts["weather"], len(errors))

    def _clear_realtime_data(self):
        with _conn(self.router.db_path) as conn:
            conn.execute("DELETE FROM dynamic_events WHERE source='realtime'")
            conn.execute("DELETE FROM dynamic_weather WHERE source='realtime'")
            conn.commit()

    # ── TDX 即時車速 ──────────────────────────────────────────────

    def _load_vd_positions(self):
        if self._vd_positions:
            return
        total = 0
        for city in TDX_VD_CITIES:
            data = self.tdx.get(TDX_VD_STATIC_TPL.format(city=city))
            if data is None:
                continue
            for vd in data.get("VDs", []):
                vid = vd.get("VDID")
                lat = vd.get("PositionLat")
                lon = vd.get("PositionLon")
                rc = vd.get("RoadClass", 3)
                if vid and lat is not None and lon is not None:
                    try:
                        lat, lon = float(lat), float(lon)
                        if 21.5 <= lat <= 26.5 and 118 <= lon <= 122.5:
                            self._vd_positions[vid] = (lat, lon, int(rc))
                            total += 1
                    except (ValueError, TypeError):
                        pass
        logger.info("Loaded %d VD positions from %d cities", total, len(TDX_VD_CITIES))

    def sync_traffic_speed(self) -> int:
        self._load_vd_positions()
        if not self._vd_positions:
            logger.warning("No VD positions available, skipping speed sync")
            return 0

        candidates = []
        for city in TDX_VD_CITIES:
            data = self.tdx.get(TDX_VD_LIVE_TPL.format(city=city))
            if data is None:
                continue
            for vd in data.get("VDLives", []):
                vid = vd.get("VDID")
                if vid not in self._vd_positions:
                    continue
                lat, lon, rc = self._vd_positions[vid]
                speed = self._extract_vd_speed(vd)
                if speed is None or speed < 0:
                    continue
                free_speed = ROAD_CLASS_SPEED.get(rc, 50)
                if speed >= free_speed * 0.70:
                    continue
                severity = min(1.0, max(0.0, 1.0 - speed / free_speed))
                candidates.append((severity, vid, lat, lon, speed, free_speed))

        candidates.sort(reverse=True)
        now = _utc_now()
        rows = []
        for severity, vid, lat, lon, speed, free_speed in candidates[:300]:
            eid = f"rt_spd_{uuid.uuid4().hex[:10]}"
            desc = f"VD {vid} speed={speed:.0f}km/h (free={free_speed})"
            rows.append((eid, "congestion", severity, None,
                         lat, lon, 0.3, desc, 1, now, "realtime"))

        if rows:
            with _conn(self.router.db_path) as conn:
                conn.executemany(
                    "INSERT INTO dynamic_events VALUES(?,?,?,?,?,?,?,?,?,?,?)", rows)
                conn.commit()
        logger.info("TDX speed: %d congestion events (from %d candidates)", len(rows), len(candidates))
        return len(rows)

    @staticmethod
    def _extract_vd_speed(vd: dict) -> Optional[float]:
        flows = vd.get("LinkFlows") or []
        if not flows:
            return None
        lanes = flows[0].get("Lanes") or []
        speeds = []
        for lane in lanes:
            s = lane.get("Speed")
            if s is not None:
                try:
                    sv = float(s)
                    if sv > 0:
                        speeds.append(sv)
                except (ValueError, TypeError):
                    pass
        return sum(speeds) / len(speeds) if speeds else None

    # ── TDX 路況新聞 ─────────────────────────────────────────────

    def sync_incidents(self) -> int:
        data = self.tdx.get(TDX_NEWS)
        if data is None:
            return 0
        news_list = data.get("Newses") or data.get("value") or []
        if not news_list:
            return 0

        rows = []
        count = 0
        skipped_no_loc = 0
        for news in news_list:
            lat = news.get("Latitude") or news.get("StartLatitude")
            lon = news.get("Longitude") or news.get("StartLongitude")
            if lat is None or lon is None:
                skipped_no_loc += 1
                continue
            try:
                lat, lon = float(lat), float(lon)
            except (ValueError, TypeError):
                continue
            if not (21.5 <= lat <= 26.5 and 118 <= lon <= 122.5):
                continue

            cat = news.get("NewsCategory", 4)
            event_type = NEWS_CATEGORY_MAP.get(cat, "congestion")
            severity = NEWS_SEVERITY.get(event_type, 0.5)
            title = news.get("Title", "")
            desc = news.get("Description", title)[:200]
            eid = f"rt_inc_{uuid.uuid4().hex[:10]}"
            rows.append((eid, event_type, severity, None,
                         lat, lon, 0.5, f"[TDX] {desc}", 1, _utc_now(), "realtime"))
            count += 1

        if rows:
            with _conn(self.router.db_path) as conn:
                conn.executemany(
                    "INSERT INTO dynamic_events VALUES(?,?,?,?,?,?,?,?,?,?,?)", rows)
                conn.commit()
        if skipped_no_loc:
            logger.info("TDX incidents: %d events, %d skipped (no location)", count, skipped_no_loc)
        else:
            logger.info("TDX incidents: %d events from news", count)
        return count

    # ── CWA 氣象 ─────────────────────────────────────────────────

    def sync_weather(self) -> int:
        count = 0
        rain_map = {}

        rain_data = self.cwa.get(CWA_RAIN_STATION)
        if rain_data:
            for station in (rain_data.get("records", {}).get("Station") or
                            rain_data.get("records", {}).get("data", {}).get("stationStatus", [])):
                sid = station.get("StationId", "")
                geo = station.get("GeoInfo", {})
                coords = geo.get("Coordinates", [{}])
                if isinstance(coords, list) and coords:
                    c = coords[0]
                else:
                    c = coords if isinstance(coords, dict) else {}
                slat = c.get("StationLatitude") or geo.get("Latitude")
                slon = c.get("StationLongitude") or geo.get("Longitude")
                rain_el = station.get("RainfallElement", {})
                hr1 = rain_el.get("Past1hr", {}).get("Precipitation")
                if hr1 is not None and slat is not None:
                    try:
                        rain_map[sid] = {
                            "lat": float(slat), "lon": float(slon),
                            "rain_1hr": max(0, float(hr1)) if float(hr1) >= 0 else 0,
                        }
                    except (ValueError, TypeError):
                        pass

        wx_data = self.cwa.get(CWA_WEATHER_STATION)
        if not wx_data:
            return count

        stations = (wx_data.get("records", {}).get("Station") or
                    wx_data.get("records", {}).get("data", {}).get("stationStatus", []))
        rows = []
        for station in stations:
            sid = station.get("StationId", "")
            geo = station.get("GeoInfo", {})
            coords = geo.get("Coordinates", [{}])
            if isinstance(coords, list) and coords:
                c = coords[0]
            else:
                c = coords if isinstance(coords, dict) else {}
            slat = c.get("StationLatitude") or geo.get("Latitude")
            slon = c.get("StationLongitude") or geo.get("Longitude")
            if slat is None or slon is None:
                continue
            try:
                lat, lon = float(slat), float(slon)
            except (ValueError, TypeError):
                continue
            if not (21.5 <= lat <= 26.5 and 118 <= lon <= 122.5):
                continue

            wx = station.get("WeatherElement", {})
            wind_speed = wx.get("WindSpeed", 0)
            try:
                wind_speed = float(wind_speed) if wind_speed not in (None, "", "-999") else 0
            except (ValueError, TypeError):
                wind_speed = 0

            rain_1hr = 0
            if sid in rain_map:
                rain_1hr = rain_map[sid]["rain_1hr"]
            else:
                precip = wx.get("Now", {}).get("Precipitation") or wx.get("Precipitation")
                if precip is not None and str(precip) not in ("-999", "-99"):
                    try:
                        rain_1hr = max(0, float(precip))
                    except (ValueError, TypeError):
                        pass

            rain_level = min(1.0, rain_1hr / 80.0)
            wind_level = min(1.0, max(0, (wind_speed - 5) / 25.0))
            vis_level = 0.0
            vis_desc = wx.get("Visibility", {})
            if isinstance(vis_desc, dict):
                vis_desc = vis_desc.get("Description", "")
            if isinstance(vis_desc, str):
                try:
                    vis_km = float("".join(c for c in vis_desc if c.isdigit() or c == ".") or "99")
                    if vis_km < 0.2:
                        vis_level = 1.0
                    elif vis_km < 1:
                        vis_level = 0.8
                    elif vis_km < 5:
                        vis_level = 0.5
                    elif vis_km < 10:
                        vis_level = 0.2
                except ValueError:
                    pass

            if rain_level < 0.05 and wind_level < 0.05 and vis_level < 0.05:
                continue

            wid = f"rt_wx_{uuid.uuid4().hex[:10]}"
            rows.append((wid, lat, lon, 30.0,
                         rain_level, wind_level, vis_level, 0.0,
                         1, _utc_now(), "realtime"))
            count += 1

        if rows:
            with _conn(self.router.db_path) as conn:
                conn.executemany(
                    "INSERT INTO dynamic_weather VALUES(?,?,?,?,?,?,?,?,?,?,?)", rows)
                conn.commit()
        logger.info("CWA weather: %d stations with impact", count)
        return count
