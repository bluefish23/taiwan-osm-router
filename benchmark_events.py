#!/usr/bin/env python3
"""事件壓力測試：事件數量放大到上萬筆，以及大量事件同時湧入。

全部在資料庫「副本」上進行（正式的 taiwan_osm.db 只讀），結束後刪除副本。
事件位置取自真實道路（隨機挑路網上的點），類型依比例混合。

    python benchmark_events.py scale [--mode system|original] [--levels 0,500,...,10000]
        已有 N 個事件時：全量重算、新增一個事件到生效、找路各要多久
    python benchmark_events.py burst [--mode system|original] [--events 1000] [--threads 32]
        同一個行程內多執行緒同時新增事件（和 API 的執行緒池相同），同時持續找路；
        結束後與全量重算逐條路段比對，確認同時湧入沒有算錯。第二輪在途中插入全量重算
    python benchmark_events.py http [--events 1000] [--workers 64]
        真的啟動 API 伺服器（指向副本），用 HTTP 同時送出大量 POST /events

mode：system = 路口收縮＋事件傳播（PARTITION_MODE=1、EVENT_PROPAGATION=1）；
      original = 原始 OSMRouter（只載主幹路網）。
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

SRC_DB = "taiwan_osm.db"
START, END = (25.047, 121.517), (24.147, 120.673)          # 台北 → 台中
TYPES = (["accident"] * 30 + ["construction"] * 20 + ["congestion"] * 40 + ["manual"] * 8
         + ["closure"] * 2)
OUT_DIR = "benchmark_results"


# ── 共用 ──────────────────────────────────────────────

def make_copy(tag: str) -> str:
    dst = os.path.join(tempfile.gettempdir(), f"evbench_{tag}.db")
    for ext in ("", "-wal", "-shm"):
        if os.path.exists(dst + ext):
            os.remove(dst + ext)
    t0 = time.time()
    src = sqlite3.connect(f"file:{SRC_DB}?mode=ro", uri=True)
    out = sqlite3.connect(dst)
    src.backup(out)
    out.close()
    src.close()
    print(f"資料庫副本：{dst}（{time.time() - t0:.0f}s）", flush=True)
    return dst


def drop_copy(path: str) -> None:
    for ext in ("", "-wal", "-shm"):
        try:
            os.remove(path + ext)
        except OSError:
            pass


def build_router(db: str, mode: str):
    t0 = time.time()
    if mode == "system":
        from osm_propagation import PropagationRouter
        r = PropagationRouter(db)
    else:
        from osm_router import OSMRouter
        r = OSMRouter(db)
    r.init_dynamic_schema()
    r.ensure_component_ids(verbose=False)
    r.load_graph(verbose=False)
    if mode == "system":
        r.build_link_graph(verbose=False)
        r.build_propagation_index(verbose=False)
    r.clear_events()
    print(f"路由器（{mode}）就緒：{time.time() - t0:.0f}s", flush=True)
    return r


def road_points(r, n: int, seed: int, closures: bool = True):
    """路網上的隨機點（主幹道路的路段起點）。closures=False 時不含封路事件。"""
    rng = random.Random(seed)
    nodes = [k for k, v in r._graph.items() if v]
    types = TYPES if closures else [t for t in TYPES if t != "closure"]
    pts = []
    for _ in range(n):
        e = r._graph[rng.choice(nodes)][0]
        pts.append((e.la, e.loa, rng.choice(types)))
    return pts


def route_once(r):
    sn = r.nearest_node(*START)["node_id"]
    en = r.nearest_node(*END)["node_id"]
    t0 = time.perf_counter()
    res = r.route(sn, en)
    return time.perf_counter() - t0, res.total_km, res.total_min


def pct(values, q):
    v = sorted(values)
    return v[min(len(v) - 1, int(round(q / 100 * (len(v) - 1))))] if v else float("nan")


def snapshot(r, eids):
    out = {}
    for eid in eids:
        loc = r._edge_index.get(eid)
        if loc:
            e = r._graph[loc[0]][loc[1]]
            out[eid] = (e.cost, e.risk)
    return out


def compare_with_recompute(r, label: str) -> dict:
    """目前記憶體中的成本 vs 全量重算的結果（逐條路段比對）。"""
    touched = set(r._modified_edges)
    before = snapshot(r, touched)
    t0 = time.perf_counter()
    r.recompute_dynamic_cost()
    t_rec = time.perf_counter() - t0
    touched |= set(r._modified_edges)
    after = snapshot(r, touched)
    base = lambda e: (e.dist / max(e.spd, 3.0)) * 60                  # noqa: E731
    cost_diff = risk_diff = 0.0
    bad = 0
    for eid in touched:
        loc = r._edge_index.get(eid)
        e = r._graph[loc[0]][loc[1]]
        b = before.get(eid, (base(e), 0.0))
        a = after.get(eid, (base(e), 0.0))
        dc, dr = abs(b[0] - a[0]), abs(b[1] - a[1])
        cost_diff, risk_diff = max(cost_diff, dc), max(risk_diff, dr)
        # 風險容許 0.001：增量套用用未四捨五入的嚴重度，重算讀資料庫裡存到小數 4 位的值
        bad += (dc > 1e-6) or (dr > 1e-3)
    links_bad = None
    if hasattr(r, "_edge2link") and getattr(r, "_link_graph_ready", False):
        links_bad = 0
        for key in {r._edge2link[e] for e in touched if e in r._edge2link}:
            lk = r._links[key[0]][key[1]]
            fresh = r._make_link(lk.frm, lk.to, r._edges_of(lk.edge_ids), lk.interior)
            if sorted(lk.by_hw) != sorted(fresh.by_hw) or abs(lk.risk - fresh.risk) > 1e-9:
                links_bad += 1
    res = {"edges_compared": len(touched), "edges_differ": bad,
           "max_cost_diff": cost_diff, "max_risk_diff": risk_diff,
           "links_inconsistent": links_bad, "recompute_s": round(t_rec, 1)}
    print(f"  [{label}] 與全量重算比對：{len(touched):,} 條路段，不一致 {bad} 條"
          f"（成本差 {cost_diff:.2e}、風險差 {risk_diff:.2e}）；link 不一致 {links_bad}", flush=True)
    return res


# ── scale：事件數量放大 ───────────────────────────────

def run_scale(mode: str, levels: list[int], closures: bool = True) -> dict:
    db = make_copy(f"scale_{mode}")
    try:
        r = build_router(db, mode)
        pts = road_points(r, max(levels) + 1000, seed=1, closures=closures)
        k, rows = 0, []
        print(f"\n{'已有事件':>8} | {'全量重算':>9} | {'新增一個事件（中位/最慢）':>20} | {'找路':>7}")
        for level in levels:
            while k < level:
                lat, lon, et = pts[k]
                r.add_event(et, lat=lat, lon=lon, description="bench")
                k += 1
            t0 = time.perf_counter()
            r.recompute_dynamic_cost()
            t_full = time.perf_counter() - t0
            inc = []
            for _ in range(20):
                lat, lon, et = pts[k]
                k += 1
                t0 = time.perf_counter()
                eid = r.add_event(et, lat=lat, lon=lon, description="bench_inc")
                r.apply_event_incremental(eid)
                inc.append((time.perf_counter() - t0) * 1000)
            row = {"events": level, "full_recompute_s": round(t_full, 2),
                   "incremental_ms_median": round(statistics.median(inc), 1),
                   "incremental_ms_max": round(max(inc), 1)}
            try:
                routes = [route_once(r) for _ in range(3)]
                row.update(route_s_median=round(statistics.median(x[0] for x in routes), 3),
                           route_km=round(routes[-1][1], 2))
                route_txt = f"{row['route_s_median']:>6.3f}s"
            except ValueError as e:                      # 封路太多時起終點之間可能被切斷
                row["route_error"] = str(e)
                route_txt = "找不到路"
            rows.append(row)
            print(f"{level:>8,} | {t_full:>8.1f}s | {row['incremental_ms_median']:>9.1f} / "
                  f"{row['incremental_ms_max']:>7.1f} ms | {route_txt}", flush=True)
        return {"mode": mode, "closures": closures, "rows": rows}
    finally:
        drop_copy(db)


# ── burst：同時大量事件（同一行程、多執行緒）──────────

def _storm(r, pts, threads: int, recompute_at: float | None, guard=None):
    """guard：與 osm_api 相同的互斥鎖（寫入＋套用、全量重算各自持鎖）；None = 不加鎖。"""
    from contextlib import nullcontext
    lock = guard if guard is not None else nullcontext()
    lat_ms, errors = [], []
    route_lat, route_err = [], []
    done = threading.Event()

    def add(p):
        lat, lon, et = p
        t0 = time.perf_counter()
        try:
            with lock:
                eid = r.add_event(et, lat=lat, lon=lon, description="burst")
                r.apply_event_incremental(eid)
            lat_ms.append((time.perf_counter() - t0) * 1000)
        except Exception as e:                                   # 收集所有錯誤，不中斷
            errors.append(repr(e))

    def prober():
        while not done.is_set():
            try:
                route_lat.append(route_once(r)[0])
            except Exception as e:
                route_err.append(repr(e))

    rec = {}

    def recomputer():
        t0 = time.perf_counter()
        try:
            with lock:
                r.recompute_dynamic_cost()
        except Exception as e:
            errors.append("recompute: " + repr(e))
        rec["s"] = time.perf_counter() - t0

    pt = threading.Thread(target=prober, daemon=True)
    pt.start()
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futs = []
        for i, p in enumerate(pts):
            futs.append(ex.submit(add, p))
            if recompute_at is not None and i == int(len(pts) * recompute_at):
                rt = threading.Thread(target=recomputer)
                rt.start()
        for f in futs:
            f.result()
    if recompute_at is not None:
        rt.join()
    wall = time.perf_counter() - t0
    done.set()
    pt.join()
    return {"events": len(pts), "threads": threads, "wall_s": round(wall, 1),
            "events_per_s": round(len(pts) / wall, 1),
            "apply_ms_median": round(statistics.median(lat_ms), 1) if lat_ms else None,
            "apply_ms_p95": round(pct(lat_ms, 95), 1), "apply_ms_max": round(max(lat_ms), 1) if lat_ms else None,
            "errors": errors[:10], "error_count": len(errors),
            "route_queries": len(route_lat), "route_s_median": round(statistics.median(route_lat), 3) if route_lat else None,
            "route_s_max": round(max(route_lat), 3) if route_lat else None,
            "route_errors": route_err[:5], "recompute_during_s": round(rec.get("s", 0), 1) if rec else None}


def run_burst(mode: str, n: int, threads: int, guarded: bool, closures: bool = True) -> dict:
    db = make_copy(f"burst_{mode}")
    try:
        r = build_router(db, mode)
        # 與 osm_api._events_guard 相同：切割模式用路由器的互斥鎖，原始模式另建一把
        guard = getattr(r, "_event_mutex", threading.RLock()) if guarded else None
        base = [route_once(r) for _ in range(3)]
        out = {"mode": mode, "guarded": guarded, "closures": closures,
               "route_baseline_s": round(statistics.median(x[0] for x in base), 3)}
        pts = road_points(r, 2 * n, seed=2, closures=closures)
        print(f"\n[第 1 輪] {n} 個事件、{threads} 條執行緒同時送入，同時持續找路"
              f"（{'與 API 相同加鎖' if guarded else '不加鎖'}）", flush=True)
        out["round1"] = _storm(r, pts[:n], threads, None, guard)
        print("  ", {k: v for k, v in out["round1"].items() if k != "errors"}, flush=True)
        out["round1"]["db_active_events"] = len(r.list_events())
        out["round1"]["consistency"] = compare_with_recompute(r, "第 1 輪")
        print(f"\n[第 2 輪] 再送 {n} 個，送到一半時同時觸發全量重算", flush=True)
        out["round2"] = _storm(r, pts[n:], threads, 0.5, guard)
        print("  ", {k: v for k, v in out["round2"].items() if k != "errors"}, flush=True)
        out["round2"]["db_active_events"] = len(r.list_events())
        out["round2"]["consistency"] = compare_with_recompute(r, "第 2 輪")
        return out
    finally:
        drop_copy(db)


# ── http：真的啟動伺服器 ─────────────────────────────

def _req(url, method="GET", body=None, timeout=120):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.loads(resp.read() or b"null")
        return resp.status, payload, time.perf_counter() - t0


def run_http(n: int, workers: int, port: int = 8765) -> dict:
    db = make_copy("http")
    log_path = os.path.join(tempfile.gettempdir(), "evbench_server.log")
    env = dict(os.environ, DB_PATH=db, PRELOAD_ALL_ROADS="0", PARTITION_MODE="1",
               EVENT_PROPAGATION="1", AUTO_SYNC_INTERVAL="0", MODEL_UPDATE_HOURS="0")
    base = f"http://127.0.0.1:{port}"
    with open(log_path, "w", encoding="utf-8") as log:
        srv = subprocess.Popen([sys.executable, "-m", "uvicorn", "osm_api:app", "--host", "127.0.0.1",
                                "--port", str(port), "--log-level", "warning"],
                               env=env, stdout=log, stderr=subprocess.STDOUT)
    try:
        t0 = time.time()
        while True:
            try:
                if _req(base + "/stats", timeout=5)[0] == 200:
                    break
            except Exception:
                pass
            if srv.poll() is not None or time.time() - t0 > 900:
                raise RuntimeError("伺服器沒有啟動成功，見 " + log_path)
            time.sleep(3)
        print(f"伺服器就緒（{time.time() - t0:.0f}s）", flush=True)
        route_body = {"start_lat": START[0], "start_lon": START[1], "end_lat": END[0], "end_lon": END[1]}
        base_lat = [_req(base + "/route", "POST", route_body)[2] for _ in range(3)]
        rng = random.Random(3)
        # 事件位置：伺服器路網上的點不方便取得，改用台灣西部主要走廊的隨機點（chk 範圍內）
        pts = [(rng.uniform(22.6, 25.2), rng.uniform(120.2, 121.6), rng.choice(TYPES[:-2]))
               for _ in range(n)]
        statuses, lat, errs = {}, [], []
        route_lat, route_err = [], []
        done = threading.Event()

        def post(p):
            try:
                st, _, dt = _req(base + "/events", "POST",
                                 {"event_type": p[2], "lat": p[0], "lon": p[1], "description": "http_burst"})
                statuses[st] = statuses.get(st, 0) + 1
                lat.append(dt)
            except Exception as e:
                errs.append(repr(e))

        def prober():
            while not done.is_set():
                try:
                    route_lat.append(_req(base + "/route", "POST", route_body)[2])
                except Exception as e:
                    route_err.append(repr(e))

        pt = threading.Thread(target=prober, daemon=True)
        pt.start()
        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for i, p in enumerate(pts):
                ex.submit(post, p)
                if i == n // 2:
                    ex.submit(lambda: _req(base + "/dynamic/recompute", "POST", {"mode": "balanced"}))
        wall = time.perf_counter() - t0
        done.set()
        pt.join()
        while _req(base + "/dynamic/recompute/status")[1].get("running"):
            time.sleep(2)
        _, evs, _ = _req(base + "/events")
        _, r1, _ = _req(base + "/route", "POST", route_body)
        _req(base + "/dynamic/recompute", "POST", {"mode": "balanced"})
        time.sleep(1)
        while _req(base + "/dynamic/recompute/status")[1].get("running"):
            time.sleep(2)
        _, r2, _ = _req(base + "/route", "POST", route_body)
        km = lambda r: r.get("total_km", r.get("distance_km"))   # noqa: E731
        mins = lambda r: r.get("total_min", r.get("duration_min"))  # noqa: E731
        out = {"events": n, "workers": workers, "wall_s": round(wall, 1),
               "events_per_s": round(n / wall, 1), "status": statuses, "errors": errs[:10],
               "error_count": len(errs),
               "post_ms_median": round(statistics.median(lat) * 1000, 1) if lat else None,
               "post_ms_p95": round(pct(lat, 95) * 1000, 1), "post_ms_max": round(max(lat) * 1000, 1) if lat else None,
               "route_baseline_s": round(statistics.median(base_lat), 3),
               "route_during_median_s": round(statistics.median(route_lat), 3) if route_lat else None,
               "route_during_max_s": round(max(route_lat), 3) if route_lat else None,
               "route_errors": route_err[:5], "events_listed": len(evs),
               "route_after_burst": [km(r1), mins(r1)], "route_after_recompute": [km(r2), mins(r2)]}
        print(json.dumps(out, indent=2, ensure_ascii=False), flush=True)
        with open(log_path, encoding="utf-8", errors="replace") as f:
            log_text = f.read()
        out["server_tracebacks"] = log_text.count("Traceback")
        out["server_db_locked"] = log_text.count("database is locked")
        return out
    finally:
        srv.terminate()
        try:
            srv.wait(timeout=30)
        except subprocess.TimeoutExpired:
            srv.kill()
        drop_copy(db)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("test", choices=["scale", "burst", "http"])
    ap.add_argument("--mode", choices=["system", "original"], default="system")
    ap.add_argument("--levels", default="0,500,1000,2000,5000,10000")
    ap.add_argument("--events", type=int, default=1000)
    ap.add_argument("--threads", type=int, default=32)
    ap.add_argument("--workers", type=int, default=64)
    ap.add_argument("--guard", action="store_true", help="burst：與 API 相同的加鎖方式")
    ap.add_argument("--no-closure", action="store_true", help="scale / burst：事件不含封路")
    a = ap.parse_args()
    if a.test == "scale":
        res = run_scale(a.mode, [int(x) for x in a.levels.split(",")], not a.no_closure)
        name = f"scale_{a.mode}{'_noclosure' if a.no_closure else ''}"
    elif a.test == "burst":
        res = run_burst(a.mode, a.events, a.threads, a.guard, not a.no_closure)
        name = (f"burst_{a.mode}{'_guarded' if a.guard else ''}"
                f"{'_noclosure' if a.no_closure else ''}")
    else:
        res = run_http(a.events, a.workers)
        name = "http"
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, f"{name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(res, f, indent=2, ensure_ascii=False)
    print(f"\n結果：{path}")


if __name__ == "__main__":
    main()
