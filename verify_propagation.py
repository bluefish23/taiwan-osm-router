# -*- coding: utf-8 -*-
"""事件傳播模式（osm_propagation.py）驗證。

    python verify_propagation.py            # 正確性、實地行為、路線影響（只讀資料庫）
    python verify_propagation.py --db-test  # 另在資料庫「副本」上測增量 vs 全量重算、刪除還原

只讀部分不寫入任何資料；--db-test 先把資料庫備份成副本，所有寫入都在副本上，結束後刪除。
"""
import argparse
import collections
import math
import os
import sqlite3
import sys
import tempfile
import time

import osm_propagation as OP
from make_event_figures import affected_edges, apply_in_memory, ro_conn
from osm_router import BASE_SEVERITY, MIN_SPEED

DB = "taiwan_osm.db"
ACCIDENT = (24.73133, 120.96280)          # 國道一號新竹寶山段（簡報的開場例子）
PLACES = [("國道一號・新竹寶山", 24.73133, 120.96280),
          ("市區省道・台北忠孝東路", 25.04154, 121.54445),
          ("社區道路・寶山環城東路", 24.75372, 120.96615),
          ("蘇花公路", 24.20800, 121.67259)]
START, END = (25.047, 121.517), (24.147, 120.673)


def build(db_path):
    r = OP.PropagationRouter(db_path)
    r.load_graph(verbose=False)
    # 與路口收縮驗證相同：不併入端點局部圖，結果只取決於主幹路網
    r._load_local_graph = lambda bbox, pad=0.005: (collections.defaultdict(list), None)
    r.build_link_graph(verbose=False)
    if not r.build_propagation_index(verbose=True):
        sys.exit("事件傳播索引建立失敗")
    return r


def check_push_exact(r):
    """計畫書的推送結果 vs 精確解 (I − B)⁻¹·res₀。"""
    import numpy as np
    import scipy.sparse as sp
    from scipy.sparse.linalg import splu
    print("\n[1] 殘差推送 vs 精確解")
    size = r._trans.shape[0]
    lu = splu(sp.csc_matrix(sp.identity(size) - r._trans))
    worst = 0.0
    for seed, _ in r._find_seeds(*ACCIDENT):
        res0 = r._share[:, seed].toarray().ravel()
        exact = lu.solve(res0)
        (idx, vals), _ = r._influence(seed)
        push = np.zeros(size)
        push[idx] = vals
        keep = exact >= OP.PUSH_TOL
        err = float(np.abs(push[keep] - exact[keep]).max()) if keep.any() else 0.0
        missed = int((keep & (push == 0)).sum())
        worst = max(worst, err)
        print(f"    事件 link {seed}：精確解中影響 ≥ {OP.PUSH_TOL} 的 link {int(keep.sum())} 條，"
              f"推送漏掉 {missed} 條，最大誤差 {err:.4f}")
    return worst


def check_index(r):
    """直接相依索引：同一個地點第二次發生事件時直接查表。"""
    print("\n[2] 直接相依索引（flag N→A）")
    with r._dep_lock:
        r._dep_index.clear()
    t0 = time.perf_counter(); a, _ = r.edge_influence(*ACCIDENT); t_miss = time.perf_counter() - t0
    t0 = time.perf_counter(); b, info = r.edge_influence(*ACCIDENT); t_hit = time.perf_counter() - t0
    same = a == b
    print(f"    第一次（計算並存入索引）{t_miss * 1000:.1f} ms；第二次（查表）{t_hit * 1000:.1f} ms；"
          f"命中 {info['index_hits']} 個；結果相同：{same}")
    return same


def check_places(r):
    print("\n[3] 實際地點的傳播範圍（車禍）")
    print(f"    {'地點':16s}{'事件link':>8s}{'影響link':>9s}{'影響路段':>9s}{'最遠距離':>9s}{'x≥0.37':>8s}")
    out = []
    for name, lat, lon in PLACES:
        infl, info = r.edge_influence(lat, lon)
        reach = 0.0
        for eid in infl:
            fn, idx = r._edge_index[eid]
            e = r._graph[fn][idx]
            reach = max(reach, OP.haversine_km(lat, lon, e.la, e.loa))
        strong = sum(1 for x in infl.values() if x >= math.exp(-1))
        print(f"    {name:16s}{info['seeds']:>8d}{info['links_reached']:>9d}{len(infl):>9d}"
              f"{reach:>8.2f}km{strong:>8d}")
        out.append((name, reach, len(infl)))
    return out


def check_route(r):
    """台北→台中：只有原方框法 vs 方框法 + 傳播（都只改記憶體）。"""
    print("\n[4] 路線影響（台北→台中，平衡模式，只改記憶體）")
    lat, lon = ACCIDENT
    sn = r.nearest_node(*START)["node_id"]; en = r.nearest_node(*END)["node_id"]
    base = r.route(sn, en)
    pred_r, pred_s, _ = r._predict_impact(lat, lon, "accident", BASE_SEVERITY["accident"])
    con = ro_conn()
    rows = affected_edges(con, r, lat, lon, pred_r)
    apply_in_memory(r, [x["edge_id"] for x in rows], pred_s, "accident")
    r._refresh_links([x["edge_id"] for x in rows])
    box = r.route(sn, en)
    prop = r.propagate_event(lat, lon, "accident", pred_s)
    both = r.route(sn, en)
    print(f"    事件前          {base.total_km:8.2f} km {base.total_min:7.1f} 分")
    print(f"    原方框法        {box.total_km:8.2f} km {box.total_min:7.1f} 分（{len(rows)} 條路段）")
    print(f"    方框法 + 傳播   {both.total_km:8.2f} km {both.total_min:7.1f} 分"
          f"（再加 {prop['edges']} 條路段、{prop['links_updated']} 條 link、最遠 {prop['reach_km']} km、"
          f"{prop['ms']} ms）")


def check_closed(r):
    """封閉路段（成本 1e8）在傳播後必須仍是封閉的，不能被乘數公式改回有限成本。"""
    print("\n[5] 封閉路段不被傳播重新開放")
    infl, _ = r.edge_influence(*ACCIDENT)
    eid = max(infl, key=infl.get)
    fn, idx = r._edge_index[eid]
    orig = r._graph[fn][idx]
    r._graph[fn][idx] = orig._replace(cost=OP.CLOSED_COST)
    try:
        r.propagate_event(*ACCIDENT, "accident", 1.0)
        kept = r._graph[fn][idx].cost >= OP.CLOSED_COST
    finally:
        r._graph[fn][idx] = orig
        r._refresh_links([eid])
    print(f"    影響最強的路段（x={infl[eid]:.2f}）先設為封閉，傳播後仍封閉：{kept}")
    return kept


def snapshot(r, eids):
    out = {}
    for eid in eids:
        fn, idx = r._edge_index[eid]
        e = r._graph[fn][idx]
        out[eid] = (e.cost, e.risk)
    return out


def links_consistent(r, eids):
    """這些路段所屬的 link，其聚合值是否等於由目前路段重新計算的結果。回傳不一致的條數。"""
    bad = 0
    for key in {r._edge2link[e] for e in eids if e in r._edge2link}:
        lk = r._links[key[0]][key[1]]
        fresh = r._make_link(lk.frm, lk.to, r._edges_of(lk.edge_ids), lk.interior)
        if sorted(lk.by_hw) != sorted(fresh.by_hw) or abs(lk.risk - fresh.risk) > 1e-9:
            bad += 1
    return bad


def db_test():
    """在資料庫副本上：增量套用 vs 全量重算一致、刪除事件後完全還原。"""
    tmp = os.path.join(tempfile.gettempdir(), "proptest_taiwan_osm.db")
    print(f"\n[6] 資料庫副本測試：備份到 {tmp}")
    t0 = time.time()
    src = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    dst = sqlite3.connect(tmp)
    src.backup(dst)
    dst.close(); src.close()
    print(f"    備份完成（{time.time() - t0:.0f}s）")
    try:
        r = build(tmp)
        sn = r.nearest_node(*START)["node_id"]; en = r.nearest_node(*END)["node_id"]
        base = r.route(sn, en)
        eid = r.add_event("accident", lat=ACCIDENT[0], lon=ACCIDENT[1], description="propagation test")
        res = r.apply_event_incremental(eid)
        touched = set(r._modified_edges)
        inc = snapshot(r, touched)
        after_inc = r.route(sn, en)
        r.recompute_dynamic_cost()
        rec = snapshot(r, touched)
        # 重算已把仍生效的事件傳播過；之後再套用同一事件（例如 API 重送）不得重複疊加
        once = r.apply_event_incremental(eid).get("propagation", {}).get("reason") == "already_applied"
        diff = max(abs(inc[k][0] - rec[k][0]) + abs(inc[k][1] - rec[k][1]) for k in touched)
        bad_rec = links_consistent(r, touched)
        print(f"    增量套用：方框 {res['applied']} 條 + 傳播 {res['propagation']['edges']} 條路段")
        print(f"    重算後再套用同一事件：傳播{'跳過（不重複疊加）' if once else '又執行了一次'}")
        print(f"    增量 vs 全量重算：{len(touched)} 條路段成本最大差 {diff:.2e}；link 不一致 {bad_rec} 條")
        r.remove_event(eid)
        r.recompute_dynamic_cost()
        dirty = 0
        for k in touched:
            fn, idx = r._edge_index[k]
            e = r._graph[fn][idx]
            if abs(e.cost - (e.dist / max(e.spd, MIN_SPEED)) * 60) > 1e-9 or e.risk != 0.0:
                dirty += 1
        bad_del = links_consistent(r, touched)
        restored = r.route(sn, en)
        back = abs(restored.total_km - base.total_km) < 1e-6
        print(f"    刪除事件後重算：未還原的路段 {dirty} 條、link 不一致 {bad_del} 條")
        print(f"    路線：事件前 {base.total_km:.2f} km → 事件後 {after_inc.total_km:.2f} km → "
              f"刪除後 {restored.total_km:.2f} km（{'已還原' if back else '未還原'}）")
        return once and diff < 1e-9 and bad_rec == 0 and dirty == 0 and bad_del == 0 and back
    finally:
        for ext in ("", "-wal", "-shm"):
            if os.path.exists(tmp + ext):
                os.remove(tmp + ext)
        print("    副本已刪除")


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-test", action="store_true", help="另在資料庫副本上測寫入路徑")
    args = ap.parse_args()
    r = build(DB)
    err = check_push_exact(r)
    same = check_index(r)
    check_places(r)
    check_route(r)
    kept = check_closed(r)
    ok = err < OP.PUSH_TOL and same and kept
    if args.db_test:
        ok = db_test() and ok
    print(f"\n結果：{'全部通過' if ok else '有項目未通過'}")


if __name__ == "__main__":
    main()
