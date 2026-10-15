#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
osm_propagation.py — 事件沿路網往上游傳播（教授計畫書表 CM03 的演算法用在導航事件）

原系統的事件只改一個固定方框，影響不沿路網傳播；真實事故的回堵卻是沿上游
逐段遞減（164 件國道一號事故實測，見 gcn/results/impact_calibration.json）。
本模組依計畫書的作法處理這段傳播：

  計畫書                           本模組
  ───────────────────────────────  ─────────────────────────────────────────
  以核心節點切子圖                  路口收縮圖：路口 = 核心節點，link = 子圖
  活動節點的新狀態沿傳播鏈更新      事件所在的 link 是活動節點，影響往上游推送
  增量 PageRank 殘差推送            直接呼叫 gcn/incremental_pagerank._push
  只處理有狀態更新的子圖            殘差低於門檻就停，只碰到被推到的 link
  核心節點間的直接相依（α、flag）   每條事件 link 的影響向量算一次後存索引（N→A）

傳播數學
--------
r(L)：影響抵達 link L 下游端的強度（事件點為 1）。往上游走過 L 衰減
exp(-km_L/λ)，再依車道數比例分給所有匯入 L 起點的 link（排除 L 的反向）：

    r = res₀ + B·r，   B[P, L] = share(P | L) · exp(-km_L / λ)

即 r = Σ Bᵏ·res₀，正是計畫書增量 PageRank 的殘差推送（阻尼已含在 B 裡）。
B 是線性的，所以每條事件 link 的解只需算一次，之後查表（直接相依索引）。
路段上的影響 x = r(L)·exp(-d/λ)，d 為路段中點到 L 下游端的距離；
時間乘數 1 + (m₀ − 1)·x，m₀ = 嚴重度 × 事件乘數，與原方法用同一個 m₀。

λ = 2.5 km 的由來：實測每往上游一個門架區段（中位數 4.25 km），扣除約 1.6%
的雜訊底後，超額減速剩 17%–20%，兩段各自推得 λ 為 2.4 與 2.6 km。
單一道路上影響降到門檻 0.05 約在 7.5 km，與實測回堵 p75 的 6.43 km 相當。

與原方法的關係（只增不改）
--------------------------
原本的方框法完整保留、照常先執行；本模式再「加上」往上游的傳播，乘數一樣取
較大值。只對突發事件（事故、施工、手動）傳播：壅塞與預測壅塞本身就是逐點量測
或預測的值，再傳播會重複計算；封路與山崩維持原方法，封閉路段也不會被傳播改動。
`EVENT_PROPAGATION=1`（需 `PARTITION_MODE=1`）才啟用；scipy 不可用時自動停用。
"""
from __future__ import annotations

import collections
import math
import os
import sys
import threading
import time
from typing import Dict, List, Optional, Tuple

from osm_partition import PartitionedRouter
from osm_router import (INCIDENT_MULT, MIN_SPEED, RISK_ADDITION, VALID_HIGHWAYS,
                        _conn, haversine_km)

PROPAGATE_TYPES = ("accident", "construction", "manual")
DECAY_KM = 2.5            # λ：見模組說明
PUSH_TOL = 0.05           # 影響低於事件點的 5% 就不套用
# 建索引時推得更細：推送在每個節點丟掉低於門檻的殘差，多條小路徑匯入同一條 link 時
# 會一起被丟掉；用 1/10 的門檻推送，再以 PUSH_TOL 篩選，誤差即遠小於 PUSH_TOL
INDEX_TOL = PUSH_TOL / 10
SNAP_KM = 0.06            # 事件點吸附到路段的距離
SNAP_MAX_KM = 0.25        # 吸附不到時放寬到這個距離
SAME_ROAD_KM = 0.05       # 與最近路段同等級、且在此距離內的路段視為同一條路（分隔道路兩側）
DEP_CACHE_MAX = 4096      # 直接相依索引最多保留的事件 link 數（最久未用的先丟）
CLOSED_COST = 1e8         # 與 osm_router 封路的成本一致（A* 視為不可通行）
# 分配上游影響用的車道數代理值：資料庫沒有車道數，依各道路等級的典型車道數
LANE_PROXY = {
    "motorway": 3.0, "trunk": 2.0, "primary": 2.0, "secondary": 1.5, "tertiary": 1.0,
    "motorway_link": 1.0, "trunk_link": 1.0, "primary_link": 1.0,
    "secondary_link": 1.0, "tertiary_link": 1.0,
}
LANE_DEFAULT = 0.5
_EVENT_COLS = "event_id, lat, lon, event_type, severity, highway"

_GCN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gcn")


def _load_push():
    """取用計畫書實作（gcn/incremental_pagerank.py）的殘差推送核心。"""
    if _GCN_DIR not in sys.path:
        sys.path.insert(0, _GCN_DIR)
    import numpy as np
    import scipy.sparse as sp
    from incremental_pagerank import _push
    return np, sp, _push


class PropagationRouter(PartitionedRouter):
    """路口收縮圖 + 事件往上游傳播。未建傳播索引時行為與 PartitionedRouter 相同。"""

    def __init__(self, db_path: str):
        super().__init__(db_path)
        self._prop_ready = False
        self._link_keys: List[Tuple[int, int]] = []
        self._link_index: Dict[Tuple[int, int], int] = {}
        self._share = None                       # csc：share[P, L]
        self._trans = None                       # csr：B[P, L]
        # 直接相依索引：事件 link → (受影響 link, r)。在索引裡＝flag A，不在＝flag N
        self._dep_index: "collections.OrderedDict[int, tuple]" = collections.OrderedDict()
        self._dep_lock = threading.Lock()
        # 上次全量重算後已傳播過的事件：同一事件再套用時不重複疊加風險
        self._propagated: set = set()
        self._np = self._push = None
        self._prop_stats: dict = {}

    # ── 建索引 ──────────────────────────────────────────────

    def build_link_graph(self, verbose: bool = True) -> dict:
        self._prop_ready = False          # link 編號會重排，舊的傳播索引作廢
        return super().build_link_graph(verbose=verbose)

    def build_propagation_index(self, verbose: bool = True) -> bool:
        """建立傳播用的轉移矩陣 B。必須在 build_link_graph() 之後呼叫。"""
        if not self._link_graph_ready:
            raise RuntimeError("請先呼叫 build_link_graph()")
        try:
            np, sp, push = _load_push()
        except ImportError as e:
            print(f"  事件傳播停用（缺少套件）：{e}", flush=True)
            return False
        t0 = time.time()
        keys = [(j, i) for j, bucket in self._links.items() for i in range(len(bucket))]
        links = [self._links[j][i] for j, i in keys]
        into: Dict[int, List[int]] = {}
        lanes = []
        for n, lk in enumerate(links):
            into.setdefault(lk.to, []).append(n)
            first = self._edges_of(lk.edge_ids[:1])
            lanes.append(LANE_PROXY.get(first[0].hw if first else "", LANE_DEFAULT))

        rows, cols, vals = [], [], []
        for n, lk in enumerate(links):
            # 匯入 L 起點的 link，排除 L 自己的反向（對向車道不會被回堵）
            preds = [p for p in into.get(lk.frm, ())
                     if not (links[p].frm == lk.to and links[p].to == lk.frm)]
            tot = sum(lanes[p] for p in preds)
            for p in preds:
                rows.append(p)
                cols.append(n)
                vals.append(lanes[p] / tot)
        size = len(keys)
        share = sp.csc_matrix((vals, (rows, cols)), shape=(size, size))
        decay = np.array([math.exp(-lk.km / DECAY_KM) for lk in links])

        self._np, self._push = np, push
        self._link_keys = keys
        self._link_index = {k: n for n, k in enumerate(keys)}
        self._share = share
        self._trans = sp.csr_matrix(share @ sp.diags(decay))
        with self._dep_lock:
            self._dep_index = collections.OrderedDict()
        self._prop_ready = True
        self._prop_stats = {"links": size, "transitions": len(vals),
                            "decay_km": DECAY_KM, "push_tol": PUSH_TOL, "index_tol": INDEX_TOL,
                            "seconds": round(time.time() - t0, 1)}
        if verbose:
            print(f"  事件傳播索引：{size:,} 條 link、{len(vals):,} 個上游連接"
                  f"　λ={DECAY_KM} km　({self._prop_stats['seconds']}s)", flush=True)
        return True

    # ── 傳播計算 ────────────────────────────────────────────

    def _influence(self, seed: int):
        """事件 link 的單位影響向量（直接相依索引：第一次算、之後查表）。回傳 (值, 是否命中)。"""
        with self._dep_lock:
            hit = self._dep_index.get(seed)
            if hit is not None:
                self._dep_index.move_to_end(seed)
                return hit, True
        np = self._np
        res = self._share[:, seed].toarray().ravel()    # 分給匯入 seed 起點的各 link
        x, _ = self._push(self._trans, res, 1.0, INDEX_TOL)
        nz = np.nonzero(x)[0]
        val = (nz, x[nz].copy())
        with self._dep_lock:
            self._dep_index[seed] = val
            if len(self._dep_index) > DEP_CACHE_MAX:
                self._dep_index.popitem(last=False)
        return val, False

    def _find_seeds(self, lat, lon, highway: Optional[str] = None) -> List[Tuple[int, int]]:
        """事件點所在的道路，回傳 [(link 編號, 事件點在 link 中的邊位置)]。

        事件所在的路＝最近的路段；同等級、且離事件點不超過最近距離 + 50 公尺的路段
        也算同一條路——事件沒有方向資訊，分隔道路兩側都要當事件 link。
        較高等級但較遠的路（例如事故在高架橋下的平面道路）不算。
        """
        cand = []                                    # (距離, 道路等級, edge_id)
        for snap in (SNAP_KM, SNAP_MAX_KM):
            dlat = snap / 110.574
            dlon = snap / (111.320 * math.cos(math.radians(lat)))
            with _conn(self.db_path) as conn:
                rows = conn.execute(
                    "SELECT edge_id, highway, lat_a, lon_a FROM osm_edges "
                    "WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ?",
                    (lat - dlat, lat + dlat, lon - dlon, lon + dlon)).fetchall()
            for r in rows:
                if r["edge_id"] not in self._edge2link:
                    continue
                if highway and r["highway"] != highway:
                    continue
                d = haversine_km(lat, lon, r["lat_a"], r["lon_a"])
                if d <= snap:
                    cand.append((d, r["highway"], r["edge_id"]))
            if cand:
                break
        if not cand:
            return []
        d0, hw0, _ = min(cand)
        best: Dict[int, Tuple[float, str]] = {}
        for d, hw, eid in cand:
            if hw != hw0 or d > d0 + SAME_ROAD_KM:
                continue
            n = self._link_index[self._edge2link[eid]]
            if n not in best or d < best[n][0]:
                best[n] = (d, eid)
        out = []
        for n, (_, eid) in best.items():
            j, i = self._link_keys[n]
            out.append((n, self._links[j][i].edge_ids.index(eid)))
        return out

    def edge_influence(self, lat, lon, highway: Optional[str] = None):
        """事件點往上游的影響 {edge_id: x}，以及統計。只計算，不改任何成本。"""
        lam = DECAY_KM
        infl: Dict[str, float] = {}
        touched, hits = set(), 0

        def spread(edges, r0):
            """r0 為抵達這串路段下游端的影響，往上游逐段衰減。"""
            d = 0.0
            for e in reversed(edges):
                x = r0 * math.exp(-(d + e.dist / 2) / lam)
                if x < PUSH_TOL:
                    break
                if x > infl.get(e.eid, 0.0):
                    infl[e.eid] = x
                d += e.dist

        seeds = self._find_seeds(lat, lon, highway)
        for seed, k in seeds:
            j, i = self._link_keys[seed]
            up = self._edges_of(self._links[j][i].edge_ids[:k]) or []
            spread(up, 1.0)                              # 事件 link 中、事件點上游的部分
            touched.add(seed)
            (idx, vals), hit = self._influence(seed)
            hits += hit
            scale = math.exp(-sum(e.dist for e in up) / lam)
            for n, r0 in zip(idx.tolist(), vals.tolist()):
                r = r0 * scale
                if r < PUSH_TOL:
                    continue
                pj, pi = self._link_keys[n]
                spread(self._edges_of(self._links[pj][pi].edge_ids) or [], r)
                touched.add(n)
        return infl, {"seeds": len(seeds), "links_reached": len(touched),
                      "index_hits": hits, "index_size": len(self._dep_index)}

    def _apply_influence(self, infl, sev, etype) -> List[str]:
        """與 apply_event_incremental 的非封路分支同一套公式，乘數依影響 x 縮放。

        呼叫端須持有 _graph_lock。
        """
        m0 = sev * INCIDENT_MULT.get(etype, 1.25)
        ra0 = RISK_ADDITION.get(etype, 2) * sev
        changed = []
        for eid, x in infl.items():
            loc = self._edge_index.get(eid)
            if loc is None:
                continue
            fn, idx = loc
            e = self._graph[fn][idx]
            if e.cost >= CLOSED_COST:
                # 封閉路段不能動：乘數公式會把速度夾在下限、寫回有限成本，等於重新開放
                continue
            orig = (e.dist / max(e.spd, MIN_SPEED)) * 60
            cur = e.cost / orig if orig > 0 else 1.0
            final = max(cur, 1.0 + (m0 - 1.0) * x)
            new_spd = max(e.spd / max(final, 0.01), MIN_SPEED)
            self._graph[fn][idx] = e._replace(cost=(e.dist / new_spd) * 60,
                                              risk=e.risk + ra0 * x)
            self._modified_edges.add(eid)
            changed.append(eid)
        return changed

    def propagate_event(self, lat, lon, etype, sev, highway: Optional[str] = None) -> dict:
        """事件往上游傳播並套用到路網（只改記憶體，與原本的增量法相同）。"""
        if not self._prop_ready or etype not in PROPAGATE_TYPES or not (lat or lon):
            return {"propagated": False}
        t0 = time.perf_counter()
        infl, info = self.edge_influence(lat, lon, highway)
        with self._graph_lock:                   # 套用與 link 同步一次完成，路由看不到中間狀態
            changed = self._apply_influence(infl, sev, etype)
            n_links = self._refresh_links(changed) if changed else 0
        reach = 0.0
        for eid in changed:
            fn, idx = self._edge_index[eid]
            e = self._graph[fn][idx]
            reach = max(reach, haversine_km(lat, lon, e.la, e.loa))
        return {"propagated": True, "edges": len(changed), "links_updated": n_links,
                "reach_km": round(reach, 2),
                "ms": round((time.perf_counter() - t0) * 1000, 1), **info}

    def _propagate_row(self, ev) -> dict:
        hw = ev["highway"] if ev["highway"] in VALID_HIGHWAYS else None
        self._propagated.add(ev["event_id"])
        return self.propagate_event(float(ev["lat"] or 0), float(ev["lon"] or 0),
                                    ev["event_type"], float(ev["severity"] or 1), hw)

    # ── 接上原本的事件流程 ──────────────────────────────────

    def apply_event_incremental(self, event_id):
        """原方框法照常套用，再加上往上游的傳播。同一事件只傳播一次。"""
        with self._event_mutex:
            res = super().apply_event_incremental(event_id)
            if not self._prop_ready or res.get("event_type") not in PROPAGATE_TYPES:
                return res
            if event_id in self._propagated:
                res["propagation"] = {"propagated": False, "reason": "already_applied"}
                return res
            with _conn(self.db_path) as conn:
                ev = conn.execute(f"SELECT {_EVENT_COLS} FROM dynamic_events WHERE event_id=?",
                                  (event_id,)).fetchone()
            if ev is not None:
                res["propagation"] = self._propagate_row(ev)
            return res

    def _apply_dynamic_to_graph(self):
        """全量重算的記憶體階段：父類別重設、重載、同步 link 之後，在同一把鎖內
        對仍生效的突發事件重新傳播——路由不會看到「已重設、尚未傳播」的中間狀態。"""
        with self._graph_lock:
            super()._apply_dynamic_to_graph()
            self._propagated = set()
            if not self._prop_ready:
                return
            ph = ",".join("?" * len(PROPAGATE_TYPES))
            with _conn(self.db_path) as conn:
                rows = conn.execute(
                    f"SELECT {_EVENT_COLS} FROM dynamic_events "
                    f"WHERE is_active=1 AND event_type IN ({ph})", PROPAGATE_TYPES).fetchall()
            for ev in rows:
                self._propagate_row(ev)

    def stats(self):
        s = super().stats()
        s["event_propagation"] = {**self._prop_stats, "enabled": self._prop_ready,
                                  "cached_sources": len(self._dep_index)}
        return s
