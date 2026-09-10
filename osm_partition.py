#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
osm_partition.py — 路口收縮路網圖（計畫書 p.1–2 的子圖分割落到真實路網）

問題
----
A* 目前跑在「每條邊平均 34 公尺」的圖上，而 95.7%（主幹路網）的節點只是
描繪彎道用的形狀點，不是路口。搜尋每前進 34 公尺就要付一次堆積操作，
長程因此撞上 max_iters 上限而失敗（台北→高雄實測展開 200 萬節點後放棄）。

作法
----
把「無向度數 ≠ 2」的節點視為路口（junction），兩個路口之間的形狀點鏈
收縮成一條 link，A* 改跑在 link 圖上。

與計畫書的對應
--------------
計畫書定義核心節點為 out-degree > 1。實測該規則在雙向路網上失效——
路網以雙向邊儲存，形狀點的 out-degree 恆為 2，導致 67.7% 的節點被判為核心、
平均子圖只有 1.5 個節點，等於沒切。本模組改用其無向對應「度數 ≠ 2」
（含度數 1 的斷頭路——收縮時它必須是 link 的端點）。

註：`gcn/subgraph_partition.py` 的 `partition()` 無法直接重用——它吃 scipy
稀疏矩陣與連續整數索引，且輸出是「核心節點認領的節點集合」，不是本模組需要的
「兩路口之間的邊序列」。此處沿用的是**判定規則**，鏈走訪為另行實作。
`proposal_check()` 以計畫書 p.1 的 21 節點範例圖交叉驗證兩種判定的差異。

不動原演算法
------------
本類別繼承 OSMRouter，只覆寫熱點方法；`osm_router.py` 除了兩處行為等價的
優化外未被更動。未建 link 圖時所有行為完全退回父類別。
"""
from __future__ import annotations

import collections
import heapq
import math
import sqlite3
import sys
import time
from typing import Dict, List, Optional, Set, Tuple

from osm_router import (MAX_SPEED, MODE_WEIGHTS, SIGNAL_DELAY_MIN,
                        TURN_PENALTY_MIN, VALID_HIGHWAYS, OSMRouter,
                        RouteResult, _CTR, _bearing, _conn, _night_risk_score,
                        _time_speed_map, _turn_type, haversine_km)

# 走訪一條鏈的步數上限，防止純度數 2 的環路（例如環狀道路）造成無限迴圈
MAX_CHAIN = 20000

# 與 osm_router._route_inner 的號誌豁免條件一致（國道系統不計號誌延遲）
_MOTORWAY_HW = ("motorway", "motorway_link", "trunk", "trunk_link")


class Link:
    """兩個路口之間的一條路段。

    刻意用 class 而非 namedtuple：事件套用後成本要就地更新，
    namedtuple 不可變，每次都得整條替換。
    """

    __slots__ = ("frm", "to", "edge_ids", "km", "by_hw", "risk",
                 "signal", "turn_in", "interior", "b_in", "b_out")

    def __init__(self, frm, to, edge_ids, km, by_hw, risk, signal,
                 turn_in, interior, b_in, b_out):
        self.frm = frm              # 起點 junction
        self.to = to                # 終點 junction
        self.edge_ids = edge_ids    # 成員邊 id（tuple，順序即行進順序）
        self.km = km                # 長度總和
        # ((highway, Σcost, Σ(cost×risk)), ...)
        # 分兩個累加量的原因：原始每邊成本為
        #     (cost/tsf) × (1 + (risk + night) × w_risk)
        #   = cost/tsf × (1 + night×w_risk) + (cost×risk)/tsf × w_risk
        # 兩項都可依 highway 分組先加總，聚合後與逐邊加總完全相等。
        self.by_hw = by_hw
        self.risk = risk            # 成員邊 risk 總和（僅供顯示與除錯）
        self.signal = signal        # 號誌延遲總和（分），含抵達 to 的那一次
        self.turn_in = turn_in      # link 內部的轉彎懲罰總和（分）
        self.interior = interior    # 內部節點（起訖點虛擬切分用）
        self.b_in = b_in            # 第一條邊的方位角（離開 frm 的方向）
        self.b_out = b_out          # 最後一條邊的方位角（抵達 to 的方向）

    def __repr__(self):
        return (f"Link({self.frm}→{self.to}, {len(self.edge_ids)} 邊, "
                f"{self.km:.3f} km)")


class PartitionedRouter(OSMRouter):
    """以路口收縮的路網圖跑 A*。未建 link 圖時完全等同 OSMRouter。"""

    def __init__(self, db_path: str):
        super().__init__(db_path)
        self._links: Dict[int, List[Link]] = {}
        # 一個節點可能同時屬於正向與反向兩條 link，故值為 list
        self._node2link: Dict[int, List[Tuple[int, int, int]]] = {}
        self._edge2link: Dict[str, Tuple[int, int]] = {}
        self._junctions: Set[int] = set()
        self._dirty_links: Set[Tuple[int, int]] = set()
        self._link_graph_ready = False
        self._link_stats: dict = {}

    # ── 建圖 ────────────────────────────────────────────────

    def _undirected_degree(self) -> collections.Counter:
        """算每個節點的相異鄰居數。

        不建 dict[node, set]（數百萬個 set 太耗記憶體），改先收集無向邊對
        再計數：無向邊約為有向邊的一半，記憶體可控。
        """
        pairs = set()
        for u, edges in self._graph.items():
            for e in edges:
                v = e.to
                if u == v:                      # 自環不計入度數
                    continue
                pairs.add((u, v) if u < v else (v, u))
        deg = collections.Counter()
        for a, b in pairs:
            deg[a] += 1
            deg[b] += 1
        return deg

    def _walk_chain(self, j: int, first_edge) -> Optional[Link]:
        """從路口 j 沿 first_edge 走到下一個路口，收成一條 link。

        回傳的 link 若 `to` 不在 junctions 內，代表走訪被迫中斷
        （該節點的有向結構異常，例如兩條出邊都往前、或單向路斷頭）。
        呼叫端會把這種節點升格成路口後重建，以維持
        「link 端點必為路口」這個不變式。
        """
        sig = self._signal_nodes
        chain = [first_edge]
        interior: List[int] = []

        prev, cur = j, first_edge.to
        guard = 0
        while cur not in self._junctions and guard < MAX_CHAIN:
            guard += 1
            interior.append(cur)
            # 度數 2 的內部節點：取「不是走回頭路」的那條出邊
            nxt = [x for x in self._graph.get(cur, ()) if x.to != prev]
            if len(nxt) != 1:
                # 資料異常（平行邊、單向斷頭…）：就此收尾，不強求
                break
            e = nxt[0]
            chain.append(e)
            prev, cur = cur, e.to

        if guard >= MAX_CHAIN:
            return None                          # 疑似環路，放棄這條鏈

        return self._make_link(j, cur, chain, tuple(interior))

    def _edges_of(self, edge_ids):
        """由 edge_id 取回目前的 Edge 物件。

        必須每次重查——Edge 是 namedtuple，事件套用時以 _replace 產生新物件
        寫回 self._graph，直接存參照會拿到過期的成本。
        """
        out = []
        for eid in edge_ids:
            loc = self._edge_index.get(eid)
            if loc is None:
                return None
            fn, idx = loc
            out.append(self._graph[fn][idx])
        return out

    def _make_link(self, frm, to, chain, interior) -> Link:
        """由一串連續的邊算出 link 的所有聚合量。"""
        sig = self._signal_nodes
        cost = collections.Counter()       # highway -> Σ cost
        cost_risk = collections.Counter()  # highway -> Σ (cost × risk)
        km = risk = signal = turn_in = 0.0
        for i, e in enumerate(chain):
            cost[e.hw] += e.cost
            cost_risk[e.hw] += e.cost * e.risk
            km += e.dist
            risk += e.risk
            # 號誌：與原 A* 同一條判定——看這條邊的終點，且國道系統不計
            if e.to in sig and e.hw not in _MOTORWAY_HW:
                signal += SIGNAL_DELAY_MIN
            # 轉彎：原 A* 對每一組相鄰邊都會計算，link 內部的部分先折進來，
            # 使收縮後的成本與逐邊加總一致（跨路口的那一次在搜尋時算）
            if i:
                p = chain[i - 1]
                pb = _bearing(p.la, p.loa, p.lb, p.lob)
                cb = _bearing(e.la, e.loa, e.lb, e.lob)
                diff = (cb - pb + 180) % 360 - 180
                turn_in += TURN_PENALTY_MIN.get(_turn_type(diff), 0.0)

        by_hw = tuple((hw, c, cost_risk[hw]) for hw, c in cost.items())
        first, last = chain[0], chain[-1]
        return Link(frm, to, tuple(e.eid for e in chain), km, by_hw, risk,
                    signal, turn_in, interior,
                    _bearing(first.la, first.loa, first.lb, first.lob),
                    _bearing(last.la, last.loa, last.lb, last.lob))

    def _slice_link(self, lk: Link, i0: int, i1: int, frm: int, to: int):
        """取 link 的一段成為虛擬 link（起訖點落在 link 內部時用）。"""
        chain = self._edges_of(lk.edge_ids[i0:i1])
        if not chain:
            return None
        return self._make_link(frm, to, chain, lk.interior[i0:max(i1 - 1, i0)])

    def _build_pass(self):
        """以目前的 self._junctions 走一遍所有鏈，回傳建好的索引與待補強的路口。"""
        links: Dict[int, List[Link]] = {}
        node2link: Dict[int, List[Tuple[int, int, int]]] = {}
        edge2link: Dict[str, Tuple[int, int]] = {}
        forced: Set[int] = set()
        n_link = n_edge_in_link = dropped = 0

        for j in self._junctions:
            out = self._graph.get(j)
            if not out:
                continue
            bucket: List[Link] = []
            for e in out:
                lk = self._walk_chain(j, e)
                if lk is None:
                    dropped += 1
                    continue
                if lk.to not in self._junctions:
                    forced.add(lk.to)        # 走訪中斷，該節點須升格成路口
                idx = len(bucket)
                bucket.append(lk)
                for pos, nid in enumerate(lk.interior):
                    node2link.setdefault(nid, []).append((j, idx, pos))
                for eid in lk.edge_ids:
                    edge2link[eid] = (j, idx)
                n_link += 1
                n_edge_in_link += len(lk.edge_ids)
            if bucket:
                links[j] = bucket
        return links, node2link, edge2link, forced, n_link, n_edge_in_link, dropped

    def build_link_graph(self, verbose: bool = True) -> dict:
        """由 self._graph 建出路口收縮圖。必須在 load_graph() 之後呼叫。"""
        if not self._graph_loaded:
            raise RuntimeError("請先呼叫 load_graph()")

        t0 = time.time()
        if verbose:
            print("建立路口收縮圖...", flush=True)

        deg = self._undirected_degree()
        self._junctions = {n for n, d in deg.items() if d != 2}
        n_nodes = len(deg)
        if verbose:
            print(f"  節點 {n_nodes:,}　路口（度數≠2）{len(self._junctions):,}"
                  f"　({time.time() - t0:.1f}s)", flush=True)
        del deg

        total_edges = sum(len(v) for v in self._graph.values())

        # 度數規則不足以涵蓋兩種情形，需迭代補強路口集合：
        #   (a) 走訪被迫中斷的節點（有向結構異常）——否則 link 端點不是路口
        #   (b) 全員度數皆為 2 的環路——沒有任何路口可當起點，整段不會被收進 link
        # 兩者都靠「把該節點升格成路口後重建」解決。實測各只有數百個節點。
        forced_total, passes = 0, 0
        for passes in range(1, 5):
            links, node2link, edge2link, forced, n_link, n_edge_in_link, dropped = \
                self._build_pass()
            covered_ok = n_edge_in_link >= total_edges * 0.9999
            if not forced and covered_ok:
                break
            # (b)：找出還沒被收進任何 link 的邊，各取其起點升格成路口
            seeds = set()
            if not covered_ok:
                for u, edges in self._graph.items():
                    for e in edges:
                        if e.eid not in edge2link:
                            seeds.add(u)
                            break
            if not forced and not seeds:
                break
            forced_total += len(forced)
            self._junctions |= forced | seeds
            if verbose:
                print(f"  第 {passes} 輪：補強路口 +{len(forced)}（走訪中斷）"
                      f" +{len(seeds)}（環路）", flush=True)

        self._links = links
        self._node2link = node2link
        self._edge2link = edge2link
        self._dirty_links = set()
        self._link_graph_ready = True
        self._link_stats = {
            "nodes": n_nodes,
            "junctions": len(self._junctions),
            "forced_junctions": forced_total,
            "passes": passes,
            "links": n_link,
            "edges": total_edges,
            "edges_in_links": n_edge_in_link,
            "coverage": n_edge_in_link / total_edges if total_edges else 0.0,
            "dropped_chains": dropped,
            "node_ratio": n_nodes / max(len(self._junctions), 1),
            "edge_ratio": total_edges / max(n_link, 1),
            "mean_edges_per_link": n_edge_in_link / max(n_link, 1),
            "seconds": round(time.time() - t0, 1),
        }
        if verbose:
            s = self._link_stats
            print(f"  路口 {s['junctions']:,}（含補強 {s['forced_junctions']:,}）"
                  f"　link {s['links']:,} 條　平均 {s['mean_edges_per_link']:.1f} 邊/條")
            print(f"  節點壓縮 {s['node_ratio']:.1f}x"
                  f"　邊壓縮 {s['edge_ratio']:.1f}x（有向邊 → link）")
            print(f"  邊覆蓋率 {s['coverage']:.2%}"
                  + (f"　放棄的鏈 {dropped}" if dropped else ""))
            print(f"  完成 ({s['seconds']}s，{passes} 輪)", flush=True)
        return self._link_stats

    # ── 搜尋 ────────────────────────────────────────────────

    def _link_cost(self, lk: Link, w, night_r, tsf_map, tsf_dflt, penalty_edges):
        """一條 link 的成本（分鐘）。含封閉道路時回傳 None 表示不可通行。

        邊成本的部分與原始 A* 逐邊加總完全相等（見 Link.by_hw 的說明），
        內部轉彎與號誌已於建圖時折入；跨路口的轉彎由呼叫端另外加。
        """
        tot = 0.0
        for hw, c, cr in lk.by_hw:
            tsf = tsf_map.get(hw, tsf_dflt)
            tot += c / tsf * (w["time"] + night_r * w["risk"]) + cr / tsf * w["risk"]
        if tot >= 1e8:
            return None                      # 含封閉道路，整條不可走
        tot += lk.turn_in + lk.signal
        if penalty_edges:
            # 近似：原始 A* 只對「被懲罰的那幾條邊」乘 1.6，此處對整條 link 乘。
            # 由於 penalty_edges 是「前一條路線的全部邊」，而路線多半整條 link
            # 進出，兩者差異很小。僅影響備選路線的取法，不影響主線。
            for eid in lk.edge_ids:
                if eid in penalty_edges:
                    tot *= 1.6
                    break
        return tot

    def _links_out_of(self, node: int):
        """從 node 出發可走的 link。node 是路口就直接取；否則切出後半段。"""
        if node in self._junctions:
            return list(self._links.get(node, ()))
        out = []
        for j, idx, pos in self._node2link.get(node, ()):
            lk = self._links[j][idx]
            # interior[pos] 是走完 edge_ids[0..pos] 後抵達的節點，
            # 因此從它出發的是 edge_ids[pos+1:]
            sub = self._slice_link(lk, pos + 1, len(lk.edge_ids), node, lk.to)
            if sub is not None and sub.edge_ids:
                out.append(sub)
        return out

    def _links_into(self, node: int):
        """回傳 {路口: [虛擬 link]}，代表從該路口可直接走到 node。"""
        res: Dict[int, List[Link]] = {}
        if node in self._junctions:
            return res
        for j, idx, pos in self._node2link.get(node, ()):
            lk = self._links[j][idx]
            sub = self._slice_link(lk, 0, pos + 1, j, node)
            if sub is not None and sub.edge_ids:
                res.setdefault(j, []).append(sub)
        return res

    def _expand(self, seq: List[Link], start_node: int, tsf_map, tsf_dflt):
        """把 link 序列還原成與原始 _route_inner 完全相同格式的邊明細。"""
        edges, tot_km, tot_min = [], 0.0, 0.0
        for lk in seq:
            chain = self._edges_of(lk.edge_ids)
            if chain is None:
                return None
            last = len(chain) - 1
            for i, e in enumerate(chain):
                frm = lk.frm if i == 0 else lk.interior[i - 1]
                to = lk.to if i == last else lk.interior[i]
                seg = e.cost / tsf_map.get(e.hw, tsf_dflt)
                edges.append({
                    "edge_id": e.eid, "highway": e.hw, "name": e.name,
                    "length_km": e.dist, "speed_kmh": e.spd,
                    "travel_min": seg, "risk_score": e.risk,
                    "from_node": frm, "to_node": to,
                    "lat_a": e.la, "lon_a": e.loa,
                    "lat_b": e.lb, "lon_b": e.lob,
                })
                tot_km += e.dist
                tot_min += seg
        nodes = [start_node] + [ed["to_node"] for ed in edges]
        return nodes, edges, tot_km, tot_min

    def _route_inner(self, start_node, end_node, mode="balanced",
                     penalty_edges: set = None, budget_iters=None):
        """在 link 圖上跑 A*。任何無法處理的情形都退回父類別。"""
        if not self._link_graph_ready:
            return super()._route_inner(start_node, end_node, mode,
                                        penalty_edges, budget_iters)
        if start_node not in self._ncoords or end_node not in self._ncoords:
            return super()._route_inner(start_node, end_node, mode,
                                        penalty_edges, budget_iters)

        e_la, e_lo = self._ncoords[end_node]
        w = MODE_WEIGHTS.get(mode, MODE_WEIGHTS["balanced"])
        night_r = _night_risk_score()
        tsf_map, tsf_dflt = _time_speed_map()

        out_of_start = self._links_out_of(start_node)
        into_end = self._links_into(end_node)

        # 起訖同在一條 link 上、且終點在起點下游：直接切一段連起來
        if start_node not in self._junctions and end_node not in self._junctions:
            s_pos = {(j, i): p for j, i, p in self._node2link.get(start_node, ())}
            for j, idx, pe in self._node2link.get(end_node, ()):
                ps = s_pos.get((j, idx))
                if ps is not None and pe > ps:
                    sub = self._slice_link(self._links[j][idx], ps + 1, pe + 1,
                                           start_node, end_node)
                    if sub is not None and sub.edge_ids:
                        out_of_start.append(sub)

        def h(nid):
            la, lo = self._ncoords.get(nid, (e_la, e_lo))
            return (haversine_km(la, lo, e_la, e_lo) / MAX_SPEED) * 60

        heap = [(h(start_node), next(_CTR), start_node, 0.0)]
        best = {start_node: 0.0}
        came_from: Dict[int, Tuple[int, Link]] = {}
        hard_cap = min(max(len(self._links), 1) * 3, 2_000_000)
        max_iters = min(budget_iters, hard_cap) if budget_iters else hard_cap
        iters = 0

        while heap:
            iters += 1
            if iters > max_iters:
                self._last_search_iters = iters
                raise ValueError(f"路徑搜尋超時（{iters} 步），請縮短距離或改用不同模式。")
            f, _, node, g = heapq.heappop(heap)

            if node == end_node:
                seq, cur = [], end_node
                while cur in came_from:
                    prev, lk = came_from[cur]
                    seq.append(lk)
                    cur = prev
                seq.reverse()
                built = self._expand(seq, start_node, tsf_map, tsf_dflt)
                if built is None:
                    break                      # 邊索引失效 → 退回全圖搜尋
                nodes, edges, tot_km, tot_min = built
                self._last_search_iters = iters
                return RouteResult(nodes, edges, tot_km, tot_min, g, mode)

            if g > best.get(node, float("inf")):
                continue

            prev_entry = came_from.get(node)
            prev_b = prev_entry[1].b_out if prev_entry is not None else None

            cand = out_of_start if node == start_node else self._links.get(node, ())
            if node != start_node and node in into_end:
                cand = list(cand) + into_end[node]

            for lk in cand:
                c = self._link_cost(lk, w, night_r, tsf_map, tsf_dflt, penalty_edges)
                if c is None:
                    continue
                if prev_b is not None:
                    diff = (lk.b_in - prev_b + 180) % 360 - 180
                    c += TURN_PENALTY_MIN.get(_turn_type(diff), 0.0)
                ng = g + c
                nxt = lk.to
                if ng >= best.get(nxt, float("inf")):
                    continue
                best[nxt] = ng
                came_from[nxt] = (node, lk)
                heapq.heappush(heap, (ng + h(nxt), next(_CTR), nxt, ng))

        # link 圖上找不到（或展開失敗）：退回原圖，保證不會比現況差
        return super()._route_inner(start_node, end_node, mode,
                                    penalty_edges, budget_iters)

    # ── 事件同步 ────────────────────────────────────────────

    def _refresh_links(self, edge_ids) -> int:
        """重算這些邊所屬 link 的聚合量。回傳更新的 link 數。

        Link 是可變物件（非 namedtuple），故就地更新即可，
        不必動 self._links 的結構，也不會使 _node2link / _edge2link 失效。
        """
        targets = set()
        for eid in edge_ids:
            loc = self._edge2link.get(eid)
            if loc is not None:
                targets.add(loc)
        n = 0
        with self._graph_lock:
            for j, idx in targets:
                try:
                    lk = self._links[j][idx]
                except (KeyError, IndexError):
                    continue
                chain = self._edges_of(lk.edge_ids)
                if chain is None:
                    continue
                fresh = self._make_link(lk.frm, lk.to, chain, lk.interior)
                lk.km, lk.by_hw, lk.risk = fresh.km, fresh.by_hw, fresh.risk
                lk.signal, lk.turn_in = fresh.signal, fresh.turn_in
                lk.b_in, lk.b_out = fresh.b_in, fresh.b_out
                n += 1
        return n

    def _event_edge_ids(self, event_id) -> List[str]:
        """取出事件影響範圍內的 edge_id。

        父類別套用事件時不回報改了哪些邊，故依它寫回 DB 的預測半徑重查一次。
        多付一次已建索引的查詢（實測約 3 ms），換取不必複製父類別的邏輯。
        """
        with _conn(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            ev = conn.execute(
                "SELECT lat, lon, radius_km, highway FROM dynamic_events "
                "WHERE event_id=?", (event_id,)).fetchone()
            if ev is None:
                return []
            lat, lon = float(ev["lat"] or 0), float(ev["lon"] or 0)
            rad = float(ev["radius_km"] or 0.5)
            dlat = rad / 110.574
            dlon = rad / (111.320 * math.cos(math.radians(lat)))
            sql = ("SELECT edge_id FROM osm_edges "
                   "WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ?")
            params = [lat - dlat, lat + dlat, lon - dlon, lon + dlon]
            hw = ev["highway"]
            if hw and hw in VALID_HIGHWAYS:
                sql += " AND highway=?"
                params.append(hw)
            return [r[0] for r in conn.execute(sql, params)]

    def apply_event_incremental(self, event_id):
        """套用事件後，同步更新受影響 link 的聚合成本。

        少了這一步，切割模式會完全看不到事件——A* 讀的是 link 的聚合值，
        而事件只改了成員邊。
        """
        res = super().apply_event_incremental(event_id)
        if self._link_graph_ready and res.get("applied"):
            res["links_updated"] = self._refresh_links(self._event_edge_ids(event_id))
        return res

    def recompute_dynamic_cost(self, mode="balanced"):
        """全圖重算後，把所有曾被改動過的 link 一併同步。"""
        super().recompute_dynamic_cost(mode=mode)
        if self._link_graph_ready:
            self._refresh_links(list(self._modified_edges))

    # ── 自我驗證 ────────────────────────────────────────────

    def verify_contraction(self, verbose: bool = True) -> dict:
        """檢查收縮結果的結構完整性。problems 為空代表通過。"""
        problems: List[str] = []
        s = self._link_stats

        # 1. 每條 link 的端點都必須是路口
        bad_end = sum(1 for bucket in self._links.values() for lk in bucket
                      if lk.frm not in self._junctions or lk.to not in self._junctions)
        if bad_end:
            problems.append(f"{bad_end} 條 link 的端點不是路口")

        # 2. link 的內部節點都不可以是路口
        bad_interior = sum(1 for bucket in self._links.values() for lk in bucket
                           if any(n in self._junctions for n in lk.interior))
        if bad_interior:
            problems.append(f"{bad_interior} 條 link 的內部含路口")

        # 3. 邊覆蓋率：未被任何 link 收進去的邊代表走訪有漏，該處將無法路由
        if s.get("coverage", 0) < 0.99:
            problems.append(f"邊覆蓋率僅 {s.get('coverage', 0):.1%}（應接近 100%）")

        # 4. 長度守恆：link 的 km 應等於其成員邊的 km 總和（抽樣）
        checked = mismatch = 0
        for bucket in list(self._links.values())[:2000]:
            for lk in bucket:
                tot, ok = 0.0, True
                for eid in lk.edge_ids:
                    loc = self._edge_index.get(eid)
                    if loc is None:
                        ok = False
                        break
                    fn, idx = loc
                    tot += self._graph[fn][idx].dist
                if ok:
                    checked += 1
                    if abs(tot - lk.km) > 1e-6:
                        mismatch += 1
        if mismatch:
            problems.append(f"{mismatch}/{checked} 條 link 的長度與成員邊總和不符")

        if verbose:
            print("\n收縮結構自我驗證")
            print(f"  端點皆為路口　　{'OK' if not bad_end else 'FAIL'}")
            print(f"  內部無路口　　　{'OK' if not bad_interior else 'FAIL'}")
            print(f"  邊覆蓋率　　　　{s.get('coverage', 0):.2%}")
            print(f"  長度守恆　　　　{checked - mismatch}/{checked} 條相符")
            print("  結果：" + ("通過" if not problems else "有問題"))
            for p in problems:
                print("    !! " + p)
        return {"ok": not problems, "problems": problems}


def proposal_check() -> bool:
    """以計畫書 p.1 的 21 節點範例圖交叉驗證兩種核心節點判定。

    計畫書原文規則（out-degree > 1）在該有向範例圖上應得 {5,7,13,15,16}；
    本模組採用的「無向度數 ≠ 2」是它在無向路網上的對應定義。
    """
    sys.path.insert(0, "gcn")
    from subgraph_partition import find_core_nodes, proposal_figure_graph

    adj = proposal_figure_graph()
    cores = sorted(int(x) for x in find_core_nodes(adj))
    expected = [5, 7, 13, 15, 16]

    coo = adj.tocoo()
    nbr = collections.defaultdict(set)
    for r, c in zip(coo.row, coo.col):
        if r != c:
            nbr[int(r)].add(int(c))
            nbr[int(c)].add(int(r))
    undirected = sorted(n for n, st in nbr.items() if len(st) != 2)

    print("計畫書 21 節點範例圖")
    print(f"  out-degree > 1（計畫書原文）: {cores}")
    print(f"  預期（計畫書 p.2）          : {expected}   "
          f"{'一致' if cores == expected else '不一致'}")
    print(f"  無向度數 ≠ 2（本模組採用）  : {undirected}")
    return cores == expected


if __name__ == "__main__":
    import os

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    print("=" * 70)
    print("  路口收縮圖 — 建立與自我驗證")
    print("=" * 70)
    ok = proposal_check()
    print()

    db = os.getenv("DB_PATH", "taiwan_osm.db")
    all_roads = os.getenv("PRELOAD_ALL_ROADS", "0") == "1"
    r = PartitionedRouter(db)
    r.init_dynamic_schema()
    r.ensure_component_ids(verbose=False)
    r.load_graph(verbose=True, all_roads=all_roads)
    r.build_link_graph(verbose=True)
    res = r.verify_contraction(verbose=True)
    sys.exit(0 if res["ok"] and ok else 1)
