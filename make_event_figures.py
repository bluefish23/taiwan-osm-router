# -*- coding: utf-8 -*-
"""事件更新視覺圖：用真實路網與系統自己的更新程式，畫出一個事件如何改變路網成本。

只讀資料庫：事件在記憶體中套用（公式與 apply_event_incremental 的非封路分支相同），
不寫入 dynamic_events / osm_edges。輸出 event_figures/*.png（16:9，2000×1125）。

    python make_event_figures.py
"""
import collections
import heapq
import math
import os
import sqlite3
import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle

import osm_propagation as OP
import osm_router
from osm_router import (BASE_RADIUS, BASE_SEVERITY, INCIDENT_MULT, MAJOR_HW,
                        MIN_SPEED, RISK_ADDITION)

DB = "taiwan_osm.db"
OUT = "event_figures"
# 台北→台中路線正中間的國道一號（新竹寶山段），與路口收縮驗證測試同一點、同一種事件
EVENT = {"lat": 24.73133, "lon": 120.96280, "type": "accident"}
START, END = (25.047, 121.517), (24.147, 120.673)
# 行車時間隨時段變（尖峰降速、夜間風險），路線也可能跟著變；固定在平日上午 10 點，
# 每次重畫的數字才會一致（與簡報、verify_propagation 的路線數字相同）
FIG_HOUR = 10
# 三處都只給事件位置（與後台新增事件相同），道路關鍵度由系統自行判斷；
# 社區道路選在 200 公尺內只有 residential 的地點，避免同級道路排序不定
COMPARE = [("國道一號・新竹寶山段", "中山高速公路", 24.73133, 120.96280),
           ("市區省道・台北忠孝東路", "忠孝東路四段", 25.04154, 121.54447),
           ("社區道路・寶山環城東路", "環城東路", 24.75372, 120.96615)]

MAIN, ACC, INK, MUTED = "#0E6070", "#C2571A", "#1F2A2E", "#6B7C82"
ROUTE_OLD = "#7E9095"
ROAD_STYLE = [  # (道路類型, 顏色, 線寬)
    ({"motorway", "motorway_link", "trunk", "trunk_link"}, "#9AA9AE", 3.0),
    ({"primary", "primary_link", "secondary", "secondary_link"}, "#B4C1C5", 1.8),
    ({"tertiary", "tertiary_link"}, "#C9D3D6", 1.2),
]
MINOR_STYLE = ("#E2E8EA", 0.6)
HW_NAME = {"motorway": "國道", "trunk": "快速道路", "primary": "省道", "secondary": "縣道",
           "tertiary": "鄉道"}

plt.rcParams["font.sans-serif"] = ["Microsoft JhengHei", "PingFang TC", "Noto Sans TC"]
plt.rcParams["axes.unicode_minus"] = False


# ─────────────────────────── 幾何 ───────────────────────────
class Proj:
    """經緯度 → 以中心點為原點的公里座標。"""

    def __init__(self, lat0, lon0):
        self.lat0, self.lon0 = lat0, lon0
        self.kx, self.ky = 111.320 * math.cos(math.radians(lat0)), 110.574

    def xy(self, lat, lon):
        return (lon - self.lon0) * self.kx, (lat - self.lat0) * self.ky

    def seg(self, la, loa, lb, lob):
        return [self.xy(la, loa), self.xy(lb, lob)]


def ro_conn():
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=30)
    con.row_factory = sqlite3.Row
    return con


def roads_in(con, pj, half_x, half_y, major_only=False):
    dlat, dlon = half_y / pj.ky, half_x / pj.kx
    sql = ("SELECT lat_a, lon_a, lat_b, lon_b, highway FROM osm_edges "
           "WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ?")
    args = [pj.lat0 - dlat, pj.lat0 + dlat, pj.lon0 - dlon, pj.lon0 + dlon]
    if major_only:
        sql += f" AND highway IN ({','.join('?' * len(MAJOR_HW))})"
        args += list(MAJOR_HW)
    return con.execute(sql, args).fetchall()


# ─────────────────────────── 繪圖元件 ───────────────────────────
def new_fig(title, subtitle):
    fig = plt.figure(figsize=(13.333, 7.5), dpi=150, facecolor="white")
    fig.text(0.04, 0.925, title, fontsize=24, fontweight="bold", color=MAIN, va="center")
    fig.text(0.04, 0.868, subtitle, fontsize=14, color=INK, va="center")
    return fig


def map_axes(fig, rect=(0.04, 0.07, 0.56, 0.75)):
    ax = fig.add_axes(rect)
    ax.set_aspect("equal")
    ax.set_xticks([]); ax.set_yticks([])
    for s in ax.spines.values():
        s.set_color("#DCE4E6")
    ax.set_facecolor("#FBFCFC")
    return ax


def axes_aspect(fig, ax):
    # original=True：set_aspect("equal") 會讓 get_position() 回傳已被縮成正方形的框
    bb = ax.get_position(original=True)
    return (bb.width * fig.get_figwidth()) / (bb.height * fig.get_figheight())


def fit(ax, fig, cx, cy, half_y):
    """以 (cx, cy) 為中心、縱向半寬 half_y 公里，橫向依軸框比例展開。"""
    a = axes_aspect(fig, ax)
    ax.set_xlim(cx - half_y * a, cx + half_y * a)
    ax.set_ylim(cy - half_y, cy + half_y)


def fit_points(ax, fig, xs, ys, pad):
    a = axes_aspect(fig, ax)
    cx, cy = (min(xs) + max(xs)) / 2, (min(ys) + max(ys)) / 2
    half = max((max(ys) - min(ys)) / 2, (max(xs) - min(xs)) / 2 / a) * 1.12 + pad
    fit(ax, fig, cx, cy, half)
    return cx, cy, half, a


def draw_roads(ax, pj, rows):
    groups = collections.defaultdict(list)
    for r in rows:
        groups[r["highway"]].append(pj.seg(r["lat_a"], r["lon_a"], r["lat_b"], r["lon_b"]))
    styled = set()
    for kinds, color, lw in ROAD_STYLE:
        segs = [s for k in kinds for s in groups.get(k, [])]
        styled |= kinds
        ax.add_collection(LineCollection(segs, colors=color, linewidths=lw, zorder=2,
                                         capstyle="round"))
    minor = [s for k, v in groups.items() if k not in styled for s in v]
    ax.add_collection(LineCollection(minor, colors=MINOR_STYLE[0], linewidths=MINOR_STYLE[1],
                                     zorder=1))


def draw_event(ax, x=0.0, y=0.0):
    ax.scatter([x], [y], s=900, color=ACC, alpha=0.18, zorder=8, linewidths=0)
    ax.scatter([x], [y], s=170, color=ACC, edgecolors="white", linewidths=2.2, zorder=9)


def box(ax, half, color, ls="-", lw=2.2, alpha=1.0):
    ax.add_patch(Rectangle((-half, -half), 2 * half, 2 * half, fill=False, ec=color,
                           ls=ls, lw=lw, alpha=alpha, zorder=6))


def label(ax, text, xy, xytext, color=INK, size=12):
    ax.annotate(text, xy=xy, xytext=xytext, fontsize=size, color=color, zorder=10,
                ha="left", va="center",
                bbox=dict(boxstyle="round,pad=0.35", fc="white", ec="#DCE4E6", alpha=0.95),
                arrowprops=dict(arrowstyle="-", color=color, lw=1.2))


def scale_bar(ax, km=1.0):
    x0, x1 = ax.get_xlim(); y0, y1 = ax.get_ylim()
    sx, sy = x0 + (x1 - x0) * 0.04, y0 + (y1 - y0) * 0.05
    ax.plot([sx, sx + km], [sy, sy], color=INK, lw=2.5, zorder=11, solid_capstyle="butt")
    ax.text(sx + km / 2, sy + (y1 - y0) * 0.022, f"{km:g} 公里", ha="center", fontsize=11,
            color=INK, zorder=11)


def legend(ax, items, loc="lower right"):
    handles = [Line2D([0], [0], color=c, lw=lw, ls=ls, marker=m, markersize=9,
                      markerfacecolor=c, markeredgecolor="white") for _, c, lw, ls, m in items]
    lg = ax.legend(handles, [t for t, *_ in items], loc=loc, fontsize=11, frameon=True,
                   framealpha=0.95, edgecolor="#DCE4E6")
    lg.set_zorder(12)


def panel(fig, items, x=0.635, y=0.80):
    """右側說明欄：items = [(小標, 內文), ...]，內文以換行分行（每行約 18 個中文字）。"""
    for head, body in items:
        fig.text(x, y, head, fontsize=12.5, color=MUTED, va="top")
        y -= 0.042
        fig.text(x, y, body, fontsize=15.5, color=INK, va="top", linespacing=1.45)
        y -= 0.052 * (body.count("\n") + 1) + 0.035


def save(fig, name):
    path = os.path.join(OUT, name)
    fig.savefig(path, dpi=150, facecolor="white")
    plt.close(fig)
    print("  ", path)


# ─────────────────────────── 系統計算（只讀） ───────────────────────────
def affected_edges(con, router, lat, lon, radius_km):
    """與 apply_event_incremental 相同的選取：起點落在 ±r 方框內、且在路網圖中的路段。"""
    dlat = radius_km / 110.574
    dlon = radius_km / (111.320 * math.cos(math.radians(lat)))
    rows = con.execute(
        "SELECT edge_id, highway, lat_a, lon_a, lat_b, lon_b FROM osm_edges "
        "WHERE lat_a BETWEEN ? AND ? AND lon_a BETWEEN ? AND ?",
        (lat - dlat, lat + dlat, lon - dlon, lon + dlon)).fetchall()
    return [r for r in rows if r["edge_id"] in router._edge_index]


def apply_in_memory(router, edge_ids, pred_s, etype):
    """apply_event_incremental 的非封路分支，只改記憶體、不寫 DB。回傳 (舊, 新) Edge。"""
    mult = INCIDENT_MULT.get(etype, 1.25)
    ra = RISK_ADDITION.get(etype, 2) * pred_s
    pairs = []
    for eid in edge_ids:
        fn, idx = router._edge_index[eid]
        e = router._graph[fn][idx]
        orig = (e.dist / max(e.spd, MIN_SPEED)) * 60
        cur = e.cost / orig if orig > 0 else 1.0
        final = max(cur, pred_s * mult)
        new_spd = max(e.spd / max(final, 0.01), MIN_SPEED)
        ne = e._replace(cost=(e.dist / new_spd) * 60, risk=e.risk + ra)
        router._graph[fn][idx] = ne
        router._modified_edges.add(eid)
        pairs.append((e, ne))
    return pairs


def timed_route(router, sn, en):
    t0 = time.perf_counter()
    res = router.route(sn, en)
    return res, time.perf_counter() - t0


def snap(con, name, lat, lon):
    d = 0.05
    rows = con.execute(
        "SELECT lat_a, lon_a FROM osm_edges WHERE name=? AND lat_a BETWEEN ? AND ? "
        "AND lon_a BETWEEN ? AND ?", (name, lat - d, lat + d, lon - d, lon + d)).fetchall()
    best = min(rows, key=lambda r: (r["lat_a"] - lat) ** 2 + (r["lon_a"] - lon) ** 2)
    return best["lat_a"], best["lon_a"]


# ─────────────────────────── 六張圖 ───────────────────────────
def fig1_event(con, pj, road_name):
    fig = new_fig("① 事件發生：國道一號新竹寶山段發生車禍",
                  "系統收到的只有「事件類型 + 位置」，影響多大要自己判斷")
    ax = map_axes(fig)
    fit(ax, fig, 0, 0, 2.3)
    draw_roads(ax, pj, roads_in(con, pj, ax.get_xlim()[1] + 0.3, 2.6))
    draw_event(ax)
    label(ax, f"車禍\n{road_name}", (0, 0), (-2.0, 1.1), ACC, 13)
    scale_bar(ax)
    legend(ax, [("國道、快速道路", ROAD_STYLE[0][1], 3, "-", None),
                ("省道、縣道", ROAD_STYLE[1][1], 1.8, "-", None),
                ("巷弄（底圖）", MINOR_STYLE[0], 1.5, "-", None)])
    t = EVENT["type"]
    panel(fig, [
        ("事件類型", f"車禍：基礎影響範圍 {BASE_RADIUS[t]} 公里\n基礎嚴重度 {BASE_SEVERITY[t]:.2f}"),
        ("事件來源", "管理員手動、TDX 即時路況\n或 T-GCN 預測，都走同一條路"),
        ("下一步", "依這個地點的道路特性，\n預測實際的影響範圍"),
    ])
    save(fig, "1_事件發生.png")


def fig2_predict(con, pj, pred, nearest):
    r_km, s, f = pred
    t = EVENT["type"]
    fig = new_fig("② 看三件事，算出這個地點的影響範圍",
                  "依事件所在道路的重要性、附近的替代道路、路口的分流能力，調整影響範圍與嚴重度")
    ax = map_axes(fig)
    fit(ax, fig, 0, 0, 1.6)
    draw_roads(ax, pj, roads_in(con, pj, ax.get_xlim()[1] + 0.3, 1.9))
    box(ax, 1.0, MUTED, ls=(0, (5, 4)), lw=1.6)
    box(ax, r_km, ACC, lw=2.6)
    nx, ny = pj.xy(nearest["lat"], nearest["lon"])
    ax.scatter([nx], [ny], s=110, color=MAIN, edgecolors="white", linewidths=1.8, zorder=9)
    draw_event(ax)
    label(ax, f"1 公里內 {f['n_edges_1km']:,} 條路段", (0.98, 0.85), (1.15, 1.32), MUTED)
    label(ax, f"最近的路口連 {f['node_degree']} 條路", (nx, ny), (nx + 0.35, ny - 0.55), MAIN)
    label(ax, f"預測範圍 ±{r_km:.2f} 公里", (r_km, -r_km), (r_km + 0.25, -r_km - 0.3), ACC)
    scale_bar(ax, 0.5)
    panel(fig, [
        ("這條路多重要", f"道路關鍵度 {f['omega']:.2f}\n（國道 1.00、省道 0.65、巷弄 0.15）"),
        ("附近有沒有別條路", f"1 公里內 {f['n_edges_1km']:,} 條路段\n替代道路密度 {f['kappa']:.2f}（滿分 1.00）"),
        ("路口能不能分流", f"最近路口連 {f['node_degree']} 條路\n分流能力 {f['delta']:.2f}（滿分 1.00）"),
        ("結果", f"影響範圍 ±{r_km:.2f} 公里、嚴重度 {s:.2f}\n"
                f"（基礎 {BASE_RADIUS[t]} 公里、{BASE_SEVERITY[t]:.2f}）"),
    ], y=0.80)
    save(fig, "2_預測影響範圍.png")


def fig3_update(con, pj, pred, pairs, rows):
    r_km, s, _ = pred
    t = EVENT["type"]
    mult, ra = s * INCIDENT_MULT[t], RISK_ADDITION[t] * s
    fig = new_fig(f"③ 只改範圍內的 {len(pairs):,} 條路段，其餘路網一條都不碰",
                  "用位置索引直接取出方框內的路段，在記憶體裡改它們的行車時間與風險")
    ax = map_axes(fig)
    fit(ax, fig, 0, 0, 1.6)
    draw_roads(ax, pj, roads_in(con, pj, ax.get_xlim()[1] + 0.3, 1.9))
    hit = [pj.seg(r["lat_a"], r["lon_a"], r["lat_b"], r["lon_b"]) for r in rows]
    ax.add_collection(LineCollection(hit, colors=ACC, linewidths=3.2, zorder=5,
                                     capstyle="round"))
    box(ax, r_km, ACC, lw=1.4, alpha=0.6)
    draw_event(ax)
    scale_bar(ax, 0.5)
    legend(ax, [("成本已更新的路段", ACC, 3.2, "-", None),
                ("未受影響的路段", ROAD_STYLE[0][1], 2.4, "-", None)])
    by_hw = collections.Counter(HW_NAME.get(r["highway"].replace("_link", ""), "其他")
                                for r in rows)
    mix = "、".join(f"{k} {v}" for k, v in by_hw.most_common(4))
    old, new = max(pairs, key=lambda p: p[0].dist if p[0].hw == "motorway" else 0)
    panel(fig, [
        ("更新了哪些路段", f"共 {len(pairs):,} 條：{mix}\n巷弄不在找路用的主幹路網內"),
        ("行車時間", f"× {mult:.2f}\n嚴重度 {s:.2f} × 車禍乘數 {INCIDENT_MULT[t]:.2f}"),
        ("風險分數", f"+ {ra:.1f}（車禍 {RISK_ADDITION[t]} 分 × 嚴重度）"),
        ("舉一段國道為例", f"{old.dist * 1000:.0f} 公尺：{old.cost * 60:.1f} 秒 → "
                       f"{new.cost * 60:.1f} 秒"),
        ("一次更新服務三種模式", "時段與路線模式在找路時才代入"),
    ], y=0.80)
    save(fig, "3_更新範圍內路段.png")


def fig4_links(con, pj, router, rows, link_keys, n_links):
    fig = new_fig(f"④ 這些路段屬於 {n_links} 段路口之間的路，只重算這 {n_links} 段",
                  "切割路網把兩個路口之間收成一段；事件改到哪幾條路段，就只重算它們所在的那幾段")
    ax = map_axes(fig)
    link_segs, ends = [], []
    for j, idx in link_keys:
        lk = router._links[j][idx]
        for e in router._edges_of(lk.edge_ids) or []:
            link_segs.append(pj.seg(e.la, e.loa, e.lb, e.lob))
        for nid in (lk.frm, lk.to):
            ends.append(pj.xy(*router._ncoords[nid]))
    xs = [p[0] for s in link_segs for p in s]; ys = [p[1] for s in link_segs for p in s]
    cx, cy, half, a = fit_points(ax, fig, xs, ys, 0.3)
    draw_roads(ax, pj, roads_in(con, pj, abs(cx) + half * a + 0.3, abs(cy) + half + 0.3,
                                major_only=True))
    ax.add_collection(LineCollection(link_segs, colors=MAIN, linewidths=5.5, alpha=0.55,
                                     zorder=4, capstyle="round"))
    hit = [pj.seg(r["lat_a"], r["lon_a"], r["lat_b"], r["lon_b"]) for r in rows]
    ax.add_collection(LineCollection(hit, colors=ACC, linewidths=2.6, zorder=5))
    ax.scatter([p[0] for p in ends], [p[1] for p in ends], s=55, color=MAIN,
               edgecolors="white", linewidths=1.5, zorder=7)
    draw_event(ax)
    scale_bar(ax, 1.0)
    legend(ax, [("需要重算的路口之間的路", MAIN, 5, "-", None),
                ("事件直接改到的路段", ACC, 2.6, "-", None),
                ("路口", MAIN, 0, "", "o")])
    total = sum(len(v) for v in router._links.values())
    panel(fig, [
        ("切割路網是什麼", "兩個路口之間的形狀點收成一段，\nA* 一步就跨過一整段"),
        ("事件發生時", f"{len(rows):,} 條路段查表，找到所屬的\n{n_links} 段，重算總時間與總風險"),
        ("其餘", f"全台主幹路網共 {total:,} 段，\n其餘全部不動"),
        ("為什麼延伸到範圍外", "一段從路口算到下一個路口，\n碰到其中一部分就整段重算"),
    ], y=0.80)
    save(fig, "4_同步切割路網.png")


def fig5_route(con, pj, before, after, t_after, r_km):
    fig = new_fig("⑤ 重新搜尋：路線自動避開事故路段",
                  "在已更新的路網上從頭找一次，不是修補舊路線")
    ax = map_axes(fig)
    ids_b = {e["edge_id"] for e in before.edges}; ids_a = {e["edge_id"] for e in after.edges}
    diff = [e for e in before.edges if e["edge_id"] not in ids_a] + \
           [e for e in after.edges if e["edge_id"] not in ids_b]
    pts = [pj.xy(e["lat_a"], e["lon_a"]) for e in diff] + [(0.0, 0.0)]
    cx, cy, half, a = fit_points(ax, fig, [p[0] for p in pts], [p[1] for p in pts], 1.0)
    draw_roads(ax, pj, roads_in(con, pj, abs(cx) + half * a + 0.5, abs(cy) + half + 0.5,
                                major_only=True))
    for res, color, lw, ls, z in ((before, ROUTE_OLD, 4.0, (0, (4, 3)), 5),
                                  (after, MAIN, 4.5, "-", 6)):
        segs = [pj.seg(e["lat_a"], e["lon_a"], e["lat_b"], e["lon_b"]) for e in res.edges]
        ax.add_collection(LineCollection(segs, colors=color, linewidths=lw, linestyles=ls,
                                         zorder=z, capstyle="round"))
    box(ax, r_km, ACC, lw=1.8)
    draw_event(ax)
    scale_bar(ax, 2.0)
    legend(ax, [("事件前的路線", ROUTE_OLD, 4, (0, (4, 3)), None),
                ("事件後的路線", MAIN, 4.5, "-", None),
                ("事件位置", ACC, 0, "", "o")])
    panel(fig, [
        ("路線", "台北 → 台中，平衡模式"),
        ("事件前", f"{before.total_km:.2f} 公里、{before.total_min:.1f} 分鐘"),
        ("事件後", f"{after.total_km:.2f} 公里、{after.total_min:.1f} 分鐘"),
        ("重新搜尋耗時", f"{t_after:.2f} 秒（切割路網）"),
        ("導航中", "前端每 15 秒檢查一次事件，\n落在剩餘路線上就從目前位置重算"),
    ], y=0.80)
    save(fig, "5_路線反應.png")


def fig6_compare(con, router):
    fig = new_fig("同一場車禍，影響範圍依道路的重要性而不同",
                  "三張圖比例尺相同；實線方框為系統預測的影響範圍")
    results = []
    for i, (title, name, lat, lon) in enumerate(COMPARE):
        lat, lon = snap(con, name, lat, lon)
        r_km, s, f = router._predict_impact(lat, lon, EVENT["type"], BASE_SEVERITY[EVENT["type"]])
        pj = Proj(lat, lon)
        x = 0.04 + i * 0.32
        ax = map_axes(fig, (x, 0.27, 0.28, 0.55))
        fit(ax, fig, 0, 0, 1.15)
        draw_roads(ax, pj, roads_in(con, pj, ax.get_xlim()[1] + 0.3, 1.45))
        box(ax, r_km, ACC, lw=2.6)
        draw_event(ax)
        scale_bar(ax, 0.5)
        fig.text(x, 0.215, title, fontsize=15.5, fontweight="bold", color=INK)
        fig.text(x, 0.16, f"影響範圍 ±{r_km:.2f} 公里", fontsize=16, color=ACC, fontweight="bold")
        fig.text(x, 0.108, f"道路關鍵度 {f['omega']:.2f}　嚴重度 {s:.2f}", fontsize=12.5, color=INK)
        fig.text(x, 0.068, f"1 公里內 {f['n_edges_1km']:,} 條路段", fontsize=12.5, color=MUTED)
        results.append((title, lat, lon, r_km, s, f))
    save(fig, "6_同一場車禍不同地點.png")
    return results


def draw_influence(ax, pj, router, infl, lw=3.4, alpha=1.0):
    """依影響強度著色畫出傳播到的路段（單一色相，越深越強）。"""
    cmap = plt.get_cmap("Oranges")
    segs, cols = [], []
    for eid, x in sorted(infl.items(), key=lambda kv: kv[1]):
        fn, idx = router._edge_index[eid]
        e = router._graph[fn][idx]
        segs.append(pj.seg(e.la, e.loa, e.lb, e.lob))
        cols.append(cmap(0.3 + 0.7 * x))
    ax.add_collection(LineCollection(segs, colors=cols, linewidths=lw, zorder=5,
                                     capstyle="round", alpha=alpha))
    return segs


def _polyline(pj, edges):
    """edges 已依往上游的順序排（第一條最靠近事件）；回傳由下游端往上游的折線與各段長度。"""
    pts = [pj.xy(edges[0].lb, edges[0].lob)] + [pj.xy(e.la, e.loa) for e in edges]
    lens = [math.dist(p, q) for p, q in zip(pts, pts[1:])]
    return pts, lens


def propagation_paths(router, pj, lat, lon, infl):
    """把傳播到的路段整理成「從事件點往上游」的折線，供畫箭頭。

    回傳 [(折線, 各段長度, 各段 edge_id, d0)]，d0 是折線起點（下游端）到事件點的路網距離。
    傳播的數值仍取自 edge_influence；這裡只重建走向：由事件 link 出發，沿「匯入」關係
    往上游做最短距離搜尋，每條 link 只取一次。
    """
    paths, dist, heap, reached = [], {}, [], set()

    def reach(node, d):
        if d < dist.get(node, math.inf):
            dist[node] = d
            heapq.heappush(heap, (d, node))

    for seed, k in router._find_seeds(lat, lon):
        j, i = router._link_keys[seed]
        lk = router._links[j][i]
        up = list(reversed(router._edges_of(lk.edge_ids[:k]) or []))
        d_frm = 0.0
        if up:
            pts, lens = _polyline(pj, up)
            paths.append((pts, lens, [e.eid for e in up], 0.0))
            d_frm = sum(lens)
        reach(lk.frm, d_frm)
        (idx, vals), _ = router._influence(seed)
        scale = math.exp(-sum(e.dist for e in up) / OP.DECAY_KM)
        reached |= {n for n, v in zip(idx.tolist(), vals.tolist()) if v * scale >= OP.PUSH_TOL}

    into = collections.defaultdict(list)
    for n in reached:
        j, i = router._link_keys[n]
        into[router._links[j][i].to].append(router._links[j][i])
    done = set()
    while heap:
        d, v = heapq.heappop(heap)
        if v in done:
            continue
        done.add(v)
        for lk in into.get(v, ()):
            edges = list(reversed(router._edges_of(lk.edge_ids) or []))
            if not edges or edges[0].eid not in infl:
                continue
            pts, lens = _polyline(pj, edges)
            paths.append((pts, lens, [e.eid for e in edges], d))
            reach(lk.frm, d + sum(lens))
    return paths


def _point_at(pts, lens, t):
    """折線上距起點 t 公里的點，與該點所在的段號。"""
    for n, seg in enumerate(lens):
        if t <= seg or n == len(lens) - 1:
            f = min(t / seg, 1.0) if seg > 0 else 0.0
            (x0, y0), (x1, y1) = pts[n], pts[n + 1]
            return (x0 + (x1 - x0) * f, y0 + (y1 - y0) * f), n
        t -= seg
    return pts[-1], len(lens) - 1


def draw_arrows(ax, paths, infl, spacing, length):
    """沿傳播方向每隔 spacing 公里畫一個箭頭（指向上游、與車流相反）。

    位置以「到事件點的路網距離」對齊，岔開的支路各自接續，不會在路口擠成一團；
    箭頭顏色與大小都隨影響強度，越往上游越淡越小。回傳 [(箭頭尖端, 路網距離, edge_id)]。
    """
    cmap = plt.get_cmap("Oranges")
    out = []
    for pts, lens, eids, d0 in paths:
        total = sum(lens)
        if total <= 0:
            continue
        g = max(1, math.ceil(d0 / spacing)) * spacing     # 第一個箭頭離事件點一個間距，不壓在標記上
        while g < d0 + total:
            t = g - d0
            _, n = _point_at(pts, lens, t)
            x = infl.get(eids[n])
            if x is None:                        # 已超出傳播範圍，再往上游只會更弱
                break
            a = min(max(t - length / 2, 0.0), max(total - length, 0.0))
            tail, _ = _point_at(pts, lens, a)
            tip, _ = _point_at(pts, lens, min(a + length, total))
            ann = ax.annotate("", xy=tip, xytext=tail, zorder=7,
                              arrowprops=dict(arrowstyle="-|>,head_length=0.55,head_width=0.32",
                                              color=cmap(0.4 + 0.6 * x), lw=1.4 + 1.6 * x,
                                              mutation_scale=11 + 13 * x, shrinkA=0, shrinkB=0))
            ann.arrow_patch.set_clip_path(ax.patch)      # 地圖邊緣的箭頭不畫出框外
            out.append((tip, g, eids[n]))
            g += spacing
    return out


def zoom_inset(fig, ax, con, pj, router, infl, paths, zh, rect=(0.5, 0.48, 0.48, 0.5)):
    """事件周圍的放大圖：路網密的地方影響分成許多短支路，放大才看得出各自的方向。"""
    ins = ax.inset_axes(rect)
    ins.set_zorder(20)
    ins.set_xticks([]); ins.set_yticks([])
    ins.set_facecolor("#FBFCFC")
    for s in ins.spines.values():
        s.set_color(MUTED); s.set_linewidth(1.2)
    a = axes_aspect(fig, ax) * rect[2] / rect[3]
    ins.set_xlim(-zh * a, zh * a); ins.set_ylim(-zh, zh)
    draw_roads(ins, pj, roads_in(con, pj, zh * a + 0.1, zh + 0.1))
    draw_influence(ins, pj, router, infl, lw=3.0, alpha=0.4)
    draw_arrows(ins, paths, infl, spacing=0.2, length=0.12)
    draw_event(ins)
    scale_bar(ins, 0.5)
    mag = (ax.get_ylim()[1] - ax.get_ylim()[0]) * rect[3] / (2 * zh)
    ins.text(0.03, 0.96, f"事件周圍放大 {mag:.1f} 倍", transform=ins.transAxes, fontsize=11.5,
             color=INK, va="top", zorder=12,
             bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="none", alpha=0.9))
    ax.indicate_inset_zoom(ins, edgecolor=MUTED)


def label_branches(ax, router, arrows):
    """在南北兩側最遠的箭頭旁標出是哪個方向的車流在回堵（由路段行進方向判斷）。"""
    for side in (1, -1):
        cand = [a for a in arrows if a[0][1] * side > 0]
        if not cand:
            continue
        (x, y), _, eid = max(cand, key=lambda a: a[1])
        fn, idx = router._edge_index[eid]
        e = router._graph[fn][idx]
        lanes = "南下" if e.lb < e.la else "北上"
        ax.annotate(f"{lanes}車道的車流\n回堵往{'北' if side > 0 else '南'}延伸",
                    xy=(x, y), xytext=(x + 0.9, y - 0.15 * side), fontsize=12, color=INK,
                    ha="left", va="center", zorder=10,
                    bbox=dict(boxstyle="round,pad=0.35", fc="white", ec="#DCE4E6", alpha=0.95),
                    arrowprops=dict(arrowstyle="-", color=MUTED, lw=1.0))


def influence_colorbar(fig, rect):
    from matplotlib.colorbar import ColorbarBase
    from matplotlib.colors import LinearSegmentedColormap, Normalize
    base = plt.get_cmap("Oranges")
    cmap = LinearSegmentedColormap.from_list("infl", [base(0.3 + 0.7 * t / 10) for t in range(11)])
    cax = fig.add_axes(rect)
    cb = ColorbarBase(cax, cmap=cmap, norm=Normalize(0, 1), orientation="horizontal")
    cb.set_ticks([0, 0.5, 1])
    cb.ax.tick_params(labelsize=11, colors=INK)
    cb.outline.set_edgecolor("#DCE4E6")
    cax.set_title("影響強度（事件點 = 1）", fontsize=11.5, color=MUTED, loc="left")


def fig7_propagation(con, pj, router, infl, info, t_first, t_hit, r_km):
    reach = _reach(router, EVENT["lat"], EVENT["lon"], infl)
    fig = new_fig("依計畫書演算法：事件沿路網往上游傳播",
                  "原方框法照常保留；再把影響沿上游傳播鏈推送，每段衰減、遇岔路依車道數分流")
    ax = map_axes(fig)
    segs = draw_influence(ax, pj, router, infl, lw=2.6, alpha=0.4)
    pts = [p for s in segs for p in s] + [(0.0, 0.0)]
    cx, cy, half, a = fit_points(ax, fig, [p[0] for p in pts], [p[1] for p in pts], 0.6)
    draw_roads(ax, pj, roads_in(con, pj, abs(cx) + half * a + 0.5, abs(cy) + half + 0.5,
                                major_only=True))
    paths = propagation_paths(router, pj, EVENT["lat"], EVENT["lon"], infl)
    label_branches(ax, router, draw_arrows(ax, paths, infl, spacing=0.6, length=0.34))
    box(ax, r_km, MAIN, lw=2.0)
    draw_event(ax)
    scale_bar(ax, 1.0)
    legend(ax, [("原方框法的範圍", MAIN, 2, "-", None),
                ("影響傳播方向（往上游）", ACC, 2, "-", ">")], loc="upper left")
    influence_colorbar(fig, [0.635, 0.115, 0.27, 0.022])
    panel(fig, [
        ("計畫書的對應", "事件 = 活動節點，路口 = 核心節點\n用增量 PageRank 殘差推送往上游傳"),
        ("這場車禍", f"{info['links_reached']} 段路口之間的路、{len(infl)} 條路段\n"
                    f"最遠 {reach:.1f} 公里（實測回堵 p75 6.43 公里）"),
        ("直接相依索引", f"第一次計算 {t_first * 1000:.0f} 毫秒，存入索引\n"
                       f"同一地點再發生：查表 {t_hit * 1000:.1f} 毫秒"),
        ("箭頭方向", "與車流相反，往上游；兩個車道各自傳"),
    ], y=0.80)
    save(fig, "7_事件沿上游傳播.png")


def _reach(router, lat, lon, infl):
    best = 0.0
    for eid in infl:
        fn, idx = router._edge_index[eid]
        e = router._graph[fn][idx]
        best = max(best, OP.haversine_km(lat, lon, e.la, e.loa))
    return best


def fig8_compare_propagation(con, router):
    fig = new_fig("路網密的地方影響很快分散，單一道路則傳得遠",
                  "同一場車禍、同一套演算法、相同比例尺；箭頭為傳播方向，越大越深代表影響越強")
    # 市區的影響只到 1.2 公里、分成許多短支路：視野往右上移，讓出右上角放大圖（比例尺不變）
    places = [("市區省道・台北忠孝東路", 25.04154, 121.54445, (1.5, 0.9)),
              ("蘇花公路", 24.20800, 121.67259, None)]
    for i, (title, lat, lon, shift) in enumerate(places):
        infl, info = router.edge_influence(lat, lon)
        pj = Proj(lat, lon)
        x = 0.04 + i * 0.48
        ax = map_axes(fig, (x, 0.25, 0.44, 0.57))
        fit(ax, fig, *(shift or (0.0, 0.0)), 3.2)
        (x0, x1), (y0, y1) = ax.get_xlim(), ax.get_ylim()
        draw_roads(ax, pj, roads_in(con, pj, max(-x0, x1) + 0.3, max(-y0, y1) + 0.3))
        draw_influence(ax, pj, router, infl, lw=2.4, alpha=0.4)
        paths = propagation_paths(router, pj, lat, lon, infl)
        draw_arrows(ax, paths, infl, spacing=0.45, length=0.26)
        draw_event(ax)
        scale_bar(ax, 1.0)
        if shift:
            zoom_inset(fig, ax, con, pj, router, infl, paths, zh=0.65)
        strong = sum(1 for v in infl.values() if v >= math.exp(-1))
        fig.text(x, 0.205, title, fontsize=16, fontweight="bold", color=INK)
        fig.text(x, 0.15, f"最遠 {_reach(router, lat, lon, infl):.1f} 公里", fontsize=16,
                 color=ACC, fontweight="bold")
        fig.text(x, 0.098, f"擴散到 {info['links_reached']} 段路、{len(infl)} 條路段；"
                           f"影響強度 0.37 以上 {strong} 條", fontsize=12.5, color=INK)
    influence_colorbar(fig, [0.72, 0.87, 0.2, 0.018])
    save(fig, "8_傳播比較_市區與單一道路.png")


def main():
    os.makedirs(OUT, exist_ok=True)
    osm_router._tw_hour = lambda: FIG_HOUR       # 時段係數與夜間風險都經由這個函式取時間
    router = OP.PropagationRouter(DB)
    router.load_graph(verbose=True)
    # 與驗證測試相同：不併入端點局部圖，結果只取決於主幹路網
    router._load_local_graph = lambda bbox, pad=0.005: (collections.defaultdict(list), None)
    router.build_link_graph(verbose=True)
    router.build_propagation_index(verbose=True)
    con = ro_conn()
    lat, lon, etype = EVENT["lat"], EVENT["lon"], EVENT["type"]
    pj = Proj(lat, lon)
    t0 = time.perf_counter(); infl, info = router.edge_influence(lat, lon)
    t_first = time.perf_counter() - t0
    hits = []
    for _ in range(7):                       # 查表只有一毫秒上下，單次計時受雜訊影響大，取中位數
        t0 = time.perf_counter(); router.edge_influence(lat, lon); hits.append(time.perf_counter() - t0)
    t_hit = sorted(hits)[len(hits) // 2]

    pred = router._predict_impact(lat, lon, etype, BASE_SEVERITY[etype])
    r_km, s, f = pred
    near = router.nearest_node(lat, lon)
    road = con.execute("SELECT name FROM osm_edges WHERE highway='motorway' AND lat_a BETWEEN ? "
                       "AND ? AND lon_a BETWEEN ? AND ? AND name!='' LIMIT 1",
                       (lat - .003, lat + .003, lon - .003, lon + .003)).fetchone()
    print(f"預測：半徑 {r_km:.3f} km、嚴重度 {s:.3f}、{f}")

    sn = router.nearest_node(*START)["node_id"]; en = router.nearest_node(*END)["node_id"]
    before, t_before = timed_route(router, sn, en)
    rows = affected_edges(con, router, lat, lon, r_km)
    pairs = apply_in_memory(router, [r["edge_id"] for r in rows], s, etype)
    link_keys = {router._edge2link[r["edge_id"]] for r in rows if r["edge_id"] in router._edge2link}
    n_links = router._refresh_links([r["edge_id"] for r in rows])
    after, t_after = timed_route(router, sn, en)
    print(f"路段 {len(pairs)}、link {n_links}；事件前 {before.total_km:.2f} km {before.total_min:.1f} 分"
          f"（{t_before:.2f}s）→ 事件後 {after.total_km:.2f} km {after.total_min:.1f} 分（{t_after:.2f}s）")

    print("輸出：")
    fig1_event(con, pj, road["name"] if road else "國道一號")
    fig2_predict(con, pj, pred, near)
    fig3_update(con, pj, pred, pairs, rows)
    fig4_links(con, pj, router, rows, link_keys, n_links)
    fig5_route(con, pj, before, after, t_after, r_km)
    for title, la, lo, rr, ss, ff in fig6_compare(con, router):
        print(f"  比較 {title} ({la:.5f}, {lo:.5f})：半徑 {rr:.2f} km、嚴重度 {ss:.2f}、{ff}")
    fig7_propagation(con, pj, router, infl, info, t_first, t_hit, r_km)
    fig8_compare_propagation(con, router)


if __name__ == "__main__":
    main()
