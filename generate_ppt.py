#!/usr/bin/env python3
"""generate_ppt.py — 畢專報告 PPT（完全根據目前系統程式碼）"""
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE

LIGHT = RGBColor(0xF4, 0xF4, 0xF4)
DARK  = RGBColor(0x00, 0x46, 0x51)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
BLACK = RGBColor(0x1A, 0x1A, 0x1A)
GRAY  = RGBColor(0x66, 0x66, 0x66)
LG    = RGBColor(0xBB, 0xBB, 0xBB)
ACC   = RGBColor(0x00, 0xAA, 0xCC)
GRN   = RGBColor(0x00, 0xFF, 0xAA)
RED88 = RGBColor(0xFF, 0x88, 0x88)
GRN88 = RGBColor(0x88, 0xFF, 0x88)
TBLH  = RGBColor(0x00, 0x3A, 0x44)
TBLR1 = RGBColor(0x00, 0x55, 0x63)
TBLR2 = RGBColor(0x00, 0x4A, 0x58)
BOXBG = RGBColor(0x00, 0x5A, 0x6A)
FONT  = "Microsoft JhengHei"

prs = Presentation()
prs.slide_width  = Inches(20)
prs.slide_height = Inches(11.25)
pn = [0]

# ── helpers ──────────────────────────────────────────────

def bg(s, dark=True):
    f = s.background.fill; f.solid(); f.fore_color.rgb = DARK if dark else LIGHT

def pgn(s):
    pn[0] += 1
    t = s.shapes.add_textbox(Inches(18.5), Inches(10.3), Inches(1.2), Inches(0.6))
    p = t.text_frame.paragraphs[0]; p.text = str(pn[0])
    p.font.size = Pt(14); p.font.name = FONT; p.alignment = PP_ALIGN.RIGHT
    p.font.color.rgb = LG if s.background.fill.fore_color.rgb == DARK else GRAY

def tx(s, l, t, w, h, txt, sz=20, c=WHITE, b=False, al=PP_ALIGN.LEFT):
    tb = s.shapes.add_textbox(Inches(l), Inches(t), Inches(w), Inches(h))
    tf = tb.text_frame; tf.word_wrap = True
    p = tf.paragraphs[0]; p.text = txt; p.font.size = Pt(sz); p.font.color.rgb = c
    p.font.name = FONT; p.font.bold = b; p.alignment = al
    return tf

def ap(tf, txt, sz=20, c=WHITE, b=False, sp=8):
    p = tf.add_paragraph(); p.text = txt; p.font.size = Pt(sz); p.font.color.rgb = c
    p.font.name = FONT; p.font.bold = b; p.space_before = Pt(sp)
    return p

def tbl(s, l, t, w, h, data, cw=None, fs=16):
    nr, nc = len(data), len(data[0])
    sh = s.shapes.add_table(nr, nc, Inches(l), Inches(t), Inches(w), Inches(h))
    tb = sh.table
    if cw:
        for i, ww in enumerate(cw): tb.columns[i].width = Inches(ww)
    for r in range(nr):
        for ci in range(nc):
            cell = tb.cell(r, ci); cell.text = data[r][ci]
            cell.vertical_anchor = MSO_ANCHOR.MIDDLE
            for pp in cell.text_frame.paragraphs:
                pp.font.size = Pt(fs); pp.font.name = FONT; pp.alignment = PP_ALIGN.CENTER
                pp.font.bold = r == 0; pp.font.color.rgb = WHITE
            cell.fill.solid()
            cell.fill.fore_color.rgb = TBLH if r == 0 else (TBLR1 if r % 2 else TBLR2)

def sec(title):
    s = prs.slides.add_slide(prs.slide_layouts[6]); bg(s, False)
    tx(s, 2, 4.0, 16, 2, title, 48, BLACK, True, PP_ALIGN.CENTER); pgn(s); return s

def csl(title):
    s = prs.slides.add_slide(prs.slide_layouts[6]); bg(s)
    tx(s, 1.0, 0.5, 14, 1, title, 36, WHITE, True); pgn(s); return s

def box(s, l, t, w, h):
    b = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(l), Inches(t), Inches(w), Inches(h))
    b.fill.solid(); b.fill.fore_color.rgb = BOXBG; b.line.color.rgb = ACC; b.line.width = Pt(2)
    return b

# ============================================================
# 1  封面
# ============================================================
s = prs.slides.add_slide(prs.slide_layouts[6]); bg(s, False)
tx(s, 2, 2.5, 16, 2, "台灣 OSM 動態路網導航系統", 52, BLACK, True, PP_ALIGN.CENTER)
tx(s, 2, 5.0, 16, 1, "基於增量式預測模型的即時路徑規劃", 28, GRAY, al=PP_ALIGN.CENTER)
tx(s, 2, 7.0, 16, 1, "OpenStreetMap × TDX 交通資料 × CWA 氣象署", 22, GRAY, al=PP_ALIGN.CENTER)
pn[0] += 1

# ============================================================
# 2  大綱
# ============================================================
s = csl("大綱")
tf = tx(s, 1.5, 1.8, 10, 8, "", 26); tf.paragraphs[0].text = ""
items = ["一、介紹與研究動機", "二、系統架構", "三、執行流程圖",
         "四、核心成本公式", "五、各項成本因子（事件／天氣／夜間／轉彎／號誌）",
         "六、增量式事件預測模型（重點）", "七、效能對比實驗",
         "八、現場 Demo", "九、結論與未來方向"]
for item in items:
    ap(tf, f"  {item}", 26, WHITE, sp=14)

# ============================================================
# 3  section
# ============================================================
sec("介紹與研究動機")

# ============================================================
# 4  研究動機
# ============================================================
s = csl("介紹與研究動機")
tf = tx(s, 1.5, 1.8, 8.5, 5, "", 22); tf.paragraphs[0].text = ""
ap(tf, "問題", 26, ACC, True)
ap(tf, "• Google Maps 路線計算為黑箱，公式不透明", 20, LG)
ap(tf, "• 無法自由選擇「最快」或「最安全」模式", 20, LG)
ap(tf, "• 台灣 TDX 車速偵測、CWA 氣象資料未整合進路線規劃", 20, LG)
ap(tf, "• 事件更新後，路線要多久才反映？不知道", 20, LG)
ap(tf, "", 12)
ap(tf, "我們的目標", 26, ACC, True)
ap(tf, "• 公式完全透明、可解釋的導航系統", 20, LG)
ap(tf, "• 整合 TDX 1,361 個車速偵測器 + CWA 700+ 氣象站", 20, LG)
ap(tf, "• 三種路線模式：fastest / balanced / safest", 20, LG)
ap(tf, "• 事件新增 ~20ms 內即時更新路線（增量預測）", 20, LG)

tf2 = tx(s, 11, 2.5, 7.5, 7, "", 20); tf2.paragraphs[0].text = ""
ap(tf2, "vs Google Maps", 24, ACC, True)
ap(tf2, "", 10)
ap(tf2, "✗ Google 公式不公開", 18, RED88)
ap(tf2, "✓ 我們公式 100% 透明（原始碼開放）", 18, GRN88)
ap(tf2, "", 10)
ap(tf2, "✗ Google 無天氣成本", 18, RED88)
ap(tf2, "✓ 我們整合降雨/風速/能見度/颱風警報", 18, GRN88)
ap(tf2, "", 10)
ap(tf2, "✗ Google 無風險偏好", 18, RED88)
ap(tf2, "✓ 我們提供三種模式（risk 權重 0.10/0.40/0.90）", 18, GRN88)
ap(tf2, "", 10)
ap(tf2, "✗ Google 事件更新時間不明", 18, RED88)
ap(tf2, "✓ 我們 ~20ms 增量更新，即時生效", 18, GRN88)

# ============================================================
# 5  section
# ============================================================
sec("系統架構")

# ============================================================
# 6  系統架構
# ============================================================
s = csl("系統架構")
layers = [
    ("index.html — 前端地圖", "Leaflet.js 單檔 SPA，點擊/拖曳設定起終點", 2.0),
    ("osm_api.py — FastAPI 伺服器", "13 個 REST 端點，座標驗證 (lat 21.5~26.5, lon 118~122.5)", 3.8),
    ("osm_router.py — A* 路由引擎", "預載主幹路網到記憶體，A* + haversine 啟發式搜尋", 5.6),
    ("taiwan_osm.db — SQLite", "376 萬節點 / 765 萬邊 / WAL 模式 / timeout=30", 7.4),
]
for label, desc, top in layers:
    b = box(s, 1.5, top, 9, 1.1)
    tf = b.text_frame; tf.word_wrap = True
    p = tf.paragraphs[0]; p.text = label; p.font.size = Pt(20); p.font.color.rgb = ACC
    p.font.name = FONT; p.font.bold = True; p.alignment = PP_ALIGN.CENTER
    p2 = tf.add_paragraph(); p2.text = desc; p2.font.size = Pt(14); p2.font.color.rgb = LG
    p2.font.name = FONT; p2.alignment = PP_ALIGN.CENTER
for y in [3.3, 5.1, 6.9]:
    tx(s, 5.5, y, 1, 0.4, "▼", 20, LG, al=PP_ALIGN.CENTER)

tx(s, 12, 2.0, 7, 0.5, "外部即時資料來源", 24, ACC, True)
tf = tx(s, 12, 2.7, 7, 6, "", 18); tf.paragraphs[0].text = ""
ap(tf, "realtime_sync.py", 18, WHITE, True)
ap(tf, "", 6)
ap(tf, "TDX — 車速偵測（VD）", 18, ACC, True)
ap(tf, "• 19 縣市 × 1,361 站，OAuth2 認證", 16, LG)
ap(tf, "• 實測車速 < 自由流速 70% → 壅塞事件", 16, LG)
ap(tf, "• 城市間呼叫間隔 1.5 秒（避免 429）", 16, LG)
ap(tf, "", 8)
ap(tf, "TDX — 路況新聞", 18, ACC, True)
ap(tf, "• News/Highway 端點，依 NewsCategory 分類", 16, LG)
ap(tf, "• accident / construction / closure / congestion", 16, LG)
ap(tf, "", 8)
ap(tf, "CWA — 氣象站 + 雨量站", 18, ACC, True)
ap(tf, "• 自動氣象站 700+ / 雨量站 400+", 16, LG)
ap(tf, "• 降雨正規化 /80mm, 風速 (v-8)/22", 16, LG)
ap(tf, "• 累積雨量 ≥200mm → 山崩偵測三級", 16, LG)
ap(tf, "", 8)
ap(tf, "同步間隔：預設 300 秒（AUTO_SYNC_INTERVAL）", 16, WHITE, True)

# ============================================================
# 7  section
# ============================================================
sec("執行流程圖")

# ============================================================
# 8  流程圖
# ============================================================
s = csl("系統執行流程")
flow_top = [
    ("載入路網", "load_graph()\n主幹 motorway→tertiary", 0.8),
    ("資料同步", "TDX VD + News\nCWA 氣象 + 雨量", 4.6),
    ("使用者輸入", "起終點 + 模式\nfastest/balanced/safest", 8.4),
    ("nearest_node", "擴大搜尋最近\n路網節點", 12.2),
    ("成本計算", "8 項因子\n建立邊成本", 16.0),
]
flow_bot = [
    ("A* 搜尋", "haversine 啟發式\nmax 2M iterations", 16.0),
    ("路線回傳", "GeoJSON + 分析報告\n+ 替代路線", 12.2),
    ("事件/天氣", "手動或 TDX/CWA\n新增事件", 8.4),
    ("增量預測", "三因子模型\n~20ms 更新圖", 4.6),
    ("即時重路由", "下次 route()\n自動反映", 0.8),
]
for label, desc, left in flow_top:
    b = box(s, left, 2.2, 3.2, 1.6)
    tf = b.text_frame; tf.word_wrap = True
    p = tf.paragraphs[0]; p.text = label; p.font.size = Pt(22); p.font.color.rgb = WHITE
    p.font.name = FONT; p.font.bold = True; p.alignment = PP_ALIGN.CENTER
    p2 = tf.add_paragraph(); p2.text = desc; p2.font.size = Pt(13); p2.font.color.rgb = LG
    p2.font.name = FONT; p2.alignment = PP_ALIGN.CENTER
for label, desc, left in flow_bot:
    b = box(s, left, 6.5, 3.2, 1.6)
    tf = b.text_frame; tf.word_wrap = True
    p = tf.paragraphs[0]; p.text = label; p.font.size = Pt(22); p.font.color.rgb = WHITE
    p.font.name = FONT; p.font.bold = True; p.alignment = PP_ALIGN.CENTER
    p2 = tf.add_paragraph(); p2.text = desc; p2.font.size = Pt(13); p2.font.color.rgb = LG
    p2.font.name = FONT; p2.alignment = PP_ALIGN.CENTER

tx(s, 1.1, 9.2, 17.5, 1.2,
   "圖的上半部是一次性啟動流程；下半部是持續運行的動態更新迴圈。"
   "事件透過 apply_event_incremental() 直接修改記憶體中的邊成本，不需重算整張圖。",
   20, WHITE)

# ============================================================
# 9  section
# ============================================================
sec("核心成本公式")

# ============================================================
# 10  公式總覽
# ============================================================
s = csl("每條路的成本怎麼算？")
tx(s, 1.0, 1.8, 18, 1.2,
   "edge_cost = adj_time × ( time_weight + risk × risk_weight ) + turn_penalty + signal_delay",
   26, ACC, True)
tx(s, 1.5, 3.5, 17, 1,
   "adj_time = ( dist_km / ( speed × time_factor ) ) × 60 分鐘", 22, WHITE)
tx(s, 1.5, 4.5, 17, 1,
   "risk = dynamic_risk + night_risk", 22, WHITE)

tx(s, 1.5, 6.0, 7, 0.5, "三種模式的權重", 24, ACC, True)
tbl(s, 1.5, 6.7, 7, 2.0, [
    ["模式", "time_weight", "risk_weight"],
    ["fastest（最快）", "1.0", "0.10"],
    ["balanced（平衡）", "1.0", "0.40"],
    ["safest（最安全）", "1.0", "0.90"],
], cw=[2.5, 2.25, 2.25], fs=16)

tf = tx(s, 10, 6.0, 8, 3, "", 18); tf.paragraphs[0].text = ""
ap(tf, "實際效果", 22, ACC, True)
ap(tf, "", 6)
ap(tf, "fastest：幾乎忽略風險，選最快路", 18, LG)
ap(tf, "balanced：時間和風險各半考量", 18, LG)
ap(tf, "safest：大幅懲罰風險路段，願意繞遠路", 18, LG)
ap(tf, "", 8)
ap(tf, "risk_weight 差 9 倍（0.10 vs 0.90）", 16, WHITE, True)
ap(tf, "→ 有事件時路線差異明顯", 16, LG)

# ============================================================
# 11  成本因子 — 道路速度 + 時段
# ============================================================
s = csl("因子 ①②：道路等級速度 + 尖峰時段")
tx(s, 1.5, 1.8, 8, 0.5, "① HIGHWAY_SPEEDS（km/h）", 22, ACC, True)
tbl(s, 1.5, 2.5, 7.5, 4.5, [
    ["道路等級", "速度", "說明"],
    ["motorway", "110", "國道"],
    ["trunk", "90", "快速道路"],
    ["primary", "60", "省道"],
    ["secondary", "50", "縣道"],
    ["tertiary", "40", "鄉道"],
    ["residential", "30", "住宅區"],
    ["living_street", "10", "行人優先"],
], cw=[2.5, 1.5, 3.5], fs=15)

tx(s, 10.5, 1.8, 8, 0.5, "② TIME_SPEED_MULT（時段乘數）", 22, ACC, True)
tbl(s, 10.5, 2.5, 8, 4.5, [
    ["道路 \\ 時段", "離峰", "早尖峰 7-9", "晚尖峰 17-19", "日間"],
    ["motorway", "×1.00", "×0.85", "×0.85", "×0.92"],
    ["trunk", "×1.00", "×0.80", "×0.80", "×0.88"],
    ["primary", "×1.00", "×0.70", "×0.70", "×0.82"],
    ["secondary", "×1.00", "×0.65", "×0.65", "×0.78"],
    ["tertiary", "×1.00", "×0.62", "×0.62", "×0.75"],
], cw=[2.0, 1.5, 1.5, 1.5, 1.5], fs=14)

tx(s, 1.5, 7.5, 17, 1.5,
   "尖峰時段 tertiary 降速至 62%（40→24.8 km/h），motorway 僅降至 85%（110→93.5 km/h）\n"
   "→ 尖峰時段系統更傾向走國道（相對降速少）", 20, LG)

# ============================================================
# 12  事件成本
# ============================================================
s = csl("因子 ③：事件影響（INCIDENT_MULT）")
tx(s, 1.5, 1.8, 17, 0.8,
   "事件透過速度乘數降低通行速度 + 風險加分讓系統傾向繞道", 22, LG)
tbl(s, 1.5, 3.0, 9, 4.5, [
    ["事件類型", "速度乘數", "降速幅度", "風險加分"],
    ["accident 事故", "×1.80", "降速 44%", "+8 × severity"],
    ["construction 施工", "×1.55", "降速 35%", "+5 × severity"],
    ["congestion 壅塞", "×1.35", "降速 26%", "+3 × severity"],
    ["manual 手動", "×1.25", "降速 20%", "+2 × severity"],
    ["closure 封路", "×999", "完全不走", "+100"],
    ["landslide_high", "×3.00", "降速 67%", "+10 × severity"],
    ["landslide_warning", "×1.50", "降速 33%", "+6 × severity"],
], cw=[2.5, 2.0, 2.0, 2.5], fs=15)

tf = tx(s, 11.5, 3.0, 7, 5.5, "", 18); tf.paragraphs[0].text = ""
ap(tf, "數值來源", 22, ACC, True)
ap(tf, "", 6)
ap(tf, "• FHWA：事故造成延遲 +25%~200%", 16, LG)
ap(tf, "  → accident ×1.80 取中間值", 14, GRAY)
ap(tf, "", 6)
ap(tf, "• FHWA：施工區容量降 20~50%", 16, LG)
ap(tf, "  → construction ×1.55", 14, GRAY)
ap(tf, "", 6)
ap(tf, "• VD 壅塞判斷：車速 < 自由流速 70%", 16, LG)
ap(tf, "  → congestion ×1.35", 14, GRAY)
ap(tf, "", 6)
ap(tf, "• 水保局山崩風險三級閾值", 16, LG)
ap(tf, "  累積雨量 200/350/600mm", 14, GRAY)

# ============================================================
# 13  天氣成本
# ============================================================
s = csl("因子 ④：天氣影響")
tx(s, 1.5, 1.8, 17, 0.8,
   "天氣乘數 = 1.0 + 0.40×rain + 0.20×wind + 0.35×visibility + 0.30×warning", 22, ACC, True)
tbl(s, 1.5, 3.0, 9.5, 3.5, [
    ["因子", "權重", "最大影響", "正規化方式", "來源"],
    ["rain 降雨", "0.40", "+40% 成本", "實測 mm / 80mm", "CWA 雨量站"],
    ["wind 風速", "0.20", "+20% 成本", "(v − 8) / 22 m/s", "CWA 氣象站"],
    ["visibility", "0.35", "+35% 成本", "分級 0~1", "CWA 氣象站"],
    ["warning 警報", "0.30", "+30% 成本", "有警報=1", "CWA"],
], cw=[1.8, 1.2, 1.5, 2.5, 2.5], fs=14)

tf = tx(s, 12, 3.0, 7, 5, "", 18); tf.paragraphs[0].text = ""
ap(tf, "天氣風險加分", 20, ACC, True)
ap(tf, "", 6)
ap(tf, "risk_weather = 0.6×rain + 0.35×wind", 16, LG)
ap(tf, "             + 0.6×vis + 0.5×warning", 16, LG)
ap(tf, "", 10)
ap(tf, "山崩偵測（山區測站 lat ≥ 23.0°N）", 20, ACC, True)
ap(tf, "", 6)
ap(tf, "• 累積雨量 ≥200mm → landslide_warning", 16, LG)
ap(tf, "• 累積雨量 ≥350mm → landslide_high", 16, LG)
ap(tf, "• 累積雨量 ≥600mm → landslide_closure", 16, LG)

tx(s, 1.5, 7.5, 17, 1.5,
   "四項天氣同時最大時：成本 +125%（1.0 + 0.40 + 0.20 + 0.35 + 0.30）\n"
   "降雨影響最大，因為研究顯示每增加 1mm/h 降雨，車速降低機率增加 5.8%（Iowa State）", 18, LG)

# ============================================================
# 14  夜間 + 轉彎 + 號誌
# ============================================================
s = csl("因子 ⑤⑥⑦：夜間風險 / 轉彎延遲 / 號誌延遲")

tx(s, 1.0, 1.8, 5.5, 0.5, "⑤ NIGHT_RISK", 22, ACC, True)
tbl(s, 1.0, 2.5, 5.5, 3.5, [
    ["時段", "風險值", "說明"],
    ["0:00 ~ 4:00", "+0.25", "深夜最危險"],
    ["4:00 ~ 6:00", "+0.15", "凌晨"],
    ["6:00 ~ 7:00", "+0.05", "天剛亮"],
    ["7:00 ~ 17:00", "0.00", "白天"],
    ["17:00 ~ 19:00", "+0.05", "黃昏"],
    ["19:00 ~ 24:00", "+0.15", "夜間"],
], cw=[1.8, 1.5, 2.2], fs=14)

tx(s, 7.0, 1.8, 5.5, 0.5, "⑥ TURN_PENALTY_MIN", 22, ACC, True)
tbl(s, 7.0, 2.5, 5.5, 3.0, [
    ["轉彎類型", "角度差", "延遲（分）"],
    ["straight 直行", "< 20°", "0.00"],
    ["slight 微轉", "20~60°", "0.05（3 秒）"],
    ["turn 轉彎", "60~130°", "0.17（10 秒）"],
    ["sharp 急轉", "130~170°", "0.25（15 秒）"],
    ["uturn 迴轉", "> 170°", "0.42（25 秒）"],
], cw=[1.8, 1.5, 2.2], fs=14)

tx(s, 13.0, 1.8, 6, 0.5, "⑦ SIGNAL_DELAY_MIN", 22, ACC, True)
tf = tx(s, 13.0, 2.5, 6, 4, "", 18); tf.paragraphs[0].text = ""
ap(tf, "每個紅綠燈 +0.33 分鐘（≈20 秒）", 18, LG)
ap(tf, "", 8)
ap(tf, "• 高速公路/快速道路免計", 16, LG)
ap(tf, "  （motorway, motorway_link,", 14, GRAY)
ap(tf, "   trunk, trunk_link）", 14, GRAY)
ap(tf, "", 8)
ap(tf, "• 號誌資料來源：", 16, LG)
ap(tf, "  osm_nodes.is_signal = 1", 14, GRAY)
ap(tf, "  （traffic_signals tag）", 14, GRAY)
ap(tf, "", 8)
ap(tf, "城市幹道號誌佔行程 20~40%", 16, LG)
ap(tf, "來源：HCM Ch.19", 14, GRAY)

tx(s, 1.0, 7.8, 18, 1.5,
   "夜間風險在 safest 模式下影響最大（risk_weight=0.90）：深夜 +0.25 × 0.90 = 每段多 +22.5% 風險成本\n"
   "NHTSA 2008：夜間佔 25% 里程但造成 50% 死亡事故", 18, LG)

# ============================================================
# 15  section — 增量預測模型
# ============================================================
sec("增量式事件預測模型")

# ============================================================
# 16  為什麼需要增量
# ============================================================
s = csl("為什麼需要增量預測？")
tx(s, 1.5, 2.0, 17, 1.2,
   "舊方法：recompute_dynamic_cost() — 掃描全台 765 萬條邊，UPDATE 每一條", 24, WHITE, True)
tx(s, 1.5, 3.5, 17, 0.8,
   "問題：全量掃描 7.6M 邊要 ~4.3 秒，且需等待 TDX/CWA 網路同步（~60 秒）", 22, RED88)

tx(s, 1.5, 5.0, 17, 1.2,
   "新方法：apply_event_incremental() — 只修改事件影響範圍內的邊，直接改記憶體", 24, WHITE, True)
tx(s, 1.5, 6.5, 17, 0.8,
   "結果：~20ms 完成，事件新增後下次 route() 立刻反映", 22, GRN88)

tx(s, 1.5, 8.0, 17, 1.5,
   "但是：影響範圍多大？嚴重度多高？\n"
   "→ 不能用固定值 — 台北市區車禍 vs 蘇花公路車禍，影響天差地遠\n"
   "→ 需要根據事件地點的「道路特性」來預測", 22, ACC)

# ============================================================
# 17  自動嚴重度
# ============================================================
s = csl("BASE_SEVERITY：嚴重度自動決定")
tx(s, 1.5, 1.8, 17, 0.8,
   "使用者只需選「事件類型 + 地點」— 嚴重度和影響範圍全部由模型計算", 22, LG)

tx(s, 1.5, 3.0, 8, 0.5, "第一步：查 BASE_SEVERITY 取基礎值", 22, ACC, True)
tbl(s, 1.5, 3.7, 7.5, 4.0, [
    ["事件類型", "基礎嚴重度", "代碼對應"],
    ["accident 車禍", "0.80", "BASE_SEVERITY['accident']"],
    ["construction 施工", "0.50", "BASE_SEVERITY['construction']"],
    ["closure 封路", "1.00", "BASE_SEVERITY['closure']"],
    ["congestion 壅塞", "0.60", "BASE_SEVERITY['congestion']"],
    ["manual 手動", "0.50", "BASE_SEVERITY['manual']"],
    ["landslide_high", "0.85", "BASE_SEVERITY['landslide_high']"],
], cw=[2.5, 2.0, 3.0], fs=14)

tx(s, 10.5, 3.0, 8, 0.5, "同時查 BASE_RADIUS 取基礎影響範圍", 22, ACC, True)
tbl(s, 10.5, 3.7, 7.5, 4.0, [
    ["事件類型", "基礎半徑 (km)", "代碼對應"],
    ["accident 車禍", "0.80", "BASE_RADIUS['accident']"],
    ["construction 施工", "0.50", "BASE_RADIUS['construction']"],
    ["closure 封路", "1.50", "BASE_RADIUS['closure']"],
    ["congestion 壅塞", "1.00", "BASE_RADIUS['congestion']"],
    ["manual 手動", "0.50", "BASE_RADIUS['manual']"],
    ["landslide_high", "1.50", "BASE_RADIUS['landslide_high']"],
], cw=[2.5, 2.0, 3.0], fs=14)

tx(s, 1.5, 8.0, 17, 0.5, "第二步：_predict_impact() 用三因子（ω κ δ）調整 severity 和 radius", 20, ACC, True)

tx(s, 1.5, 8.5, 17, 1,
   "add_event() 時 severity=None → 自動查 BASE_SEVERITY → _predict_impact() 調整 → 寫回 DB",
   18, WHITE, True)

# ============================================================
# 18  三因子詳解
# ============================================================
s = csl("三因子預測模型：ω、κ、δ")

tx(s, 1.0, 1.8, 5.8, 0.6, "ω — 道路關鍵度 ROAD_CRITICALITY", 20, ACC, True)
tbl(s, 1.0, 2.6, 5.8, 3.5, [
    ["道路等級", "ω 值", "意義"],
    ["motorway", "1.00", "最關鍵，無替代"],
    ["trunk", "0.85", "快速道路"],
    ["primary", "0.65", "省道幹線"],
    ["secondary", "0.45", "縣道"],
    ["tertiary", "0.30", "鄉道，替代多"],
    ["residential", "0.15", "巷弄，影響小"],
], cw=[1.8, 1.0, 3.0], fs=14)

tx(s, 7.2, 1.8, 5.8, 0.6, "κ — 替代路線密度", 20, ACC, True)
tf = tx(s, 7.2, 2.6, 5.8, 3.5, "", 16); tf.paragraphs[0].text = ""
ap(tf, "κ = min(1, edges_in_1km / 200)", 16, WHITE, True)
ap(tf, "", 6)
ap(tf, "1km bbox 內的邊數 → 衡量有多少替代路", 14, LG)
ap(tf, "", 8)
ap(tf, "台北市區：~350+ 邊 → κ = 1.0", 14, LG)
ap(tf, "  非常多替代路，影響被壓低", 12, GRAY)
ap(tf, "", 6)
ap(tf, "蘇花公路：~15 邊 → κ = 0.08", 14, LG)
ap(tf, "  幾乎沒有替代路，影響放大", 12, GRAY)
ap(tf, "", 6)
ap(tf, "新竹郊區：~80 邊 → κ = 0.40", 14, LG)
ap(tf, "  有一些替代路", 12, GRAY)

tx(s, 13.4, 1.8, 5.8, 0.6, "δ — 節點連通度", 20, ACC, True)
tf2 = tx(s, 13.4, 2.6, 5.8, 3.5, "", 16); tf2.paragraphs[0].text = ""
ap(tf2, "δ = min(1, degree / 8)", 16, WHITE, True)
ap(tf2, "", 6)
ap(tf2, "最近路口有幾條路分出去", 14, LG)
ap(tf2, "", 8)
ap(tf2, "大十字路口：degree=6 → δ=0.75", 14, LG)
ap(tf2, "  車流容易分散，影響小", 12, GRAY)
ap(tf2, "", 6)
ap(tf2, "直線路段：degree=2 → δ=0.25", 14, LG)
ap(tf2, "  一堵就完全堵住", 12, GRAY)
ap(tf2, "", 6)
ap(tf2, "死巷：degree=1 → δ=0.125", 14, LG)
ap(tf2, "  完全沒有分流", 12, GRAY)

tx(s, 1.0, 6.5, 18, 0.5, "學術依據", 20, ACC, True)
tf_ref = tx(s, 1.0, 7.1, 18, 1.2, "", 12); tf_ref.paragraphs[0].text = ""
ap(tf_ref, "ω：Jenelius et al. (2006) 'Importance and Exposure in Road Network Vulnerability Analysis' — 高等級道路中斷造成更大旅行成本增加", 12, GRAY)
ap(tf_ref, "κ：Allen et al. (2024) 'Network Redundancy Reduces Criticality' — 替代路線密度直接降低路段中斷嚴重性", 12, GRAY)
ap(tf_ref, "δ：Ganin et al. (2017) Science Advances — 節點度數越高的路網韌性越強", 12, GRAY)
ap(tf_ref, "組合：ASCE (2018) 'Modeling Framework for Affected Area' — 事件影響範圍由道路等級 + 替代路徑 + 分流能力三因子決定", 12, GRAY)

tx(s, 1.0, 8.5, 18, 1.0, "", 18)
tf3 = tx(s, 1.0, 8.5, 18, 1.0, "", 18); tf3.paragraphs[0].text = ""
ap(tf3, "預測公式", 22, ACC, True)
ap(tf3, "predicted_radius  = BASE_RADIUS[type] × (1 + ω) × (1 − 0.5κ)     predicted_severity = BASE_SEVERITY[type] × (1 + ω × (1−κ) × (1−δ))", 16, WHITE, True)

# ============================================================
# 19  地點對比
# ============================================================
s = csl("同樣車禍，不同地點 → 預測結果不同")
tx(s, 1.5, 1.8, 17, 0.8,
   "以 accident（BASE_SEVERITY=0.80, BASE_RADIUS=0.8km）為例", 22, LG)
tbl(s, 1.0, 3.0, 18, 4.0, [
    ["地點", "ω 道路關鍵度", "κ 替代密度", "δ 連通度", "預測半徑 km", "預測嚴重度", "放大幅度"],
    ["台北市區（省道）", "0.65", "1.00（350邊）", "0.75", "0.74", "0.80", "不變"],
    ["新竹郊區（巷弄）", "0.15", "0.40（80邊）", "0.50", "0.72", "0.84", "+5%"],
    ["蘇花公路（省道）", "0.65", "0.08（15邊）", "0.25", "1.26", "1.16", "+45%"],
    ["國道山區（國道）", "1.00", "0.15（30邊）", "0.25", "1.51", "1.31", "+64%"],
], cw=[2.5, 2.0, 2.5, 2.0, 2.0, 2.0, 2.0], fs=14)

tx(s, 1.5, 7.5, 17, 2,
   "直覺解讀：\n"
   "• 台北市區 — 旁邊一堆路可繞，影響範圍小，嚴重度維持不變\n"
   "• 蘇花公路 — 幾乎沒有替代路線，影響範圍從 0.8km 放大到 1.26km，嚴重度 +45%\n"
   "• 國道山區 — 道路最關鍵且替代少，影響範圍近乎翻倍，嚴重度 +64%", 18, LG)

# ============================================================
# 20  增量更新流程
# ============================================================
s = csl("apply_event_incremental() 運作流程")
tf = tx(s, 1.5, 1.8, 17, 8, "", 18); tf.paragraphs[0].text = ""
ap(tf, "1. add_event(type, lat, lon)", 20, ACC, True)
ap(tf, "   severity=None → 自動查 BASE_SEVERITY 表", 16, LG)
ap(tf, "   INSERT INTO dynamic_events，回傳 event_id", 16, LG)
ap(tf, "", 10)
ap(tf, "2. apply_event_incremental(event_id)", 20, ACC, True)
ap(tf, "   a. 從 DB 讀出事件 → 呼叫 _predict_impact(lat, lon, type, severity)", 16, LG)
ap(tf, "   b. 計算 ω（查附近最高等級道路）、κ（1km 內邊數 / 200）、δ（最近節點 degree / 8）", 16, LG)
ap(tf, "   c. 得到 predicted_radius 和 predicted_severity", 16, LG)
ap(tf, "   d. 用 predicted_radius 建 bbox，查 osm_edges 取得受影響的邊", 16, LG)
ap(tf, "   e. 把 predicted_severity 和 predicted_radius 寫回 DB（UPDATE dynamic_events）", 16, LG)
ap(tf, "   f. 在記憶體 _graph 中直接修改每條受影響邊的 cost 和 risk", 16, LG)
ap(tf, "      closure → cost=1e8, risk=100（等同封路）", 14, GRAY)
ap(tf, "      其他 → new_speed = speed / (severity × INCIDENT_MULT), risk += RISK_ADDITION × severity", 14, GRAY)
ap(tf, "   g. 記錄到 _modified_edges（方便後續還原）", 16, LG)
ap(tf, "", 10)
ap(tf, "3. 下次 route() 呼叫時", 20, ACC, True)
ap(tf, "   A* 搜尋直接讀到已修改的邊成本，路線自動繞開事件區域", 16, LG)
ap(tf, "   整個過程 ~20ms，使用者幾乎無感", 16, LG)
ap(tf, "", 10)
ap(tf, "舊方法 recompute_dynamic_cost() 保留供天氣使用（天氣影響範圍大，需全量掃描）", 16, GRAY)

# ============================================================
# 21~24  截圖佔位
# ============================================================
screenshots = [
    ("場景一：台北→桃園機場（封路）",
     "國道五股段封路 → 路線從 49.6km 變為 63.0km（+13.5km / +8.4min）"),
    ("場景二：台北→台中（封路）",
     "國道中段封路 → 路線從 172.1km 變為 192.2km（+20.1km / +23.5min）"),
    ("場景三：台中市區（封路）",
     "建國路封路 → 成本增加 72%（1.7km 路程多花 1.5 分鐘）"),
    ("場景四：台北→基隆（車禍）",
     "基隆路車禍 → 路線多繞 0.3km（+1.0min）"),
]
for title, caption in screenshots:
    s = prs.slides.add_slide(prs.slide_layouts[6]); bg(s)
    tx(s, 1.0, 0.5, 14, 1, title, 32, WHITE, True)
    b = box(s, 2, 2.0, 16, 7.5)
    tx(s, 7, 5, 6, 1, "[ 截圖放這裡 ]", 28, LG, al=PP_ALIGN.CENTER)
    tx(s, 2, 10.0, 16, 0.8, caption, 20, ACC)
    pgn(s)

# ============================================================
# 25  section
# ============================================================
sec("效能對比實驗")

# ============================================================
# 26  效能數據
# ============================================================
s = csl("增量預測 vs 全量重算")
tbl(s, 1.5, 2.0, 9, 3.5, [
    ["比較項目", "增量預測（新）", "全量重算（舊）"],
    ["方法", "apply_event_incremental()", "recompute_dynamic_cost()"],
    ["計算方式", "記憶體直接修改 _graph", "SQL UPDATE 7.6M rows"],
    ["計算路段數", "~1,100 條", "7,600,000 條"],
    ["花費時間", "~20 ms", "~4,300 ms"],
    ["加速倍率", "~218×", "基準"],
], cw=[3.0, 3.0, 3.0], fs=16)

b = box(s, 12, 2.0, 6.5, 3.5)
b.line.width = Pt(3)
tf = b.text_frame; tf.word_wrap = True
p = tf.paragraphs[0]; p.text = "快 ~218 倍"; p.font.size = Pt(52)
p.font.color.rgb = GRN; p.font.bold = True; p.font.name = FONT; p.alignment = PP_ALIGN.CENTER
p2 = tf.add_paragraph(); p2.text = "20ms vs 4.3秒"; p2.font.size = Pt(24)
p2.font.color.rgb = LG; p2.font.name = FONT; p2.alignment = PP_ALIGN.CENTER
p3 = tf.add_paragraph(); p3.text = "POST /benchmark/recompute"; p3.font.size = Pt(14)
p3.font.color.rgb = GRAY; p3.font.name = FONT; p3.alignment = PP_ALIGN.CENTER

tx(s, 1.5, 6.0, 17, 0.6, "實測路線驗證（事件前 vs 事件後）", 24, ACC, True)
tbl(s, 1.5, 6.8, 17, 3, [
    ["場景", "事件前", "事件後", "差異"],
    ["台北→桃園機場（closure）", "49.6 km / 37.2 min", "63.0 km / 45.6 min", "+13.5 km / +8.4 min"],
    ["台北→台中（closure）", "172.1 km / 111.8 min", "192.2 km / 135.3 min", "+20.1 km / +23.5 min"],
    ["台中市區（closure）", "1.7 km / 2.3 min", "1.7 km / 3.8 min", "成本 +72%"],
    ["台北→基隆（accident）", "23.7 km / 17.3 min", "24.0 km / 18.3 min", "+0.3 km / +1.0 min"],
], cw=[3.5, 3.5, 3.5, 3.5], fs=14)

# ============================================================
# 27  section
# ============================================================
sec("現場 Demo")

# ============================================================
# 28  結論
# ============================================================
s = csl("結論")
tf = tx(s, 1.5, 1.8, 9, 7, "", 22); tf.paragraphs[0].text = ""
ap(tf, "已完成", 26, ACC, True)
ap(tf, "", 8)
ap(tf, "• 8 項成本因子（全部有學術或政府資料依據）", 18, LG)
ap(tf, "  速度、時段、事件、天氣、夜間、轉彎、號誌、山崩", 14, GRAY)
ap(tf, "", 6)
ap(tf, "• 增量預測模型 — ~20ms 即時更新（快 ~218 倍）", 18, LG)
ap(tf, "  三因子自動判斷影響範圍和嚴重度", 14, GRAY)
ap(tf, "", 6)
ap(tf, "• 嚴重度自動決定（BASE_SEVERITY + 三因子調整）", 18, LG)
ap(tf, "  使用者只選類型 + 地點，不需手動填嚴重度", 14, GRAY)
ap(tf, "", 6)
ap(tf, "• 整合 TDX（1,361 VD 站 + 路況新聞）", 18, LG)
ap(tf, "• 整合 CWA（700+ 氣象站 + 400+ 雨量站）", 18, LG)
ap(tf, "", 6)
ap(tf, "• 三種路線模式（fastest / balanced / safest）", 18, LG)
ap(tf, "• 替代路線、GPS 導航", 18, LG)

tf2 = tx(s, 11, 1.8, 7.5, 7, "", 22); tf2.paragraphs[0].text = ""
ap(tf2, "未來方向", 26, ACC, True)
ap(tf2, "", 8)
ap(tf2, "• 道路坡度成本（DEM 高程資料）", 18, LG)
ap(tf2, "", 6)
ap(tf2, "• VD 歷史流量模型（替代即時偵測）", 18, LG)
ap(tf2, "", 6)
ap(tf2, "• 路面品質（IRI 國際平整度指標）", 18, LG)
ap(tf2, "", 6)
ap(tf2, "• 淹水風險整合（水利署即時資料）", 18, LG)
ap(tf2, "", 6)
ap(tf2, "• 多起事件交互影響模型", 18, LG)

# ============================================================
# 29  參考文獻
# ============================================================
s = csl("參考文獻")
refs_cost = [
    "[1] FHWA / Highway Capacity Manual Ed. 7.1 (2025) — 事件延遲、號誌延遲",
    "[2] Agarwal et al. (2005) Weather Impact on Freeway — 降雨/車速關係",
    "[3] NHTSA (2008) Nighttime Driving — 25% miles = 50% fatalities",
    "[4] McGill University — Turn Delay: 15~25% of urban trip time",
    "[5] FHWA — Speed/Capacity for Work Zones — construction ×1.55",
    "[6] Tandfonline (2017) Landslide Risk Assessment in Taiwan",
    "[7] HCM Ch.19 — Signal Delay Function (Webster 1958)",
]
refs_model = [
    "[8] Jenelius et al. (2006) Importance & Exposure in Road Network Vulnerability — Transp. Res. A — ω 道路關鍵度",
    "[9] Scott et al. (2006) Network Robustness Index — J. Transport Geography — 道路等級與中斷影響",
    "[10] Allen et al. (2024) Network Redundancy Reduces Criticality — Transp. Res. Record — κ 替代路線密度",
    "[11] Ganin et al. (2017) Resilience & Efficiency in Transportation — Science Advances — δ 節點連通度",
    "[12] Sun & Qian (2019) Role of Road Network Features in Incident Impacts — Transp. Res. B — 拓撲指標預測事件影響",
    "[13] ASCE (2018) Modeling Framework for Affected Area — 三因子決定事件影響範圍",
    "[14] Mattsson & Jenelius (2015) Vulnerability and Resilience of Transport Systems — Transp. Res. A — 綜述",
]
refs_data = [
    "[15] TDX 運輸資料流通服務平台 — VD 車速偵測器 API",
    "[16] CWA 中央氣象署開放資料平台 — 自動氣象站/雨量站 API",
    "[17] 台灣水保局 — 累積雨量山崩風險三級閾值 (200/350/600mm)",
]
tf = tx(s, 0.8, 1.5, 8.5, 8.5, "", 12); tf.paragraphs[0].text = ""
ap(tf, "成本因子依據", 14, ACC, True)
for ref in refs_cost:
    ap(tf, ref, 11, LG, sp=4)
ap(tf, "", 6)
ap(tf, "資料來源", 14, ACC, True)
for ref in refs_data:
    ap(tf, ref, 11, LG, sp=4)

tf2 = tx(s, 10.0, 1.5, 9, 8.5, "", 12); tf2.paragraphs[0].text = ""
ap(tf2, "三因子預測模型依據", 14, ACC, True)
for ref in refs_model:
    ap(tf2, ref, 11, LG, sp=4)

# === Save ===
out = "demo_presentation_v2.pptx"
prs.save(out)
print(f"PPT saved: {out} ({len(prs.slides)} slides)")
