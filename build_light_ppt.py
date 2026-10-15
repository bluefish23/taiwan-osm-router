# -*- coding: utf-8 -*-
"""重做畢業專題簡報：自訂淺色版型、六章、28 頁。

內容取自 ppt.pptx（46 頁深色版，唯讀），圖片取自 scratchpad\\img（已抽出）
與 gcn/results 的原始 PNG。原始簡報不修改，輸出 demo_ppt_light.pptx。

版型：13.333x7.5 in、白底、微軟正黑體；主色 0E6070、強調色 C2571A。
教授若偏好標楷體，改 FONT 常數重跑即可。
"""
import math
import os

from pptx import Presentation
from pptx.chart.data import CategoryChartData
from pptx.dml.color import RGBColor
from pptx.enum.chart import XL_CHART_TYPE, XL_LABEL_POSITION
from pptx.enum.shapes import MSO_CONNECTOR, MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.oxml import parse_xml
from pptx.oxml.ns import nsdecls, qn
from pptx.util import Inches, Pt

OUT = r"D:\python\demo_ppt_light.pptx"
IMG = (r"C:\Users\bluef\AppData\Local\Temp\claude\d--python"
       r"\711e305b-c7bc-43f1-a6b9-7c6b8367c79f\scratchpad\img")
RES = r"D:\python\gcn\results"
VIDEO_URL = "https://youtu.be/-rbNGVx3yA0"     # 換影片改這一行

WHITE = RGBColor(0xFF, 0xFF, 0xFF)
PANEL = RGBColor(0xF2, 0xF6, 0xF7)
MAIN = RGBColor(0x0E, 0x60, 0x70)
ACC = RGBColor(0xC2, 0x57, 0x1A)
INK = RGBColor(0x1F, 0x2A, 0x2E)
MUTED = RGBColor(0x6B, 0x7C, 0x82)
LINE = RGBColor(0xCB, 0xD9, 0xDC)
FONT = "Microsoft JhengHei"
L, C = PP_ALIGN.LEFT, PP_ALIGN.CENTER

X0, X1 = 0.7, 12.63
CHAPTERS = ["一、介紹", "二、系統架構與流程", "三、路網切割與路徑搜尋",
            "四、交通預測模型訓練", "五、成果展示", "六、結論"]


class Deck:
    """淺色版型的版面元件。"""

    def __init__(self):
        self.prs = Presentation()
        self.prs.slide_width = Inches(13.333)
        self.prs.slide_height = Inches(7.5)
        self.layout = self.prs.slide_layouts[6]

    def blank(self):
        s = self.prs.slides.add_slide(self.layout)
        s.background.fill.solid()
        s.background.fill.fore_color.rgb = WHITE
        return s

    @staticmethod
    def _fmt(font, sz, c, b, name=FONT):
        font.size = Pt(sz)
        font.color.rgb = c
        font.bold = b
        font.name = name
        # python-pptx 只會寫 a:latin；中文要另外指定 a:ea，否則落到新細明體
        try:
            rPr = font._rPr
            ea = rPr.find(qn("a:ea"))
            if ea is None:
                ea = rPr.makeelement(qn("a:ea"), {})
                latin = rPr.find(qn("a:latin"))
                if latin is not None:
                    latin.addnext(ea)
                else:
                    rPr.append(ea)
            ea.set("typeface", name)
        except Exception:
            pass

    def tx(self, s, l, t, w, h, txt, sz=17, c=INK, b=False, al=L):
        tb = s.shapes.add_textbox(Inches(l), Inches(t), Inches(w), Inches(h))
        tf = tb.text_frame
        tf.word_wrap = True
        p = tf.paragraphs[0]
        p.text = txt
        p.alignment = al
        self._fmt(p.font, sz, c, b)
        return tf

    def ap(self, tf, txt, sz=17, c=INK, b=False, sp=8, al=L):
        p = tf.add_paragraph()
        p.text = txt
        p.alignment = al
        p.space_before = Pt(sp)
        self._fmt(p.font, sz, c, b)
        return p

    def rich(self, s, l, t, w, h, parts, sz=17, al=L):
        tb = s.shapes.add_textbox(Inches(l), Inches(t), Inches(w), Inches(h))
        tf = tb.text_frame
        tf.word_wrap = True
        p = tf.paragraphs[0]
        p.alignment = al
        for txt, c, b in parts:
            r = p.add_run()
            r.text = txt
            self._fmt(r.font, sz, c, b)
        return tf

    def title(self, s, txt, sz=30):
        self.tx(s, X0, 0.38, X1 - X0, 0.72, txt, sz, MAIN, True)
        self.line(s, X0, 1.16, X1, 1.16, MAIN, 2.5)

    def sub(self, s, txt, t=1.3, sz=18, c=INK):
        self.tx(s, X0 + 0.15, t, X1 - X0 - 0.3, 0.45, txt, sz, c, True)

    def bullets(self, s, l, t, w, items, sz=17, gap=14):
        """items: (小標, 說明) 或 (None, 整句)。"""
        tb = s.shapes.add_textbox(Inches(l), Inches(t), Inches(w), Inches(0.5))
        tf = tb.text_frame
        tf.word_wrap = True
        for i, (head, body) in enumerate(items):
            p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
            if i:
                p.space_before = Pt(gap)
            r = p.add_run()
            r.text = ("• " + head + "：") if head else "• "
            self._fmt(r.font, sz, MAIN, True)
            r2 = p.add_run()
            r2.text = body
            self._fmt(r2.font, sz, INK, False)
        return tf

    def tbl(self, s, l, t, w, h, data, cw=None, fs=14, hl=None):
        hl = hl or {}
        sh = s.shapes.add_table(len(data), len(data[0]),
                                Inches(l), Inches(t), Inches(w), Inches(h))
        tb = sh.table
        if cw:
            for i, ww in enumerate(cw):
                tb.columns[i].width = Inches(ww)
        for r, row in enumerate(data):
            for ci, val in enumerate(row):
                cell = tb.cell(r, ci)
                cell.text = val
                cell.vertical_anchor = MSO_ANCHOR.MIDDLE
                for pp in cell.text_frame.paragraphs:
                    pp.alignment = C
                    if r == 0:
                        self._fmt(pp.font, fs + 1, WHITE, True)
                    else:
                        self._fmt(pp.font, fs, hl.get(r, INK), r in hl)
                cell.fill.solid()
                cell.fill.fore_color.rgb = (MAIN if r == 0
                                            else (WHITE if r % 2 else PANEL))
        return tb

    def panel(self, s, l, t, w, h, fill=PANEL, line=None):
        b = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE,
                               Inches(l), Inches(t), Inches(w), Inches(h))
        b.adjustments[0] = 0.06
        b.fill.solid()
        b.fill.fore_color.rgb = fill
        if line is None:
            b.line.fill.background()
        else:
            b.line.color.rgb = line
            b.line.width = Pt(1.5)
        b.text_frame.word_wrap = True
        return b

    def notebox(self, s, l, t, w, h, head, lines, sz=14, hc=MAIN):
        b = self.panel(s, l, t, w, h, PANEL, hc)
        tf = b.text_frame
        tf.margin_left = tf.margin_right = Inches(0.18)
        tf.margin_top = Inches(0.12)
        p = tf.paragraphs[0]
        p.text = head
        p.alignment = L
        self._fmt(p.font, sz + 2, hc, True)
        for ln in lines:
            pp = tf.add_paragraph()
            pp.text = ln
            pp.alignment = L
            pp.space_before = Pt(5)
            self._fmt(pp.font, sz, INK, False)
        return b

    def flow(self, s, l, t, w, h, head, lines, hc=MAIN):
        b = self.panel(s, l, t, w, h, PANEL, hc)
        tf = b.text_frame
        tf.margin_left = tf.margin_right = Inches(0.08)
        tf.vertical_anchor = MSO_ANCHOR.MIDDLE
        p = tf.paragraphs[0]
        p.text = head
        p.alignment = C
        self._fmt(p.font, 15, hc, True)
        for ln in lines:
            pp = tf.add_paragraph()
            pp.text = ln
            pp.alignment = C
            pp.space_before = Pt(4)
            self._fmt(pp.font, 12, INK, False)
        return b

    def stat(self, s, l, t, w, h, head, value, vc=ACC):
        b = self.panel(s, l, t, w, h, PANEL, MAIN)
        tf = b.text_frame
        tf.vertical_anchor = MSO_ANCHOR.MIDDLE
        p = tf.paragraphs[0]
        p.text = head
        p.alignment = C
        self._fmt(p.font, 14, MAIN, True)
        pp = tf.add_paragraph()
        pp.text = value
        pp.alignment = C
        pp.space_before = Pt(4)
        self._fmt(pp.font, 24, vc, True)
        return b

    def arrow(self, s, l, t, w, h, c=MAIN, down=False):
        a = s.shapes.add_shape(
            MSO_SHAPE.DOWN_ARROW if down else MSO_SHAPE.RIGHT_ARROW,
            Inches(l), Inches(t), Inches(w), Inches(h))
        a.fill.solid()
        a.fill.fore_color.rgb = c
        a.line.fill.background()
        return a

    def line(self, s, x1, y1, x2, y2, c=LINE, lw=1.5):
        ln = s.shapes.add_connector(MSO_CONNECTOR.STRAIGHT,
                                    Inches(x1), Inches(y1), Inches(x2), Inches(y2))
        ln.line.color.rgb = c
        ln.line.width = Pt(lw)
        return ln

    def dot(self, s, cx, cy, d, c):
        o = s.shapes.add_shape(MSO_SHAPE.OVAL, Inches(cx - d / 2),
                               Inches(cy - d / 2), Inches(d), Inches(d))
        o.fill.solid()
        o.fill.fore_color.rgb = c
        o.line.fill.background()
        return o

    def road(self, s, x0, x1, yc, n_seg, amp, straight=False):
        if straight:
            self.line(s, x0, yc, x1, yc, MAIN, 5)
            pts = [(x0, yc), (x1, yc)]
        else:
            pts = [(x0 + (x1 - x0) * i / n_seg,
                    yc + amp * math.sin(i * 2 * math.pi / (n_seg / 2)))
                   for i in range(n_seg + 1)]
            for (ax, ay), (bx, by) in zip(pts, pts[1:]):
                self.line(s, ax, ay, bx, by, MUTED, 1.75)
            for x, y in pts[1:-1]:
                self.dot(s, x, y, 0.13, MUTED)
        for (x, y), sgn in ((pts[0], -1), (pts[-1], 1)):
            self.line(s, x, y, x + sgn * 0.32, y - 0.28, MAIN, 2)
            self.line(s, x, y, x + sgn * 0.32, y + 0.28, MAIN, 2)
            self.dot(s, x, y, 0.28, ACC)
        return pts

    def pic(self, s, name, l, t, w=None, h=None, folder=None):
        path = os.path.join(folder or IMG, name)
        if not os.path.exists(path):
            self.tx(s, l, t, w or 3, 0.4, "[缺圖：%s]" % name, 12, ACC)
            return None
        kw = {}
        if w:
            kw["width"] = Inches(w)
        if h:
            kw["height"] = Inches(h)
        return s.shapes.add_picture(path, Inches(l), Inches(t), **kw)

    def bars(self, s, l, t, w, h, cats, vals, colors, labels, fs=13):
        cd = CategoryChartData()
        cd.categories = cats
        cd.add_series("v", vals)
        gf = s.shapes.add_chart(XL_CHART_TYPE.COLUMN_CLUSTERED, Inches(l),
                                Inches(t), Inches(w), Inches(h), cd)
        ch = gf.chart
        ch.has_legend = False
        ch.has_title = False
        ch.font.name = FONT
        ch.font.size = Pt(fs)
        ch.font.color.rgb = INK
        plot = ch.plots[0]
        plot.gap_width = 55
        plot.vary_by_categories = False
        ser = plot.series[0]
        for i, (c, lab) in enumerate(zip(colors, labels)):
            pt = ser.points[i]
            pt.format.fill.solid()
            pt.format.fill.fore_color.rgb = c
            pt.format.line.fill.background()
            dl = pt.data_label
            dl.text_frame.text = lab
            dl.position = XL_LABEL_POSITION.OUTSIDE_END
            for para in dl.text_frame.paragraphs:
                for r in para.runs:
                    self._fmt(r.font, fs, c, True)
        va = ch.value_axis
        va.visible = False
        va.has_major_gridlines = False
        va.minimum_scale = 0
        va.maximum_scale = max(vals) * 1.18
        ca = ch.category_axis
        ca.tick_labels.font.size = Pt(fs)
        ca.tick_labels.font.color.rgb = INK
        ca.format.line.color.rgb = LINE
        no_fill = ("<c:spPr %s><a:noFill/><a:ln><a:noFill/></a:ln></c:spPr>"
                   % nsdecls("c", "a"))
        cs = ch._chartSpace
        if cs.find(qn("c:spPr")) is None:
            cs.find(qn("c:chart")).addnext(parse_xml(no_fill))
        pa = cs.find(qn("c:chart")).find(qn("c:plotArea"))
        if pa.find(qn("c:spPr")) is None:
            pa.append(parse_xml(no_fill))
        return ch

    def divider(self, s, idx):
        self.title(s, "目錄")
        for i, name in enumerate(CHAPTERS):
            y = 1.75 + i * 0.78
            cur = (i == idx)
            if cur:
                bar = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(2.0),
                                         Inches(y + 0.08), Inches(0.1), Inches(0.4))
                bar.fill.solid()
                bar.fill.fore_color.rgb = ACC
                bar.line.fill.background()
            self.tx(s, 2.3, y, 9.0, 0.55, name, 24 if cur else 22,
                    MAIN if cur else MUTED, cur)

    def caption(self, s, l, t, w, txt, sz=13):
        self.tx(s, l, t, w, 0.35, txt, sz, MUTED)

    def page_no(self, s, n):
        self.tx(s, 12.15, 6.98, 0.6, 0.35, str(n), 12, MUTED, al=C)


# ════════════════════ 封面 ════════════════════
def s_cover(d):
    s = d.blank()
    top = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0), Inches(0),
                             Inches(13.333), Inches(0.3))
    top.fill.solid()
    top.fill.fore_color.rgb = MAIN
    top.line.fill.background()
    d.tx(s, 0, 2.25, 13.333, 0.4, "畢業專題", 16, ACC, True, C)
    d.tx(s, 0, 2.7, 13.333, 0.9, "動態路網導航系統", 40, MAIN, True, C)
    d.tx(s, 0, 3.66, 13.333, 0.5, "Dynamic Road Network Navigation", 18, MUTED, False, C)
    d.line(s, 5.6, 4.35, 7.73, 4.35, ACC, 2.5)
    d.tx(s, 0, 4.6, 13.333, 0.5, "組員：黃奕睿　林秉達　葉芢杰", 17, INK, False, C)


# ════════════════════ 一、介紹 ════════════════════
def s_motivation(d):
    s = d.blank()
    d.title(s, "研究動機")
    d.bullets(s, 1.0, 1.6, 11.4, [
        ("即時資料沒被用上", "台灣已有 TDX 車速偵測與 CWA 氣象即時資料，卻沒有整合進路線規劃。"),
        ("事件發生無法反應", "路線算完就固定，突然發生的事故、施工、封路都繞不開。"),
        ("重算成本太高", "全台 765 萬條路段重算一次要 4.3 秒，事件一多就跟不上。"),
        ("使用者需求不同", "趕時間的人與求安全的人，需要的不是同一條路線。"),
    ], sz=20, gap=30)


def s_objective(d):
    s = d.blank()
    d.title(s, "研究目的")
    d.bullets(s, 1.0, 1.6, 11.4, [
        ("整合即時資料", "TDX 車速偵測器 1,361 站與 CWA 700+ 氣象站，每 5 分鐘自動同步。"),
        ("事件即時生效", "以增量更新取代全量重算，新增事件約 20 毫秒反映到路線上。"),
        ("依道路特性預測影響", "用三因子模型自動決定事件的嚴重程度與影響範圍。"),
        ("降低搜尋成本", "以路口收縮切分路網，長程路徑搜尋加速 13.5 倍。"),
        ("提供多種路線選擇", "最快 / 平衡 / 最安全三種模式，並提供替代路線。"),
    ], sz=20, gap=26)


# ════════════════════ 二、系統架構與流程 ════════════════════
def s_architecture(d):
    s = d.blank()
    d.title(s, "系統架構")
    d.pic(s, "p06_1.png", 0.8, 1.55, w=4.0)
    d.caption(s, 0.8, 5.55, 4.0, "▲ 前端地圖介面（Leaflet 單檔 SPA）")
    boxes = [("API 伺服器", ["驗證座標、回傳路線與 GeoJSON"]),
             ("路由引擎", ["A* 搜尋 + 動態成本計算"]),
             ("資料庫", ["376 萬節點 / 765 萬條路段"]),
             ("導航結果", ["最佳路線、替代路線與 ETA"])]
    for i, (head, lines) in enumerate(boxes):
        y = 1.55 + i * 1.02
        d.flow(s, 5.15, y, 3.4, 0.8, head, lines)
        if i < 3:
            d.arrow(s, 6.72, y + 0.82, 0.26, 0.18, MAIN, down=True)
    d.notebox(s, 8.9, 1.55, 3.6, 3.5, "外部即時資料", [
        "TDX 車速偵測器：1,361 站 × 19 縣市",
        "TDX 路況新聞：事故 / 施工 / 封路",
        "CWA 氣象站：700+ 站、雨量站 400+",
        "每 5 分鐘自動同步一次",
    ], sz=13)
    d.tx(s, 0.8, 6.1, 11.8, 0.5,
         "即時資料進來後直接改寫路網成本，使用者下一次搜尋就會反映最新路況。",
         16, INK, True)


def s_flowchart(d):
    s = d.blank()
    d.title(s, "執行流程圖")
    d.pic(s, "p07_1.png", 2.1, 1.45, w=9.2)
    d.caption(s, 2.1, 6.72, 9.2,
              "使用者送出起訖點 → 解析最近節點 → A* 搜尋 → 回傳路線與分析報告")


def s_cost(d):
    s = d.blank()
    d.title(s, "路線成本怎麼算？")
    d.panel(s, X0, 1.3, X1 - X0, 0.62)
    d.rich(s, X0, 1.42, X1 - X0, 0.45, [
        ("每條路的成本 = ", INK, True), ("行車時間", ACC, True),
        (" × (時間權重 + ", INK, True), ("風險", ACC, True),
        (" × 風險權重) + 轉彎等待 + 紅綠燈等待", INK, True)], 18, C)
    d.tx(s, 0.9, 2.15, 5.5, 0.4, "影響「行車時間」的因素", 18, MAIN, True)
    d.bullets(s, 0.9, 2.65, 5.5, [
        (None, "道路等級速度：國道 110、省道 60、巷弄 30 km/h"),
        (None, "尖峰時段自動降速 15~38%"),
        (None, "事件：事故 −45%、施工 −35%、封路直接不走"),
        (None, "天氣：下雨、起霧、強風降速"),
    ], sz=15, gap=12)
    d.tx(s, 6.9, 2.15, 5.6, 0.4, "影響「風險分數」的因素", 18, MAIN, True)
    d.bullets(s, 6.9, 2.65, 5.6, [
        (None, "事件風險：事故 +8 分、封路 +100 分"),
        (None, "天氣風險：雨 +40%、霧 +35%、風 +20% 成本"),
        (None, "夜間風險：夜間佔 25% 里程卻有 50% 死亡事故"),
        (None, "轉彎 +10 秒、迴轉 +25 秒、每個紅綠燈 +20 秒"),
    ], sz=15, gap=12)
    d.panel(s, X0, 5.35, X1 - X0, 1.15)
    d.tx(s, 1.0, 5.5, 11.3, 0.4, "三種模式的差異", 16, MAIN, True)
    d.tx(s, 1.0, 5.92, 11.3, 0.45,
         "最快：幾乎只看時間，容忍風險　｜　平衡：時間與安全兼顧（預設）　｜　"
         "最安全：會繞路避開危險路段", 15, INK)
    d.caption(s, X0, 6.6, 11.9,
              "每項成本因子都有文獻依據（FHWA、HCM、NHTSA 等），詳見參考文獻。")


def s_incremental(d):
    s = d.blank()
    d.title(s, "增量式事件更新與影響範圍")
    d.tbl(s, 0.7, 1.3, 11.93, 1.1, [
        ["比較項目", "增量更新（現行）", "全量重算（原本）"],
        ["計算路段數", "約 1,100 條（事件半徑內）", "7,600,000 條"],
        ["花費時間", "約 20 毫秒", "約 4,300 毫秒（4.3 秒）"],
    ], cw=[3.2, 4.5, 4.23], fs=15, hl={1: MAIN})
    d.tx(s, 0.9, 2.62, 11.5, 0.4,
         "影響範圍與嚴重度不用固定值，改由事件地點的道路特性決定：", 16, INK, True)
    d.rich(s, 0.9, 3.08, 11.5, 0.4, [
        ("預測嚴重度 = 基礎嚴重度 × (1 + ", INK, False), ("ω", ACC, True),
        (" × (1 − ", INK, False), ("κ", ACC, True), (") × (1 − ", INK, False),
        ("δ", ACC, True), ("))", INK, False)], 16)
    d.rich(s, 0.9, 3.5, 11.5, 0.4, [
        ("預測影響範圍 = 基礎範圍 × (1 + ", INK, False), ("ω", ACC, True),
        (" × (1 − 0.5", INK, False), ("κ", ACC, True), ("))", INK, False)], 16)
    for i, (h, ln) in enumerate([
            ("ω 道路關鍵度", ["國道 1.00 → 巷弄 0.15"]),
            ("κ 替代路線密度", ["1 km 內邊數 / 200"]),
            ("δ 節點連通度", ["路口分流能力 degree / 8"])]):
        d.flow(s, 0.9 + i * 3.9, 4.05, 3.6, 0.95, h, ln)
    d.tbl(s, 0.9, 5.25, 11.5, 1.3, [
        ["同樣一場車禍", "三因子方框（原方法）", "沿上游傳播（計畫書演算法）"],
        ["台北忠孝東路：多條路可分流", "半徑 0.66 km、嚴重度 0.80", "最遠 1.2 km"],
        ["蘇花公路：沒有替代道路", "半徑 0.66 km、嚴重度 0.80", "最遠 6.1 km"],
    ], cw=[4.5, 3.5, 3.5], fs=14, hl={2: ACC})
    d.caption(s, 0.9, 6.62, 11.5,
              "κ 以 34 公尺的路段計數，1 km 內幾乎都超過 200 條（蘇花 278、台北 5,590），"
              "方框分不出兩地；沿路網傳播才分得出來。")


# ════════════════════ 三、路網切割與路徑搜尋 ════════════════════
def s_astar(d):
    s = d.blank()
    d.title(s, "路徑搜尋：A* 演算法")
    d.sub(s, "每按一次「搜尋」，系統用 A* 從起點一路往外展開，直到碰到終點")
    d.rich(s, 0, 1.9, 13.333, 0.7, [
        ("f(n)", ACC, True), ("  =  ", INK, True), ("g(n)", MAIN, True),
        ("  +  ", INK, True), ("h(n)", MAIN, True)], 32, C)
    d.rich(s, 1.5, 2.72, 10.5, 0.4, [
        ("g(n)", MAIN, True),
        ("　已經花的成本：時間、風險、轉彎、紅綠燈（上一章的成本公式）", INK, False)], 15)
    d.rich(s, 1.5, 3.12, 10.5, 0.4, [
        ("h(n)", MAIN, True),
        ("　估計剩下的成本：n 到終點的直線距離 ÷ 120 km/h", INK, False)], 15)
    steps = [("① 取出", ["佇列中 f 最小的點"]),
             ("② 到終點了？", ["是 → 沿路回溯", "得到路線"]),
             ("③ 展開鄰居", ["算相鄰路段", "走過去的新成本"]),
             ("④ 更新", ["比原本便宜就記下", "放回佇列，回到 ①"])]
    for i, (h, ln) in enumerate(steps):
        x = 0.8 + i * 3.05
        d.flow(s, x, 3.75, 2.7, 1.45, h, ln)
        if i < 3:
            d.arrow(s, x + 2.78, 4.35, 0.22, 0.28)
    d.tx(s, 0.9, 5.45, 11.6, 0.45,
         "h 永遠不會高估（全台速限最高 110 km/h < 120）→ 不會漏掉更便宜的路線",
         17, ACC, True)
    d.tx(s, 0.9, 5.95, 11.6, 0.9,
         "但 h 很保守：台北→台中的估計值只有實際的 64%，"
         "搜尋仍會往四周多看很多點 —— 這就是速度的瓶頸。", 15, INK)


def s_shape_points(d):
    s = d.blank()
    d.title(s, "為什麼找路慢？路網裡 95% 的點不是路口")
    d.sub(s, "主幹路網 793,898 個點，真正的路口只有 35,668 個（4.5%）")
    d.road(s, 1.9, 11.4, 3.15, 17, 0.3)
    d.tx(s, 1.2, 2.45, 1.4, 0.35, "路口", 14, ACC, True)
    d.tx(s, 11.1, 2.45, 1.4, 0.35, "路口", 14, ACC, True)
    d.tx(s, 3.6, 3.75, 6.2, 0.35, "中間這些「形狀點」只是描繪彎道，沒有岔路可選",
         14, MUTED, al=C)
    d.bullets(s, 1.0, 4.35, 11.4, [
        (None, "平均每段路只有 34 公尺，一個路口到下一個路口平均隔 17 段"),
        (None, "A* 每走一段就要做一次佇列操作 —— 在形狀點上根本沒得選，卻照樣付成本"),
        (None, "台北→台中一次搜尋：展開 268,742 段路，花 1.19 秒"),
    ], sz=16, gap=14)
    d.tx(s, 1.0, 6.25, 11.4, 0.5, "問題不在 A*，而在 A* 跑的「圖」太細", 20, ACC, True)


def s_proposal_rule(d):
    s = d.blank()
    d.title(s, "計畫書的切子圖規則：直接套用會失效")
    d.notebox(s, 0.75, 1.35, 5.8, 1.85, "計畫書的規則", [
        "出度 > 1 的節點 = 核心節點，核心節點之間切成子圖",
        "✓ 在計畫書 21 節點的範例上重現核心節點 [5, 7, 13, 15, 16]",
    ], sz=14)
    d.notebox(s, 6.85, 1.35, 5.78, 1.85, "放到真實路網", [
        "78.6% 的路段是雙向道 —— 有去就有回",
        "路中間的點也有 2 條出邊（往前、往回）→ 幾乎全變成核心節點",
    ], sz=14, hc=ACC)
    d.tbl(s, 0.75, 3.45, 11.88, 1.65, [
        ["核心節點判定", "核心比例", "平均子圖大小", "效果"],
        ["計畫書：出度 > 1", "67.7%", "1.5 個點", "幾乎沒切到"],
        ["修正：無向度數 ≠ 2（路口 + 路的盡頭）", "4.5%", "17.3 段路（約 590 m）",
         "點數少 22 倍"],
    ], cw=[4.9, 2.1, 3.0, 1.88], fs=15, hl={2: ACC})
    d.tx(s, 0.9, 5.35, 11.5, 0.45,
         "照計畫書的精神「以核心節點切子圖」，只修正核心節點的定義", 17, MAIN, True)
    d.caption(s, 0.9, 5.9, 11.5,
              "無向度數 = 不分方向、這個點連著幾條路；等於 2 代表只是一條路的中段，不是路口。")


def s_contraction(d):
    s = d.blank()
    d.title(s, "路口收縮：兩個路口之間收成一條 link")
    d.tx(s, 0.75, 1.42, 1.6, 0.35, "收縮前", 15, MAIN, True)
    d.road(s, 2.6, 9.8, 1.62, 17, 0.16)
    d.tx(s, 10.2, 1.42, 2.4, 0.35, "A* 走 17 步", 15, INK, True)
    d.tx(s, 0.75, 2.45, 1.6, 0.35, "收縮後", 15, MAIN, True)
    d.road(s, 2.6, 9.8, 2.62, 1, 0, straight=True)
    d.tx(s, 10.2, 2.45, 2.4, 0.35, "A* 走 1 步", 15, ACC, True)
    steps = [("① 找路口", ["無向度數 ≠ 2 的點", "主幹路網 35,668 個"]),
             ("② 收成 link", ["從路口沿路走到", "下一個路口為止"]),
             ("③ 成本先加總", ["依道路類型存", "時間與風險的總和"]),
             ("④ A* 跑 link 圖", ["找到後再展開", "回原本的路段"])]
    for i, (h, ln) in enumerate(steps):
        x = 0.8 + i * 3.05
        d.flow(s, x, 3.15, 2.7, 1.25, h, ln)
        if i < 3:
            d.arrow(s, x + 2.78, 3.65, 0.22, 0.25)
    d.tbl(s, 2.4, 4.65, 8.5, 1.15, [
        ["", "收縮前", "收縮後", "縮小"],
        ["節點", "793,898", "35,668", "22.3 倍"],
        ["路段 / link", "1,344,026", "77,725", "17.3 倍"],
    ], cw=[2.2, 2.2, 2.1, 2.0], fs=14, hl={2: MAIN})
    d.tx(s, 0.9, 6.0, 11.5, 0.45,
         "正確性：成本加總誤差 1.8 × 10⁻¹³、16 / 16 條路線逐段相同、50 / 50 條路線成功",
         15, ACC, True)
    d.caption(s, 0.9, 6.5, 11.5,
              "建圖只在伺服器啟動時做一次（8.6 秒）；回傳前端的路線格式不變，前端不用改。")


def s_performance(d):
    s = d.blank()
    d.title(s, "效能比較：與原系統")
    d.tbl(s, 0.7, 1.32, 11.93, 1.7, [
        ["設定", "台北→台中（含 3 條備選）", "50 條隨機路線總耗時", "成功"],
        ["原系統：全路網載入", "25.59 秒", "372 秒", "40 / 50"],
        ["原系統：只載主幹路網", "7.06 秒", "185.3 秒", "50 / 50"],
        ["主幹路網 + 路口收縮", "0.371 秒", "13.7 秒", "50 / 50"],
    ], cw=[3.6, 3.3, 3.2, 1.83], fs=15, hl={3: ACC})
    d.tx(s, 0.75, 3.15, 7.0, 0.35, "從台北出發，單次搜尋加速倍數（主幹路網 → 收縮後）",
         14, MAIN, True)
    d.bars(s, 0.75, 3.5, 6.9, 3.1, ["板橋", "桃園", "新竹", "台中", "嘉義", "高雄"],
           [7, 10, 12, 14, 20, 19], [MAIN] * 4 + [ACC] * 2,
           ["7×", "10×", "12×", "14×", "20×", "19×"])
    d.stat(s, 8.0, 3.35, 4.6, 1.0, "50 條路線總耗時", "185.3 秒 → 13.7 秒")
    d.stat(s, 8.0, 4.5, 4.6, 1.0, "台北→高雄", "2.62 秒 → 0.135 秒")
    d.tx(s, 8.0, 5.65, 4.6, 0.9,
         "伺服器實測（含 3 條備選）：板橋 0.11 秒、台中 0.49 秒、高雄 0.93 秒", 13, INK)
    d.caption(s, 0.75, 6.62, 7.0, "越長的路線沿途形狀點越多，省下的步數越多。")


# ════════════════════ 四、交通預測模型訓練 ════════════════════
def s_tgcn(d):
    s = d.blank()
    d.title(s, "T-GCN：預測 30 分鐘後的車速")
    d.sub(s, "原本只看「當下」路況 —— 塞了才繞；改成預計會塞就先繞")
    boxes = [("輸入", ["過去 60 分鐘", "378 個路段", "車速 + 車流量"]),
             ("① GCN 圖卷積", ["聚合上下游鄰居", "（空間依賴）", "塞車往下游擴散"]),
             ("② 殘差連接", ["保留路段自己的", "車速，不被鄰居蓋掉"]),
             ("③ GRU", ["看過去一小時趨勢", "（時間依賴）"]),
             ("輸出", ["未來 5/15/30 分鐘", "預測車速（km/h）"])]
    for i, (h, ln) in enumerate(boxes):
        x = 0.72 + i * 2.4
        d.flow(s, x, 2.0, 2.2, 1.85, h, ln, ACC if i == 4 else MAIN)
        if i < 4:
            d.arrow(s, x + 2.24, 2.82, 0.16, 0.22)
    d.tx(s, 0.9, 4.2, 11.5, 0.4, "為什麼三步一定要一起？", 18, MAIN, True)
    d.bullets(s, 0.9, 4.68, 11.5, [
        (None, "只看自己的歷史 → 看不到上游正在塞（純 GRU：MAE 2.20）"),
        (None, "只看鄰居、蓋掉自己的規律 → 更糟（無殘差版本明顯惡化）"),
        (None, "三者合起來才最準（T-GCN：MAE 2.11 km/h）"),
    ], sz=16, gap=12)
    d.caption(s, 0.9, 6.5, 11.5,
              "壅塞會沿著道路往下游擴散，這種「圖上的傳播」正是 GCN 最適合的資料型態。")


def s_training(d):
    s = d.blank()
    d.title(s, "訓練資料與預測準確度")
    d.tx(s, 0.75, 1.3, 5.9, 0.4, "資料來源與路網圖", 17, MAIN, True)
    d.bullets(s, 0.75, 1.75, 5.9, [
        (None, "高公局 TDCS M05A 門架路段平均車速"),
        (None, "5 條國道 378 個路段，21 天 × 每 5 分鐘 = 6,048 個時間點"),
        (None, "節點 = 門架路段；邊 = 前一段終點接下一段起點"),
        (None, "378 節點 / 421 條邊，沿行車方向連成鏈"),
    ], sz=14, gap=10)
    d.tbl(s, 0.75, 3.95, 5.9, 1.9, [
        ["模型設定", "內容"],
        ["GCN 層數 / 隱藏維度", "2 層 / 64"],
        ["時間模型", "GRU + 殘差連接"],
        ["學習率 / 優化器", "0.001 / Adam"],
        ["輸入 → 輸出", "過去 60 分 → 未來 5/15/30 分"],
    ], cw=[2.9, 3.0], fs=13)
    d.tbl(s, 6.95, 1.3, 5.68, 1.95, [
        ["平均誤差 MAE（km/h）", "5 分", "15 分", "30 分"],
        ["T-GCN（GCN + GRU + 殘差）", "2.11", "2.62", "3.13"],
        ["GRU（拿掉 GCN）", "2.20", "2.68", "3.23"],
        ["naive（延續上一刻）", "2.70", "3.23", "3.82"],
        ["歷史平均 HA", "4.00", "4.00", "4.00"],
    ], cw=[2.78, 0.95, 0.95, 1.0], fs=12, hl={1: MAIN})
    d.pic(s, "accuracy_comparison.png", 6.95, 3.5, w=5.68, folder=RES)
    d.caption(s, 6.95, 5.85, 5.68, "▲ 左：交通車速預測（越低越準）　右：GCN 節點分類驗證")
    d.tx(s, 0.75, 6.25, 11.9, 0.5,
         "預測越遠，鄰居的資訊越重要；殘差連接是必要條件，拿掉後比純 GRU 還差。",
         15, INK, True)


def s_anomaly(d):
    s = d.blank()
    d.title(s, "異常偵測與事件參數校準")
    d.tx(s, 0.75, 1.3, 7.0, 0.4, "模型能自己發現事故嗎？", 17, MAIN, True)
    d.bullets(s, 0.75, 1.75, 7.0, [
        (None, "把高公局真實事故通報當「標準答案」（169 件，123 件對齊路段）"),
        (None, "預測與實際車速的落差（殘差）超過門檻 → 標記異常"),
    ], sz=14, gap=10)
    d.tbl(s, 0.75, 2.95, 7.0, 1.3, [
        ["門檻", "標記命中率（精確度）", "抓到的事故（召回率）"],
        ["殘差 > 3.0", "13.3%（亂猜 2.3% 的 5.7 倍）", "61.0%（75 / 123 件）"],
        ["殘差 > 3.5", "15.4%（6.6 倍）", "55.3%（68 / 123 件）"],
    ], cw=[1.5, 3.0, 2.5], fs=12, hl={1: MAIN})
    d.tx(s, 0.75, 4.4, 7.0, 0.8,
         "模型沒看過任何事故標籤，只靠「實際車速偏離預測」就抓到 61% 的真實事故。",
         15, ACC, True)
    d.tx(s, 0.75, 5.25, 7.0, 0.4, "用真實事故校準三因子模型的參數", 17, MAIN, True)
    d.bullets(s, 0.75, 5.7, 7.0, [
        (None, "事故路段降速中位數 1.283 倍（前 25% 嚴重：2.065 倍）"),
        (None, "往上游回堵 2.15 km → 原設定最大 1.6 km 低估，校準成 1.1 km"),
    ], sz=13, gap=8)
    d.pic(s, "anomaly_pr_curve.png", 8.4, 1.5, w=4.2, folder=RES)
    d.caption(s, 8.4, 4.65, 4.2, "▲ 精確度—召回率曲線")
    d.tx(s, 8.4, 5.1, 4.2, 1.2,
         "精確度偏低是正常的：標準答案只有「事故」，但模型也抓到施工、"
         "匝道壅塞等真實減速。", 12, MUTED)


def s_prediction_result(d):
    s = d.blank()
    d.title(s, "實際預測結果")
    d.sub(s, "預測結果寫成事件，直接套用原本的成本機制 —— 路由自動避開會塞的路段")
    d.pic(s, "p36_1.png", 0.8, 1.9, w=7.2)
    d.pic(s, "p36_2.png", 8.5, 1.9, w=3.2)
    d.tx(s, 0.8, 5.85, 11.8, 0.9,
         "紫色圓圈是 T-GCN 預測會壅塞的路段。「壅塞」判定看的是相對速度 —— "
         "該路段目前車速與它順暢時的落差，所以山路與市區各有自己的標準。", 14, INK)


# ════════════════════ 五、成果展示 ════════════════════
def s_video(d):
    s = d.blank()
    d.title(s, "展示影片")
    d.pic(s, "p39_1.png", 2.4, 1.5, w=8.5)
    d.rich(s, 0, 5.85, 13.333, 0.4,
           [("影片連結：", MAIN, True), (VIDEO_URL, INK, False)], 16, C)
    d.tx(s, 0, 6.35, 13.333, 0.8,
         "示範內容：設定起訖點 → 三種模式切換 → 新增事件後路線即時改變 → "
         "預測壅塞圖層與替代路線", 14, MUTED, al=C)


def s_event_compare(d):
    s = d.blank()
    d.title(s, "事件前後的路線變化：同一組起訖點自動改道")
    cells = [("台北市區・正常路線", "p40_1.png", 1.3, 1.62),
             ("台北市區・事件發生", "p40_2.png", 7.0, 1.62),
             ("新竹郊區・正常路線", "p41_1.png", 1.3, 4.35),
             ("新竹郊區・事件發生", "p41_2.png", 7.0, 4.35)]
    for label, name, x, y in cells:
        d.tx(s, x, y - 0.32, 4.7, 0.3, label, 13, MAIN, True)
        d.pic(s, name, x, y, w=4.7)


def s_closure_compare(d):
    s = d.blank()
    d.title(s, "國道封路的路線對比")
    d.sub(s, "台北 → 台中，國道封閉後改走替代道路", sz=16)
    d.tx(s, 1.5, 1.75, 4.3, 0.3, "封路前", 14, MAIN, True)
    d.pic(s, "p43_1.png", 1.5, 2.05, h=4.2)
    d.tx(s, 7.3, 1.75, 4.3, 0.3, "封路後", 14, ACC, True)
    d.pic(s, "p43_2.png", 7.3, 2.05, h=4.2)
    d.caption(s, 0.9, 6.45, 11.8,
              "距離 162.74 → 173.79 km、時間 108.8 → 113.1 分鐘；"
              "另一組測試「台北 → 桃園機場」同樣可正確繞開封閉路段。")


# ════════════════════ 六、結論 ════════════════════
def s_conclusion_tech(d):
    s = d.blank()
    d.title(s, "結論：四項技術成果")
    d.bullets(s, 0.9, 1.75, 11.6, [
        ("事件增量更新",
         "新增事件只改半徑內約 1,100 條路段，20 毫秒生效，取代 4.3 秒的全量重算。"),
        ("事件沿上游傳播",
         "依計畫書的增量 PageRank 推送，影響沿路網往上游衰減：蘇花公路 6.1 km、台北市區 1.2 km。"),
        ("路網切割：路口收縮",
         "節點少 22.3 倍，50 條路線總耗時 185.3 → 13.7 秒，路線與原系統逐段相同。"),
        ("T-GCN 交通預測",
         "30 分鐘預測誤差 2.11 km/h 勝過所有基準；殘差自動抓到 61% 的真實事故。"),
    ], sz=18, gap=32)


def s_conclusion_value(d):
    s = d.blank()
    d.title(s, "結論：效益與未來方向")
    d.tx(s, 0.85, 1.4, 5.7, 0.4, "效益", 20, MAIN, True)
    d.bullets(s, 0.85, 1.9, 5.7, [
        (None, "事件與天氣一進來就反映到路線，不必等待重算"),
        (None, "每項成本因子都有文獻依據，不是黑盒子"),
        (None, "長程路線從「算不完」變成 1 秒內完成"),
        (None, "預測事件與手動、即時事件互不干擾，模型可獨立更新"),
    ], sz=15, gap=14)
    d.tx(s, 7.0, 1.4, 5.6, 0.4, "未來方向", 20, MAIN, True)
    d.bullets(s, 7.0, 1.9, 5.6, [
        (None, "加入道路坡度（DEM 高程資料）"),
        (None, "歷史流量模型（VD 歷史數據）"),
        (None, "淹水風險（水利署即時資料）"),
        (None, "雙向搜尋 / 更強的啟發式，再縮小搜尋範圍"),
        (None, "link 差值更新：事件只補差值，不必整條重算"),
    ], sz=15, gap=14)
    d.panel(s, X0, 5.75, X1 - X0, 0.85)
    d.tx(s, 1.0, 5.95, 11.3, 0.5,
         "核心貢獻：把「即時資料 → 事件影響預測 → 路網切割加速 → 路線更新」"
         "串成一條可即時運作的鏈。", 16, MAIN, True)


def s_references(d):
    s = d.blank()
    d.title(s, "參考文獻")
    left = [("成本因子依據", None)]
    left += [(None, t) for t in [
        "[1] FHWA / Highway Capacity Manual Ed. 7.1 (2025) — 事件延遲、號誌延遲",
        "[2] Agarwal et al. (2005) Weather Impact on Freeway — 降雨與車速關係",
        "[3] NHTSA (2008) Nighttime Driving — 25% miles = 50% fatalities",
        "[4] McGill University — Turn Delay: 15~25% of urban trip time",
        "[5] FHWA — Speed/Capacity for Work Zones — construction ×1.55",
        "[6] Tandfonline (2017) Landslide Risk Assessment in Taiwan",
        "[7] HCM Ch.19 — Signal Delay Function (Webster 1958)",
    ]]
    left += [("資料來源", None)]
    left += [(None, t) for t in [
        "[15] TDX 運輸資料流通服務平台 — VD 車速偵測器 API",
        "[16] CWA 中央氣象署開放資料平台 — 氣象站 / 雨量站 API",
        "[17] 台灣水保局 — 累積雨量山崩風險三級閾值 (200/350/600mm)",
    ]]
    right = [("三因子預測模型依據", None)]
    right += [(None, t) for t in [
        "[8] Jenelius et al. (2006) Importance & Exposure in Road Network Vulnerability — ω",
        "[9] Scott et al. (2006) Network Robustness Index — 道路等級與中斷影響",
        "[10] Allen et al. (2024) Network Redundancy Reduces Criticality — κ",
        "[11] Ganin et al. (2017) Resilience & Efficiency in Transportation, "
        "Science Advances — δ",
        "[12] Sun & Qian (2019) Role of Road Network Features in Incident Impacts",
        "[13] ASCE (2018) Modeling Framework for Affected Area",
        "[14] Mattsson & Jenelius (2015) Vulnerability and Resilience of Transport Systems",
    ]]
    right += [("T-GCN 交通預測依據", None)]
    right += [(None, t) for t in [
        "[18] Kipf & Welling (2016) Semi-Supervised Classification with GCN — arXiv:1609.02907",
        "[19] Zhao et al. (2019) T-GCN: Temporal Graph Convolutional Network — IEEE T-ITS",
        "[20] 高公局 TDCS M05A / ETag 即時資料 — 訓練與即時預測資料源",
    ]]
    for items, x, w in ((left, 0.75, 5.9), (right, 6.85, 5.78)):
        tf = None
        for head, body in items:
            if head:
                if tf is None:
                    tf = d.tx(s, x, 1.3, w, 0.35, head, 14, MAIN, True)
                else:
                    d.ap(tf, head, 14, MAIN, True, sp=12)
            else:
                d.ap(tf, body, 10.5, INK, False, sp=4)


def build():
    d = Deck()
    plan = [s_cover,
            lambda dd: dd.divider(dd.blank(), 0), s_motivation, s_objective,
            lambda dd: dd.divider(dd.blank(), 1), s_architecture, s_flowchart,
            s_cost, s_incremental,
            lambda dd: dd.divider(dd.blank(), 2), s_astar, s_shape_points,
            s_proposal_rule, s_contraction, s_performance,
            lambda dd: dd.divider(dd.blank(), 3), s_tgcn, s_training,
            s_anomaly, s_prediction_result,
            lambda dd: dd.divider(dd.blank(), 4), s_video, s_event_compare,
            s_closure_compare,
            lambda dd: dd.divider(dd.blank(), 5), s_conclusion_tech,
            s_conclusion_value, s_references]
    for fn in plan:
        fn(d)
    for i, s in enumerate(d.prs.slides, 1):
        if i > 1:
            d.page_no(s, i)
    d.prs.save(OUT)
    print("saved:", OUT, "| slides:", len(d.prs.slides))


if __name__ == "__main__":
    build()
