# -*- coding: utf-8 -*-
"""系統原理簡報（照「專題(1).pptx」的絲縷版型）：讓第一次看的人也懂系統怎麼運作。

版型：直接以 專題(1).pptx 當範本——開啟後移除它原有的投影片，沿用它的佈景主題
（絲縷）、版面配置、標題箭頭、標楷體 / Times New Roman 與頁碼。範本檔本身不會被修改。

寫法：
  - 標題一律是直述句（不用問句），16 字以內
  - 每章前一張「大綱」，目前章節以紫色標示（與參考簡報相同）
  - 內文「● 粗體重點：白話說明」，19～26pt；畫面只留一兩個關鍵數字
  - 拿掉的細節數字放在各頁備忘稿
全場用同一個例子：台北 → 台中，途中國道一號（新竹寶山段）發生車禍。

圖片來源：event_figures/*.png（make_event_figures.py，時段固定上午 10 點）、
scratchpad\\img（自 ppt.pptx 抽出，唯讀；缺圖時自動重新抽出）。輸出 demo_ppt_system.pptx。

    python build_system_ppt.py
"""
import copy
import math
import os
import sys

from PIL import Image
from matplotlib import colormaps
from pptx import Presentation
from pptx.chart.data import CategoryChartData
from pptx.dml.color import RGBColor
from pptx.enum.chart import XL_CHART_TYPE, XL_LEGEND_POSITION
from pptx.enum.dml import MSO_LINE
from pptx.enum.shapes import MSO_CONNECTOR, MSO_SHAPE, MSO_SHAPE_TYPE, PP_PLACEHOLDER
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.oxml import parse_xml
from pptx.oxml.ns import nsdecls, qn
from pptx.util import Inches, Pt

from build_light_ppt import IMG, VIDEO_URL

TEMPLATE = r"D:\python\專題(1).pptx"
OUT = r"D:\python\demo_ppt_system.pptx"
FIG = r"D:\python\event_figures"
SRC_PPT = r"D:\python\ppt.pptx"
SRC_PAGES = {6, 7, 36, 39, 40, 41, 43}       # 只存在於 ppt.pptx 的圖所在頁碼
# event_figures 的裁切比例（左、右、上、下）
MAP_CROP = (0.036, 0.396, 0.176, 0.066)
FIG8_LEFT = (0.036, 0.516, 0.176, 0.246)
FIG8_RIGHT = (0.516, 0.036, 0.176, 0.246)

# 絲縷佈景主題的色票（dk2）與參考簡報標示目前章節的紫色
DARK = RGBColor(0x2E, 0x53, 0x69)
BLACK = RGBColor(0x00, 0x00, 0x00)            # 參考簡報的標題是黑色（母片預設是藍色）
INK = RGBColor(0x26, 0x26, 0x26)
MUTED = RGBColor(0x5F, 0x6F, 0x78)
HL = RGBColor(0x70, 0x30, 0xA0)
ACC = RGBColor(0xC2, 0x57, 0x1A)             # 影響強度（與 event_figures 的橘色一致）
GOOD = RGBColor(0x2E, 0x7D, 0x4F)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
PANEL = RGBColor(0xF7, 0xFA, 0xFB)
ROW = RGBColor(0xE6, 0xEE, 0xF1)
LINE = RGBColor(0xB8, 0xC7, 0xCC)
ROAD = RGBColor(0xB4, 0xC1, 0xC5)
STOP = RGBColor(0xDC, 0xE4, 0xE6)
L, C = PP_ALIGN.LEFT, PP_ALIGN.CENTER
X0, X1 = 1.75, 12.75                          # 內容區左右界（左側留給版型的弧線）
EA_FONT = "標楷體"

CHAPTERS = ["介紹", "系統流程", "事件影響傳播", "路網切割", "交通預測", "成果展示", "結論"]


def ensure_images():
    """scratchpad 的抽圖會被暫存清理刪掉；缺圖時從 ppt.pptx 重新抽出（唯讀）。"""
    need = [f"p{n:02d}_1.png" for n in SRC_PAGES]
    if all(os.path.exists(os.path.join(IMG, f)) for f in need):
        return
    os.makedirs(IMG, exist_ok=True)

    def pics(shapes):
        for sh in shapes:
            if sh.shape_type == MSO_SHAPE_TYPE.GROUP:
                yield from pics(sh.shapes)
            elif sh.shape_type == MSO_SHAPE_TYPE.PICTURE:
                yield sh

    for n, slide in enumerate(Presentation(SRC_PPT).slides, 1):
        if n in SRC_PAGES:
            for k, sh in enumerate(pics(slide.shapes), 1):
                with open(os.path.join(IMG, f"p{n:02d}_{k}.png"), "wb") as f:
                    f.write(sh.image.blob)


def orange(x):
    """與 make_event_figures 的影響強度色階相同（Oranges，0.3 + 0.7x）。"""
    r, g, b, _ = colormaps["Oranges"](0.3 + 0.7 * x)
    return RGBColor(int(r * 255), int(g * 255), int(b * 255))


class WispDeck:
    """以參考簡報為範本的版面元件。字型不指定，一律沿用佈景主題（標楷體 / Times New Roman）。"""

    def __init__(self):
        self.prs = Presentation(TEMPLATE)
        ids = self.prs.slides._sldIdLst
        for sld in list(ids):                       # 移除範本原有的投影片（只在記憶體中）
            self.prs.part.drop_rel(sld.rId)
            ids.remove(sld)
        self.l_cover = self.prs.slide_layouts[0]
        self.l_body = self.prs.slide_layouts[1]

    # ── 文字 ──────────────────────────────────────────────
    @staticmethod
    def fmt(font, sz, c=INK, b=False):
        font.size = Pt(sz)
        font.bold = b
        if c is not None:
            font.color.rgb = c

    @staticmethod
    def bullet(p, hang=True):
        """與參考簡報相同的實心圓點（Wingdings 'l'）。"""
        pPr = p._p.get_or_add_pPr()
        if hang:
            pPr.set("marL", str(Inches(0.38)))
            pPr.set("indent", str(-Inches(0.38)))
        pPr.append(parse_xml('<a:buFont %s typeface="Wingdings" pitchFamily="2" charset="2"/>'
                             % nsdecls("a")))
        pPr.append(parse_xml('<a:buChar %s char="l"/>' % nsdecls("a")))

    def write_items(self, tf, items, sz=26, gap=14, hc=INK, bullets=True, hang=True):
        """items：(粗體重點, 說明) 或 (None, 整句)。"""
        tf.word_wrap = True
        for i, (head, body) in enumerate(items):
            p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
            p.alignment = L
            p.space_before = Pt(0 if i == 0 else gap)
            if bullets:
                self.bullet(p, hang)
            if head:
                r = p.add_run()
                r.text = head + "："
                self.fmt(r.font, sz, hc, True)
            r = p.add_run()
            r.text = body
            self.fmt(r.font, sz, INK, False)
        return tf

    def tx(self, s, l, t, w, h, txt, sz=22, c=INK, b=False, al=L, anchor=None):
        tb = s.shapes.add_textbox(Inches(l), Inches(t), Inches(w), Inches(h))
        tf = tb.text_frame
        tf.word_wrap = True
        if anchor is not None:
            tf.vertical_anchor = anchor
        for i, line in enumerate(txt.split("\n")):
            p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
            p.alignment = al
            r = p.add_run()
            r.text = line
            self.fmt(r.font, sz, c, b)
        return tf

    def items_box(self, s, l, t, w, h, items, sz=22, gap=12, hc=INK):
        tb = s.shapes.add_textbox(Inches(l), Inches(t), Inches(w), Inches(h))
        return self.write_items(tb.text_frame, items, sz, gap, hc)

    def stack_box(self, s, l, t, w, h, items, hs=21, bs=19, gap=14, hc=DARK):
        """窄欄用：粗體重點獨立一行（有圓點），說明縮排在下一行，避免句子被斷在奇怪的地方。"""
        tf = s.shapes.add_textbox(Inches(l), Inches(t), Inches(w), Inches(h)).text_frame
        tf.word_wrap = True
        for i, (head, body) in enumerate(items):
            p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
            p.space_before = Pt(0 if i == 0 else gap)
            self.bullet(p)
            r = p.add_run()
            r.text = head
            self.fmt(r.font, hs, hc, True)
            pp = tf.add_paragraph()
            pp.space_before = Pt(2)
            pPr = pp._p.get_or_add_pPr()
            pPr.set("marL", str(Inches(0.38)))
            pPr.set("indent", "0")
            pPr.append(parse_xml('<a:buNone %s/>' % nsdecls("a")))
            rr = pp.add_run()
            rr.text = body
            self.fmt(rr.font, bs, INK)
        return tf

    @staticmethod
    def notes(s, text):
        s.notes_slide.notes_text_frame.text = text

    # ── 投影片 ────────────────────────────────────────────
    def _slide_number(self, s):
        for ph in self.l_body.placeholders:
            if ph.placeholder_format.type == PP_PLACEHOLDER.SLIDE_NUMBER:
                s.shapes._spTree.append(copy.deepcopy(ph._element))

    def new(self, title, body=False, tsz=40):
        """標題及內容版面。body=True 時回傳內文版面配置區（已移到內容區），否則移除它。"""
        s = self.prs.slides.add_slide(self.l_body)
        t = s.shapes.title
        t.left, t.top, t.width, t.height = Inches(1.85), Inches(0.48), Inches(10.9), Inches(1.1)
        t.text_frame.text = title
        self.fmt(t.text_frame.paragraphs[0].runs[0].font, tsz, BLACK, True)
        ph = [p for p in s.placeholders if p.placeholder_format.idx == 1][0]
        if body:
            ph.left, ph.top = Inches(X0), Inches(1.75)
            ph.width, ph.height = Inches(X1 - X0), Inches(5.0)
        else:
            ph._element.getparent().remove(ph._element)
            ph = None
        self._slide_number(s)
        return s, ph

    def bullets_slide(self, title, items, sz=26, gap=18, note=None):
        s, ph = self.new(title, body=True)
        self.write_items(ph.text_frame, items, sz, gap, hang=False)
        if note:
            self.notes(s, note)
        return s

    def outline(self, cur):
        s, ph = self.new("大綱", body=True, tsz=44)
        ph.left, ph.top, ph.width, ph.height = Inches(4.2), Inches(1.75), Inches(7.0), Inches(5.0)
        tf = ph.text_frame
        for i, name in enumerate(CHAPTERS):
            p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
            p.space_before = Pt(6)
            r = p.add_run()
            r.text = name
            self.fmt(r.font, 32, HL if i == cur else None, i == cur)
        return s

    # ── 圖形 ──────────────────────────────────────────────
    def panel(self, s, l, t, w, h, fill=PANEL, line=None, lw=1.5):
        b = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(l), Inches(t),
                               Inches(w), Inches(h))
        b.adjustments[0] = 0.08
        b.fill.solid()
        b.fill.fore_color.rgb = fill
        if line is None:
            b.line.fill.background()
        else:
            b.line.color.rgb = line
            b.line.width = Pt(lw)
        b.shadow.inherit = False
        tf = b.text_frame
        tf.word_wrap = True
        tf.margin_left = tf.margin_right = Inches(0.15)
        return b

    def card(self, s, l, t, w, h, head, body, hc=DARK, hs=22, bs=18):
        b = self.panel(s, l, t, w, h, PANEL, hc)
        tf = b.text_frame
        tf.vertical_anchor = MSO_ANCHOR.MIDDLE
        p = tf.paragraphs[0]
        p.alignment = C
        r = p.add_run()
        r.text = head
        self.fmt(r.font, hs, hc, True)
        for line in body.split("\n"):
            pp = tf.add_paragraph()
            pp.alignment = C
            pp.space_before = Pt(4)
            rr = pp.add_run()
            rr.text = line
            self.fmt(rr.font, bs, INK)
        return b

    def label_box(self, s, l, t, w, h, txt, fill=DARK, sz=24):
        b = self.panel(s, l, t, w, h, fill)
        tf = b.text_frame
        tf.vertical_anchor = MSO_ANCHOR.MIDDLE
        p = tf.paragraphs[0]
        p.alignment = C
        r = p.add_run()
        r.text = txt
        self.fmt(r.font, sz, WHITE, True)
        return b

    def stat(self, s, l, t, w, h, head, value, vc=ACC):
        b = self.panel(s, l, t, w, h, PANEL, DARK)
        tf = b.text_frame
        tf.vertical_anchor = MSO_ANCHOR.MIDDLE
        p = tf.paragraphs[0]
        p.alignment = C
        r = p.add_run()
        r.text = head
        self.fmt(r.font, 20, DARK, True)
        pp = tf.add_paragraph()
        pp.alignment = C
        pp.space_before = Pt(4)
        rr = pp.add_run()
        rr.text = value
        self.fmt(rr.font, 30, vc, True)
        return b

    def band(self, s, t, txt, sz=22, c=DARK):
        """內容區底部的一句結論（淺底圓角框）。"""
        self.panel(s, X0, t, X1 - X0, 0.75)
        self.tx(s, X0, t, X1 - X0, 0.75, txt, sz, c, True, C, MSO_ANCHOR.MIDDLE)

    def tbl(self, s, l, t, w, h, data, cw, fs=20):
        tb = s.shapes.add_table(len(data), len(data[0]), Inches(l), Inches(t),
                                Inches(w), Inches(h)).table
        for i, ww in enumerate(cw):
            tb.columns[i].width = Inches(ww)
        for r, row in enumerate(data):
            for ci, val in enumerate(row):
                cell = tb.cell(r, ci)
                cell.vertical_anchor = MSO_ANCHOR.MIDDLE
                cell.fill.solid()
                cell.fill.fore_color.rgb = DARK if r == 0 else (WHITE if r % 2 else ROW)
                tf = cell.text_frame
                for k, line in enumerate(val.split("\n")):
                    p = tf.paragraphs[0] if k == 0 else tf.add_paragraph()
                    p.alignment = C
                    run = p.add_run()
                    run.text = line
                    if r == 0:
                        self.fmt(run.font, fs + 1, WHITE, True)
                    else:
                        self.fmt(run.font, fs, DARK if ci == 0 else INK, ci == 0)
        return tb

    def arrow(self, s, l, t, w, h, c=DARK, down=False):
        a = s.shapes.add_shape(MSO_SHAPE.DOWN_ARROW if down else MSO_SHAPE.RIGHT_ARROW,
                               Inches(l), Inches(t), Inches(w), Inches(h))
        a.fill.solid()
        a.fill.fore_color.rgb = c
        a.line.fill.background()
        a.shadow.inherit = False
        return a

    def line(self, s, x1, y1, x2, y2, c=LINE, lw=1.5):
        ln = s.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, Inches(x1), Inches(y1),
                                    Inches(x2), Inches(y2))
        ln.line.color.rgb = c
        ln.line.width = Pt(lw)
        return ln

    def arrow_line(self, s, x1, y1, x2, y2, c, lw=2.5):
        ln = self.line(s, x1, y1, x2, y2, c, lw)
        ln.line._get_or_add_ln().append(parse_xml(
            '<a:tailEnd %s type="triangle" w="med" len="med"/>' % nsdecls("a")))
        return ln

    def dot(self, s, cx, cy, d, c):
        o = s.shapes.add_shape(MSO_SHAPE.OVAL, Inches(cx - d / 2), Inches(cy - d / 2),
                               Inches(d), Inches(d))
        o.fill.solid()
        o.fill.fore_color.rgb = c
        o.line.fill.background()
        o.shadow.inherit = False
        return o

    def val_dot(self, s, cx, cy, d, text, fill, fc=WHITE, sz=18):
        o = self.dot(s, cx, cy, d, fill)
        tf = o.text_frame
        tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
        tf.vertical_anchor = MSO_ANCHOR.MIDDLE
        for i, line in enumerate(text.split("\n")):
            p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
            p.alignment = C
            r = p.add_run()
            r.text = line
            self.fmt(r.font, sz, fc, True)
        return o

    def car(self, s, cx, cy, fill, w=0.34, h=0.2):
        c = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(cx - w / 2),
                               Inches(cy - h / 2), Inches(w), Inches(h))
        c.fill.solid()
        c.fill.fore_color.rgb = fill
        c.line.fill.background()
        c.shadow.inherit = False

    def road(self, s, x0, x1, yc, n_seg, amp, straight=False):
        """兩端是路口（橘點），中間是形狀點的示意道路。"""
        if straight:
            self.line(s, x0, yc, x1, yc, DARK, 5)
            pts = [(x0, yc), (x1, yc)]
        else:
            pts = [(x0 + (x1 - x0) * i / n_seg,
                    yc + amp * math.sin(i * 2 * math.pi / (n_seg / 2))) for i in range(n_seg + 1)]
            for (ax, ay), (bx, by) in zip(pts, pts[1:]):
                self.line(s, ax, ay, bx, by, MUTED, 1.75)
            for x, y in pts[1:-1]:
                self.dot(s, x, y, 0.13, MUTED)
        for (x, y), sgn in ((pts[0], -1), (pts[-1], 1)):
            self.line(s, x, y, x + sgn * 0.32, y - 0.28, DARK, 2)
            self.line(s, x, y, x + sgn * 0.32, y + 0.28, DARK, 2)
            self.dot(s, x, y, 0.28, ACC)

    def crop_pic(self, s, path, l, t, w, crop):
        iw, ih = Image.open(path).size
        cl, cr, ct, cb = crop
        h = w * ((1 - ct - cb) * ih) / ((1 - cl - cr) * iw)
        p = s.shapes.add_picture(path, Inches(l), Inches(t), Inches(w), Inches(h))
        p.crop_left, p.crop_right, p.crop_top, p.crop_bottom = cl, cr, ct, cb
        p.line.color.rgb = LINE
        p.line.width = Pt(0.75)
        return h

    def map_pic(self, s, name, l, t, w, crop=MAP_CROP):
        return self.crop_pic(s, os.path.join(FIG, name), l, t, w, crop)

    def pic(self, s, name, l, t, w=None, h=None):
        kw = {}
        if w:
            kw["width"] = Inches(w)
        if h:
            kw["height"] = Inches(h)
        p = s.shapes.add_picture(os.path.join(IMG, name), Inches(l), Inches(t), **kw)
        p.line.color.rgb = LINE
        p.line.width = Pt(0.75)
        return p

    def group_chart(self, s, l, t, w, h, cats, series, colors, fs=14):
        """分組長條圖，每根長條標數值（一位小數）。"""
        cd = CategoryChartData()
        cd.categories = cats
        for name, vals in series:
            cd.add_series(name, vals)
        ch = s.shapes.add_chart(XL_CHART_TYPE.COLUMN_CLUSTERED, Inches(l), Inches(t),
                                Inches(w), Inches(h), cd).chart
        ch.has_title = False
        ch.font.size = Pt(fs)
        ch.font.color.rgb = INK
        ch.font.name = "Times New Roman"
        rpr = ch._chartSpace.find(qn("c:txPr")).find(".//" + qn("a:defRPr"))
        rpr.append(parse_xml('<a:ea %s typeface="%s"/>' % (nsdecls("a"), EA_FONT)))
        ch.has_legend = True
        ch.legend.position = XL_LEGEND_POSITION.TOP
        ch.legend.include_in_layout = False
        ch.legend.font.size = Pt(fs)
        plot = ch.plots[0]
        plot.gap_width = 70
        plot.overlap = -5
        plot.has_data_labels = True
        dl = plot.data_labels
        dl.number_format = "0.0"
        dl.number_format_is_linked = False
        dl.font.size = Pt(fs - 2)
        for ser, c in zip(plot.series, colors):
            ser.format.fill.solid()
            ser.format.fill.fore_color.rgb = c
            ser.format.line.fill.background()
        va = ch.value_axis
        va.minimum_scale = 0
        va.major_unit = 1.0
        va.has_major_gridlines = True
        va.major_gridlines.format.line.color.rgb = LINE
        va.tick_labels.number_format = "0"
        va.tick_labels.number_format_is_linked = False
        va.format.line.fill.background()
        ch.category_axis.format.line.color.rgb = LINE
        ch.category_axis.tick_labels.font.size = Pt(fs + 2)
        no_fill = ("<c:spPr %s><a:noFill/><a:ln><a:noFill/></a:ln></c:spPr>"
                   % nsdecls("c", "a"))
        cs = ch._chartSpace
        if cs.find(qn("c:spPr")) is None:
            cs.find(qn("c:chart")).addnext(parse_xml(no_fill))
        pa = cs.find(qn("c:chart")).find(qn("c:plotArea"))
        if pa.find(qn("c:spPr")) is None:
            pa.append(parse_xml(no_fill))
        return ch

    def compare_chart(self, s, l, t, w, h, cats, series, colors, fs=16, major=1.0):
        """多組長條放在同一個縱軸上（單位：秒）；小的那組幾乎看不到——這正是重點。"""
        cd = CategoryChartData()
        cd.categories = cats
        for name, vals in series:
            cd.add_series(name, vals)
        ch = s.shapes.add_chart(XL_CHART_TYPE.COLUMN_CLUSTERED, Inches(l), Inches(t),
                                Inches(w), Inches(h), cd).chart
        ch.has_title = False
        ch.font.size = Pt(fs)
        ch.font.color.rgb = INK
        ch.font.name = "Times New Roman"
        rpr = ch._chartSpace.find(qn("c:txPr")).find(".//" + qn("a:defRPr"))
        rpr.append(parse_xml('<a:ea %s typeface="%s"/>' % (nsdecls("a"), EA_FONT)))
        ch.has_legend = True
        ch.legend.position = XL_LEGEND_POSITION.TOP
        ch.legend.include_in_layout = False
        ch.legend.font.size = Pt(fs + 2)
        plot = ch.plots[0]
        plot.gap_width = 60
        plot.overlap = -10
        for ser, c in zip(plot.series, colors):
            ser.format.fill.solid()
            ser.format.fill.fore_color.rgb = c
            ser.format.line.fill.background()
        va = ch.value_axis
        va.minimum_scale = 0
        va.major_unit = major
        va.has_major_gridlines = True
        va.major_gridlines.format.line.color.rgb = LINE
        va.tick_labels.number_format = '0" 秒"'
        va.tick_labels.number_format_is_linked = False
        va.format.line.fill.background()
        ch.category_axis.format.line.color.rgb = LINE
        no_fill = ("<c:spPr %s><a:noFill/><a:ln><a:noFill/></a:ln></c:spPr>"
                   % nsdecls("c", "a"))
        cs = ch._chartSpace
        if cs.find(qn("c:spPr")) is None:
            cs.find(qn("c:chart")).addnext(parse_xml(no_fill))
        pa = cs.find(qn("c:chart")).find(qn("c:plotArea"))
        if pa.find(qn("c:spPr")) is None:
            pa.append(parse_xml(no_fill))
        return ch


# ════════════════════ 封面 ════════════════════
def s_cover(d):
    s = d.prs.slides.add_slide(d.l_cover)
    tf = s.shapes.title.text_frame
    tf.text = "結合交通預測之即時動態導航系統"
    d.fmt(tf.paragraphs[0].runs[0].font, 40, BLACK, True)
    p = tf.add_paragraph()
    r = p.add_run()
    r.text = "A Real-Time Dynamic Navigation System Integrating Traffic Prediction"
    d.fmt(r.font, 22, BLACK, False)
    sub = [ph for ph in s.placeholders if ph.placeholder_format.idx == 1][0]
    sub.text_frame.text = "組員：黃奕睿　林秉達　葉芢杰"
    d.fmt(sub.text_frame.paragraphs[0].runs[0].font, 24, BLACK, False)


# ════════════════════ 一、介紹 ════════════════════
def s_motivation(d):
    d.bullets_slide("研究動機", [
        ("即時資料未被利用", "路況與天氣資料沒用在路線規劃。"),
        ("出事後反應太慢", "突發的車禍、施工無法即時繞開。"),
        ("全部重算太慢", "全台重算一次要好幾秒，跟不上事件。"),
        ("只加成本不夠", "看不出事故的影響會傳到多遠。"),
    ], sz=26)


def s_objective(d):
    d.bullets_slide("研究目的", [
        (None, "整合交通、天氣與 AI 預測，讓路線反映即時路況。"),
        (None, "事件發生時只更新受影響的道路，立即反映到路線上。"),
        (None, "讓事件影響沿道路往後傳播，反映真實的回堵。"),
        (None, "以路網切割加快找路速度，路線結果維持不變。"),
    ], sz=26)


def s_vs_gps(d):
    s, _ = d.new("與一般導航的差別")
    d.tbl(s, X0, 1.85, X1 - X0, 3.9, [
        ["項目", "只把事件當成本加上去", "本系統"],
        ["車禍影響範圍", "固定大小的範圍", "沿道路往後傳，依路網決定遠近"],
        ["路況改變時", "全台灣重新計算", "只更新事故附近的道路"],
        ["找路方式", "逐點搜尋", "路口之間併成一段，一次跨過"],
        ["繞路時機", "塞車後才繞", "預測會塞就提前繞"],
    ], cw=[2.5, 3.5, 5.0], fs=20)
    d.band(s, 6.0, "找路方法（A*）與一般導航相同，差別在事件發生之後")
    d.notes(s, "「只把事件當成本加上去」是我們的第一版。對應數字：台北市區最遠約 1.2 公里、蘇花公路約 6.1 公里；"
               "更新 4.3 秒 → 約 20 毫秒；找路快約 13.5 倍；預測 5～30 分鐘後車速，誤差 2.11～3.13 km/h。")


def s_example(d):
    s, _ = d.new("全場範例：國道一號車禍")
    d.map_pic(s, "5_路線反應.png", X0, 1.75, 6.3)
    d.items_box(s, 8.35, 1.9, 4.4, 3.2, [
        ("路線", "台北 → 台中"),
        ("車禍前", "走國道一號"),
        ("車禍後", "自動改道"),
        ("代價", "多繞約 11 公里"),
    ], sz=22, gap=16, hc=DARK)
    d.tx(s, 8.35, 5.35, 4.4, 0.9, "後續各章依序說明\n系統如何完成這次改道", 20, MUTED)
    d.notes(s, "平衡模式、平日上午 10 點的時段速度：車禍前 162.74 公里、113.2 分鐘；"
               "車禍後 173.79 公里、117.7 分鐘。不同時段數字會不同（尖峰會降速）。")


# ════════════════════ 二、系統流程 ════════════════════
FLOW_FONT = "Microsoft JhengHei"              # 流程圖內文字用正黑體（與參考簡報的架構圖一致）
F_BLUE = RGBColor(0x24, 0x4F, 0xB3)
F_GREEN = RGBColor(0x2E, 0x7D, 0x32)
F_ORANGE = RGBColor(0xEE, 0x8A, 0x22)
F_LINE = RGBColor(0x26, 0x26, 0x26)


def _ffont(font, sz, bold, color=INK):
    font.size = Pt(sz)
    font.bold = bold
    font.color.rgb = color
    font.name = FLOW_FONT
    rPr = font._rPr
    ea = rPr.find(qn("a:ea"))
    if ea is None:
        ea = rPr.makeelement(qn("a:ea"), {})
        rPr.append(ea)
    ea.set("typeface", FLOW_FONT)


def _fshape(s, kind, l, t, w, h, head, sub=None, color=F_BLUE, hs=12, ss=10.5, lw=1.5):
    """流程圖元件：白底、彩色框線，粗體標題＋一行說明。"""
    b = s.shapes.add_shape(kind, Inches(l), Inches(t), Inches(w), Inches(h))
    b.fill.solid()
    b.fill.fore_color.rgb = WHITE
    b.line.color.rgb = color
    b.line.width = Pt(lw)
    b.shadow.inherit = False
    tf = b.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = MSO_ANCHOR.MIDDLE
    tf.margin_left = tf.margin_right = Inches(0.04)
    tf.margin_top = tf.margin_bottom = Inches(0.01)
    p = tf.paragraphs[0]
    p.alignment = C
    r = p.add_run()
    r.text = head
    _ffont(r.font, hs, True)
    if sub:
        pp = tf.add_paragraph()
        pp.alignment = C
        rr = pp.add_run()
        rr.text = sub
        _ffont(rr.font, ss, False)
    return b


def _flabel(d, s, l, t, w, txt, color=INK, sz=10.5, bold=True, al=C):
    tf = s.shapes.add_textbox(Inches(l), Inches(t), Inches(w), Inches(0.28)).text_frame
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
    p = tf.paragraphs[0]
    p.alignment = al
    r = p.add_run()
    r.text = txt
    _ffont(r.font, sz, bold, color)


def _fpath(d, s, pts, color=F_LINE, lw=1.25, dashed=False, arrow=True):
    """折線：pts 依序相連，最後一段帶箭頭。"""
    for i, (a, b) in enumerate(zip(pts, pts[1:])):
        last = i == len(pts) - 2
        ln = (d.arrow_line(s, *a, *b, color, lw) if (last and arrow)
              else d.line(s, *a, *b, color, lw))
        if dashed:
            ln.line.dash_style = MSO_LINE.DASH


def s_arch_diagram(d):
    s, _ = d.new("系統架構圖")
    bg = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(1.75), Inches(1.42),
                            Inches(11.05), Inches(5.86))
    bg.fill.solid()
    bg.fill.fore_color.rgb = WHITE
    bg.line.color.rgb = LINE
    bg.line.width = Pt(0.75)
    bg.shadow.inherit = False
    for (t, h, color) in ((1.5, 3.5, F_BLUE), (5.1, 2.1, F_GREEN)):
        lane = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(1.85), Inches(t),
                                  Inches(10.85), Inches(h))
        lane.adjustments[0] = 0.03
        lane.fill.background()
        lane.line.color.rgb = color
        lane.line.width = Pt(1.75)
        lane.shadow.inherit = False
    _flabel(d, s, 2.0, 1.58, 3.5, "A. 線上導航流程", F_BLUE, 15, al=L)
    _flabel(d, s, 2.0, 5.17, 3.5, "B. 背景路況更新流程", F_GREEN, 15, al=L)

    # 符號依 ISO 5807：開始/結束＝圓角端點、輸入＝平行四邊形、處理＝矩形、判斷＝菱形
    # （寫條件、出口標結果）、儲存資料＝圓柱、註解＝括號＋虛線；每條路徑都以端點結束。

    # ── A. 線上導航：由上而下 ──
    cx, bx, bw = 6.9, 5.2, 3.4
    _fshape(s, MSO_SHAPE.FLOWCHART_TERMINATOR, bx, 1.62, bw, 0.34, "開始：使用者設定起終點")
    _fshape(s, MSO_SHAPE.FLOWCHART_DATA, 5.0, 2.06, 3.8, 0.48, "輸入路線條件", "起點、終點、路線模式")
    _fshape(s, MSO_SHAPE.FLOWCHART_DECISION, 5.4, 2.66, 3.0, 0.56, "座標在台灣範圍內",
            color=F_ORANGE, hs=11.5)
    _fshape(s, MSO_SHAPE.RECTANGLE, bx, 3.34, bw, 0.34, "找出最近的道路節點")
    _fshape(s, MSO_SHAPE.RECTANGLE, bx, 3.80, bw, 0.48, "A* 路徑搜尋", "在切割路網上找最佳與備選路線")
    _fshape(s, MSO_SHAPE.RECTANGLE, bx, 4.40, bw, 0.48, "展開回實際道路", "整理距離、時間、途經事件")
    for y0, y1 in ((1.96, 2.06), (2.54, 2.66), (3.22, 3.34), (3.68, 3.80), (4.28, 4.40)):
        d.arrow_line(s, cx, y0, cx, y1, F_LINE, 1.25)
    _flabel(d, s, cx + 0.12, 3.21, 0.4, "是", sz=10.5, al=L)
    # 座標不在範圍內 → 結束
    d.arrow_line(s, 8.4, 2.94, 9.3, 2.94, F_LINE, 1.25)
    _flabel(d, s, 8.4, 2.66, 0.9, "否", sz=10.5)
    _fshape(s, MSO_SHAPE.FLOWCHART_TERMINATOR, 9.3, 2.74, 2.5, 0.4, "結束：提示重新選點")
    # 註解（括號＋虛線）：A* 的成本組成
    tf = s.shapes.add_textbox(Inches(2.0), Inches(3.80), Inches(2.25), Inches(0.48)).text_frame
    tf.word_wrap = True
    tf.margin_top = tf.margin_bottom = 0
    tf.vertical_anchor = MSO_ANCHOR.MIDDLE
    for k, (txt, b) in enumerate((("道路成本", True), ("時間、風險、轉彎、紅綠燈", False))):
        p = tf.paragraphs[0] if k == 0 else tf.add_paragraph()
        p.alignment = PP_ALIGN.RIGHT
        r = p.add_run()
        r.text = txt
        _ffont(r.font, 12 if b else 10.5, b)
    for a, b in (((4.4, 3.80), (4.4, 4.28)), ((4.28, 3.80), (4.4, 3.80)), ((4.28, 4.28), (4.4, 4.28))):
        d.line(s, *a, *b, F_LINE, 1.25)
    _fpath(d, s, [(4.4, 4.04), (bx, 4.04)], dashed=True, arrow=False)
    # 讀取路網成本（儲存資料）
    st = _fshape(s, MSO_SHAPE.CAN, 9.55, 3.72, 1.5, 0.6, "路網成本", "記憶體中")
    st.adjustments[0] = 0.18
    d.arrow_line(s, 9.55, 4.04, bx + bw, 4.04, F_LINE, 1.25)
    _flabel(d, s, bx + bw, 3.76, 0.95, "讀取", sz=10)
    # 輸出 → 結束
    _fshape(s, MSO_SHAPE.FLOWCHART_TERMINATOR, 9.2, 4.44, 2.3, 0.4, "結束：地圖顯示路線")
    d.arrow_line(s, bx + bw, 4.64, 9.2, 4.64, F_LINE, 1.25)

    # ── B. 背景路況更新：由左而右 ──
    top, bot, mid = 5.86, 6.78, 6.32
    _fshape(s, MSO_SHAPE.FLOWCHART_TERMINATOR, 2.0, top - 0.25, 2.1, 0.5, "開始：新增事件",
            "管理員手動、AI 預測", color=F_GREEN, hs=11.5)
    _fshape(s, MSO_SHAPE.FLOWCHART_TERMINATOR, 2.0, bot - 0.25, 2.1, 0.5, "開始：定時同步",
            "每 5 分鐘 TDX、CWA", color=F_GREEN, hs=11.5)
    _fpath(d, s, [(4.1, top), (4.3, top), (4.3, mid), (4.5, mid)])
    _fpath(d, s, [(4.1, bot), (4.3, bot), (4.3, mid)], arrow=False)
    db = _fshape(s, MSO_SHAPE.CAN, 4.5, mid - 0.34, 1.75, 0.68, "事件資料庫", "事件、天氣",
                 color=F_GREEN)
    db.adjustments[0] = 0.18
    d.arrow_line(s, 6.25, mid, 6.5, mid, F_LINE, 1.25)
    _fshape(s, MSO_SHAPE.FLOWCHART_DECISION, 6.5, mid - 0.3, 2.0, 0.6, "更新類型",
            color=F_ORANGE)
    _fpath(d, s, [(7.5, mid - 0.3), (7.5, top), (8.85, top)])
    _fpath(d, s, [(7.5, mid + 0.3), (7.5, bot), (8.85, bot)])
    _flabel(d, s, 7.6, top - 0.3, 1.2, "單一事件", sz=10)
    _flabel(d, s, 7.6, bot + 0.03, 1.2, "整批更新", sz=10)
    _fshape(s, MSO_SHAPE.RECTANGLE, 8.85, top - 0.25, 2.0, 0.5, "增量更新", "只改附近並沿路往後傳",
            color=F_GREEN)
    _fshape(s, MSO_SHAPE.RECTANGLE, 8.85, bot - 0.25, 2.0, 0.5, "全量重算", "整張路網重新計算",
            color=F_GREEN)
    _fpath(d, s, [(10.85, top), (11.0, top), (11.0, mid), (11.15, mid)])
    _fpath(d, s, [(10.85, bot), (11.0, bot), (11.0, mid)], arrow=False)
    _fshape(s, MSO_SHAPE.RECTANGLE, 11.15, mid - 0.25, 1.4, 0.5, "更新路網成本", "同步切割路網",
            color=F_GREEN)
    d.arrow_line(s, 11.85, mid + 0.25, 11.85, 6.76, F_LINE, 1.25)
    _fshape(s, MSO_SHAPE.FLOWCHART_TERMINATOR, 11.3, 6.76, 1.1, 0.34, "結束", color=F_GREEN)

    # ── B 寫入、A 讀取同一份路網成本：下一次搜尋立即反映 ──
    _fpath(d, s, [(11.85, mid - 0.25), (11.85, 4.04), (11.05, 4.04)])
    _flabel(d, s, 11.92, 4.5, 0.6, "寫入", sz=10, al=L)
    d.notes(s, "A：POST /route。座標須在 lat 21.5～26.5、lon 118～122.5，超出回傳 400。"
               "最近節點以載入時的節點快照搜尋；A* 在路口收縮圖上搜尋（含 3 條備選），再展開回原路段。"
               "成本 = 行車時間 × (時間權重 + 風險 × 風險權重) + 轉彎 + 號誌。\n"
               "B：手動事件（POST /events）與 AI 預測（POST /predict/run）走增量更新：新增一個事件到生效約 33 毫秒（中位數，含寫入與傳播），"
               "其中沿上游傳播 1～12 毫秒；每 5 分鐘的 TDX/CWA 同步、刪除事件、天氣變動走全量重算（2.6～4.3 秒，背景執行）。"
               "兩條路都只改記憶體中的路網成本並同步切割路網，下一次搜尋立即反映。")


def s_layers(d):
    s, _ = d.new("系統分層")
    layers = [("資料", "路況、交通新聞、天氣、管理員輸入、AI 預測"),
              ("事件", "統一寫成同一種格式：發生地點與嚴重程度"),
              ("路網", "每條道路目前的行車時間與危險程度"),
              ("路線", "找出最合適的路線，顯示在地圖上")]
    for i, (head, body) in enumerate(layers):
        y = 1.8 + i * 1.12
        d.label_box(s, X0, y, 1.8, 0.82, head)
        d.panel(s, 3.75, y, X1 - 3.75, 0.82)
        d.tx(s, 3.95, y, X1 - 4.15, 0.82, body, 22, INK, anchor=MSO_ANCHOR.MIDDLE)
        if i < 3:
            d.arrow(s, X0 + 0.78, y + 0.84, 0.24, 0.26, ACC, down=True)
    d.band(s, 6.3, "新資料只要寫成「事件」即可接上，找路程式不需修改", 21)
    d.notes(s, "資料來源：TDX 車速偵測器 1,361 站、TDX 路況新聞、CWA 700+ 氣象站，每 5 分鐘同步。"
               "路網 765 萬條路段，切割後 7.8 萬段。AI 預測（T-GCN）寫成「預測壅塞」事件接入。")


def s_search(d):
    s = d.bullets_slide("找路方法：A* 搜尋", [
        ("方向", "每一步選「已花時間＋預估剩餘時間」最少的方向前進。"),
        ("不會錯過", "預估值刻意偏低，保證找到最佳路線。"),
        ("成本", "行車時間、危險程度、轉彎與紅綠燈都計入。"),
        ("三種模式", "最快、平衡、最安全，差別在對危險的重視程度。"),
    ], sz=26, gap=20,
        note="A*：f = g + h，h = 直線距離 ÷ 120 km/h；全台速限最高 110 km/h，所以 h 不會高估。"
             "轉彎 10 秒、迴轉 25 秒、每個紅綠燈 20 秒（高速公路不計）。")
    d.band(s, 6.15, "此部分與一般導航相同，本系統的重點在事件發生之後", 21, ACC)


def s_event_life(d):
    s, _ = d.new("事件發生後的四個步驟")
    steps = [("① 影響範圍", "找出這場車禍\n會影響的道路"),
             ("② 更新道路", "只改這些道路的\n行車時間"),
             ("③ 同步地圖", "同步到找路用的\n簡化地圖"),
             ("④ 重新找路", "在新路況上搜尋\n自動繞開")]
    for i, (h, b) in enumerate(steps):
        x = X0 + i * 2.83
        d.card(s, x, 1.9, 2.55, 2.0, h, b)
        if i < 3:
            d.arrow(s, x + 2.58, 2.75, 0.22, 0.3)
    d.panel(s, X0, 4.25, X1 - X0, 0.95)
    d.tx(s, X0 + 0.25, 4.25, X1 - X0 - 0.5, 0.95,
         "全台路況每 5 分鐘整批更新時才整張重算，於背景執行，不影響使用者。", 20, INK,
         anchor=MSO_ANCHOR.MIDDLE)
    d.band(s, 5.6, "新增一個事件，不到一眨眼的時間就反映到路線上")
    d.notes(s, "這場車禍：① 事故點周圍 ±0.80 公里，往上游最遠 6.0 公里；② 方框內相連 55 條＋傳播 166 條路段；"
               "③ 2 段（全台 77,725 段）；④ 重新搜尋不到 0.1 秒。"
               "新增一個事件到生效約 33 毫秒（中位數，含傳播 1～12 毫秒）；整張重算 2.6～4.3 秒。")


def s_local_update(d):
    s, _ = d.new("依道路特性更新受影響的道路")
    d.tx(s, X0, 1.62, X1 - X0, 0.8,
         "系統依事故所在道路的特性判斷影響範圍：道路越重要，影響範圍越大；\n"
         "只更新範圍內、沿道路與事故相連的道路（橘色），範圍外或不相連的道路完全不動。", 19, INK)
    path = os.path.join(FIG, "9_道路特性比較.png")
    iw, ih = Image.open(path).size
    w = 9.8
    p = s.shapes.add_picture(path, Inches(X0 + (X1 - X0 - w) / 2), Inches(2.5), Inches(w))
    p.line.color.rgb = LINE
    p.line.width = Pt(0.75)
    d.notes(s, "同一場車禍（嚴重度 0.80）放在兩種道路，兩圖比例尺相同。影響範圍由道路特性推估："
               "道路關鍵度（國道 1.00、省道 0.65、社區道路 0.15）、附近替代道路密度、路口分流能力；"
               "實測替代道路密度在主要道路上幾乎都達上限，差異主要來自道路等級。"
               "更新的路段與系統相同：±r 方框內、沿道路與事故點相連的路段。國道沿線 1.6 公里內沒有交流道，底下的平面道路與國道不相連，所以國道車禍只更新國道本身（兩個方向，55 條路段，原方框法 1,546 條）；社區道路的例子中，方框上緣那條不相連的小路也不再更新（552 → 426 條）。"
               "只更新範圍內的路段：新增事件約 0.03 秒生效，整張重算約 4 秒；"
               "時段與路線模式在找路時才代入，所以改一次三種模式都適用。")


def s_linear(d):
    s, _ = d.new("事件再多也不會算不完")
    cats = ["0", "5,000", "1 萬", "2 萬"]
    # benchmark_events.py scale --no-closure（事件放在真實道路上、路口收縮＋事件傳播），單位：秒
    full = [2.6, 72.2, 145.2, 287.1]
    inc = [0.034, 0.033, 0.036, 0.031]
    d.compare_chart(s, X0, 1.7, 6.6, 4.0, cats,
                    [("全部重算", full), ("新增一個事件", inc)], [MUTED, ACC], major=60)
    d.tx(s, X0, 5.7, 6.6, 0.4, "橫軸：系統中已有的事件數；新增一個事件約 0.03 秒，小到看不見",
         15, MUTED, al=C)
    d.stack_box(s, 8.65, 1.9, 4.1, 4.0, [
        ("不會連鎖", "每個事件只影響附近"),
        ("會停下來", "影響越傳越小就停止"),
        ("線性成長", "事件加倍，工作也加倍"),
    ])
    d.band(s, 6.25, "找路速度與事件數量無關：已有 2 萬個事件時仍約 0.1 秒")
    d.notes(s, "benchmark_events.py scale（資料庫副本、事件隨機放在主幹道路上、路口收縮＋事件傳播）。"
               "已有 0／5,000／1 萬／2 萬個事件：全量重算 2.6／72.2／145.2／287.1 秒（線性成長）；"
               "新增一個事件到生效 34／33／36／31 毫秒（中位數）；台北→台中找路 0.07／0.08／0.09／0.13 秒。"
               "k 個事件更新 O(k·m)，m 為每個事件碰到的路段數；找路 O(E log E)，與事件數無關。"
               "含 2% 封路時，1 萬個事件（約 200 處封路）會把台北與台中之間的主幹道路切斷而找不到路——"
               "這是封路數量的問題，不是速度問題。")


def s_burst(d):
    s, _ = d.new("大量事件同時湧入")
    for i, (head, val) in enumerate((("同時送出", "1,000 個事件"), ("處理結果", "全部成功"),
                                     ("與全量重算比對", "結果一致"))):
        d.stat(s, X0 + i * 3.7, 1.8, 3.4, 1.35, head, val)
    d.stack_box(s, 2.0, 3.55, 10.6, 3.0, [
        ("排隊處理", "依序套用，每秒約處理 4 個，不會遺漏也不會重複"),
        ("找路照常", "湧入期間仍可找路，稍慢但不中斷"),
        ("找到並修正", "新增事件與全量重算剛好同時發生時會重複計算，已加鎖修正"),
    ], hs=20, bs=18, gap=12)
    d.notes(s, "benchmark_events.py http：真的啟動 API 伺服器（資料庫副本），64 個連線同時送出 1,000 個 POST /events，"
               "途中觸發一次全量重算。全部回應 200、0 錯誤、1,000 個事件都在；共 233 秒（每秒 4.3 個），"
               "每個請求等待中位數 8.1 秒、最久 105 秒（Python 的鎖不保證先來先處理）。"
               "同時持續找路：中位數 0.99 秒（平常 0.20 秒）、最久 8.6 秒（遇到全量重算時）。"
               "湧入後的路線與全量重算後完全相同（162.83 公里、125.94 分鐘，傍晚尖峰時段）。"
               "修正前（benchmark_events.py burst，500＋500 個事件、途中重算）：3,934 條路段風險被算兩次（+6.4）；"
               "修正後 0 條。修正方式：osm_api 的「寫入事件＋套用」與「全量重算」互斥（_events_guard）。")


# ════════════════════ 三、事件影響傳播 ════════════════════
def s_queue_idea(d):
    s, _ = d.new("車禍影響沿道路往後傳")
    yr, xa, xj = 3.3, 11.95, 7.3
    d.line(s, 2.1, yr, xa + 0.55, yr, ROAD, 14)
    d.line(s, xj, yr, xj - 1.6, yr + 1.35, ROAD, 10)
    d.arrow_line(s, 6.9, 2.3, 10.9, 2.3, MUTED, 2)
    d.tx(s, 6.9, 1.8, 4.0, 0.4, "行車方向", 18, MUTED, True, C)
    x, gap = xa - 0.55, 0.42
    while x > 2.3:                                   # 越靠近事故越密、越深
        v = math.exp(-(xa - x) / 3.2) * (0.78 if x < xj else 1.0)
        d.car(s, x, yr, orange(v))
        x -= gap
        gap *= 1.09
    for k in range(1, 4):                            # 交流道分走一部分車
        t = k / 4.2
        d.car(s, xj - 1.6 * t, yr + 1.35 * t, orange(0.22))
    d.val_dot(s, xa, yr, 0.66, "車禍", ACC, sz=16)
    for lx, ly, head, body in ((10.0, 3.85, "① 事故點最塞", "越靠近越慢"),
                               (2.6, 3.85, "② 越往後越不塞", "影響逐漸變小"),
                               (7.2, 4.4, "③ 遇到交流道", "車流分散，影響變小")):
        d.tx(s, lx, ly, 3.4, 0.9, head + "\n" + body, 19, INK)
    d.band(s, 5.75, "影響範圍不是固定的圓，而是由道路的連接方式決定", 21)
    d.notes(s, "示意圖。實際數字：每往上游約 2.5 公里，影響剩約 37%；遇岔路依車道數分配。"
               "衰減速度由 164 件國道一號事故的回堵資料推得。")


def s_two_places(d):
    s, _ = d.new("路網疏密決定影響範圍")
    path = os.path.join(FIG, "8_傳播比較_市區與單一道路.png")
    for crop, x, head, body in ((FIG8_LEFT, 1.85, "台北市區", "道路密集，影響很快分散"),
                                (FIG8_RIGHT, 7.4, "蘇花公路", "沒有替代道路，一路往後排")):
        h = d.crop_pic(s, path, x, 1.7, 5.3, crop)
        d.tx(s, x, 1.8 + h, 5.3, 0.9, head + "\n" + body, 20, INK)
    d.tx(s, X0, 6.6, X1 - X0, 0.4, "箭頭為影響傳播方向，越大越深表示影響越強；兩圖比例尺相同", 15, MUTED)
    d.notes(s, "同一套演算法、同一種事件。台北忠孝東路：最遠 1.2 公里，76 段路、305 條路段，強影響（≥0.37）25 條。"
               "蘇花公路：最遠 6.1 公里，7 段路、311 條路段，強影響 154 條。原本的方框法在兩地都是 ±0.66 公里。")


def s_this_accident(d):
    s, _ = d.new("範例車禍的影響範圍")
    d.map_pic(s, "7_事件沿上游傳播.png", X0, 1.75, 6.4)
    d.stack_box(s, 8.45, 1.85, 4.3, 4.8, [
        ("橘色箭頭", "影響傳播的方向"),
        ("箭頭大小", "越大越深，影響越強"),
        ("藍色方框", "原方法處理的範圍"),
        ("結果", "最遠約 6 公里，與真實回堵相近"),
    ], gap=12)
    d.notes(s, "影響 7 段路口之間的路、166 條路段，最遠 6.0 公里；高公局實測國道一號事故回堵 p75 為 6.43 公里。"
               "事件沒有方向資訊，所以兩個方向都往各自的上游傳。")


def s_push(d):
    s, _ = d.new("影響逐段往後傳遞")
    y, yr = 3.1, 4.6
    d.arrow_line(s, 3.0, 1.95, 11.5, 1.95, MUTED, 1.5)
    d.tx(s, 3.0, 1.5, 8.5, 0.4, "行車方向", 18, MUTED, True, C)
    nodes = {"E": (12.0, y, "車禍\n100%", 1.0), "J": (9.4, y, "37%", 0.37),
             "M1": (7.2, y, "28%", 0.28), "M2": (4.8, y, "10%", 0.10),
             "R1": (7.2, yr, "9%", 0.09)}
    edges = [("E", "J", "每段剩約 1/3", False), ("J", "M1", "車道多分得多", False),
             ("M1", "M2", "再剩約 1/3", False), ("J", "R1", "車道少分得少", True)]
    for a, b, lab, diag in edges:
        (x1, y1, *_), (x2, y2, *_) = nodes[a], nodes[b]
        dy = 0.25 if diag else 0.0
        d.arrow_line(s, x1 - 0.45, y1 + dy, x2 + 0.45, y2 - dy, ACC, 3)
        if diag:
            d.tx(s, x2 + 1.3, (y1 + y2) / 2 - 0.1, 2.8, 0.4, lab, 18, INK)
        else:
            d.tx(s, x2 + 0.1, y1 - 0.62, x1 - x2 - 0.2, 0.4, lab, 18, INK, al=C)
    for key, (cx, cy, txt, v) in nodes.items():
        d.val_dot(s, cx, cy, 1.0 if key == "E" else 0.85, txt, orange(v),
                  WHITE if v >= 0.25 else INK, 16 if key == "E" else 18)
    for x1, x2, yy in ((4.8, 2.5, y), (7.2, 4.8, yr)):
        d.arrow_line(s, x1 - 0.45, yy, x2 + 0.42, yy, ACC, 1.5)
        d.val_dot(s, x2, yy, 0.85, "停", STOP, MUTED, 18)
        d.tx(s, x2 - 1.0, yy + 0.47, 2.0, 0.4, "小於 5% 即停", 16, MUTED, al=C)
    d.band(s, 5.75, "影響逐段變小，小於 5% 即停止，因此不會無限擴散", 21)
    d.tx(s, X0, 6.55, X1 - X0, 0.4, "圓圈數字為影響強度，車禍處為 100%", 16, MUTED)
    d.notes(s, "依實際公式畫的示意：每 2.5 公里乘 exp(−1) ≈ 0.37；岔路依車道數代理值分配"
               "（例：主線 3 車道、匝道 1 車道 → 3/4、1/4）；低於 0.05 就停。"
               "計算方法是增量 PageRank 的殘差推送（Andersen, Chung & Lang, 2006）：PageRank 讓網頁重要性"
               "沿連結逐層傳遞，這裡讓車禍影響沿道路逐段傳遞。全台主幹路網切成 77,725 段；"
               "同一地點第一次計算約十幾毫秒，結果記錄下來，再發生時查表不到 1 毫秒。")


def s_graph_terms(d):
    s, _ = d.new("把路網當成一張圖")
    yr, js, xe = 2.75, (2.3, 5.5, 8.7, 11.9), 10.3
    bx, by = 4.1, 3.75                                # 匯入的岔路
    d.line(s, js[0] - 0.3, yr, js[3] + 0.4, yr, ROAD, 12)
    d.line(s, js[1], yr, bx, by, ROAD, 9)
    for (x1, y1, x2, y2), v in (((js[2], yr, js[3], yr), 1.0), ((js[1], yr, js[2], yr), 0.45),
                                ((js[0], yr, js[1], yr), 0.2), ((js[1], yr, bx, by), 0.15)):
        d.line(s, x1, y1, x2, y2, orange(v), 12 if y1 == y2 else 9)
        n = int(math.hypot(x2 - x1, y2 - y1) / 0.4)
        for k in range(1, n):                         # 形狀點：只描繪彎道，不是路口
            d.dot(s, x1 + (x2 - x1) * k / n, y1 + (y2 - y1) * k / n, 0.09, WHITE)
    for x in js:
        d.dot(s, x, yr, 0.36, DARK)
    ev = d.val_dot(s, xe, yr, 0.62, "車禍", WHITE, ACC, 13)
    ev.line.color.rgb = ACC
    ev.line.width = Pt(2.5)
    for x1, y1, x2, y2 in ((xe - 0.4, 2.25, js[2] + 0.2, 2.25), (js[2] - 0.2, 2.25, js[1] + 0.2, 2.25),
                           (js[1] - 0.2, 2.25, js[0] + 0.2, 2.25), (js[1] - 0.25, 3.05, bx + 0.15, 3.8)):
        d.arrow_line(s, x1, y1, x2, y2, ACC, 2.5)
    d.tx(s, 2.3, 1.7, 3.6, 0.4, "傳播鏈（往上游）", 17, ACC, True)
    d.arrow_line(s, 10.3, 1.9, 12.3, 1.9, MUTED, 1.5)
    d.tx(s, 8.7, 1.7, 1.6, 0.4, "行車方向", 16, MUTED, True)
    d.tx(s, js[2] - 0.9, 3.0, 1.8, 0.4, "核心節點", 16, DARK, True, C)
    d.tx(s, 6.0, 3.0, 2.2, 0.4, "子圖", 16, DARK, True, C)
    d.tx(s, xe - 0.9, 3.12, 1.8, 0.4, "活動節點", 16, ACC, True, C)
    for i, (head, body, c) in enumerate((("核心節點", "路口：車子可以\n選擇方向的地方", DARK),
                                         ("子圖", "兩個路口之間\n的一段路", DARK),
                                         ("活動節點", "發生事件的路段，\n從這裡開始計算", ACC),
                                         ("傳播鏈", "從事件往上游、\n沿道路相連的路徑", ACC))):
        d.card(s, X0 + i * 2.8, 4.1, 2.6, 1.5, head, body, c, 20, 16)
    d.band(s, 5.85, "只計算活動節點所在的傳播鏈，其餘子圖完全不動", 21)
    d.notes(s, "核心節點＝路口（無向度數 ≠ 2，含道路盡頭），全台主幹路網 35,668 個；"
               "子圖＝兩路口之間的形狀點鏈，共 77,725 段；活動節點＝事件所在的那一段；"
               "影響沿傳播鏈往上游推送，遇到匯入的岔路依車道數分配。"
               "圖中顏色深淺示意影響強度，越往上游越淡。")


def s_pagerank(d):
    s, _ = d.new("增量 PageRank：只算受影響的部分")
    d.tx(s, X0, 1.6, X1 - X0, 0.8,
         "PageRank 讓網頁的重要性沿著連結傳遞；\n"
         "系統用同樣的方法，讓車禍的影響沿著道路傳遞。", 19, INK)
    for x, head, c, items in (
            (X0, "全部重算", MUTED, ["每有一個事件，全台每段路都重算",
                                    "大部分道路其實沒有變化", "路網越大，算得越久"]),
            (7.4, "增量推送", ACC, ["只從事件所在的路段開始",
                                   "每段路只記「還要往上游傳多少」",
                                   "剩下的小於 5% 就停止"])):
        d.panel(s, x, 2.55, 5.35, 2.35, PANEL, c)
        d.tx(s, x + 0.25, 2.65, 4.9, 0.5, head, 24, c, True)
        d.items_box(s, x + 0.25, 3.2, 4.9, 1.6, [(None, t) for t in items], sz=20, gap=8)
    d.band(s, 5.2, "全台 77,725 段路中，範例車禍只需處理 7 段", 21, ACC)
    d.tx(s, X0, 6.05, X1 - X0, 0.4, "計算量只與受影響的範圍有關，與路網大小無關", 17, MUTED, al=C)
    d.notes(s, "殘差推送（Andersen, Chung & Lang, 2006）：每段路有影響估計值 x 與待傳殘差 r。"
               "每次取出殘差最大的一段，把 r 加進 x，再依「車道比例 × exp(−長度 ÷ 2.5 公里)」分給上游各段；"
               "所有殘差都小於 0.05 時停止。等同求解 x = r₀ + B·x，但只碰到受影響的路段。"
               "驗證：與精確解 (I − B)⁻¹ 比較，影響 ≥ 0.05 的路段無遺漏，最大誤差 0.017。"
               "國道一號寶山車禍：7 段路口之間的路、166 條路段，最遠 6.0 公里。")


def s_dep_index(d):
    s, _ = d.new("直接相依索引：算過的直接查表")
    lx, lw, rx, rw = 1.9, 3.4, 6.0, 2.7
    lc, rc = lx + lw / 2, rx + rw / 2
    _fshape(s, MSO_SHAPE.FLOWCHART_TERMINATOR, lx, 1.8, lw, 0.72, "事件發生在某段路",
            "（活動節點）", F_BLUE, 17, 14)
    _fshape(s, MSO_SHAPE.FLOWCHART_DECISION, lx, 2.85, lw, 1.1, "索引中已有記錄", None, F_BLUE, 16)
    _fshape(s, MSO_SHAPE.FLOWCHART_PROCESS, lx, 4.3, lw, 0.72, "直接取出影響", "查表，不必重算",
            F_GREEN, 17, 14)
    _fshape(s, MSO_SHAPE.FLOWCHART_PROCESS, rx, 2.95, rw, 0.9, "增量 PageRank", "計算影響",
            F_ORANGE, 17, 14)
    _fshape(s, MSO_SHAPE.FLOWCHART_PROCESS, rx, 4.3, rw, 0.72, "存入索引", "flag 改為 A",
            F_ORANGE, 17, 14)
    _fshape(s, MSO_SHAPE.FLOWCHART_TERMINATOR, lx, 5.4, lw, 0.72, "套用到受影響路段", None,
            F_BLUE, 17)
    _fpath(d, s, [(lc, 2.52), (lc, 2.85)])
    _fpath(d, s, [(lc, 3.95), (lc, 4.3)])
    _fpath(d, s, [(lc, 5.02), (lc, 5.4)])
    _fpath(d, s, [(lx + lw, 3.4), (rx, 3.4)])
    _fpath(d, s, [(rc, 3.85), (rc, 4.3)])
    _fpath(d, s, [(rc, 5.02), (rc, 5.76), (lx + lw, 5.76)])
    _flabel(d, s, lc + 0.1, 3.98, 1.4, "是 · flag A", F_GREEN, 13, al=L)
    _flabel(d, s, lx + lw, 3.06, rx - lx - lw, "否", F_ORANGE, 13)
    _flabel(d, s, lx + lw, 3.47, rx - lx - lw, "flag N", F_ORANGE, 13)
    d.stack_box(s, 9.0, 1.85, 3.75, 4.3, [
        ("直接相依", "記下這段路出事時，會影響哪些路段、影響多少"),
        ("遠處直接更新", "不必等傳播鏈一段一段算完"),
        ("flag N → A", "第一次發生時計算並存入索引，之後直接查表"),
        ("效果", "首次十幾毫秒，之後不到 1 毫秒"),
    ], hs=19, bs=16, gap=12)
    d.notes(s, "索引以事件所在路段為鍵，存「受影響路段與影響值」，相當於核心節點之間的直接相依係數；"
               "在索引中＝flag A，不在＝flag N。建索引時用較嚴的門檻（0.005）推送，"
               "避免多條路徑匯入同一段路時一起丟掉殘差（用 0.05 時實測誤差 0.059、漏掉 3 段）。"
               "最多保留 4,096 個事件路段（最久未用的先移除）。實測第一次 14 毫秒、查表 0.7 毫秒。")


def s_verify(d):
    s, _ = d.new("正確性與適用範圍")
    for x, head, c, items in (
            (X0, "正確性驗證", DARK, ["兩種算法結果一致", "刪除車禍後路網完全還原",
                                     "封閉道路不會被重新開放", "同一事件重複送出不重複計算"]),
            (7.4, "適用範圍", ACC, ["原方法保留，傳播為額外加上", "適用車禍、施工、手動事件",
                                   "只在主要道路上傳播", "為可選功能，需另外開啟"])):
        d.panel(s, x, 1.8, 5.35, 3.55, PANEL, c)
        d.tx(s, x + 0.25, 1.9, 4.9, 0.5, head, 24, c, True)
        d.items_box(s, x + 0.25, 2.5, 4.9, 2.8, [(None, t) for t in items], sz=20, gap=10)
    d.band(s, 5.65, "傳播距離依 164 件國道一號車禍的實際回堵資料校正", 21)
    d.notes(s, "驗證（verify_propagation.py）：推送 vs 精確解，影響 ≥0.05 的路段無遺漏，最大誤差 0.017；"
               "增量套用 vs 整張重算，595 條路段成本完全相同；刪除事件後路線回到 162.74 公里。"
               "壅塞與預測本來就是逐點量測值，不再傳播；封路與山崩維持原方法。"
               "開啟方式：EVENT_PROPAGATION=1，需搭配 PARTITION_MODE=1（且 PRELOAD_ALL_ROADS=0）。")


# ════════════════════ 四、路網切割 ════════════════════
def s_shape_points(d):
    s, _ = d.new("95% 的點不是路口")
    d.road(s, 2.6, 11.9, 2.75, 17, 0.3)
    d.tx(s, 1.9, 1.9, 1.4, 0.45, "路口", 20, ACC, True)
    d.tx(s, 11.6, 1.9, 1.4, 0.45, "路口", 20, ACC, True)
    d.tx(s, X0, 3.35, X1 - X0, 0.45, "中間的點只用來描繪彎道，沒有岔路可選", 19, MUTED, al=C)
    d.items_box(s, 2.2, 4.1, 10.3, 1.4, [
        (None, "找路時每經過一個點都要計算一次"),
        (None, "這些點沒有選擇，卻同樣耗費時間"),
    ], sz=24, gap=14)
    d.band(s, 5.85, "問題在於地圖過細，而非找路方法", 22, ACC)
    d.notes(s, "主幹路網 793,898 個點，真正的路口只有 35,668 個（4.5%）；平均每段路 34 公尺。"
               "台北→台中一次搜尋要展開 268,742 段路。")


def s_cut_rule(d):
    s, _ = d.new("以路口切分路網")
    rows = [("分岔點規則", "分岔超過一條路的點作為核心節點，以核心節點切分地圖", DARK),
            ("雙向道問題", "雙向道路中間的點也被視為分岔，幾乎每點都成為核心節點", ACC),
            ("本組修正", "只有真正的路口（三條路以上交會）與道路盡頭才是核心節點", GOOD)]
    for i, (head, body, c) in enumerate(rows):
        y = 1.8 + i * 1.3
        d.label_box(s, X0, y, 2.4, 1.05, head, c, 22)
        d.panel(s, 4.35, y, X1 - 4.35, 1.05)
        d.tx(s, 4.55, y, X1 - 4.75, 1.05, body, 20, INK, anchor=MSO_ANCHOR.MIDDLE)
    d.band(s, 5.85, "以路口為核心節點切分，每個子圖就是兩個路口之間的道路", 21)
    d.notes(s, "真實路網 78.6% 是雙向道：依出度 > 1 判斷，核心節點比例 67.7%（平均子圖 1.5 個點），"
               "等於沒切；改為無向度數 ≠ 2 後只剩 4.5%（平均子圖 17.3 段路、約 590 公尺）。")


def s_contraction(d):
    s, _ = d.new("路口之間併成一段")
    d.tx(s, X0, 1.85, 1.5, 0.45, "併之前", 20, DARK, True)
    d.road(s, 3.5, 10.6, 2.08, 17, 0.16)
    d.tx(s, 11.0, 1.85, 1.8, 0.45, "17 步", 20, INK, True)
    d.tx(s, X0, 3.05, 1.5, 0.45, "併之後", 20, DARK, True)
    d.road(s, 3.5, 10.6, 3.28, 1, 0, straight=True)
    d.tx(s, 11.0, 3.05, 1.8, 0.45, "1 步", 20, ACC, True)
    d.items_box(s, 2.0, 4.15, 10.6, 2.4, [
        (None, "如同搭捷運只看停靠站，不必計算沿途每根電線桿"),
        (None, "找到路線後再展開回原本道路，地圖上的路線完全相同"),
        (None, "地圖上的點減少 22 倍，找路速度大幅提升"),
    ], sz=22, gap=14)
    d.notes(s, "節點 793,898 → 35,668（22.3 倍）；路段 1,344,026 → 77,725 段（17.3 倍）。"
               "正確性：成本加總誤差 1.8×10⁻¹³；16/16 條路線逐段相同；50/50 條路線成功。"
               "建圖只在伺服器啟動時做一次（約 8.6 秒）。")


def s_link_sync(d):
    s, _ = d.new("事件同步到簡化地圖")
    d.map_pic(s, "4_同步切割路網.png", X0, 1.75, 6.4)
    d.stack_box(s, 8.45, 1.85, 4.3, 4.8, [
        ("橘色", "車禍直接影響的道路"),
        ("深色", "所在路段整段同步"),
        ("其他道路", "完全不動"),
        ("修正", "刪除車禍後也會同步"),
    ], gap=12)
    d.notes(s, "這場車禍：方框內相連 55 條路段（國道兩個方向），屬於 2 段路口之間的路；往上游傳播影響的 7 段也一併同步。"
               "修正前實測：刪除事件並重算後有 22 段殘留事件成本，路線仍在繞路。")


def s_performance(d):
    s, _ = d.new("找路速度的提升")
    d.stat(s, X0, 1.85, 5.35, 1.4, "50 條隨機路線總耗時", "185 秒 → 14 秒")
    d.stat(s, 7.4, 1.85, 5.35, 1.4, "台北 → 高雄", "2.6 秒 → 0.1 秒")
    d.items_box(s, 2.0, 3.65, 10.6, 2.8, [
        (None, "路線越長，可跳過的點越多，節省的時間越多"),
        (None, "算出的路線與原系統完全相同"),
        (None, "原本長距離路線常算不完，現在全部成功"),
    ], sz=22, gap=16)
    d.notes(s, "原系統全路網載入：台北→台中（含 3 條備選）25.59 秒、50 條 372 秒、成功 40/50；"
               "只載主幹：7.06 秒、185.3 秒、50/50；加路口收縮：0.371 秒、13.7 秒、50/50。"
               "台北→高雄 2.62 → 0.135 秒。單次加速倍數：板橋 7×、台中 14×、嘉義 20×、高雄 19×。")


# ════════════════════ 五、交通預測 ════════════════════
def s_predict_idea(d):
    s, _ = d.new("預計會塞就先繞")
    d.tx(s, X0, 1.65, X1 - X0, 0.5, "如同出門前查看天氣預報：現在沒下雨，不代表 30 分鐘後不下",
         20, INK, True)
    x0, x1, ya = 4.3, 11.3, 2.75
    d.arrow_line(s, x0, ya, x1 + 0.3, ya, MUTED, 2)
    for x, lab in ((x0, "現在出發"), (x1, "30 分鐘後到達")):
        d.dot(s, x, ya, 0.18, DARK)
        d.tx(s, x - 1.9, ya - 0.5, 3.8, 0.4, lab, 17, INK, True, C)
    for i, (head, now, later, good) in enumerate([
            ("只看現況", "該路段目前順暢\n照原路線行駛", "到達時已經塞住\n困在車陣中", False),
            ("本系統", "AI 預測該路段\n30 分鐘後會塞", "已改走其他道路\n順利通過", True)]):
        y = 3.3 + i * 1.5
        d.tx(s, X0, y + 0.35, 1.9, 0.5, head, 20, DARK if good else MUTED, True)
        d.card(s, 3.65, y, 3.5, 1.25, "現在", now, DARK if good else MUTED, 19, 17)
        d.arrow(s, 7.3, y + 0.48, 1.6, 0.3, DARK if good else MUTED)
        d.card(s, 9.1, y, 3.6, 1.25, "30 分鐘後", later, GOOD if good else ACC, 19, 17)
    d.band(s, 6.25, "預測結果同樣寫成「事件」，沿用相同的處理流程", 20)


def _fig(d, s, name, l, t, w):
    """放 event_figures 的整張圖並加細框；回傳高度。"""
    path = os.path.join(FIG, name)
    iw, ih = Image.open(path).size
    p = s.shapes.add_picture(path, Inches(l), Inches(t), Inches(w))
    p.line.color.rgb = LINE
    p.line.width = Pt(0.75)
    return w * ih / iw


def s_pred_data(d):
    s, _ = d.new("訓練資料：國道真實車速")
    d.crop_pic(s, os.path.join(FIG, "pred_1_國道路網.png"), 2.0, 1.6, 4.1,
               (0.13, 0.0, 0.02, 0.02))
    d.stack_box(s, 6.75, 1.85, 6.0, 4.6, [
        ("資料來源", "高速公路局門架偵測的路段車速"),
        ("範圍", "5 條國道、378 個路段"),
        ("時間", "連續 3 週，每 5 分鐘一筆"),
        ("路網關係", "前後相接的路段連成一張路網圖"),
    ], hs=22, bs=20, gap=16)
    d.notes(s, "高公局 TDCS M05A 門架路段平均車速（ETC 門架間旅行時間換算，各車種以流量加權）。"
               "國 1、國 3、國 5、國 1 高架、國 3 甲；2026/5/10～5/30，共 6,048 個時間點。"
               "路網圖：路段的終點門架 = 下一段的起點門架就連一條邊，共 378 個節點、421 條邊，"
               "國道之間經系統交流道相連。左圖顏色是 5/29 17:35 的實際車速。")


def s_pred_model(d):
    s, _ = d.new("T-GCN 模型架構")
    steps = [("輸入", "過去 60 分鐘\n車速與車流", DARK),
             ("看上下游", "圖卷積\n參考相鄰路段", DARK),
             ("保留自己", "不讓鄰居\n蓋過本身車速", DARK),
             ("看時間變化", "記住過去\n一小時的趨勢", DARK),
             ("輸出", "5～30 分鐘後\n的車速", ACC)]
    for i, (h, b, c) in enumerate(steps):
        x = X0 + i * 2.24
        d.card(s, x, 1.85, 1.98, 1.7, h, b, c, 20, 16)
        if i < 4:
            d.arrow(s, x + 2.0, 2.55, 0.2, 0.3)
    d.stack_box(s, 2.0, 3.95, 10.6, 2.6, [
        ("看上下游（圖卷積）", "上游塞了，等一下這裡也會塞，所以要參考相鄰路段"),
        ("看時間變化（GRU）", "同一段路有自己的規律，例如每天早上 7 點開始變慢"),
        ("兩者合併", "同時考慮「哪裡」和「何時」，比只看其中一項更準"),
    ], hs=20, bs=18, gap=10)
    d.notes(s, "T-GCN：每個時間步先做 2 層圖卷積（隱藏維度 64）聚合上下游，再接 GRU；"
               "加入殘差連接保留路段自身車速——拿掉後 5 分鐘誤差反而比純 GRU 差。"
               "輸入過去 12 步（60 分鐘）的車速與車流量，同時輸出 5／15／30 分鐘後三個預測。")


def s_gcn_compute(d):
    s, _ = d.new("圖卷積的計算方式")
    parts = [("Â 連接關係", "哪些路段相鄰\n大部分是 0", DARK), ("X 路段特徵", "每段路的\n車速、車流", DARK),
             ("W 模型權重", "訓練時\n學到的參數", DARK), ("新的特徵", "參考了上下游\n的路段狀態", ACC)]
    for i, (head, body, c) in enumerate(parts):
        x = X0 + i * 2.8
        d.card(s, x, 1.8, 2.2, 1.55, head, body, c, 19, 16)
        if i < 3:
            d.tx(s, x + 2.2, 1.8, 0.6, 1.55, "=" if i == 2 else "×", 30, DARK, True, C,
                 MSO_ANCHOR.MIDDLE)
    for x, head, items in (
            (X0, "先算 X × W", ["兩種順序，結果完全相同", "先轉換特徵，再乘連接關係",
                               "特徵越多，省下的計算越多"]),
            (7.4, "跳過 0 的部分", ["每段路只和前後幾段相連", "連接關係幾乎全是 0",
                                  "只存、只算非零的部分（稀疏矩陣）"])):
        d.panel(s, x, 3.6, 5.35, 2.05, PANEL, DARK)
        d.tx(s, x + 0.25, 3.68, 4.9, 0.5, head, 22, DARK, True)
        d.items_box(s, x + 0.25, 4.2, 4.9, 1.4, [(None, t) for t in items], sz=18, gap=6)
    d.band(s, 5.9, "結果完全相同，只是讓電腦少做不必要的計算", 21)
    d.notes(s, "圖卷積層：Â = D^(−1/2)(A + I)D^(−1/2)，輸出 = Â·(X·W)；交通預測模型即以此順序、"
               "並以稀疏格式儲存 Â（torch.sparse.mm）。"
               "乘法順序實驗（20,000 個節點、500 維特徵、64 維輸出）：(Â·X)·W 0.052 秒、Â·(X·W) 0.022 秒，"
               "快 2.3 倍，乘法次數少 33 倍。稀疏實驗（4,000 個節點）：非零 1% 時稀疏計算快 3.1 倍、"
               "0.1% 時快 22 倍；非零超過約 5% 時一般矩陣反而較快。"
               "圖卷積實作以公開資料集 Pubmed 驗證，測試準確率 79.8%（文獻約 79%）。")


def s_pred_train(d):
    s, _ = d.new("模型訓練方式")
    d.tx(s, X0, 1.62, 1.5, 0.4, "5 月 10 日", 16, MUTED)
    d.tx(s, X1 - 1.5, 1.62, 1.5, 0.4, "5 月 30 日", 16, MUTED, al=PP_ALIGN.RIGHT)
    x = X0
    for frac, head, sub, c in ((0.7, "訓練", "約 15 天", DARK), (0.1, "驗證", "2 天", ACC),
                               (0.2, "測試", "約 4 天", GOOD)):
        w = (X1 - X0) * frac
        d.label_box(s, x, 2.05, w - 0.04, 0.8, f"{head}　{sub}" if frac > 0.15 else head, c, 20)
        x += w
    d.stack_box(s, 2.0, 3.2, 10.6, 3.3, [
        ("訓練", "用前 15 天的資料，讓模型學習車速變化的規律"),
        ("驗證", "每學完一輪就小考一次，成績不再進步就停止，避免死背"),
        ("測試", "最後 4 天完全沒給模型看過，用來公正評分"),
    ], hs=21, bs=19, gap=12)
    d.notes(s, "依時間順序切分 70%／10%／20%，測試段完全在訓練段之後，不會偷看未來。"
               "學習率 0.001、Adam、批次 32、最多 50 輪，驗證誤差連續 8 輪沒進步就提前停止；"
               "GPU 訓練約 1.5 分鐘。")


def s_pred_result(d):
    s, _ = d.new("預測與實際車速比較")
    h = _fig(d, s, "pred_2_預測與實際.png", 2.05, 1.6, 10.6)
    d.band(s, 1.75 + h, "AI 的預測比只看現在的車速更貼近實際；壅塞剛開始時仍會晚一點反應", 19)
    d.notes(s, "國道一號北上台中路段（01F0293N-01F0256N），測試期間 5/27 全天，每 5 分鐘一點。"
               "橘線是 30 分鐘前就做出的預測；虛線是「直接沿用當下車速」。"
               "這一天的 30 分鐘預測平均誤差：AI 5.0、沿用現在 5.8 km/h（壅塞路段比平均難預測）。")


def s_pred_accuracy(d):
    s, _ = d.new("預測準確度")
    d.group_chart(s, X0, 1.6, 7.0, 4.6, ["5 分鐘後", "15 分鐘後", "30 分鐘後"], [
        ("AI（看鄰居＋看自己）", [2.11, 2.62, 3.13]),
        ("只看自己", [2.20, 2.68, 3.23]),
        ("沿用現在車速", [2.70, 3.23, 3.82]),
        ("歷史平均", [4.00, 4.00, 4.00]),
    ], [ACC, DARK, MUTED, LINE])
    d.tx(s, X0, 6.2, 7.0, 0.4, "數字為平均誤差（公里/小時），越低越準", 15, MUTED, al=C)
    d.stack_box(s, 9.0, 1.9, 3.75, 4.2, [
        ("最準", "每個時間點都是 AI 最準"),
        ("30 分鐘後", "平均只差約 3 公里/小時"),
        ("比只看現在", "誤差少約兩成"),
    ], hs=21, bs=18, gap=16)
    d.notes(s, "測試段（最後 4 天）MAE，km/h：T-GCN 2.11 / 2.62 / 3.13；只看自己（GRU，不看鄰居）"
               "2.20 / 2.68 / 3.23；沿用上一刻 2.70 / 3.23 / 3.82；歷史同時段平均 4.00。"
               "30 分鐘：3.13 vs 3.82，少 18%。")


def s_predict_pipeline(d):
    s, _ = d.new("預測結果寫成事件")
    steps = [("AI 預測", "30 分鐘後的車速"), ("判斷壅塞", "低於順暢時的 7 成"),
             ("寫成事件", "預測壅塞事件"), ("自動改道", "避開將會塞的路段")]
    for i, (h, b) in enumerate(steps):
        x = X0 + i * 2.83
        d.card(s, x, 1.75, 2.55, 1.3, h, b, ACC if i == 3 else DARK, 20, 16)
        if i < 3:
            d.arrow(s, x + 2.58, 2.25, 0.22, 0.3)
    d.stack_box(s, X0, 3.45, 5.3, 3.0, [
        ("相同流程", "預測事件和車禍事件走同一套更新"),
        ("實測", "台北 → 基隆，預測會塞時自動改道"),
        ("可還原", "清除預測後完全回到原路線"),
    ], hs=20, bs=18, gap=12)
    d.pic(s, "p36_1.png", 7.25, 3.45, w=5.5)
    d.tx(s, 7.25, 6.3, 5.5, 0.4, "紫色圓圈：AI 預測將會塞的路段", 15, MUTED)
    d.notes(s, "順暢車速 = 各路段車速的第 85 百分位；預測車速低於它的 70% 才建立事件（與即時 VD 壅塞判定相同），"
               "事件放在路段中點、半徑 1 公里，時間乘數 = 順暢車速 ÷ 預測車速。"
               "台北→基隆（平衡模式）：預測前 21.20 分鐘；套用 26 個預測壅塞後改道 22.88 分鐘；"
               "清除預測後精確還原 21.20 分鐘。")


def s_pred_live(d):
    s, _ = d.new("即時預測流程")
    y, h = 1.85, 0.7
    xs = [1.9, 4.1, 6.3, 8.5, 10.7]
    _fshape(s, MSO_SHAPE.FLOWCHART_TERMINATOR, xs[0], y, 1.95, h, "開始", "啟動即時預測",
            hs=15, ss=13)
    _fshape(s, MSO_SHAPE.RECTANGLE, xs[1], y, 1.95, h, "補齊資料", "抓過去 60 分鐘", hs=15, ss=13)
    _fshape(s, MSO_SHAPE.FLOWCHART_DATA, xs[2], y, 1.95, h, "即時車速", "最新 5 分鐘",
            hs=15, ss=13)
    _fshape(s, MSO_SHAPE.RECTANGLE, xs[3], y, 1.95, h, "AI 預測", "30 分鐘後車速", hs=15, ss=13)
    _fshape(s, MSO_SHAPE.RECTANGLE, xs[4], y, 1.95, h, "寫入事件", "路線自動避開", hs=15, ss=13)
    for a, b in zip(xs, xs[1:]):
        d.arrow_line(s, a + 1.95, y + h / 2, b, y + h / 2, F_LINE, 1.5)
    cx = xs[4] + 0.975
    _fshape(s, MSO_SHAPE.FLOWCHART_DECISION, cx - 1.2, 2.95, 2.4, 0.75, "收到停止指令",
            color=F_ORANGE, hs=13)
    d.arrow_line(s, cx, y + h, cx, 2.95, F_LINE, 1.5)
    d.arrow_line(s, cx, 3.7, cx, 4.0, F_LINE, 1.5)
    _flabel(d, s, cx + 0.08, 3.68, 0.5, "是", sz=12, al=L)
    _fshape(s, MSO_SHAPE.FLOWCHART_TERMINATOR, cx - 0.6, 4.0, 1.2, 0.42, "結束", hs=14)
    lx = xs[2] + 0.975
    _fpath(d, s, [(cx - 1.2, 3.325), (lx, 3.325), (lx, y + h)], F_LINE, 1.5)
    _flabel(d, s, lx + 0.2, 3.0, 3.6, "否：5 分鐘後再抓一次", sz=13, al=L)
    d.stack_box(s, 2.0, 4.7, 10.6, 1.9, [
        ("啟動就能預測", "先補齊過去一小時的資料，不必等一小時"),
        ("持續更新", "每 5 分鐘抓最新車速，預測結果直接反映到路線"),
    ], hs=20, bs=18, gap=10)
    d.notes(s, "資料源：高公局 ETagPairLive（門架配對即時車速，與訓練用的 M05A 同格式），每 5 分鐘一檔。"
               "維護最近 12 步（60 分鐘）滾動視窗；冷啟動一次抓過去 12 檔。"
               "管理介面 POST /predict/live/start、/predict/live/stop 啟動與停止。")


def s_pred_update(d):
    s, _ = d.new("模型持續更新")
    y, h, w = 1.8, 0.75, 2.3
    xs = [1.9, 4.6, 7.3, 10.0]
    _fshape(s, MSO_SHAPE.FLOWCHART_TERMINATOR, xs[0], y, w, h, "開始", "定期執行（每天）",
            hs=15, ss=13)
    _fshape(s, MSO_SHAPE.FLOWCHART_DATA, xs[1], y, w, h, "新資料", "高公局每日車速",
            hs=15, ss=13)
    _fshape(s, MSO_SHAPE.RECTANGLE, xs[2], y, w, h, "重新訓練", "用全部累積的資料", hs=15, ss=13)
    _fshape(s, MSO_SHAPE.RECTANGLE, xs[3], y, w, h, "公平比較", "最新 3 天的資料", hs=15, ss=13)
    for a, b in zip(xs, xs[1:]):
        d.arrow_line(s, a + w, y + h / 2, b, y + h / 2, F_LINE, 1.5)
    cx = xs[3] + w / 2
    _fshape(s, MSO_SHAPE.FLOWCHART_DECISION, cx - 1.2, 2.9, 2.4, 0.8, "新模型較準",
            color=F_ORANGE, hs=14)
    d.arrow_line(s, cx, y + h, cx, 2.9, F_LINE, 1.5)
    # 是 → 換上新模型
    _fshape(s, MSO_SHAPE.RECTANGLE, xs[3], 4.05, w, 0.7, "換上新模型", "舊版保留，可切回",
            hs=15, ss=13)
    d.arrow_line(s, cx, 3.7, cx, 4.05, F_LINE, 1.5)
    _flabel(d, s, cx + 0.1, 3.72, 0.5, "是", sz=12, al=L)
    # 否 → 保留目前的模型
    _fshape(s, MSO_SHAPE.RECTANGLE, xs[2], 2.95, w, 0.7, "保留目前模型", "新模型不採用",
            hs=15, ss=13)
    d.arrow_line(s, cx - 1.2, 3.3, xs[2] + w, 3.3, F_LINE, 1.5)
    _flabel(d, s, xs[2] + w, 3.0, cx - 1.2 - xs[2] - w, "否", sz=12)
    # 兩條路都結束
    ex = xs[2] + w / 2
    _fshape(s, MSO_SHAPE.FLOWCHART_TERMINATOR, ex - 0.8, 4.18, 1.6, 0.45, "結束", hs=14)
    d.arrow_line(s, ex, 3.65, ex, 4.18, F_LINE, 1.5)
    d.arrow_line(s, xs[3], 4.4, ex + 0.8, 4.4, F_LINE, 1.5)
    d.stack_box(s, 2.0, 4.95, 10.6, 1.7, [
        ("公平比較", "用新、舊模型都沒看過的最新資料打分數，比較準才換上"),
        ("原始模型永久保留", "任何時候都能切回，不會越更新越差"),
    ], hs=20, bs=18, gap=10)
    d.notes(s, "gcn/traffic/model_update.py：補下載高公局 M05A 日資料（與原始訓練資料同來源、同前處理）→"
               "以全部累積資料重訓（架構與參數不變）→ 最新 3 天測試、再往前 2 天驗證 →"
               "平均誤差至少降低 1%、且 30 分鐘預測沒有變差才換上。"
               "原始模型 results/traffic_tgcn.ckpt 登記為 v0、永不覆寫；"
               "API：GET /predict/model、POST /predict/model/update、POST /predict/model/activate?version=v0；"
               "環境變數 MODEL_UPDATE_HOURS=24 即每天自動執行。")


def s_pred_update_result(d):
    s, _ = d.new("持續更新的實測結果")
    for i, (head, val) in enumerate((("加入新資料", "3 週"), ("預測誤差", "降低約 1～2%"),
                                     ("測試的 3 天", "每天都更準"))):
        d.stat(s, X0 + i * 3.7, 1.8, 3.4, 1.35, head, val)
    d.stack_box(s, 2.0, 3.55, 10.6, 3.0, [
        ("已經換上", "新模型通過比較，系統自動改用"),
        ("進步幅度不大", "每天固定的塞車規律，原本 3 週資料就學得差不多"),
        ("資料越多越有用", "連假、颱風等少見狀況，要累積更久才學得到"),
    ], hs=20, bs=18, gap=12)
    d.notes(s, "實測：原始 21 天（5/10～5/30）＋新下載 21 天（5/31～6/20），共 42 天；"
               "訓練 5/10～6/15，驗證 6/16～17，測試 6/18～20（新舊模型都沒看過）。"
               "平均誤差（5/15/30 分鐘平均，km/h）：原始 2.643 → 新 2.609，降低 1.3%；"
               "30 分鐘 3.255 → 3.202。逐日：6/18 3.115→3.055（1.9%）、6/19 2.486→2.461（1.0%）、"
               "6/20 2.321→2.304（0.7%）。同期「沿用現在車速」2.90～3.71。"
               "新模型登記為 v1 並啟用；簡報其他頁的準確度數字是原始模型 v0 的成績。")


def s_pred_anomaly(d):
    s, _ = d.new("異常偵測：自動發現事故")
    h = _fig(d, s, "pred_3_異常偵測.png", 2.05, 1.6, 10.6)
    d.band(s, 1.75 + h, "沒教過什麼是事故，只靠「實際比預測慢很多」就抓到六成真實事故", 19)
    d.notes(s, "殘差 = 預測車速 − 實際車速，標準化後超過 3 即標記。以高公局事故通報為標準答案："
               "測試期間 123 件國道一號事故中偵測到 75 件（61%）；標記點命中率 13.3%，是隨機的 5.7 倍。"
               "命中率偏低是因為標準答案只有事故，施工、匝道壅塞等真實減速也會被標記。"
               "圖中例子（5/30 10:52 國道一號北上 218 公里）：第一次標記與官方通報幾乎同時。")


def s_pred_calib(d):
    s, _ = d.new("用真實事故校正影響範圍")
    d.tx(s, X0, 1.65, X1 - X0, 0.5, "分析 164 件國道一號真實事故前後的車速變化", 21, INK, True)
    for i, (head, val) in enumerate((("一般事故回堵", "約 2 公里"), ("嚴重事故回堵", "約 6 公里"),
                                     ("一半事故恢復", "25 分鐘內"))):
        d.stat(s, X0 + i * 3.7, 2.4, 3.4, 1.35, head, val)
    d.stack_box(s, 2.0, 4.15, 10.6, 2.4, [
        ("影響距離", "決定車禍影響往後傳多遠（事件影響傳播一章）"),
        ("影響強度", "確認系統設定的降速程度落在真實範圍內"),
    ], hs=21, bs=19, gap=12)
    d.notes(s, "2026/5/26～30 國道一號事故 164 件，相對事故前 60～30 分鐘的基準車速。"
               "事故路段降速：中位數 1.28 倍、前 25% 嚴重 2.07 倍（系統設定 1.44 倍，介於兩者之間）。"
               "回堵長度：中位數 2.15 公里、p75 6.43 公里。恢復到基準 90%：中位數 25 分鐘。"
               "每往上游一個門架區段，超額減速只剩 17～20%，推得傳播衰減 λ = 2.5 公里。")


# ════════════════════ 六、成果展示 ════════════════════
def s_video(d):
    s, _ = d.new("展示影片")
    d.pic(s, "p39_1.png", 2.75, 1.7, w=9.0)
    d.tx(s, X0, 6.2, X1 - X0, 0.45, VIDEO_URL, 20, DARK, True, C)


def s_event_compare(d):
    s, _ = d.new("實際操作畫面")
    cells = [("台北市區：平常", "p40_1.png", X0, 2.0),
             ("台北市區：出事後", "p40_2.png", 7.4, 2.0),
             ("新竹郊區：平常", "p41_1.png", X0, 4.85),
             ("新竹郊區：出事後", "p41_2.png", 7.4, 4.85)]
    for label, name, x, y in cells:
        d.tx(s, x, y - 0.45, 5.35, 0.4, label, 19, DARK, True)
        d.pic(s, name, x, y, w=4.6)


def s_recap(d):
    s, _ = d.new("範例車禍的完整處理")
    cols = [("① 影響範圍", "7_事件沿上游傳播.png", "沿國道往兩側後方傳"),
            ("② 更新道路", "3_更新範圍內路段.png", "只改事故附近"),
            ("③ 同步地圖", "4_同步切割路網.png", "只更新相關路段"),
            ("④ 重新找路", "5_路線反應.png", "自動繞開車禍")]
    for i, (head, fig, line) in enumerate(cols):
        x = X0 + i * 2.8
        d.tx(s, x, 1.7, 2.6, 0.45, head, 20, DARK, True)
        h = d.map_pic(s, fig, x, 2.2, 2.6)
        d.tx(s, x, 2.3 + h, 2.6, 0.5, line, 18, INK)
    d.band(s, 5.4, "從車禍發生到路線改變，每一步只處理必要部分，合計不到 0.2 秒", 20)
    d.notes(s, "① 方框 ±0.80 公里＋往上游 6.0 公里；② 55＋166 條路段；③ 2 段；"
               "④ 162.74 → 173.79 公里、113.2 → 117.7 分鐘（平日上午 10 點）。")


# ════════════════════ 七、結論 ════════════════════
def s_conclusion(d):
    d.bullets_slide("結論：與一般導航的差別", [
        ("影響範圍", "沿道路往後傳播，由路網決定，不再是固定範圍。"),
        ("即時更新", "只更新受影響道路，事件再多也能即時反映。"),
        ("路網切割", "路口之間併成一段，找路更快且路線不變。"),
        ("預測繞路", "預計會塞就提前繞道，並能協助偵測事故。"),
    ], sz=26, gap=20,
        note="證據：台北 1.2 公里 vs 蘇花 6.1 公里（164 件事故校準）；新增事件約 20 毫秒 vs 整張重算 4.3 秒；"
             "找路快約 13.5 倍；預測誤差 2.11～3.13 km/h，異常偵測抓到 61% 事故。")


def s_benefits(d):
    d.bullets_slide("結論：系統效益", [
        ("即時", "事件一發生，路線馬上跟著改變。"),
        ("貼近真實", "影響範圍依真實車禍回堵資料校正。"),
        ("快速", "長距離找路從算不完變成 1 秒內。"),
        ("易擴充", "新資料寫成事件即可接上，不必改找路程式。"),
    ], sz=26, gap=20,
        note="新增事件約 20 毫秒反映到路網；衰減速度由 164 件國道一號事故回堵資料校正；"
             "台北→高雄 0.135 秒（含 3 條備選 0.93 秒），50 條隨機路線全部成功；"
             "AI 預測寫成「預測壅塞」事件，路由程式未修改。")


def s_limits(d):
    d.bullets_slide("限制與未來方向", [
        ("可選功能", "影響傳播需另外開啟，並搭配路網切割使用。"),
        ("事件方向", "目前無法得知事故方向，兩個方向都往後傳。"),
        ("嚴重程度", "嚴重車禍回堵較長，未來可依嚴重程度調整。"),
        ("刪除生效", "刪除事件要整張重算才生效，事件很多時需等較久（背景執行）。"),
        ("尖峰排隊", "大量事件同時湧入時每秒約處理 4 個，需要排隊。"),
        ("封路重疊", "封路附近又有其他事件時，要等下一次全量重算才完全正確。"),
        ("平行運算", "不同傳播鏈上的子圖可交給不同核心同時處理，進一步加速。"),
    ], sz=22, gap=12,
        note="另：三因子模型的「替代道路密度」以 34 公尺的路段計數，真實路網上幾乎都滿分；"
             "可改成計算不同道路的條數（蘇花約 10 條、台北上千條）。"
             "封路重疊：原本的事件增量方法在封路之後再套用附近事件時，會把封閉路段改成以 3 km/h 可通行，"
             "且封路會覆寫該路段已累積的風險；全量重算時順序固定，結果正確。"
             "實測 1,000 個事件（含 16 處封路）：544 條路段與全量重算不同（封閉路段被改成可通行），全量重算後恢復正確。")


REFS = [
    ("成本因子依據", [
        "FHWA. (2025). Highway Capacity Manual, Ed. 7.1. 事件延遲、號誌延遲。",
        "Agarwal, M., et al. (2005). Weather Impact on Freeway Speed and Capacity.",
        "NHTSA. (2008). Nighttime Driving: 25% of miles, 50% of fatalities.",
        "FHWA. Speed and Capacity for Work Zones. 施工乘數 ×1.55。",
        "Webster, F. V. (1958). Traffic Signal Settings. HCM Ch.19 號誌延遲。",
        "Tandfonline. (2017). Landslide Risk Assessment in Taiwan.",
    ]),
    ("資料來源", [
        "交通部 TDX 運輸資料流通服務平台：車速偵測器、路況新聞 API。",
        "中央氣象署開放資料平台：氣象站、雨量站 API。",
        "高公局 TDCS M05A、ETag：訓練、即時預測與事故回堵資料。",
    ]),
    ("事件傳播與路網", [
        "Page, L., Brin, S., Motwani, R., & Winograd, T. (1998). The PageRank Citation Ranking.",
        "Andersen, R., Chung, F., & Lang, K. (2006). Local Graph Partitioning "
        "using PageRank Vectors. FOCS. 殘差推送。",
        "Jenelius, E., et al. (2006). Importance and Exposure in Road Network Vulnerability.",
        "Mattsson, L.-G., & Jenelius, E. (2015). Vulnerability and Resilience of Transport Systems.",
    ]),
    ("交通預測", [
        "Kipf, T. N., & Welling, M. (2016). Semi-Supervised Classification with GCN. arXiv:1609.02907.",
        "Zhao, L., et al. (2019). T-GCN: A Temporal Graph Convolutional Network for "
        "Traffic Prediction. IEEE T-ITS.",
    ]),
]


def s_references(d, part):
    s, ph = d.new(f"參考文獻 {part}", body=True)
    groups = REFS[:2] if part == 1 else REFS[2:]
    tf = ph.text_frame
    first = True
    for head, items in groups:
        p = tf.paragraphs[0] if first else tf.add_paragraph()
        p.space_before = Pt(0 if first else 14)
        first = False
        p._p.get_or_add_pPr().append(parse_xml('<a:buNone %s/>' % nsdecls("a")))
        r = p.add_run()
        r.text = head
        d.fmt(r.font, 20, DARK, True)
        for it in items:
            pp = tf.add_paragraph()
            pp.space_before = Pt(6)
            d.bullet(pp, hang=False)
            rr = pp.add_run()
            rr.text = it
            d.fmt(rr.font, 16, INK)


def s_backup(d):
    d.bullets_slide("備用：公式與複雜度", [
        ("路段成本", "行車時間 × (時間權重 + 風險 × 風險權重) + 轉彎等待 + 紅綠燈等待"),
        ("A*", "f = g + h；h = 直線距離 ÷ 120 km/h（不高估，找到最佳解）"),
        ("往上游傳播", "r = r₀ + B·r，B[P, L] = 車道比例 × exp(−長度 ÷ 2.5 km)；"
                       "殘差推送求解，低於 0.05 停"),
        ("套用", "時間乘數 = 1 + (m₀ − 1)·x，x 為路段影響強度，m₀ = 嚴重度 × 事件乘數"),
        ("複雜度", "k 個事件更新 O(k·m)；找路 O(E log E)，與事件數無關"),
    ], sz=19, gap=14)


def build():
    ensure_images()
    d = WispDeck()
    chapters = [
        [s_motivation, s_objective, s_vs_gps, s_example],
        [s_arch_diagram, s_layers, s_search, s_event_life, s_local_update, s_linear, s_burst],
        [s_queue_idea, s_two_places, s_this_accident, s_graph_terms, s_push, s_pagerank,
         s_dep_index, s_verify],
        [s_shape_points, s_cut_rule, s_contraction, s_link_sync, s_performance],
        [s_predict_idea, s_pred_data, s_pred_model, s_gcn_compute, s_pred_train, s_pred_result,
         s_pred_accuracy, s_predict_pipeline, s_pred_live, s_pred_update, s_pred_update_result,
         s_pred_anomaly, s_pred_calib],
        [s_video, s_event_compare, s_recap],
        [s_conclusion, s_benefits, s_limits,
         lambda dd: s_references(dd, 1), lambda dd: s_references(dd, 2)],
    ]
    s_cover(d)
    for i, fns in enumerate(chapters):
        d.outline(i)
        for fn in fns:
            fn(d)
    s_backup(d)
    out = sys.argv[1] if len(sys.argv) > 1 else OUT      # 簡報開著時可先輸出到別處
    d.prs.save(out)
    print("saved:", out, "| slides:", len(d.prs.slides))


if __name__ == "__main__":
    build()
