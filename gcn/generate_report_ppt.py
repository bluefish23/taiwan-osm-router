#!/usr/bin/env python3
"""generate_report_ppt.py — GCN 專題報告 PPT（內容對應 gcn/REPORT.md 實驗一~五）

沿用根目錄 generate_ppt.py 的設計語言（20×11.25、深淺交替、同色票）。
用法：python gcn/generate_report_ppt.py → 輸出 gcn/gcn_report.pptx
"""
from pathlib import Path

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
PURP  = RGBColor(0xB3, 0x88, 0xFF)
RED88 = RGBColor(0xFF, 0x88, 0x88)
GRN88 = RGBColor(0x88, 0xFF, 0x88)
TBLH  = RGBColor(0x00, 0x3A, 0x44)
TBLR1 = RGBColor(0x00, 0x55, 0x63)
TBLR2 = RGBColor(0x00, 0x4A, 0x58)
BOXBG = RGBColor(0x00, 0x5A, 0x6A)
FONT  = "Microsoft JhengHei"

RESULTS = Path(__file__).resolve().parent / "results"

prs = Presentation()
prs.slide_width  = Inches(20)
prs.slide_height = Inches(11.25)
pn = [0]

# ── helpers（同 generate_ppt.py）─────────────────────────

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

def sec(title, sub=""):
    s = prs.slides.add_slide(prs.slide_layouts[6]); bg(s, False)
    tx(s, 2, 4.0, 16, 2, title, 48, BLACK, True, PP_ALIGN.CENTER)
    if sub:
        tx(s, 2, 6.0, 16, 1, sub, 22, GRAY, al=PP_ALIGN.CENTER)
    pgn(s); return s

def csl(title):
    s = prs.slides.add_slide(prs.slide_layouts[6]); bg(s)
    tx(s, 1.0, 0.5, 16, 1, title, 36, WHITE, True); pgn(s); return s

def box(s, l, t, w, h):
    b = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(l), Inches(t), Inches(w), Inches(h))
    b.fill.solid(); b.fill.fore_color.rgb = BOXBG; b.line.color.rgb = ACC; b.line.width = Pt(2)
    return b

def pic(s, path, l, t, w):
    if path.exists():
        s.shapes.add_picture(str(path), Inches(l), Inches(t), width=Inches(w))
    else:
        tx(s, l, t, w, 1, f"[圖：{path.name}]", 18, LG)

def stat(s, l, t, w, num, label, color=GRN):
    b = box(s, l, t, w, 2.3)
    tf = b.text_frame; tf.word_wrap = True
    p = tf.paragraphs[0]; p.text = num; p.font.size = Pt(40); p.font.bold = True
    p.font.color.rgb = color; p.font.name = FONT; p.alignment = PP_ALIGN.CENTER
    p2 = tf.add_paragraph(); p2.text = label; p2.font.size = Pt(15)
    p2.font.color.rgb = LG; p2.font.name = FONT; p2.alignment = PP_ALIGN.CENTER

def flowbox(s, l, t, w, h, title, tcolor, lines):
    b = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(l), Inches(t), Inches(w), Inches(h))
    b.fill.solid(); b.fill.fore_color.rgb = BOXBG; b.line.color.rgb = tcolor; b.line.width = Pt(2.5)
    tf = b.text_frame; tf.word_wrap = True
    tf.margin_left = Inches(0.12); tf.margin_right = Inches(0.12)
    p = tf.paragraphs[0]; p.text = title; p.font.size = Pt(20); p.font.bold = True
    p.font.color.rgb = tcolor; p.font.name = FONT; p.alignment = PP_ALIGN.CENTER
    for ln in lines:
        pp = tf.add_paragraph(); pp.text = ln; pp.font.size = Pt(14)
        pp.font.color.rgb = LG; pp.font.name = FONT; pp.alignment = PP_ALIGN.CENTER
        pp.space_before = Pt(7)
    return b

def arrow(s, l, t, w, h):
    a = s.shapes.add_shape(MSO_SHAPE.RIGHT_ARROW, Inches(l), Inches(t), Inches(w), Inches(h))
    a.fill.solid(); a.fill.fore_color.rgb = ACC; a.line.fill.background()
    return a

# ============================================================
# 1 封面
# ============================================================
s = prs.slides.add_slide(prs.slide_layouts[6]); bg(s, False)
tx(s, 2, 2.3, 16, 2, "圖卷積神經網路加速方法之軟體實作", 50, BLACK, True, PP_ALIGN.CENTER)
tx(s, 2, 3.9, 16, 1.5, "與交通預測應用", 50, BLACK, True, PP_ALIGN.CENTER)
tx(s, 2, 6.0, 16, 1, "於台灣 OSM 動態導航系統之整合（計畫書表 CM03）", 26, GRAY, al=PP_ALIGN.CENTER)
tx(s, 2, 7.5, 16, 1, "PyTorch 2.11 / CUDA 12.8　·　分支 gcn-implementation", 20, GRAY, al=PP_ALIGN.CENTER)
pn[0] += 1

# ============================================================
# 2 大綱
# ============================================================
s = csl("大綱")
tf = tx(s, 1.5, 1.8, 12, 8, "", 26); tf.paragraphs[0].text = ""
for item in ["一、計畫書問題回顧",
             "二、實驗一：GCN 正確性驗證（表二資料集）",
             "三、實驗二：子圖分割 + 增量 PageRank + 直接相依（計畫書核心）",
             "四、實驗三：稀疏矩陣乘法優化",
             "五、實驗四：T-GCN 交通時空預測（真實高公局資料）",
             "六、實驗五：異常偵測驗證與事件衝擊校準（真實事故 ground truth）",
             "七、系統整合與新舊對比",
             "八、結論與未來工作"]:
    ap(tf, f"  {item}", 26, WHITE, sp=13)

# ============================================================
# 3 關鍵成果總覽
# ============================================================
s = csl("關鍵成果一頁看完")
stat(s, 1.0, 2.0, 5.6, "1,880×", "增量傳播節點觸碰削減（ogbn-arxiv 17 萬節點）")
stat(s, 7.2, 2.0, 5.6, "0.41 ms", "直接相依 flag 快取命中 vs 全圖重算 703 ms")
stat(s, 13.4, 2.0, 5.6, "1.88 km/h", "T-GCN 車速預測 MAE@5min 勝過全部 baseline")
stat(s, 4.1, 5.2, 5.6, "61%", "事故自動偵測 recall（無監督，隨機基準 5.7×）", PURP)
stat(s, 10.3, 5.2, 5.6, "164 件", "真實事故實測，校準事件衝擊參數", PURP)
tx(s, 1.5, 8.3, 17, 1.5,
   "計畫書軟體方法全數實作並獲數據驗證 — p.8 全部評估指標（MAE、精確度、召回率）都有實測數據",
   22, ACC, True)

# ============================================================
# 4 計畫書問題回顧
# ============================================================
s = csl("計畫書問題回顧（表 CM03）")
tf = tx(s, 1.5, 1.8, 17, 5, "", 20); tf.paragraphs[0].text = ""
ap(tf, "GCN 計算三大瓶頸（計畫書 p.1）", 24, ACC, True)
ap(tf, "1. 節點狀態沿傳播鏈逐點更新 — 鏈越長成本越高，且需高度同步", 20, LG)
ap(tf, "2. 節點分支度差異 → 工作負載不平衡（v5 六個鄰居 vs v4 一個）", 20, LG)
ap(tf, "3. 鄰居資料分散 → 記憶體存取不規則（cache miss / page swap）", 20, LG)
ap(tf, "", 8)
ap(tf, "計畫書解法", 24, ACC, True)
ap(tf, "• out-degree > 1 核心節點分割子圖（p.2）", 20, LG)
ap(tf, "• 增量 PageRank 建核心節點間直接相依：α, β 線性表示 + Y/N/A flag 快取（p.4-5）", 20, LG)
ap(tf, "• 稀疏乘法順序 Â·(X·W) + 動態剪裁零元素（p.5-6）", 20, LG)
ap(tf, "• DDA/DFA 硬體加速器（Verilog）— 超出學期範圍，列為未來工作", 20, GRAY)
tx(s, 1.5, 8.8, 17, 1, "本專題實作以上全部軟體部分，並整合至既有導航系統", 22, GRN88, True)

# ============================================================
# 5 實驗一
# ============================================================
sec("實驗一：GCN 正確性驗證", "計畫書 p.5 公式 / p.8 參數 / p.9 表二")

s = csl("實驗一：純 PyTorch 實作 Â·(X·W)，表二資料集驗證")
tx(s, 1.5, 1.7, 17, 0.8, "不依賴現成 GNN 框架 — 完全控制乘法順序與傳播流程（實驗三的前提）", 20, LG)
tbl(s, 1.5, 2.8, 17, 3.2, [
    ["資料集（表二）", "規模", "超參數", "Test Acc", "文獻參考"],
    ["Pubmed", "19,717 節點 / 99K 邊", "2 層、hidden 64、lr 0.01", "79.8%", "~79%（Kipf & Welling）"],
    ["Pubmed", "同上", "計畫書 p.8（lr 0.001、50 ep）", "71.4%", "50 epochs 尚未收斂"],
    ["ogbn-arxiv", "169,343 節點 / 1.17M 邊", "3 層、hidden 64", "67.4%", "~71.7%（hidden 256）"],
], cw=[3.0, 4.2, 4.6, 2.2, 3.0], fs=16)
tx(s, 1.5, 6.6, 17, 1, "達文獻水準 → 實作正確。訓練全程於 GPU，Pubmed 每 epoch 14.5ms", 22, GRN88, True)

# ============================================================
# 6 實驗二
# ============================================================
sec("實驗二：子圖分割與增量傳播", "計畫書核心 p.1–2 / p.4–5")

s = csl("實驗二：子圖分割 — 與計畫書範例完全一致")
tf = tx(s, 1.5, 1.8, 17, 3, "", 20); tf.paragraphs[0].text = ""
ap(tf, "計畫書 p.1 圖一之 21 節點範例圖驗證：", 22, WHITE)
ap(tf, "核心節點（out-degree > 1）＝ {v5, v7, v13, v15, v16} — 與計畫書 p.2 完全一致", 24, GRN88, True)
ap(tf, "", 8)
ap(tf, "附註（可與教授討論）：計畫書對邊界鏈 v8, v10 的子圖歸屬前後不一致，", 18, LG)
ap(tf, "本實作採一致的「上游核心 DFS 認領」規則，已於 subgraph_partition.py docstring 註記", 18, LG)

s = csl("實驗二：三種更新方式對比（單一活動節點更新，10 次平均）")
tbl(s, 1.5, 1.8, 17, 3.6, [
    ["方法", "Pubmed", "ogbn-arxiv", "arxiv 觸碰節點數"],
    ["A. 全圖重算（傳統，計畫書 p.1 之問題）", "35.2 ms", "703.3 ms", "19,101,890"],
    ["B. 增量傳播（只沿受影響傳播鏈推送）", "26.2 ms", "293.4 ms", "10,155（少 1,880×）"],
    ["C. 直接相依 — 首次（flag N→A 建索引）", "30.9 ms", "431.4 ms", "—"],
    ["C. 直接相依 — 快取命中（flag A 查表）", "0.019 ms", "0.41 ms", "—"],
], cw=[7.0, 3.0, 3.0, 4.0], fs=16)
tf = tx(s, 1.5, 6.2, 17, 4, "", 20); tf.paragraphs[0].text = ""
ap(tf, "• 增量結果與全圖重算 L1 誤差 < 1e-5（正確性）", 20, LG)
ap(tf, "• 圖越大增量優勢越明顯：觸碰節點數差三個數量級", 20, LG)
ap(tf, "• flag 快取是最大貢獻：首次建索引後，每次更新只剩一次向量加法（0.41ms，~1,700×）", 20, GRN88, True)
ap(tf, "  → 驗證計畫書 p.4-5 直接相依設計的價值", 18, LG)

# ============================================================
# 7 實驗三
# ============================================================
sec("實驗三：稀疏矩陣乘法優化", "計畫書 p.5–6 / p.9")

s = csl("實驗三：乘法順序與零元素剪裁")
tbl(s, 1.5, 1.8, 17, 3.0, [
    ["實驗", "結果"],
    ["乘法順序（n=20K, f=500, h=64）", "Â·(X·W) 快 2.3×；FLOPs 12.8 億 → 3,840 萬（33×）"],
    ["動態剪裁零元素（n=4,000）", "density 0.001 → 22×；0.01 → 3.1×；>5% 時 dense 反而較快"],
    ["矩陣大小掃描（density 1%）", "n=16,000 時稀疏乘法快 3×"],
], cw=[6.5, 10.5], fs=16)
tf = tx(s, 1.5, 5.4, 17, 3, "", 20); tf.paragraphs[0].text = ""
ap(tf, "• 計畫書 p.5 順序論點與 p.6 剪裁論點皆獲數據支持", 20, LG)
ap(tf, "• 額外發現：dense/sparse 交叉點 ~5% 密度 — 剪裁須在高稀疏度情境才有效益", 20, GRN88, True)
ap(tf, "  （GCN 特徵/鄰接矩陣正屬此類）；能量量測以執行時間為代理指標", 18, LG)

# ============================================================
# 8 實驗四
# ============================================================
sec("實驗四：T-GCN 交通時空預測", "計畫書 p.7–8 實驗章節 · 真實高公局資料")

s = csl("實驗四：資料與模型")
tf = tx(s, 1.5, 1.8, 10.5, 7, "", 20); tf.paragraphs[0].text = ""
ap(tf, "資料", 24, ACC, True)
ap(tf, "• 高公局 TDCS M05A 門架路段平均速率", 19, LG)
ap(tf, "• 國道一號主線 148 路段 × 21 天（2026/5/10–30）", 19, LG)
ap(tf, "• 5 分鐘粒度，共 6,048 時間步，各車種流量加權", 19, LG)
ap(tf, "", 6)
ap(tf, "圖建構", 24, ACC, True)
ap(tf, "• 路段迄門架 = 下段起門架 → 建邊（146 邊，沿行車方向）", 19, LG)
ap(tf, "", 6)
ap(tf, "模型（超參數完全照計畫書 p.8）", 24, ACC, True)
ap(tf, "• T-GCN：每時間步 GCN（2 層 hidden 64）聚合上下游", 19, LG)
ap(tf, "  + GRU 捕捉時間相依 + 殘差連接", 19, LG)
ap(tf, "• 輸入 60 分鐘 → 預測 5/15/30 分鐘", 19, LG)
ap(tf, "• lr 0.001、Adam、50 epochs、early stopping", 19, LG)
pic(s, RESULTS / "traffic_pred_tgcn.png", 12.2, 2.5, 7.0)

# 原理流程圖
s = csl("實驗四：T-GCN 如何預測車速（原理）")
tx(s, 1.5, 1.6, 17, 0.8, "核心：空間看上下游鄰居、時間看自己的歷史 — 兩者分工", 24, WHITE, True)
by, bh, ay, ah = 3.4, 3.4, 4.4, 1.4
flowbox(s, 0.8, by, 3.2, bh, "輸入", ACC, ["過去 60 分鐘", "148 路段", "車速 + 車流量"])
arrow(s, 4.0, ay, 0.6, ah)
flowbox(s, 4.6, by, 3.2, bh, "① GCN 圖卷積", ACC, ["聚合上下游鄰居", "（空間依賴）", "塞車往下游擴散"])
arrow(s, 7.8, ay, 0.6, ah)
flowbox(s, 8.4, by, 3.2, bh, "② 殘差連接", GRN, ["保留路段自己車速", "不被鄰居蓋掉", "（關鍵：拿掉會變差）"])
arrow(s, 11.6, ay, 0.6, ah)
flowbox(s, 12.2, by, 3.2, bh, "③ GRU", ACC, ["看過去一小時趨勢", "（時間依賴）", "車速連續變化"])
arrow(s, 15.4, ay, 0.6, ah)
flowbox(s, 16.0, by, 3.2, bh, "輸出", GRN, ["未來 5/15/30 分鐘", "預測車速", "（km/h）"])
tx(s, 1.5, 7.6, 17, 2.5,
   "為什麼要三步一起：\n"
   "• 只看自己歷史 → 沒看到上游正在塞（純 GRU：MAE 2.00）\n"
   "• 只看鄰居、蓋掉自己規律 → 更糟（無殘差：MAE 3.07）\n"
   "• 三者合起來才最準（T-GCN：MAE 1.88）",
   20, LG)

s = csl("實驗四：測試集 MAE — T-GCN 勝過全部 baseline")
tbl(s, 1.5, 1.8, 13, 3.4, [
    ["模型（MAE km/h）", "@5min", "@15min", "@30min"],
    ["T-GCN（GCN+GRU+殘差）", "1.88", "2.57", "3.23"],
    ["GRU（無 GCN，ablation）", "2.00", "2.64", "3.33"],
    ["naive（上一時刻延續）", "2.40", "3.11", "3.87"],
    ["歷史平均 HA（週時間槽）", "4.51", "4.51", "4.51"],
], cw=[5.5, 2.5, 2.5, 2.5], fs=17)
tf = tx(s, 1.5, 5.9, 17, 4, "", 20); tf.paragraphs[0].text = ""
ap(tf, "發現一：GCN 空間資訊於所有視野帶來一致改善（ablation 成立），視野越遠越重要", 20, LG)
ap(tf, "發現二：殘差連接為必要條件 — 無殘差時 MAE@5min 惡化至 3.07（劣於純 GRU 2.00）", 20, GRN88, True)
ap(tf, "  → 與計畫書 p.1「過時狀態傳遞錯誤資訊」相呼應：空間傳播不可破壞節點自身狀態", 18, LG)

# 準確率總覽圖
s = csl("實驗四：模型準確率總覽")
pic(s, RESULTS / "accuracy_comparison.png", 1.0, 2.2, 18.0)
tx(s, 1.0, 9.4, 18, 1,
   "左：交通預測 MAE（越低越準）— T-GCN 三個視野都最低　｜　右：GCN 節點分類準確率 — 達文獻水準，證明實作正確",
   18, LG, al=PP_ALIGN.CENTER)

# 路網覆蓋擴展與資料源選擇
s = csl("實驗四：路網覆蓋擴展與資料源選擇")
tx(s, 1.5, 1.7, 17, 0.8, "評估可擴展性 — 三種覆蓋方案的精度取捨", 24, WHITE, True)
tbl(s, 1.0, 2.8, 18, 3.0, [
    ["版本", "資料源", "涵蓋", "節點數", "MAE@5min", "GCN 贏 GRU"],
    ["國道一號（主結果）", "M05A 門架", "國1", "148", "1.88", "明顯"],
    ["多國道（系統採用）", "M05A 門架", "國1/3/5+高架", "378", "2.11", "明顯"],
    ["全國道（未採用）", "VD 偵測器", "全 8 國道", "1,087", "3.90", "幾乎消失"],
], cw=[3.5, 2.5, 3.0, 2.0, 2.5, 2.5], fs=15)
tf = tx(s, 1.5, 6.2, 17, 4, "", 18); tf.paragraphs[0].text = ""
ap(tf, "系統採用 M05A 多國道版（378 段）：改幾行前綴篩選即覆蓋 2.5 倍，模型不變，30 分鐘視野反而更準", 19, GRN88, True)
ap(tf, "", 6)
ap(tf, "全國道 VD 版覆蓋最廣（8 國道）但未採用，誠實揭露原因：", 20, ACC, True)
ap(tf, "1. 點感測噪音高 — VD 是單點瞬時車速，M05A 是門架跨數公里旅行時間（平滑）→ MAE 幾乎翻倍", 18, LG)
ap(tf, "2. GCN 優勢消失 — VD 版 T-GCN(3.90) ≈ 純 GRU(3.93)，噪音蓋過空間資訊，弱化核心論點", 18, RED88)
ap(tf, "→ 取捨結論：主線用精準的 M05A，全國道 VD 列覆蓋廣度驗證（精度可靠聚合多筆讀數改善）", 18, LG)

# ============================================================
# 9 實驗五
# ============================================================
sec("實驗五：異常偵測驗證與事件衝擊校準", "真實事故 ground truth · 計畫書 p.8 精確度/召回率")

s = csl("實驗五-1：異常偵測 precision / recall")
tf = tx(s, 1.5, 1.7, 10.5, 3.4, "", 18); tf.paragraphs[0].text = ""
ap(tf, "Ground truth：高公局 LiveEvents 歷史事件檔", 20, ACC, True)
ap(tf, "• 測試期間國道一號事故 169 件（127 件落在測試窗，123 件對齊成功）", 18, LG)
ap(tf, "• 事故依方向＋里程對應門架路段，向上游擴散 2 段（回堵）", 18, LG)
ap(tf, "• 時間窗：通報前 10 分鐘 ～ 清除後 30 分鐘", 18, LG)
ap(tf, "偵測方法：殘差 z = standardize(預測速 − 實際速) 超過閾值 = 非預期減速", 18, LG)
tbl(s, 1.5, 5.2, 12, 2.4, [
    ["閾值", "標記點 precision", "事故 recall", "F1"],
    ["z > 3.0（原設定）", "13.3%（隨機 2.3% 的 5.7×）", "61.0%（75/123）", "0.219"],
    ["z > 3.5（F1 最佳）", "15.4%（6.6×）", "55.3%", "0.240"],
], cw=[3.2, 4.6, 2.8, 1.4], fs=16)
pic(s, RESULTS / "anomaly_pr_curve.png", 12.6, 1.7, 6.6)
tx(s, 1.5, 8.2, 17.5, 2.4,
   "詮釋：61% 事故可被「從未見過事故標籤」的無監督方法自動偵測。precision 偏低屬結構性現象 — "
   "ground truth 只有事故，而殘差同樣捕捉施工/匝道壅塞等真實減速（事故僅佔全事件 ~5%），多數「誤報」實為未標記的真實異常",
   17, PURP)

s = csl("實驗五-2：事件衝擊參數校準（164 件事故實測）")
tbl(s, 1.5, 1.7, 12.5, 2.9, [
    ["量測項", "中位數", "p75", "系統原參數隱含值"],
    ["事故段速度比（基準/最低速）", "1.283", "2.065", "1.44（sev 0.80×1.8）"],
    ["回堵延伸（連續上游路段）", "2.15 km", "6.43 km", "最大 1.6 km"],
    ["恢復時間（回到基準 90%）", "25 分鐘", "—", "—"],
], cw=[4.6, 2.2, 2.2, 3.5], fs=15)
tf = tx(s, 14.3, 1.7, 5.2, 3.2, "", 18); tf.paragraphs[0].text = ""
ap(tf, "• severity 0.80 合理", 18, GRN88)
ap(tf, "  （落在中位數與 p75 之間）", 15, LG)
ap(tf, "• 半徑低估 → 校準 1.1km", 18, LG)
ap(tf, "  IMPACT_CALIBRATED=1 啟用", 15, GRAY)
ap(tf, "• p75 回堵 6.4km → 方向性", 18, LG)
ap(tf, "  傳播列未來工作", 15, LG)
pic(s, RESULTS / "impact_calibration.png", 1.5, 5.0, 17.0)

# ============================================================
# 10 系統整合
# ============================================================
sec("系統整合與新舊對比", "只增不改 · 原系統事件演算法零修改")

s = csl("整合方式：只增不改")
tf = tx(s, 1.5, 1.6, 17, 3.6, "", 20); tf.paragraphs[0].text = ""
ap(tf, "• 預測以 source='prediction' 寫入既有 dynamic_events 表，重用原系統事件→成本機制", 20, LG)
ap(tf, "• 三種事件來源（manual / realtime / prediction）互不干擾，各清各的", 20, LG)
ap(tf, "• 新增 API：/predict/run、/predict/status、/predict/congestion、/revgeocode", 20, LG)
ap(tf, "• 原有端點、事件演算法、路由邏輯零修改", 20, GRN88, True)
tbl(s, 1.5, 5.6, 17, 4.2, [
    ["計畫書概念", "原系統中的對應物", "本次實作"],
    ["增量更新（只算受影響部分）", "apply_event_incremental（半徑內局部更新）", "增量 PageRank 殘差推送（實驗二 B）"],
    ["索引重用避免重算", "idx_edges_latlon 空間索引", "直接相依索引 + flag（實驗二 C）"],
    ["資料局部性", "主幹路網預載記憶體", "子圖分割（實驗二）"],
    ["稀疏運算", "—（無矩陣運算）", "spmm 順序 + 零元素剪裁（實驗三）"],
], cw=[5.0, 6.0, 6.0], fs=15)

s = csl("能力對比：原系統 vs 整合後")
tbl(s, 1.5, 1.8, 17, 6.6, [
    ["面向", "原系統", "整合後"],
    ["壅塞資訊", "TDX VD 當下車速（反應式）", "當下 + 未來 30 分鐘預測（預測式）"],
    ["繞路時機", "已經塞了才繞", "預計會塞就先繞"],
    ["資料保存", "每 5 分鐘覆蓋，無歷史", "M05A 歷史矩陣 + 可持續累積"],
    ["異常偵測", "無（被動接收事件新聞）", "殘差自動標記（recall 61%）"],
    ["事件參數", "文獻概念 + 自訂值", "164 件真實事故校準"],
    ["事件來源", "manual / realtime", "manual / realtime / prediction"],
    ["模型", "規則式成本乘數", "規則式 + 學習式（T-GCN）並存"],
    ["前端", "事件圖層", "+ 預測圖層、新事件即時通知（地點+定位）"],
], cw=[3.0, 6.5, 7.5], fs=15)

s = csl("真實路網端到端驗證")
tf = tx(s, 1.5, 1.8, 17, 5, "", 20); tf.paragraphs[0].text = ""
ap(tf, "完整循環（2.5GB 台灣路網，台北→基隆 balanced）：", 22, ACC, True)
ap(tf, "基準 21.20 分 → 觸發預測（26 壅塞事件）→ 改道 22.88 分 → 清除 → 精確還原 21.20 分", 22, GRN88, True)
ap(tf, "", 8)
ap(tf, "• 預測事件立即增量套用：26 事件 / 605 條邊 / 1.2~1.35 秒（vs 全圖 recompute 15-20 秒）", 20, LG)
ap(tf, "• 成本模型修正：壅塞折進時間項 → 所有模式都反應（原本只有 safest 改道）", 20, LG)
ap(tf, "• 查詢延遲優化：網格快取後重複查詢 5.6 秒 → 0.53 秒", 20, LG)
ap(tf, "• 全路網預載（374 萬節點 / 760 萬邊）重複查詢 0.4~0.5 秒", 20, LG)
ap(tf, "", 8)
ap(tf, "過程中發現並修正原系統既有 bug：nearest_node 起終點解析不一致（28.21 vs 29.22 km）", 18, GRAY)

# ============================================================
# 11 結論
# ============================================================
s = csl("結論與未來工作")
tf = tx(s, 1.5, 1.8, 9.5, 8, "", 20); tf.paragraphs[0].text = ""
ap(tf, "結論", 26, ACC, True)
ap(tf, "• 計畫書軟體方法全數實作並獲數據驗證", 20, LG)
ap(tf, "• 增量傳播 + 直接相依：三個數量級節點更新削減", 20, LG)
ap(tf, "• 稀疏優化論點成立，並找出適用邊界（~5%）", 20, LG)
ap(tf, "• T-GCN 短時預測 MAE < 2 km/h，整合回導航系統", 20, LG)
ap(tf, "• 異常偵測 precision/recall 以真實事故驗證完成", 20, LG)
ap(tf, "  → 計畫書 p.8 全部評估指標都有實測數據", 20, GRN88, True)
tf2 = tx(s, 11.5, 1.8, 8, 8, "", 20); tf2.paragraphs[0].text = ""
ap(tf2, "未來工作", 26, ACC, True)
ap(tf2, "1. DDA/DFA 硬體實作（Verilog/Modelsim）", 20, LG)
ap(tf2, "2. 即時 ETag 資料流 → 預測服務轉線上模式", 20, LG)
ap(tf2, "3. 市區 VD 路網擴展", 20, LG)
ap(tf2, "4. 方向性上游傳播（衰減曲線為依據）", 20, LG)
ap(tf2, "5. 全事件類型 ground truth 精細化 precision", 20, LG)

# ============================================================
# 12 Q&A
# ============================================================
s = prs.slides.add_slide(prs.slide_layouts[6]); bg(s, False)
tx(s, 2, 4.2, 16, 2, "Q & A", 60, BLACK, True, PP_ALIGN.CENTER)
tx(s, 2, 6.5, 16, 1, "重現指令與完整數據：gcn/REPORT.md 附錄 · gcn/results/*.json", 20, GRAY, al=PP_ALIGN.CENTER)
pgn(s)

out = Path(__file__).resolve().parent / "gcn_report.pptx"
prs.save(out)
print(f"PPT saved: {out} ({len(prs.slides)} slides)")
