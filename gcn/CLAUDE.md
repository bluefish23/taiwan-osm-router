# GCN 專題工作說明（gcn/ 資料夾）

> 本檔案涵蓋 gcn/ 資料夾的工作脈絡。系統整體架構見專案根目錄 CLAUDE.md。

## 專題背景

教授給了一份國科會計畫書（表 CM03「圖卷積神經網路加速」），指示「要實作」。
計畫書內容：(1) out-degree>1 核心節點切子圖、增量 PageRank 建直接相依（α,β + Y/N/A flag）
加速 GCN 狀態傳播；(2) 稀疏矩陣乘法優化（先算 X·W、動態剪裁零元素）；
(3) DDA/DFA 硬體加速器（Verilog，**本學期不做**，報告以文獻探討帶過）；
(4) 實驗：GCN 分析交通路網時空關聯。產出：學期專題 demo + 書面報告。

**使用者鐵則：原系統既有方法（尤其事件演算法）不可刪改，整合一律採新增方式。**

## 目前進度（2026-07 全部完成）

| Phase | 內容 | 關鍵結果 |
|---|---|---|
| 1 | GCN 純 PyTorch 實作 + 表二資料集驗證 | Pubmed 79.8%（文獻水準）、ogbn-arxiv 67.4% |
| 2 | 子圖分割 + 增量 PageRank + 直接相依 | arxiv 更新節點少 1,880×；flag 快取命中 0.41ms vs 全圖 703ms |
| 3 | 稀疏矩陣實驗 | Â·(X·W) 快 2.3×/FLOPs 少 33×；零剪裁最高 22×（交叉點 ~5% 密度） |
| 4 | T-GCN 交通預測（M05A 真實資料） | MAE 1.88/2.57/3.23 km/h @5/15/30min，勝過 GRU/naive/HA |
| 5 | 整合進路由系統 | predicted_congestion 事件 + 立即增量套用（26 事件/1.35s）+ 三模式都反應 |
| 6 | 異常偵測 ground truth（2026-07-16） | LiveEvents 事故對齊：z>3 recall 61%、precision 13.3%（隨機 5.7×）、best F1 0.240@z>3.5 |
| 7 | 事故衝擊校準（2026-07-16） | 164 件實測：速度比中位 1.283（隱含 sev 0.71）、回堵中位 2.15km/p75 6.43km、恢復中位 25min；IMPACT_CALIBRATED=1 啟用 |
| 8 | 路網覆蓋擴展（2026-08-06） | M05A 多國道 148→378 段（國1/3/5+高架/國3甲，MAE 2.11，**系統採用**）；VD 全國道 1087 點/8 國道但 MAE 3.90 且 GCN 優勢消失，**未採用**（見坑 #11） |

詳細數據：`results/*.json`；報告：`REPORT.md`；demo 流程：`DEMO.md`。

## 檔案地圖

| 檔案 | 用途 |
|---|---|
| `gcn_layer.py` | Â 正規化 + GCN 層（Â·(X·W) 順序）；spmm 計時 |
| `datasets.py` | Pubmed（Planetoid）/ogbn-arxiv 載入（含 torch.load weights_only 相容處理） |
| `train_benchmark.py` | Phase 1 節點分類訓練 |
| `subgraph_partition.py` | 核心節點切子圖；`proposal_figure_graph()` 重建計畫書 21 節點範例 |
| `incremental_pagerank.py` | `_push` 共用推送核心、`LocalPushPageRank`、`DirectDependencyIndex`（稀疏索引） |
| `bench_propagation.py` | Phase 2 三方法 benchmark |
| `spmm_experiments.py` | Phase 3 稀疏矩陣實驗 |
| `traffic/download_m05a.py` | 高公局 M05A 下載（`fetch_tisvcloud` 共用；**憑證問題必須走 curl**） |
| `traffic/tgcn_model.py` | TGCN（GCN+GRU+殘差）、NodeGRU（ablation） |
| `traffic/train_traffic.py` | 訓練 + baseline + checkpoint 輸出（results/traffic_tgcn.ckpt） |
| `traffic/predict_service.py` | 推論 → predicted_congestion 事件寫入 DB（source='prediction'） |
| `traffic/download_events.py` | 高公局 LiveEvents 歷史事故下載（1 分鐘檔取樣、EventID 去重；走 `fetch_tisvcloud`） |
| `traffic/anomaly_eval.py` | 殘差異常 vs 事故 ground truth：precision/recall/F1（results/anomaly_eval.json + PR 曲線） |
| `traffic/calibrate_impact.py` | 以真實事故量測衝擊（幅度/回堵/恢復），校準 BASE_SEVERITY/RADIUS（results/impact_calibration.json） |
| `traffic/download_vd.py` | VD 車輛偵測器下載+聚合+按里程建圖（全 8 國道 1087 點）；VD 版未採用見坑 #11 |
| `plot_accuracy.py` | 準確率對比圖（讀 traffic_*.json + benchmark_*.json → results/accuracy_comparison.png） |

## 重要技術決策與坑（改東西前先讀）

1. **成本模型（2026-07 改版）**：事件速度乘數折進時間項（A* 讀 `e.cost/時段係數`），
   所有模式都反應；risk 保留給偏好型因素。`predicted_congestion` 的
   severity=(自由流速/預測速)/3.5，故 sev×INCIDENT_MULT(3.5)=精確時間乘數。
2. **事件立即生效**：預測與手動事件都走 `apply_event_incremental`（~1.4s/26 事件）；
   全圖 recompute 只用於清除還原與天氣。
3. **三種事件來源互不干擾**：manual / realtime / prediction 各自清各自的
   （predict_service.clear_predictions 只刪 source='prediction'）。
4. **殘差連接是 T-GCN 的必要條件**：拿掉會輸給純 GRU（報告的重要發現，別「簡化」掉）。
5. **計畫書 p.2 範例的邊界鏈歸屬（v8,v10）前後不一致**：本實作採「上游核心 DFS 認領」，
   註記在 subgraph_partition.py docstring，是跟教授討論的點。
6. **tisvcloud（高公局）憑證缺 Subject Key Identifier**：Python ssl 會拒絕，
   下載一律走 `fetch_tisvcloud`（curl）。
7. **時間戳格式**：預測事件 created_at 必須用 `%Y-%m-%d %H:%M:%S`（同 osm_router.utc_now），
   否則 ORDER BY 排序錯亂。
8. **demo 重播索引**：`at_index=4992` = 資料第 18 天（週三）早上 8:00 尖峰；
   離峰對照用 `17*288+3*12=4932`（凌晨 3 點）。
9. 重訓會覆寫 `results/traffic_tgcn.ckpt` 與 json——**跑冒煙測試請另指定輸出或先備份**。
10. **IMPACT_CALIBRATED 開關（osm_router.py 常數區）**：=1 時 accident 的
    severity/radius 用實測校準值（0.71/1.1km），預設 0 = 原行為。用「模組層條件覆寫」
    實作，未動任何函式——遵守「原方法不可刪改」鐵則。方向性上游傳播（p75 事故回堵 6.4km）
    需要事件方向欄位（點地圖無法分辨南北向），連同實測衰減曲線列為未來工作依據。
11. **VD 全國道版為何未採用（重要決策）**：`download_vd.py` 已能抓全 8 國道 1087 主線 VD，
    但 VD 是「單點瞬時」車速（noise 高），且每 5 分鐘只取一個瞬時值 → MAE 3.90（M05A 378 段是 2.11），
    且 T-GCN 幾乎追平純 GRU（GCN 空間優勢被噪音蓋過，弱化核心論點）。故**系統主模型維持 M05A**。
    VD 精度可改善方向：每時窗聚合多筆 VD 讀數（要多下載 5×）。train_traffic 用 `--data` + `--tag _vd`
    分離輸出，VD 結果在 `results/traffic_*_vd.*`，不覆蓋 M05A。VD 矩陣含預建 edges（train 會自動用）。
    download_vd 注意：VDLive XML 有預設 namespace 要先 strip；volume 必須抓真實值（全填 1 會 std=0 → NaN）。

## 換機器 setup（原電腦 / 64GB 機器）

```bash
# 1. 取程式碼（checkpoint 與 results 都在 git 裡）
git clone https://github.com/bluefish23/taiwan-osm-router.git
cd taiwan-osm-router && git checkout gcn-implementation

# 2. 補 gitignore 掉的訓練資料（二選一）
#    a) 從外接碟複製 gcn/data/（m05a_matrix.npz 必要，raw/ 可選）
#    b) 重新下載：python gcn/traffic/download_m05a.py --start 20260510 --end 20260530

# 3. Python 環境
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt psutil
.venv/Scripts/pip install torch --index-url https://download.pytorch.org/whl/cu128
.venv/Scripts/pip install numpy scipy pandas matplotlib ogb

# 4. .env（機器上已有的話確認兩項）
#    DB_PATH=<本機 taiwan_osm.db 路徑>
#    PRELOAD_ALL_ROADS=1        # 64GB 機器建議開：路由零 DB 查詢、無首查尖峰（啟動 ~100s/RAM ~6.2GB）

# 5. 驗證（三個冒煙測試）
.venv/Scripts/python gcn/subgraph_partition.py        # 核心節點應為 [5,7,13,15,16]
.venv/Scripts/python gcn/incremental_pagerank.py      # L1 err 應 < 1e-6
.venv/Scripts/python -m uvicorn osm_api:app --port 8000   # 等「就緒」後照 DEMO.md 走一輪
```

## 下一步（未做，按優先序）

1. 即時 ETag 資料流接入，預測服務由重播模式轉線上模式
2. 雙向 A*（重複查詢 0.4s → 目標 <0.2s，選做）
3. 市區 VD 路網擴展（原系統已有 19 縣市 VD 位置快取）
4. realtime_sync 加 vd_history 歷史快照表，累積自己的訓練資料
5. 異常偵測 precision 精細化：以全事件類型（施工/壅塞）建 ground truth，區分誤報與非事故異常

（已完成並移出：異常偵測 ground truth — 見進度表 Phase 6，工具為
`traffic/download_events.py` + `traffic/anomaly_eval.py`；注意 LiveEvents 檔約 4% 缺檔屬正常、
事故僅佔全事件 ~5% 故 point-precision 天生偏低，詮釋寫在 REPORT §6）
