# 台灣 OSM 動態路網導航系統

## 專案簡介
基於 OpenStreetMap 台灣路網資料的即時動態導航系統，整合 TDX 交通資料平台（車速偵測、路況新聞）與 CWA 中央氣象署（氣象站、雨量站）即時資料，透過 A* 演算法進行路徑規劃，並根據壅塞、事故、天氣等因素動態調整路徑成本。

## 系統架構

```
┌─────────────┐    ┌─────────────┐    ┌───────────────────┐
│  index.html │◄──►│  osm_api.py │◄──►│  osm_router.py    │
│  前端 UI    │REST│  FastAPI     │    │  A* 路由引擎       │
└─────────────┘    └──────┬──────┘    └────────┬──────────┘
                          │                    │
                   ┌──────┴──────┐      ┌──────┴──────┐
                   │realtime_sync│      │taiwan_osm.db│
                   │  背景同步    │─────►│  SQLite     │
                   └──────┬──────┘      └─────────────┘
                     ▲         ▲
                     │         │
               ┌─────┴──┐ ┌───┴────┐
               │ TDX API │ │CWA API │
               │ 車速路況 │ │ 氣象   │
               └────────┘ └────────┘
```

## 檔案結構

| 檔案 | 角色 |
|------|------|
| `build_osm_graph.py` | OSM PBF → SQLite 路網建置工具（離線執行一次） |
| `osm_router.py` | A* 路徑搜尋引擎 + 動態成本計算核心 |
| `osm_partition.py` | 路口收縮圖（`PartitionedRouter`，繼承 OSMRouter）；`PARTITION_MODE=1` 時啟用 |
| `osm_propagation.py` | 事件沿路網往上游傳播（`PropagationRouter`，繼承 PartitionedRouter）；`EVENT_PROPAGATION=1` 時啟用 |
| `verify_propagation.py` | 事件傳播驗證（推送 vs 精確解、直接相依索引、實地行為；`--db-test` 在資料庫副本上測寫入路徑） |
| `make_event_figures.py` | 事件更新視覺圖（真實路網、只讀資料庫），輸出 `event_figures/*.png` |
| `make_prediction_figures.py` | 交通預測視覺圖（用 T-GCN checkpoint 跑測試段、只讀），輸出 `event_figures/pred_*.png` |
| `build_system_ppt.py` | 對外報告簡報（以 `專題(1).pptx` 為版型），輸出 `demo_ppt_system.pptx`；可加輸出路徑參數 |
| `verify_routes.py` | 路線回歸比對（50 組固定取樣，改動前存基準、改完比對） |
| `benchmark_events.py` | 事件壓力測試（資料庫副本）：`scale` 已有 0～2 萬事件的重算／新增／找路耗時；`burst` 多執行緒同時湧入並逐條比對全量重算；`http` 真的啟動伺服器同時送 POST /events |
| `measure_routes.py` | 路徑搜尋成本量測（單條 vs 含備選） |
| `analyze_complexity.py` | 演算法時間複雜度量測（唯讀，不需伺服器） |
| `osm_api.py` | FastAPI REST API 伺服器（13 個端點） |
| `realtime_sync.py` | TDX/CWA 背景同步引擎（TDXClient + CWAClient + RealtimeSyncer） |
| `index.html` | Leaflet 地圖前端 UI（單檔 SPA） |
| `taiwan_osm.db` | SQLite 路網資料庫（~7.6M 邊） |
| `env.example` | 環境變數範本 |
| `requirements.txt` | Python 相依套件 |

## 啟動方式

```bash
# 1. 安裝套件
pip install -r requirements.txt

# 2. 設定環境變數
cp env.example .env   # 填入 TDX / CWA API key

# 3. 建置路網（僅需一次）
python build_osm_graph.py --pbf taiwan.pbf --db taiwan_osm.db

# 4. 啟動 API 伺服器
uvicorn osm_api:app --host 127.0.0.1 --port 8000

# 5. 開瀏覽器 index.html（或 http://127.0.0.1:8000/docs）
```

## 核心模組說明

### osm_router.py — A* 路由引擎

- **圖載入策略**：啟動時預載主幹路網（motorway～tertiary）到記憶體；短程 ≤15km 另查局部全類型路網
- **端點局部圖快取**：路由時端點周邊局部圖按 0.01° 網格快取（`_local_cells`），同區域重複查詢不重讀 DB（首查 ~5s → 之後 ~0.5s）；合併的局部邊登記進 `_edge_index`，事件增量套用/重算可更新到
- **起終點解析**：`nearest_node` 以載入時節點快照（`_base_graph_nodes`）篩選，不受局部圖合併影響，同一請求結果恆一致
- **事件立即生效**：手動事件（POST /events）與預測事件（POST /predict/run）都走 `apply_event_incremental` 即時增量套用（~1.4s/26 事件），全圖 recompute 僅用於清除還原與天氣
- **事件範圍只取相連路段**（`EVENT_CONNECTED_ONLY`，預設 1）：方框 ±r 內只更新「沿道路與事故點相連」的路段（`event_box_edge_ids`，增量套用、全量重算、link 同步共用）。事故所在道路＝最近路段，同等級且在最近距離 +50 m 內的也算（分隔道路兩側）。實測：國道寶山 1,546 → 55 條（底下不相交的平面道路不再更新）、社區道路 552 → 426、台北忠孝東路 2,473 → 2,388。設 0 退回原方框法
- **封路只封那條路**（`CLOSURE_ROAD_ONLY`，預設 1）：`closure` 事件只封與最近路段同名（無路名時同等級且無路名）、在範圍內相連的路段，其他道路照常通行。原方框法會把範圍內所有道路封死：台北松德路一個封路封了 5,735 條路段（含快速道路、巷弄），起點在範圍內就找不到路徑；改後 79 條。`landslide_closure` 的位置是雨量站、不是道路，維持整片封閉。設 0 退回整片封閉
- **起終點在封路上可駛離**：找不到路徑時（`_route_exit_closed`）改在原始路網重搜一次，允許經過起終點 `CLOSED_EXIT_KM`（3 km）內的封閉路段，成本為原行車時間 × `CLOSED_EXIT_FACTOR`（10）；回應的 `closed_km` 為經過的封閉路段長度（> 0 時前端提示），此時不另找備選
- **三種路線模式**：
  - `fastest` — time=1.0, risk=0.10（最快到達）
  - `balanced` — time=1.0, risk=0.40（平衡速度與安全）
  - `safest` — time=1.0, risk=0.90（優先安全）
- **成本公式**：`edge_cost = adj_time × (time_weight + risk × risk_weight) + turn_penalty + signal_delay`
  - `adj_time = edge.cost / time_factor`，其中 `edge.cost = (dist / (speed / dynamic_mult)) × 60`
    ——**事件速度乘數折進時間項，所有模式都會反應**（2026-07 改版：壅塞是時間損失
    而非風險偏好）；中性狀態等價於 `(dist / (speed × time_factor)) × 60`
  - 分模式（risk_weight）保留給偏好型因素：事故風險、天氣、夜間
  - `risk = dynamic_risk + night_risk`（夜間 +0.05~0.25）
  - `turn_penalty`：轉彎延遲（直行 0s, 左右轉 10s, U-turn 25s）
  - `signal_delay`：每個號誌路口 +20s（高速公路免計）
- **事件成本乘數**：accident=1.80, construction=1.55, closure=999, congestion=1.35, manual=1.25, landslide_warning=1.5, landslide_high=3.0, landslide_closure=999, **predicted_congestion=3.5**（T-GCN 專用：severity=(自由流速/預測速)/3.5，故 sev×3.5=精確時間乘數）
- **天氣成本乘數**：`1.0 + 0.40×rain + 0.20×wind + 0.35×visibility + 0.30×warning`
- **時段速度**：尖峰 (7-9, 17-19) motorway ×0.85, primary ×0.70, tertiary ×0.62；離峰 ×1.0
- **道路速度**：motorway=110, trunk=90, primary=60, secondary=50, tertiary=40, residential=30 km/h
- **重要**：道路類別本身沒有 risk 差異，risk 來自動態事件、天氣和時段

### osm_partition.py — 路口收縮圖（`PARTITION_MODE=1`）

問題：A* 跑在「平均每邊 34 公尺」的圖上，而 **95.7% 的節點只是描繪彎道的形狀點，
不是路口**，搜尋每前進 34 公尺就付一次堆積操作。

作法：把「無向度數 ≠ 2」的節點當路口，兩路口之間的形狀點鏈收成一條 **link**，
A* 改跑 link 圖。`PartitionedRouter` 繼承 `OSMRouter`，只覆寫
`_route_inner` / `apply_event_incremental` / `recompute_dynamic_cost` / `_apply_dynamic_to_graph`；
**未建 link 圖、端點不在圖中、展開失敗時全部退回父類別**。

| 指標 | 實測（主幹路網） |
|---|---|
| 節點 | 793,898 → **35,668**（22.3×） |
| 有向邊 | 1,344,026 → **77,725 link**（17.3×） |
| 50 條路線總耗時 | 185.3s → **13.7s（−92.6%）** |
| 台北→高雄 | 2.62s → **0.135s** |

**已驗證的性質**（勿在重構時弄丟）：

1. **成本聚合與逐邊加總完全相等**（實測誤差 1.8×10⁻¹³）。關鍵是
   `Link.by_hw` 存 `(highway, Σcost, Σ(cost×risk))` **兩個**累加量——因為原式
   `(cost/tsf)×(1+(risk+night)×w_risk)` 只有拆成
   `cost/tsf×(1+night×w_risk) + (cost×risk)/tsf×w_risk` 才可依 highway 分組聚合。
2. **同一張路網下路線逐段相同**（16 條測試邊序列完全一致）。
   內部轉彎與號誌在建圖時就折進 link，跨路口的轉彎在搜尋時補算。
3. **事件反應與原圖 A* 一致**（同一事件下距離、時間完全相同）。
   `apply_event_incremental` 後必須呼叫 `_refresh_links` 同步 link 聚合，
   否則 A* 讀的是舊值、事件等於沒生效。
4. **Link 必須存 `edge_ids` 而非 Edge 物件**——Edge 是 namedtuple，
   事件套用時以 `_replace` 產生新物件寫回 `_graph`，存參照會拿到過期成本。
5. **全量重算後要同步「重算前與重算後」登記過的所有路段**所屬的 link——被刪除的
   事件，其路段在重算時已還原並移出 `_modified_edges`，只看重算後的清單會漏掉。
   修正前實測：刪除事件並重算後 22 條 link 殘留事件成本，路線仍在繞路。
   這一步放在 `_apply_dynamic_to_graph` 覆寫裡、持 `_graph_lock` 並用 try/finally，
   路由不會看到「路段已重設、link 未同步」的中間狀態；增量套用與全量重算以
   `_event_mutex` 互斥。

**已知取捨**：切割模式不使用端點局部圖（link 圖在載入後建好，局部圖是路由時
才動態併入），50 條路線中 10 條因此長 2~8%。已證實 100% 來自此因素，非搜尋誤差。
附註：原系統的局部圖本身就不是穩定基準——`_local_cells` 跨請求累積，
同一條路線在乾淨環境與跑過 47 條之後的結果並不相同。

### osm_propagation.py — 事件沿上游傳播（`EVENT_PROPAGATION=1`）

問題：原方法的事件只改一個固定方框，影響不沿路網傳播。另外，三因子的
「替代道路密度」以 34 公尺的路段計數，真實路網上幾乎一律滿分（隨機 701 個
主幹道路地點，0 個未達上限），實際只剩道路等級在影響半徑，嚴重度恆為基礎值。

作法（教授計畫書表 CM03）：路口 = 核心節點、link = 子圖；事件所在 link 為活動節點，
影響以**增量 PageRank 殘差推送**往上游傳（直接呼叫 `gcn/incremental_pagerank._push`），
每段衰減 `exp(-km/λ)`、在匯入的岔路依車道數代理值分配；每條事件 link 的影響向量
算一次存入**直接相依索引**（flag N→A）。**原方框法照常先執行**，傳播是再加上去的
（乘數取較大值），符合 gcn/CLAUDE.md「原方法不可刪改」的規則。

| 指標 | 實測 |
|---|---|
| 推送 vs 精確解 (I−B)⁻¹ | 影響 ≥ 0.05 的 link 無遺漏，最大誤差 0.0165 |
| 國道一號寶山車禍 | 7 條 link、166 條路段，最遠 6.0 km（實測回堵 p75 6.43 km） |
| 台北忠孝東路 vs 蘇花公路 | 最遠 1.2 vs 6.1 km；影響 ≥ 0.37 的路段 25 vs 154 條 |
| 直接相依索引 | 首次約 10 ms，同一地點再發生查表約 1 ms |
| 增量 vs 全量重算（DB 副本） | 194 條路段（方框內相連 55 ＋ 傳播 166）成本差 0；刪除事件後全部還原 |

**勿在重構時弄丟**：

1. λ = 2.5 km 由 `gcn/results/impact_calibration.json` 的逐門架衰減推得
   （國道一號區段中位數 4.25 km，扣除約 1.6% 雜訊底後每段剩 17–20%）。
2. 只傳播 accident / construction / manual。congestion、predicted_congestion 本身就是
   逐點量測或預測值，再傳播會重複計算；封路與山崩維持原方法。
3. 建索引用 `INDEX_TOL = PUSH_TOL / 10` 推送。用同一個門檻推送會在每個節點丟掉
   殘差，多條路徑匯入同一條 link 時一起被丟，實測誤差 0.059、漏掉 3 條 link。
4. 全量重算後對仍生效的突發事件重新傳播，放在 `_apply_dynamic_to_graph` 覆寫裡、
   與重設同一把 `_graph_lock`，與增量套用逐條相同。`_propagated` 記錄重算後已傳播的
   事件，同一事件再套用時跳過傳播（風險是累加的，重複會疊兩次）。
   傳播只改記憶體，不寫 DB，和原增量法一致。
5. 事件沒有方向欄位，分隔道路兩側都當事件 link（兩個方向都往上游傳）。
   事件所在的路＝**最近**的路段（不是等級最高的），同等級且多不到 50 m 的才併入；
   事件有 `highway` 欄位時只吸附該等級。只在主幹路網上傳播：事件 0.25 km 內沒有
   主幹道路就不傳播（例：社區道路）。
6. 封閉路段（成本 ≥ 1e8）不能被傳播改動：乘數公式會把速度夾在下限 3 km/h、
   寫回有限成本，封閉路段變成可慢速通行。原方框法的非封路分支也有同樣的問題
   （封路後附近再來一場車禍，封閉路段會變成 3 km/h 可通行），依規則未修改原方法。
7. 直接相依索引是 LRU（最多 4,096 個事件 link）並以鎖保護；重建 link 圖時傳播索引
   自動作廢，須重新 `build_propagation_index()`。

### realtime_sync.py — 即時同步引擎

- **TDXClient**：OAuth2 client_credentials 認證，token 自動刷新，429 指數退避重試（1.5s base, 3 次）
- **CWAClient**：API Key 認證，SSL verify=False（CWA 憑證問題）
- **VD 車速同步**：19 縣市逐城市呼叫，靜態位置快取到 `vd_positions` 表，即時速度與位置 JOIN，速度 < 自由流速 70% 建立壅塞事件，取前 300 筆
- **路況新聞**：TDX News/Highway 端點，依 NewsCategory 對應事件類型
- **氣象同步**：自動氣象站 + 雨量站，正規化為 0~1（rain: /80mm, wind: (v-8)/22, visibility: 分級）
- **山崩偵測**：累積雨量 ≥200mm 建立 landslide 事件（warning/high/closure 三級），針對山區測站（lat ≥ 23.0°N）
- **source 欄位區分**：即時資料 `source='realtime'`（每次同步前清除），手動資料 `source='manual'`（不受影響）
- **同步間隔**：預設 300 秒，可由 `AUTO_SYNC_INTERVAL` 環境變數設定

### osm_api.py — REST API

| 方法 | 路徑 | 功能 |
|------|------|------|
| GET | `/` | 服務狀態 |
| GET | `/stats` | 系統統計（含同步狀態） |
| GET | `/nearest?lat=&lon=` | 最近路網節點 |
| POST | `/route` | 路徑規劃（回傳路段明細 + GeoJSON + 分析報告） |
| GET | `/events` | 列出活動事件 |
| POST | `/events` | 新增手動事件 |
| DELETE | `/events/{id}` | 刪除事件（自動排背景全量重算，被刪事件改過的路段數秒後還原） |
| DELETE | `/events` | 清除所有事件+天氣（自動背景重算） |
| GET | `/weather` | 列出活動天氣 |
| POST | `/weather` | 新增手動天氣 |
| POST | `/dynamic/recompute` | 重算動態成本（執行中再要求時，跑完補跑一次） |
| GET | `/dynamic/version` | 路網成本版本：每次持 `_events_guard` 改動成本完成時 +1；前端據此重新規劃已查好的路線 |
| GET | `/sync/status` | 同步狀態 |
| POST | `/sync/trigger` | 手動觸發同步 |
| POST | `/predict/run` | 跑 T-GCN 車速預測，寫入 source='prediction' 事件（背景） |
| GET | `/predict/status` | 預測執行狀態與上次結果 |
| GET | `/predict/congestion` | 列出預測壅塞事件 |
| DELETE | `/predict/events` | 清除預測事件（不動 manual/realtime） |
| GET | `/predict/model` | 預測模型版本（目前啟用、歷次更新與指標） |
| POST | `/predict/model/update` | 背景執行模型更新：補新資料 → 重訓 → 驗證，比目前準才換上 |
| POST | `/predict/model/activate?version=` | 切換模型版本（`v0` = 原始模型） |

- 座標範圍檢查：lat 21.5~26.5, lon 118~122.5（台灣）
- 啟動時做一次全量重算：增量套用只改記憶體，資料庫的路段成本停在上一次重算，不重算的話重啟後生效中的事件不會套用、已刪除的事件反而還在
- 頁面（`/`、`/app`、`/admin`）回應 `Cache-Control: no-cache`，前端更新後瀏覽器不會沿用舊版

### gcn/ — GCN 加速實作與交通預測（教授計畫書）

- `gcn/README.md` 為總覽；`gcn/traffic/predict_service.py` 把 T-GCN 預測轉成
  `source='prediction'` 的 dynamic_events，重用既有 recompute 機制套用成本，
  與 manual/realtime 事件互不干擾（各自清各自的 source）
- 訓練資料：高公局 M05A（`gcn/traffic/download_m05a.py`，tisvcloud 憑證問題須走 curl）
- 模型 checkpoint：`gcn/results/traffic_tgcn.ckpt`（重訓會覆寫）

### index.html — 前端 UI

- **Leaflet.js 地圖**：點擊/輸入/拖曳設定起終點
- **路線顯示**：GeoJSON polyline + 距離/時間/路段資訊
- **即時圖層 (Overlay)**：
  - 壅塞：severity 顏色 circleMarker（綠→黃→紅）
  - 事件：影響圓 + 中心標記
  - 天氣：半透明圓（藍=雨、綠=風、灰=霧），各圖層獨立開關，60 秒自動刷新
- **同步面板**：同步狀態、計數、手動同步按鈕
- **事件/天氣管理**：手動新增、重算成本、清除
- **路線自動重新規劃**：每 5 秒查 `/dynamic/version` 與事件清單，有變就重新查詢已顯示的路線（不移動視角，結果有變才提示）；行駛中從目前位置重新規劃
- **導航模式**（▶ 開始行駛）：介面依手機導航慣例——上方綠色轉彎卡（圖示、到轉彎點的沿路距離、「右轉進入 X 路」），下一個轉彎在 400 m 內時再預告「然後」；下方顯示剩餘時間、距離、抵達時間、結束導航與路線概覽；目前路名、車速、拖曳後的「重新置中」。轉彎點＝路名改變處，角度取路口前後各約 40 m（不受彎道形狀點影響），25 m 內的路名閃爍與過近的轉彎點會合併。地圖朝行進方向旋轉、車子放在畫面 72% 高度；旋轉時地圖放大成畫面對角線大小的正方形，轉任何角度都蓋滿畫面。實際定位離路線超過 1 km（例如在電腦上展示）或沒有定位時改為**模擬行駛**（依各路段實際車速前進，可暫停、×1/×4/×10）

## 資料庫結構

| 表名 | 用途 | 量級 |
|------|------|------|
| `osm_nodes` | 節點 (node_id, lat, lon) | ~數百萬 |
| `osm_edges` | 邊 + 動態欄位 (dynamic_mult, closure_flag, risk_score) | ~7.6M |
| `dynamic_events` | 事件 (event_type, severity, lat, lon, radius, source) | ~300 即時 |
| `dynamic_weather` | 天氣 (rain/wind/visibility/warning_level, source) | ~50-80 即時 |
| `vd_positions` | VD 偵測器位置快取 (vd_id, lat, lon, road_class) | ~1,361 |

## 環境變數

| 變數 | 說明 | 預設值 |
|------|------|--------|
| `TDX_CLIENT_ID` | TDX OAuth2 Client ID | — |
| `TDX_CLIENT_SECRET` | TDX OAuth2 Client Secret | — |
| `CWB_API_KEY` | CWA 氣象署 API Key | — |
| `DB_PATH` | SQLite 路徑 | `taiwan_osm.db` |
| `AUTO_SYNC_INTERVAL` | 同步間隔秒數，0=停用 | `300` |
| `PRELOAD_ALL_ROADS` | 1=全路網載入記憶體（765 萬邊，啟動 ~100s、RAM ~6.2GB，路由零 DB 查詢、無首查尖峰）。**長程路線在此模式下常撞 A\* 200 萬步上限而失敗**（實測 50 條路線失敗 10 條，含台北→高雄）。0=主幹預載+端點局部圖網格快取（預設，50 條全數成功且總耗時只有一半） | `0` |
| `PARTITION_MODE` | 1=啟用路口收縮圖（見下節）。**必須搭配 `PRELOAD_ALL_ROADS=0`**，設 1 時會自動停用並印警告 | `0` |
| `EVENT_PROPAGATION` | 1=事件沿路網往上游傳播（計畫書演算法，見 osm_propagation.py 一節）。**必須搭配 `PARTITION_MODE=1`**，需要 numpy、scipy；條件不符時自動停用並印警告 | `0` |
| `CLOSURE_ROAD_ONLY` | 1=封路只封事件所在的那條路；0=範圍內所有道路整片封閉（原方法） | `1` |
| `EVENT_CONNECTED_ONLY` | 1=事件方框內只更新與事故點沿道路相連的路段；0=原方框法（方框內全部路段）| `1` |
| `MODEL_UPDATE_HOURS` | 交通預測模型每幾小時自動更新一次（gcn/traffic/model_update.py）；0=關閉，只能手動觸發。原始模型永遠保留為 v0 | `0` |

## 開發注意事項

- SQLite 使用 WAL 模式 + `timeout=30` 避免多執行緒鎖定
- **寫入事件＋增量套用、全量重算、預測事件寫入＋套用必須互斥**（osm_api `_events_guard()`：
  切割模式用路由器的 `_event_mutex`，原始模式用 `_event_lock`）。全量重算若插在寫入與套用之間，
  事件風險會被算兩次（實測 500＋500 個事件途中重算：3,934 條路段 +6.4；加鎖後 0）。
  即時預測的下載不可持鎖（`predict_live` 在鎖外、`write_live_prediction` 在鎖內）
- 事件湧入時每秒約處理 4～5 個（單一事件約 33 ms，但套用與找路共用 `_graph_lock`）；
  已有 2 萬個事件時新增一個仍約 31 ms、找路約 0.13 s，全量重算約 287 s（線性）
- 已知問題（原始事件方法，依規則未改）：封路之後再套用附近事件，封閉路段會變成 3 km/h
  可通行，且封路會覆寫已累積的風險；全量重算後恢復正確（1,000 事件含 16 處封路：544 條路段與重算不同；為封路改成只封該條路之前的實測）
- `osm_edges` 上有 `idx_edges_latlon` 空間索引，recompute 效能關鍵
- TDX 城市間 API 呼叫需間隔 1.5 秒避免 429
- VD 即時資料無座標，必須與靜態 VD 資料 JOIN（靠 VDID）
- TDX News 端點部分新聞無 lat/lon，會被跳過
- 前端為單檔 SPA，所有 CSS/JS 內嵌於 index.html

## 已知限制

- TDX News/Highway 端點有時無資料回傳（incident_count=0）
- VD 位置快取首次需逐城市呼叫 TDX 靜態端點（耗時約 30 秒）
- 路網僅支援台灣範圍，座標超出會 400 錯誤
- 短程路線（≤15km）需即時查 DB，較長程略慢
