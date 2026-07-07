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
- **三種路線模式**：
  - `fastest` — time=1.0, risk=0.10（最快到達）
  - `balanced` — time=1.0, risk=0.40（平衡速度與安全）
  - `safest` — time=1.0, risk=0.90（優先安全）
- **成本公式**：`edge_cost = adj_time × (time_weight + risk × risk_weight) + turn_penalty + signal_delay`
  - `adj_time = (dist / (speed × time_factor)) × 60` (分鐘，time_factor 為時段速度乘數)
  - `risk = dynamic_risk + night_risk`（夜間 +0.05~0.25）
  - `turn_penalty`：轉彎延遲（直行 0s, 左右轉 10s, U-turn 25s）
  - `signal_delay`：每個號誌路口 +20s（高速公路免計）
- **事件成本乘數**：accident=1.80, construction=1.55, closure=999, congestion=1.35, manual=1.25, landslide_warning=1.5, landslide_high=3.0, landslide_closure=999
- **天氣成本乘數**：`1.0 + 0.40×rain + 0.20×wind + 0.35×visibility + 0.30×warning`
- **時段速度**：尖峰 (7-9, 17-19) motorway ×0.85, primary ×0.70, tertiary ×0.62；離峰 ×1.0
- **道路速度**：motorway=110, trunk=90, primary=60, secondary=50, tertiary=40, residential=30 km/h
- **重要**：道路類別本身沒有 risk 差異，risk 來自動態事件、天氣和時段

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
| DELETE | `/events/{id}` | 刪除事件 |
| DELETE | `/events` | 清除所有事件+天氣 |
| GET | `/weather` | 列出活動天氣 |
| POST | `/weather` | 新增手動天氣 |
| POST | `/dynamic/recompute` | 重算動態成本 |
| GET | `/sync/status` | 同步狀態 |
| POST | `/sync/trigger` | 手動觸發同步 |
| POST | `/predict/run` | 跑 T-GCN 車速預測，寫入 source='prediction' 事件（背景） |
| GET | `/predict/status` | 預測執行狀態與上次結果 |
| GET | `/predict/congestion` | 列出預測壅塞事件 |
| DELETE | `/predict/events` | 清除預測事件（不動 manual/realtime） |

- 座標範圍檢查：lat 21.5~26.5, lon 118~122.5（台灣）

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

## 開發注意事項

- SQLite 使用 WAL 模式 + `timeout=30` 避免多執行緒鎖定
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
