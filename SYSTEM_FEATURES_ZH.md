# 系統功能文件 — 台灣 OSM 動態路網導航系統

完整技術文件，涵蓋所有系統模組、設計決策、架構分析與學術研究依據。

---

## 目錄

1. [系統架構總覽](#1-系統架構總覽)
2. [路由引擎 (`osm_router.py`)](#2-路由引擎-osm_routerpy)
3. [REST API (`osm_api.py`)](#3-rest-api-osm_apipy)
4. [即時同步引擎 (`realtime_sync.py`)](#4-即時同步引擎-realtime_syncpy)
5. [路網建置工具 (`build_osm_graph.py`)](#5-路網建置工具-build_osm_graphpy)
6. [導航前端 (`index.html`)](#6-導航前端-indexhtml)
7. [管理後台 (`admin.html`)](#7-管理後台-adminhtml)
8. [資料庫結構](#8-資料庫結構)
9. [安全設計](#9-安全設計)
10. [效能最佳化](#10-效能最佳化)
11. [部署方式](#11-部署方式)
12. [學術研究引用](#12-學術研究引用)

---

## 1. 系統架構總覽

### 1.1 整體架構圖

```
┌────────────────────────────────────────────────────────────────────────────────────┐
│                              使用者端 (Browser)                                     │
│  ┌───────────────────────────────────┐  ┌───────────────────────────────────────┐  │
│  │         index.html                │  │          admin.html                   │  │
│  │   Leaflet.js 地圖 + 導航 UI       │  │   管理面板 (事件/天氣/同步)            │  │
│  └───────────────┬───────────────────┘  └──────────────────┬────────────────────┘  │
│                  │ HTTP REST                                │ HTTP REST (需驗證)     │
└──────────────────┼─────────────────────────────────────────┼───────────────────────┘
                   │                                         │
┌──────────────────┼─────────────────────────────────────────┼───────────────────────┐
│                  ▼                                         ▼                       │
│  ┌───────────────────────────────────────────────────────────────────────────────┐ │
│  │                         osm_api.py (FastAPI)                                  │ │
│  │  ┌─────────┐ ┌──────────┐ ┌─────────┐ ┌──────────┐ ┌─────────┐ ┌──────────┐ │ │
│  │  │ GET /   │ │POST /    │ │GET /    │ │POST /    │ │GET /    │ │POST /    │ │ │
│  │  │ stats   │ │ route    │ │ events  │ │ events   │ │ weather │ │ sync/    │ │ │
│  │  │ nearest │ │          │ │         │ │ weather  │ │         │ │ trigger  │ │ │
│  │  │ geocode │ │          │ │         │ │ recompute│ │         │ │          │ │ │
│  │  └─────────┘ └────┬─────┘ └─────────┘ └──────────┘ └─────────┘ └──────────┘ │ │
│  └────────────────────┼──────────────────────────────────────────────────────────┘ │
│                       │                                                            │
│  ┌────────────────────▼──────────────────────────────────────────────────────────┐ │
│  │                    osm_router.py (A* 路由引擎)                                 │ │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────────────┐ │ │
│  │  │  A* 搜尋     │ │  動態成本計算 │ │  天氣/事件   │ │  轉彎/號誌/          │ │ │
│  │  │  haversine   │ │  多因子模型   │ │  乘數套用    │ │  夜間/時段成本       │ │ │
│  │  └──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────────┬───────────┘ │ │
│  └─────────┼────────────────┼────────────────┼────────────────────┼─────────────┘ │
│            │                │                │                    │               │
│  ┌─────────▼────────────────▼────────────────▼────────────────────▼─────────────┐ │
│  │                         taiwan_osm.db (SQLite WAL)                            │ │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────────┐ ┌──────────────┐ ┌───────────┐  │ │
│  │  │osm_nodes │ │osm_edges │ │dynamic_events│ │dynamic_weather│ │vd_positions│ │ │
│  │  │ ~3.76M   │ │ ~7.65M   │ │   ~300       │ │   ~50-80     │ │  ~1,361   │  │ │
│  │  └──────────┘ └──────────┘ └──────────────┘ └──────────────┘ └───────────┘  │ │
│  └──────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                   │
│  ┌──────────────────────────────────────────────────────────────────────────────┐ │
│  │                   realtime_sync.py (背景同步引擎)                             │ │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐           │ │
│  │  │    TDXClient      │  │    CWAClient      │  │  RealtimeSyncer  │           │ │
│  │  │  OAuth2 認證      │  │  API Key 認證     │  │  排程 + 協調     │           │ │
│  │  │  VD 車速          │  │  自動氣象站       │  │  來源分離       │           │ │
│  │  │  路況新聞         │  │  雨量站           │  │  錯誤恢復       │           │ │
│  │  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘           │ │
│  └───────────┼──────────────────────┼──────────────────────┼────────────────────┘ │
│              │                      │                      │                      │
│  伺服器端    │                      │                      │                      │
└──────────────┼──────────────────────┼──────────────────────┼──────────────────────┘
               │                      │                      │
   ┌───────────▼──────┐    ┌─────────▼────────┐             │
   │   TDX 交通資料    │    │   CWA 氣象資料    │             │
   │   平台 API        │    │   中央氣象署 API   │             │
   │  ┌────────────┐  │    │  ┌────────────┐  │             │
   │  │ VD 偵測器   │  │    │  │ 自動氣象站  │  │             │
   │  │ (~1,361 站) │  │    │  │ (~700+ 站) │  │             │
   │  │ 19 縣市     │  │    │  │ O-A0001    │  │             │
   │  ├────────────┤  │    │  ├────────────┤  │             │
   │  │ 路況新聞    │  │    │  │ 雨量站     │  │             │
   │  │ News/Highway│  │    │  │ (~400+ 站) │  │             │
   │  │            │  │    │  │ O-A0002    │  │             │
   │  └────────────┘  │    │  └────────────┘  │             │
   └──────────────────┘    └──────────────────┘             │
                                                             │
                                              ┌──────────────▼──────────────┐
                                              │   每 300 秒自動同步循環      │
                                              │   1. 清除即時資料            │
                                              │   2. 擷取 TDX VD + News     │
                                              │   3. 擷取 CWA 氣象 + 雨量   │
                                              │   4. 偵測山崩風險            │
                                              │   5. 重算動態成本            │
                                              │   6. WAL checkpoint          │
                                              └─────────────────────────────┘
```

### 1.2 資料流向圖

```
外部 API 資料流:

  TDX VD API ──────────────────────────────────────────────────────┐
  (即時車速, 19 縣市)                                               │
       │                                                           │
       ▼                                                           ▼
  ┌─────────────┐     JOIN      ┌──────────────┐     speed < 70%   ┌──────────────┐
  │ VD 即時速度  │────────────►  │ VD 靜態位置   │────────────────► │ congestion   │
  │ (無座標)     │     VDID     │ vd_positions  │   free_flow      │ 事件 (≤300)  │
  └─────────────┘               └──────────────┘                   └──────┬───────┘
                                                                          │
  TDX News API ────────────────────────────────────────────────────┐     │
  (路況新聞, Highway)                                               │     │
       │                                                           │     │
       ▼                     NewsCategory                          ▼     ▼
  ┌─────────────┐    mapping    ┌──────────────┐     ┌──────────────────────────┐
  │ 路況新聞    │────────────► │ accident /   │────►│   dynamic_events 表       │
  │ (含座標篩選) │             │ construction │     │   (type, severity,       │
  └─────────────┘             │ / closure    │     │    lat, lon, radius)     │
                               └──────────────┘     └────────────┬─────────────┘
                                                                  │
  CWA O-A0001 API ─────────────────────────────────────────┐     │
  (自動氣象站: 風速, 能見度)                                  │     │
       │                                                    │     │
       ▼           正規化 0~1                                ▼     │
  ┌─────────────┐ ─────────► ┌──────────────────────────────────┐ │
  │ 氣象站資料   │            │   dynamic_weather 表             │ │
  │ wind, vis   │            │   (rain, wind, vis, warning)    │ │
  └─────────────┘            └────────────┬─────────────────────┘ │
                                           │                       │
  CWA O-A0002 API ─────────────────┐      │                       │
  (雨量站: 每小時降雨)               │      │                       │
       │                            │      │     recompute_        │
       ▼           /80mm, cap 1.0   ▼      ▼     dynamic_cost()   │
  ┌─────────────┐ ────────────► 合併到     ┌──────────────────────┐ │
  │ 雨量站資料   │              weather    │  dynamic_mult        │◄┘
  │ rain_1hr    │              表         │  closure_flag        │
  └─────────────┘                          │  risk_score          │
       │                                    │  (寫回 osm_edges)    │
       │  累積雨量 ≥ 200mm                  └──────────┬───────────┘
       │  (山區測站)                                    │
       ▼                                                │
  ┌─────────────┐                                       ▼
  │ 山崩事件    │                              A* 路徑搜尋使用
  │ warning /   │────────────► dynamic_events   動態成本計算
  │ high /      │
  │ closure     │
  └─────────────┘
```

### 1.3 模組依賴關係

```
build_osm_graph.py             osm_api.py (FastAPI)
    │                             │
    │ 離線執行一次                 │ 匯入
    │ 產出 taiwan_osm.db          ├──────────► osm_router.py
    │                             │                │
    ▼                             │ 匯入           │ 讀寫
taiwan_osm.db ◄───────────────────┤           taiwan_osm.db
    ▲                             ├──────────► realtime_sync.py
    │                             │                │
    │ 讀寫                        │                │ 外部 API
    │                             │                ├──► TDX API
realtime_sync.py ─────────────────┘                └──► CWA API
```

---

## 2. 路由引擎 (`osm_router.py`)

### 2.1 圖結構與載入策略

| 項目 | 說明 |
|------|------|
| **資料來源** | OpenStreetMap 台灣 PBF，經 `build_osm_graph.py` 處理 |
| **規模** | ~376 萬節點、~765 萬有向邊，存於 SQLite |
| **記憶體圖** | 啟動時預載主幹路網（motorway→tertiary）約 134 萬邊 |
| **短程回退** | 路線距離 ≤15 km 時額外查 SQLite 全類型路網（含 residential/service） |
| **連通分量** | 每個節點有 `component_id`，路由僅在同分量內進行 |
| **資料結構** | `namedtuple('Edge', ...)` 取代 dict，節省 ~60% 記憶體 |

```python
Edge = namedtuple('Edge', ['to','eid','hw','name','dist','spd','cost','risk','la','loa','lb','lob'])
```

**端點局部載入**：路由起終點附近 ±0.02° 範圍會額外從 DB 載入所有道路類型，解決高速公路出口無法到達地方道路的問題。

### 2.2 A* 搜尋演算法

```
啟發式函數:
  h(n) = haversine(n, 目標) / 120 km/h

  ● 可容許 (admissible) — 永遠不高估，保證最佳路徑
  ● 120 km/h = 台灣最高合法速限（國道 motorway）

搜尋限制:
  ● 最大展開次數: 2,000,000
  ● 超過即中止，避免斷連子圖上的無窮搜尋

執行緒安全:
  ● threading.RLock 保護記憶體圖
  ● 同時路徑查詢序列化處理
  ● 動態成本更新等待進行中的路由完成
```

### 2.3 三種路線模式

| 模式 | time_weight | risk_weight | 用途 |
|------|-------------|-------------|------|
| `fastest` | 1.0 | 0.10 | 最短行程時間，容忍中等風險 |
| `balanced` | 1.0 | 0.40 | 預設 — 速度與安全的平衡 |
| `safest` | 1.0 | 0.90 | 優先避開危險區域，即使顯著繞路 |

### 2.4 成本公式

```
edge_cost = adj_time × (time_weight + risk × risk_weight) + turn_penalty + signal_delay

其中:
  adj_time = (dist_km / (speed_kmh × time_speed_factor)) × 60   (分鐘)
  risk = dynamic_risk_score + night_risk
  turn_penalty = 根據轉彎角度 (0 ~ 0.42 分鐘)
  signal_delay = 每個號誌節點 0.33 分鐘 (~20 秒)
```

**成本分解圖**:

```
┌────────────────────────────────────────────────────────────────────┐
│                        邊成本 (edge_cost)                          │
│                                                                    │
│  ┌────────────────────────────────────────────┐                   │
│  │  adj_time × (time_weight + risk × risk_w)  │  主要成本         │
│  │                                            │                   │
│  │  adj_time 組成:                             │                   │
│  │  ┌──────────────┐ ┌─────────────┐          │                   │
│  │  │ dist / speed │×│ event_mult  │          │                   │
│  │  │ (基礎時間)   │ │ (事件乘數)   │          │                   │
│  │  └──────────────┘ └─────────────┘          │                   │
│  │         │              ▲                    │                   │
│  │         │     ┌────────┴────────┐          │                   │
│  │         │     │ weather_mult    │          │                   │
│  │         │     │ (天氣乘數)      │          │                   │
│  │         │     └─────────────────┘          │                   │
│  │         ▼                                   │                   │
│  │  ÷ time_speed_factor (時段速度乘數)         │                   │
│  │                                            │                   │
│  │  risk 組成:                                │                   │
│  │  ┌────────────────┐ ┌─────────────┐        │                   │
│  │  │dynamic_risk    │+│ night_risk  │        │                   │
│  │  │(DB risk_score) │ │ (時段風險)   │        │                   │
│  │  └────────────────┘ └─────────────┘        │                   │
│  └────────────────────────────────────────────┘                   │
│                                                                    │
│  + ┌──────────────┐ + ┌──────────────┐                            │
│    │ turn_penalty  │   │ signal_delay │    附加成本                │
│    │ (轉彎延遲)    │   │ (號誌延遲)   │                            │
│    └──────────────┘   └──────────────┘                            │
└────────────────────────────────────────────────────────────────────┘
```

### 2.5 道路速度模型

| 道路類型 | 速度 (km/h) | 說明 |
|---------|-------------|------|
| motorway | 110 | 國道高速公路 |
| motorway_link | 70 | 國道匝道 |
| trunk | 90 | 快速道路 |
| trunk_link | 60 | 快速道路匝道 |
| primary | 60 | 省道 |
| primary_link | 45 | 省道匝道 |
| secondary | 50 | 縣道 |
| secondary_link | 40 | 縣道匝道 |
| tertiary | 40 | 鄉道 |
| tertiary_link | 30 | 鄉道匝道 |
| residential | 30 | 住宅區道路 |
| living_street | 10 | 生活巷弄 |
| service | 20 | 服務道路 |

### 2.6 動態成本調整

#### 事件乘數

| 事件類型 | 成本乘數 | 說明 |
|---------|---------|------|
| accident | 1.80 | 交通事故區域（+80%） |
| construction | 1.55 | 道路施工（+55%） |
| closure | 999 | 道路封閉（等效無限大） |
| congestion | 1.35 | 壅塞（VD 車速偵測） |
| manual | 1.25 | 使用者自訂事件 |
| landslide_warning | 1.50 | 山崩警戒（累積雨量 ≥200mm） |
| landslide_high | 3.00 | 高山崩風險（累積雨量 ≥350mm） |
| landslide_closure | 999 | 山崩封路（累積雨量 ≥600mm） |

#### 天氣乘數

```
weather_mult = 1.0 + 0.40×rain + 0.20×wind + 0.35×visibility + 0.30×warning
```

各因子正規化為 0~1：

| 因子 | 權重 | 正規化方式 | 來源 |
|------|------|-----------|------|
| **雨量** | 0.40 | `min(1.0, rain_1hr / 80)` | CWA 雨量站 |
| **風速** | 0.20 | `min(1.0, max(0, (v - 8) / 22))` | CWA 氣象站 |
| **能見度** | 0.35 | 分級: <0.2km→1.0, <1km→0.8, <5km→0.5, <10km→0.2 | CWA 氣象站 |
| **警報** | 0.30 | CWA 警報等級 0~1 | CWA |

### 2.7 夜間風險因子（學術研究依據）

NHTSA 數據顯示夜間僅占 25% 里程卻造成 50% 死亡事故，致死率為日間 3~9 倍。系統根據台灣時區 (UTC+8) 的時段加入 risk_score：

| 時段 (UTC+8) | 風險加成 | 研究依據 |
|-------------|---------|---------|
| 07:00–17:00 (日間) | +0.00 | 基線 |
| 17:00–19:00 (黃昏) | +0.05 | NHTSA: 1.5× 事故風險 |
| 19:00–00:00 (夜間) | +0.15 | NSC: 3× 致死率 |
| 00:00–04:00 (深夜) | +0.25 | NHTSA 2008: 4–9× 含疲勞 |
| 04:00–06:00 (黎明) | +0.15 | 類似夜間 |
| 06:00–07:00 (清晨) | +0.05 | 過渡時段 |

### 2.8 時段速度剖面（學術研究依據）

尖峰時段城市道路有效速度降低 30~40%。基於 TDVRP 研究，時間相依路由可消除 99% 遲到。

| 時段 | motorway | trunk | primary | secondary | tertiary | residential |
|------|----------|-------|---------|-----------|----------|-------------|
| 00:00–06:00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 | 1.00 |
| 07:00–09:00 (早峰) | 0.85 | 0.80 | 0.70 | 0.65 | 0.62 | 0.60 |
| 09:00–17:00 (日間) | 0.92 | 0.88 | 0.82 | 0.78 | 0.75 | 0.75 |
| 17:00–19:00 (晚峰) | 0.85 | 0.80 | 0.70 | 0.65 | 0.62 | 0.60 |
| 19:00–22:00 (夜間) | 0.95 | 0.93 | 0.90 | 0.88 | 0.85 | 0.88 |

高等級道路（motorway/trunk）因出入管制及較高容量，尖峰衰減較小。

### 2.9 轉彎延遲（學術研究依據）

A* 搜尋追蹤進入邊的方位角，計算與每條出邊的角度差，依據轉彎類型加入時間懲罰。McGill 大學研究指出轉彎延遲占城市行程時間 15~25%。

| 角度差 | 轉彎類型 | 延遲 (分鐘) | 研究依據 |
|--------|---------|------------|---------|
| < 20° | 直行 | 0.00 | 無延遲 |
| 20°–60° | 微轉 | 0.05 (~3秒) | Transport Geography |
| 60°–130° | 左右轉 | 0.17 (~10秒) | McGill: 5–15 秒 |
| 130°–170° | 急轉 | 0.25 (~15秒) | McGill: 15–35 秒 |
| > 170° | 迴轉 | 0.42 (~25秒) | HCM 估計 |

### 2.10 號誌密度延遲（學術研究依據）

圖建置階段從 OSM 解析 `highway=traffic_signals` 節點，儲存於 `osm_nodes.is_signal`。A* 路由時每經過一個號誌節點加 0.33 分鐘 (~20 秒)。

- **豁免**：motorway, motorway_link, trunk, trunk_link（封閉式道路無號誌）
- **影響**：城市幹道每公里 2~6 個號誌，增加 40~120 秒/km
- **台灣分布**：全島約 56,026 個號誌節點（台北 ~5,637, 高雄 ~7,083）

號誌延遲占城市行程時間 20~40% (HCM)。20 秒取自 HCM 每號誌 15~45 秒的中位數。

### 2.11 山崩 / 土石流風險（台灣特定，學術研究依據）

台灣因陡峭地形與颱風活動，頻繁發生降雨誘發山崩。山崩面積從 2004 年 170 km² 增至 2010 年颱風莫拉克後 506 km²。

CWA 同步時估算累積雨量，山區測站（緯度 ≥23.0°N）大雨時觸發山崩事件：

| 累積雨量 | 風險等級 | 成本乘數 | 處理 |
|---------|---------|---------|------|
| < 200 mm | 低 | — | 正常路由 |
| ≥ 200 mm | 警戒 | 1.50 | 路徑懲罰 |
| ≥ 350 mm | 高風險 | 3.00 | 強力避開 |
| ≥ 600 mm | 嚴重 | 999 (封閉) | 等效封路 |

### 2.12 SQL 注入防護

事件建立時 `highway` 欄位經白名單驗證（15 種合法 OSM 道路類型）。所有 SQL 查詢使用參數化 `?` 佔位符，無字串拼接。

### 2.13 最近節點搜尋

`nearest_node()` 方法在搜尋最近路網節點時，優先排除 motorway/trunk 類型節點，避免路由起終點卡在高速公路上。

```python
_MOTORWAY_HW = {"motorway", "motorway_link", "trunk", "trunk_link"}
# 搜尋結果中優先選擇非高速公路節點
non_hw = [r for r in in_graph if r["hw"] not in _MOTORWAY_HW]
pool = non_hw if non_hw else in_graph
```

---

## 3. REST API (`osm_api.py`)

### 3.1 端點總覽

共 17 個端點：9 個公開、8 個需管理員權限。

#### 公開端點

| 方法 | 路徑 | 功能 |
|------|------|------|
| GET | `/` | 提供導航前端 (index.html) |
| GET | `/status` | 健康檢查 — 服務名稱、版本、圖狀態 |
| GET | `/stats` | 系統統計 — 節點/邊數量、事件/天氣數、同步狀態 |
| GET | `/nearest?lat=&lon=` | 最近路網節點查詢 |
| POST | `/route` | 路徑規劃 — 回傳逐段明細 + GeoJSON + 距離/時間/分析 |
| GET | `/events` | 列出所有活動交通事件 |
| GET | `/weather` | 列出所有活動天氣紀錄 |
| GET | `/geocode?q=` | 地理編碼 — 搜尋 TGOS (地址) → Nominatim (POI) |
| GET | `/sync/status` | 同步引擎狀態 |

#### 管理員端點（需驗證）

| 方法 | 路徑 | 功能 |
|------|------|------|
| GET | `/admin` | 提供管理後台 (admin.html) |
| POST | `/events` | 新增手動交通事件 |
| DELETE | `/events/{id}` | 刪除特定事件 |
| DELETE | `/events` | 清除所有事件和天氣資料 |
| POST | `/weather` | 新增手動天氣紀錄 |
| POST | `/dynamic/recompute` | 重算所有動態邊成本 |
| POST | `/sync/trigger` | 手動觸發同步 |

### 3.2 認證與授權

```
請求來源判斷流程:

  ┌─────────────┐
  │   收到請求   │
  └──────┬──────┘
         │
         ▼
  ┌──────────────────┐     是     ┌─────────────┐
  │ 來自 localhost?   │──────────►│  略過驗證    │──► 允許存取
  │ (127.0.0.1/::1)  │           └─────────────┘
  └──────┬───────────┘
         │ 否
         ▼
  ┌──────────────────┐     無     ┌─────────────┐
  │ 有 Bearer token? │──────────►│  403 拒絕    │
  └──────┬───────────┘           └─────────────┘
         │ 有
         ▼
  ┌──────────────────┐    不符    ┌─────────────┐
  │ token 正確?       │──────────►│  403 拒絕    │
  │ (compare_digest)  │           └─────────────┘
  └──────┬───────────┘
         │ 正確
         ▼
    允許存取
```

- **本機免驗**：`127.0.0.1`, `::1`, `localhost` 直接通過
- **遠端存取**：需 `Authorization: Bearer <ADMIN_TOKEN>` 標頭
- **防時序攻擊**：使用 `secrets.compare_digest()` 比對 token

### 3.3 地理編碼

雙來源地理編碼，附記憶體快取：

| 順序 | 來源 | 擅長 | 說明 |
|------|------|------|------|
| 1 | TGOS（選配） | 台灣地址 | 政府地址圖資 API |
| 2 | Nominatim | POI、地標 | OpenStreetMap 地理編碼 |

快取：執行緒安全 LRU，500 筆，5 分鐘 TTL。

### 3.4 輸入驗證

| 項目 | 方式 |
|------|------|
| 座標範圍 | lat 21.5~26.5, lon 118~122.5（台灣邊界框） |
| 道路類型 | 正規表達式白名單，僅接受合法 OSM highway 類型 |
| 節點驗證 | `start_node`/`end_node` 路由前檢查是否存在於圖中 |
| 錯誤消毒 | 內部例外紀錄在伺服器端；客戶端只收到通用訊息 |

### 3.5 並行處理

| 機制 | 說明 |
|------|------|
| 圖鎖 | `threading.RLock` 保護記憶體圖，路由/更新互斥 |
| 快取鎖 | `threading.Lock` 保護地理編碼快取 |
| SQLite | WAL 模式 + `busy_timeout=30000ms` + `timeout=30s` |

---

## 4. 即時同步引擎 (`realtime_sync.py`)

### 4.1 TDX 交通資料

#### VD 車速偵測

```
同步流程:

  19 縣市  ──► 逐城市呼叫 TDX API (間隔 1.5 秒避免 429)
     │
     ▼
  首次同步: 擷取 VD 靜態位置 → 存入 vd_positions 表 (~1,361 站)
  後續同步: 直接讀取快取
     │
     ▼
  即時速度 JOIN vd_positions (透過 VDID)
     │
     ▼
  measured_speed < 70% × free_flow_speed ?
     │
     ├─ 是 → 建立 congestion 事件, severity ∝ 速度降低比
     │
     └─ 否 → 跳過
     │
     ▼
  保留前 300 筆最嚴重壅塞事件
  │  原因:
  │  (1) recompute 對每個事件做 ~765 萬邊空間查詢，300 筆控制重算時間
  │  (2) 候選按 severity 降序，300 名後速度降幅接近 70% 閾值，路由影響微乎其微
  │  (3) 全台 ~1,361 VD，實際每次同步約 ~220 筆壅塞，300 幾乎不會截斷資料
```

#### 路況新聞

- 擷取 TDX News/Highway 端點
- `NewsCategory` 對應事件類型：accident, construction, closure, congestion
- 無 lat/lon 座標的新聞項目跳過

#### 認證機制

| 項目 | 說明 |
|------|------|
| 認證方式 | OAuth2 `client_credentials` |
| Token 刷新 | 自動偵測過期並重新取得 |
| 429 重試 | 指數退避（基礎 1.5 秒，最多 3 次） |
| 認證失敗 | 5 分鐘冷卻期，避免 API 鎖定 |

### 4.2 CWA 氣象資料

#### 自動氣象站 (O-A0001-001)

- ~700+ 測站的風速與能見度資料
- 正規化為 0~1 輸入動態成本模型

#### 雨量站 (O-A0002-001)

- ~400+ 測站的每小時降雨
- 正規化：`rain_1hr / 80`（上限 1.0）

#### 山崩偵測

- 累積雨量估算：`rain_1hr × 6`（近似前 6 小時累積）
- 山區測站判定：緯度 ≥ 23.0°N
- 三級閾值：200mm (warning), 350mm (high), 600mm (closure)

#### SSL 處理

CWA 伺服器有已知 SSL 憑證問題（缺少 Subject Key Identifier）。僅 CWA 請求停用 SSL 驗證，TDX 和其他連線使用完整 SSL 驗證。

### 4.3 來源分離機制

| 來源 | 行為 |
|------|------|
| `realtime` | 同步引擎自動建立。每次同步前全部清除 |
| `manual` | 管理員經 API 建立。跨同步保留，永不自動刪除 |

```sql
-- 同步開始: 清除即時資料
DELETE FROM dynamic_events  WHERE source='realtime';
DELETE FROM dynamic_weather WHERE source='realtime';
-- 手動資料 (source='manual') 完全不受影響
```

### 4.4 同步循環

```
┌─────────────────────────────────────────────────────────────┐
│                   每 300 秒同步循環                          │
│                                                             │
│  1. DELETE source='realtime' events + weather               │
│     │                                                       │
│  2. TDX VD 車速 → congestion 事件 (≤300)                    │
│     │                                                       │
│  3. TDX News → accident/construction/closure 事件           │
│     │                                                       │
│  4. CWA 氣象站 + 雨量站 → weather 紀錄                      │
│     │                                                       │
│  5. 山崩偵測 → landslide 事件 (山區測站)                     │
│     │                                                       │
│  6. recompute_dynamic_cost() → 更新 osm_edges              │
│     │                                                       │
│  7. WAL checkpoint → 防止 WAL 檔無限成長                     │
│     │                                                       │
│  8. 記錄計數與耗時                                           │
└─────────────────────────────────────────────────────────────┘
```

預設間隔 300 秒（5 分鐘），配合 TDX 資料更新頻率。可經 `AUTO_SYNC_INTERVAL` 環境變數調整。

---

## 5. 路網建置工具 (`build_osm_graph.py`)

### 5.1 處理流程

```
taiwan.pbf (OpenStreetMap PBF)
     │
     ▼
  Pass 1: 掃描 Ways (道路)
     │  ● 篩選含 highway 標籤的 Way
     │  ● 收集 highway type, name, oneway 等屬性
     │  ● 記錄所有被道路參考的 Node ID
     │  ● 偵測號誌節點 (Regular Nodes + DenseNodes)
     │
     ▼
  Pass 2: 擷取 Node 座標
     │  ● 僅載入 Pass 1 記錄到的 Node
     │  ● 解析 Regular Nodes + DenseNodes 座標
     │
     ▼
  邊建置
     │  ● 每條 Way 展開為有向邊 (A→B, B→A)
     │  ● oneway 道路僅建單向邊
     │  ● 計算邊長 (haversine)
     │  ● 指派速度 (HIGHWAY_SPEEDS)
     │
     ▼
  連通分量分析 (離線 BFS)
     │  ● 找出最大連通分量
     │  ● 每個節點標記 component_id
     │  ● 過濾孤立子圖
     │
     ▼
  寫入 SQLite
     │  ● osm_nodes (node_id, lat, lon, component_id, is_signal)
     │  ● osm_edges (node_a, node_b, ..., speed_kmh, highway)
     │  ● 建立空間索引
     │
     ▼
  taiwan_osm.db (~400 MB)
```

### 5.2 號誌節點偵測

PBF 格式中節點以兩種形式存在，需分別解析：

| 形式 | 欄位 | 解析方式 |
|------|------|---------|
| Regular Nodes | PrimitiveGroup field 1 | 逐一檢查 key/value 標籤 |
| DenseNodes | PrimitiveGroup field 2 | 解析 keys_vals (field 10) 壓縮格式 |

DenseNodes 的 `keys_vals` 是一個壓縮的扁平陣列，以 0 分隔每個節點的 key/value 對。

篩選標籤：`highway=traffic_signals`（僅紅綠燈，排除 stop 和 crossing）

### 5.3 資料規模

| 項目 | 數量 |
|------|------|
| 路網節點 | ~3,760,000 |
| 有向邊 | ~7,650,000 |
| 號誌節點 | ~56,026 |
| 連通分量 | 73（僅保留最大分量 ~793,898 節點） |
| DB 大小 | ~400 MB |
| 建置時間 | ~23 秒（含 BFS） |

---

## 6. 導航前端 (`index.html`)

單頁應用程式 (SPA)，使用 Leaflet.js。所有 CSS/JS 內嵌於一個 HTML 檔案。

### 6.1 地圖介面

- Leaflet.js + OpenStreetMap 圖磚
- 點擊/輸入/拖曳設定起終點
- 自適應佈局，浮動面板

### 6.2 地址搜尋

- 搜尋框自動完成，查詢 `/geocode` 端點
- 防抖 (debouncing) 避免過頻 API 呼叫
- XSS 防護：`escHtml()` 消毒所有動態 HTML

### 6.3 路線顯示

- GeoJSON polyline 色碼呈現
- 路線摘要：總距離 (km)、預估時間 (min)、路段數
- 逐段明細列表（道路名稱、距離）
- 三模式切換按鈕：最快 / 平衡 / 最安全

### 6.4 GPS 導航模式

按下「開始導航」啟動，提供完整轉彎導航體驗：

```
┌──────────────────────────────────────────────────┐
│                 GPS 導航流程                       │
│                                                   │
│  啟動導航                                          │
│     │                                             │
│     ▼                                             │
│  watchPosition() 追蹤 GPS                          │
│     │                                             │
│     ▼                                             │
│  ┌─────────────────────────────┐                  │
│  │  每次 GPS 更新:              │                  │
│  │                             │                  │
│  │  1. 計算到目的地距離         │                  │
│  │     < 50m? → 抵達完成        │                  │
│  │                             │                  │
│  │  2. 計算偏離路線距離         │                  │
│  │     > 80m (連續3次)?         │                  │
│  │     → 自動重新規劃路線       │                  │
│  │                             │                  │
│  │  3. 更新目前路段追蹤         │                  │
│  │     已走路段 → 灰色          │                  │
│  │     剩餘路段 → 原色          │                  │
│  │                             │                  │
│  │  4. 計算下一轉彎指示         │                  │
│  │     方位角差 → 轉彎類型       │                  │
│  │     → HUD 顯示方向 + 距離    │                  │
│  │                             │                  │
│  │  5. 地圖視角跟隨 GPS         │                  │
│  │     自動旋轉對齊行進方向      │                  │
│  └─────────────────────────────┘                  │
│                                                   │
│  每 60 秒:                                         │
│     重新載入事件圖層                                │
│     比對事件雜湊值                                  │
│     若路線上有新事件 → 自動重規劃                    │
└──────────────────────────────────────────────────┘
```

#### 轉彎指示

- 計算相鄰邊方位角差
- 辨識 7 種轉彎：直行、微左/右、左/右轉、急左/右、迴轉
- HUD 顯示轉彎方向圖示 + 距離 + 目前道路名

#### 偏離路線偵測

- 計算 GPS 位置到最近路線邊的距離
- 閾值：80 公尺
- 需連續 3 次偏離才觸發（過濾 GPS 雜訊假陽性）
- 自動從目前位置重新規劃到原目的地

### 6.5 即時圖層 (Overlay)

三個獨立切換圖層，每 60 秒自動刷新：

| 圖層 | 視覺化 | 資料來源 |
|------|--------|---------|
| **壅塞** | 色碼圓標記（綠→黃→紅，依嚴重度） | TDX VD 車速 |
| **事件** | 半透明影響圓 + 類型色碼 | TDX 路況 + 手動事件 |
| **天氣** | 大半透明圓（藍=雨、綠=風、灰=霧） | CWA 測站 |

### 6.6 同步狀態面板

- 顯示同步狀態、上次時間、耗時
- TDX 車速/事件/CWA 測站計數
- 手動同步觸發按鈕（本機管理員）

---

## 7. 管理後台 (`admin.html`)

獨立管理介面，存取路徑 `/admin`。

### 7.1 存取控制

| 存取方式 | 驗證 |
|---------|------|
| 本機 (localhost) | 免 token，直接載入 |
| 遠端 | 需在上方輸入欄填入 admin token |

### 7.2 功能

| 功能 | 說明 |
|------|------|
| 系統統計 | 節點/邊數量、事件/天氣計數、DB 資訊 |
| 手動事件建立 | 類型、嚴重度、位置、半徑、道路篩選 |
| 手動天氣建立 | 雨/風/能見度/警報等級 |
| 事件管理 | 列表、逐筆刪除、全部清除 |
| 成本重算 | 觸發全邊動態成本重新計算 |
| 同步控制 | 查看狀態、手動觸發同步 |

---

## 8. 資料庫結構

SQLite，WAL 日誌模式，支援並行讀寫。

### 8.1 ER 圖

```
┌─────────────────────────────────────────────────────────────────┐
│                        taiwan_osm.db                            │
│                                                                 │
│  ┌─────────────────────┐      ┌─────────────────────────────┐  │
│  │     osm_nodes        │      │        osm_edges            │  │
│  │─────────────────────│      │─────────────────────────────│  │
│  │ PK node_id  INTEGER │◄────┐│ FK node_a     INTEGER       │  │
│  │    lat      REAL    │     ││ FK node_b     INTEGER       │  │
│  │    lon      REAL    │◄────┘│    lat_a      REAL          │  │
│  │    component_id INT │      │    lon_a      REAL          │  │
│  │    is_signal  INT   │      │    lat_b      REAL          │  │
│  └─────────────────────┘      │    lon_b      REAL          │  │
│                                │    dist_km    REAL          │  │
│                                │    highway    TEXT          │  │
│                                │    name       TEXT          │  │
│                                │    speed_kmh  REAL          │  │
│                                │    dynamic_mult REAL [1.0]  │  │
│                                │    closure_flag INT  [0]    │  │
│                                │    risk_score  REAL [0.0]   │  │
│                                └─────────────────────────────┘  │
│                                                                 │
│  ┌─────────────────────────┐  ┌─────────────────────────────┐  │
│  │   dynamic_events        │  │   dynamic_weather            │  │
│  │─────────────────────────│  │─────────────────────────────│  │
│  │ PK event_id    TEXT     │  │ PK weather_id   TEXT         │  │
│  │    event_type  TEXT     │  │    lat           REAL        │  │
│  │    severity    REAL     │  │    lon           REAL        │  │
│  │    highway     TEXT     │  │    radius_km     REAL [50]   │  │
│  │    lat         REAL     │  │    rain_level    REAL        │  │
│  │    lon         REAL     │  │    wind_level    REAL        │  │
│  │    radius_km   REAL     │  │    visibility_level REAL     │  │
│  │    description TEXT     │  │    warning_level REAL        │  │
│  │    is_active   INT [1]  │  │    is_active     INT [1]     │  │
│  │    created_at  TEXT     │  │    created_at    TEXT         │  │
│  │    source      TEXT     │  │    source        TEXT         │  │
│  │    ['manual'|'realtime']│  │    ['manual'|'realtime']     │  │
│  └─────────────────────────┘  └─────────────────────────────┘  │
│                                                                 │
│  ┌─────────────────────────┐                                    │
│  │   vd_positions           │                                    │
│  │─────────────────────────│                                    │
│  │ PK vd_id     TEXT       │                                    │
│  │    lat       REAL       │                                    │
│  │    lon       REAL       │                                    │
│  │    road_class TEXT      │                                    │
│  │    city      TEXT       │                                    │
│  └─────────────────────────┘                                    │
└─────────────────────────────────────────────────────────────────┘
```

### 8.2 表格詳細

| 表名 | 用途 | 量級 |
|------|------|------|
| `osm_nodes` | 路網節點 | ~376 萬 |
| `osm_edges` | 有向邊 + 動態欄位 | ~765 萬 |
| `dynamic_events` | 交通事件 | ~300 即時 + 手動 |
| `dynamic_weather` | 天氣狀況 | ~50-80 即時 + 手動 |
| `vd_positions` | VD 偵測器位置快取 | ~1,361 |

### 8.3 索引

| 索引名 | 表 | 欄位 | 用途 |
|--------|---|------|------|
| `idx_edges_latlon` | osm_edges | (lat_a, lon_a) | 動態成本空間查詢 |
| `idx_nodes_latlon` | osm_nodes | (lat, lon) | 最近節點查詢 |
| `idx_nodes_component` | osm_nodes | (component_id) | 分量路由篩選 |

---

## 9. 安全設計

### 9.1 已實作防護

| 類別 | 防護措施 |
|------|---------|
| **SQL 注入** | 所有查詢參數化 `?`。highway 白名單驗證 |
| **XSS** | `escHtml()` 消毒所有 `innerHTML` 動態內容 |
| **認證** | `require_admin` 依賴項掛載於所有寫入端點。`secrets.compare_digest()` 防時序攻擊 |
| **CORS** | 可設定允許來源。僅開放 GET/POST/DELETE 方法 |
| **錯誤洩漏** | 內部錯誤記錄於伺服器端；客戶端收到通用訊息。DB 路徑不暴露 |
| **並行** | 圖 RLock、快取 Lock、SQLite WAL + busy timeout |
| **輸入驗證** | 座標範圍、highway 白名單正規式、節點存在檢查 |

### 9.2 權限模型

```
┌──────────────┐     ┌──────────────────────────────────────────┐
│   公開使用者  │────►│ 導航、路由、讀取事件/天氣、地理編碼、統計  │
│ (Tunnel URL)  │     └──────────────────────────────────────────┘
└──────────────┘

┌──────────────┐     ┌──────────────────────────────────────────┐
│   管理員      │────►│ 所有公開功能 +                           │
│ (localhost    │     │ 事件/天氣 CRUD、同步觸發、成本重算、      │
│  或 Bearer)   │     │ 管理後台 UI                              │
└──────────────┘     └──────────────────────────────────────────┘
```

---

## 10. 效能最佳化

| 最佳化 | 細節 |
|--------|------|
| **namedtuple 邊** | 相比 dict 減少 ~60% 記憶體 |
| **游標迭代** | 圖載入時串流處理，非 `fetchall()` |
| **計數快取** | 背景執行緒快取 COUNT(*)，避免全表掃描 |
| **標的重算** | 僅重設有變更動態值的邊，非全圖 |
| **WAL checkpoint** | 重算後執行，防止 WAL 無限成長 |
| **VD 位置快取** | 靜態偵測器位置存 DB，避免重複 TDX API 呼叫 |
| **TDX 認證退避** | 認證失敗 5 分鐘冷卻，防 API 鎖定 |
| **地理編碼快取** | 500 筆 LRU，5 分鐘 TTL |
| **主圖預載** | motorway→tertiary 常駐記憶體；僅短程查全 DB |
| **號誌集合** | `is_signal` 節點預載為記憶體 set，A* 中 O(1) 查詢 |
| **時段因子快取** | 夜間風險和時段速度因子每次路由計算一次，非每邊 |
| **轉彎方位重用** | parent edge bearing 快取於 `came_from`，展開鄰居時免重算 |
| **端點局部載入** | 路由起終點 ±0.02° 載入全類型道路，解決高速公路出口問題 |

---

## 11. 部署方式

### 11.1 本機開發

```bash
uvicorn osm_api:app --host 127.0.0.1 --port 8000
```

### 11.2 本機 + 公開存取（Cloudflare Tunnel）

```bash
cloudflared tunnel --url http://127.0.0.1:8000
```

產生隨機 `*.trycloudflare.com` URL。無需帳號。管理員端點自動受保護（非 localhost → 需 token）。


---

## 12. 學術研究引用

### 天氣對行車的影響

| 來源 | 重點發現 |
|------|---------|
| FHWA / HCM Ch.11 | 降雨造成 2–17% 速度降低 |
| Iowa State (Agarwal 2005) | 小雨 3–5%、大雨 4–7% 降速 |
| MDPI Connected Vehicle (2023) | 極大雨平均 8.4% 降速 |
| ScienceDirect (號誌化路口) | 小/中/大雨容量損失 4.25%/9.18%/11.53% |
| FHWA | 能見度 <400m 造成 10–12% 降速 |
| HCM | 風 > 40 km/h 影響高車身車輛操控性 |

### 夜間行車

| 來源 | 重點發現 |
|------|---------|
| NHTSA 2008 | 夜間 25% 里程占 50% 死亡事故 |
| NSC | 夜間致死率 3× 日間 |
| NHTSA Glare Study | 深夜 0–4 時致死率 4–9× (含疲勞) |

### 轉彎延遲

| 來源 | 重點發現 |
|------|---------|
| McGill University | 轉彎占城市行程 15–25% |
| HCM | 號誌路口左轉 15–45 秒 |
| Transport Geography | 右轉 5–15 秒 |

### 時間相依路由

| 來源 | 重點發現 |
|------|---------|
| ResearchGate TDVRP | 可消除 99% 遲到 |
| ScienceDirect Torino | 尖峰城市速度降 30–50% |

### 號誌與容量

| 來源 | 重點發現 |
|------|---------|
| HCM Ch.19 | 每號誌 15–45 秒延遲 |
| ResearchGate | 號誌占城市行程 20–40% |
| EASTS | 車道數 vs 容量關係 |

### 山崩（台灣特定）

| 來源 | 重點發現 |
|------|---------|
| Tandfonline 2017 | 台灣降雨誘發山崩風險模型 |
| MDPI 2024 | 2019–2023 颱風山崩面積變化 |
| ScienceDirect 2025 | 颱風路線風險評估模型 |
| 水保局 | 累積雨量 200/350/600mm 三級閾值 |

### 完整參考文獻

1. [Analysis of Weather Impact on Travel Speed and Travel Time Reliability](https://www.researchgate.net/publication/268583170)
2. [Impact of Weather on Urban Freeway Traffic Flow (Iowa State)](https://www.intrans.iastate.edu/wp-content/uploads/2018/03/weather_impacts.pdf)
3. [Impact of Rain Intensity on Interstate Traffic Speeds (MDPI 2023)](https://www.mdpi.com/2624-8921/5/1/9)
4. [Capacity Loss Caused by Rainfall at Signalised Intersections](https://www.sciencedirect.com/org/science/article/pii/S1874447820000135)
5. [NHTSA Nighttime Glare and Driving Performance (2008)](https://www.nhtsa.gov/sites/nhtsa.gov/files/811043.pdf)
6. [National Safety Council — Driving at Night](https://www.nsc.org/road/safety-topics/driving-at-night)
7. [Intersection Turn Delay Modelling (McGill)](https://tram.mcgill.ca/Research/Publications/Turn%20Delay%20Modelling.pdf)
8. [Turn Penalties at Intersections (Transport Geography)](https://transportgeography.org/contents/methods/network-data-models/turn-penalty-intersection/)
9. [Vehicle Routing with Time-Dependent Travel Times (ResearchGate)](https://www.researchgate.net/publication/360332685)
10. [Time Dependent Travel Speed Routing: Torino (ScienceDirect)](https://www.sciencedirect.com/science/article/pii/S2352146514001872)
11. [Delay Function for Signalized Intersections (ResearchGate)](https://www.researchgate.net/publication/245292022)
12. [Highway Capacity Manual Edition 7.1 (2025)](https://nap.nationalacademies.org/resource/26432/Highway_Capacity_Manual_Edition_7.1_Chapters.pdf)
13. [Risk-based Landslide Monitoring in Taiwan (Tandfonline 2017)](https://www.tandfonline.com/doi/full/10.1080/19475705.2017.1345797)
14. [Landslide Responses to Typhoon Events 2019–2023 (MDPI)](https://www.mdpi.com/2071-1050/17/21/9673)
15. [Typhoon Route Risk Assessment Model (ScienceDirect 2025)](https://www.sciencedirect.com/science/article/pii/S2095263525001682)
16. [Weather and Road Safety: Kaohsiung, Taiwan](https://www.sciencedirect.com/science/article/pii/S2214367X23001618)
17. [Air Pollution and Weather vs Traffic Injury Severity in Taiwan (PMC 2022)](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC9223547/)
