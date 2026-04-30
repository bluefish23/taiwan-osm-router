# 台灣 OSM 動態路網導航系統 v2

## 架構

```
build_osm_graph.py   OSM PBF → SQLite（osm_nodes + osm_edges）
osm_router.py        A* 路由引擎（主幹預載 + 短程局部圖）
osm_api.py           FastAPI 後端（啟動時自動預載路網圖）
index.html           前端地圖介面（直接開啟即可使用）
```

## 為何改用 OSM PBF？

| 舊系統（CSV interchange）| 新系統（OSM PBF）|
|---|---|
| 只有交流道節點，路網稀疏 | 全台 376 萬節點、764 萬邊 |
| 跨路型需人工 connector | OSM node_id 天然交叉，自動互通 |
| 找不到一般道路 | 所有可駕駛道路類型完整收錄 |
| 無真實路名 | 完整路名（中英文）|

---

## 快速啟動

### 1. 安裝依賴
```bash
pip install fastapi uvicorn
```

### 2. 建立路網資料庫（已有 taiwan_osm.db 可跳過）
```bash
python build_osm_graph.py \
  --pbf taiwan-260419_osm.pbf \
  --db  taiwan_osm.db
# 約需 3 分鐘，產生 ~1.7 GB DB
```

### 3. 啟動後端 API
```bash
DB_PATH=taiwan_osm.db uvicorn osm_api:app --host 127.0.0.1 --port 8000
# 啟動時自動預載主幹圖，約需 15 秒，完成後顯示「就緒」

```

### 4. 開啟前端
直接在瀏覽器開啟 `index.html`，確認 API Base URL 為 `http://127.0.0.1:8000`。

# 解決前端403R(不直接開html file)
cd D:\python  #改你的工作區
python -m http.server 5500
http://127.0.0.1:5500/index.html
---

## API 端點

| 方法 | 路徑 | 說明 |
|------|------|------|
| GET  | `/stats` | 統計（含 graph_loaded 狀態）|
| GET  | `/nearest?lat=&lon=` | 最近路網節點 |
| GET  | `/events` | 列出活躍事件 |
| POST | `/events` | 新增事件（座標 + 半徑）|
| DELETE | `/events` | 清空所有事件/天氣 |
| DELETE | `/events/{id}` | 刪除單一事件 |
| POST | `/weather` | 新增天氣影響圈 |
| POST | `/dynamic/recompute` | 套用因子並重載圖（約 20s）|
| POST | `/route` | A* 路徑查詢 |

## 完整使用流程

```
1. 啟動 API → 等待「就緒」
2. 開啟 index.html
3. （可選）新增事件 / 天氣
4. （可選）點「重算 dynamic_cost」套用因子
5. 點地圖或輸入座標 → 查詢路徑
```

---

## 路由引擎設計

**路網載入策略**：
- **長程（> 15 km）**：使用啟動時預載的主幹圖（motorway → tertiary），常駐記憶體，查詢無需再讀 DB
- **短程（≤ 15 km）**：從 DB 局部載入完整路型（含 residential / service），確保巷道可用

**跨路型互通**：
- OSM 所有 way 共用同一個 node_id 做交叉路口，無需任何 connector 邏輯
- 高速公路 → 匝道 → 省道 → 市區道路，路徑自然銜接

**A* 啟發式**：
- 使用 haversine 直線距離除以最高速度（120 km/h）估算剩餘時間
- 保證找到最優解，速度比純 Dijkstra 快 3–10 倍

**動態因子**：
- 事件/天氣以地理圓範圍套用到 DB 的 `dynamic_mult` / `closure_flag` 欄位
- 執行 `/dynamic/recompute` 後會重新從 DB 載入圖，所有在途路徑自動反映

---

## 前端地圖顏色說明

| 顏色 | 道路類型 |
|------|----------|
| 藍色 | motorway 高速公路 |
| 綠色 | trunk 快速道路 |
| 橘色 | primary 主要道路 |
| 紫色 | secondary 次要道路 |
| 灰色 | tertiary 及其他 |

---

## DB 規格

| 項目 | 數值 |
|------|------|
| 檔案大小 | ~1.7 GB |
| osm_nodes | 3,760,969 |
| osm_edges | 7,647,722 |
| 資料來源 | taiwan-260419_osm.pbf（OpenStreetMap）|
| 建圖時間 | 約 3 分鐘 |

---

## 已修正的舊系統問題

| 問題 | 修正 |
|------|------|
| heapq TypeError（list[dict] 比較）| A* heap 加入 itertools.counter |
| /route 每次重算全圖 | 預載圖常駐記憶體，recompute 獨立觸發 |
| O(n²) connector 建置 | OSM 天然交叉，無需 connector |
| 路型孤島（省道無法接高速）| OSM 路口 node 共用，自然互通 |
| 憑證明文存放 | .env 管理，加入 .gitignore |
| TDX token 無 thread-lock | threading.Lock 保護 |
