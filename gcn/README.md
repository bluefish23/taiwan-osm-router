# GCN 加速與交通時空預測實作

實作教授計畫書（表 CM03「圖卷積神經網路加速」）的軟體部分，
包含四個實驗，程式與計畫書章節的對應如下。

## 環境

```bash
python -m venv .venv
.venv/Scripts/pip install torch --index-url https://download.pytorch.org/whl/cu128
.venv/Scripts/pip install numpy scipy pandas matplotlib ogb
```

## Phase 1：GCN 基礎 + 表二資料集驗證（計畫書 p.5 公式、p.8 參數、p.9 表二）

```bash
python gcn/train_benchmark.py --dataset pubmed                # 計畫書參數
python gcn/train_benchmark.py --dataset pubmed --lr 0.01 --epochs 200 --patience 30
python gcn/train_benchmark.py --dataset ogbn-arxiv --layers 3 --lr 0.01 --epochs 300 --patience 50 --dropout 0.3
```

- `gcn_layer.py`：Â = D^(-1/2)(A+I)D^(-1/2)、GCN 層（Â·(X·W)，採計畫書 p.5 的乘法順序）
- `datasets.py`：Pubmed（Planetoid）、ogbn-arxiv（表二的 Arxiv）自動下載
- 結果：Pubmed test acc **79.8%**（文獻 ~79%）、ogbn-arxiv **67.4%**（hidden 64 設定下合理）
  → GCN 實作正確性驗證通過

## Phase 2：核心節點子圖分割 + 增量 PageRank + 直接相依（計畫書 p.1–2、4–5）

```bash
python gcn/subgraph_partition.py        # 重現計畫書 p.1 圖一 21 節點範例
python gcn/incremental_pagerank.py      # 正確性自我檢查
python gcn/bench_propagation.py --dataset pubmed --updates 10
python gcn/bench_propagation.py --dataset ogbn-arxiv --updates 10
```

- `subgraph_partition.py`：out-degree > 1 → 核心節點，DFS 切子圖。
  21 節點範例的核心節點 {v5,v7,v13,v15,v16} 與計畫書 p.2 一致
- `incremental_pagerank.py`：
  - `LocalPushPageRank`：殘差推送式增量更新（只更新受影響傳播鏈）
  - `DirectDependencyIndex`：核心節點間線性影響係數（α,β）+ N/A flag 快取
- ogbn-arxiv（169K 節點）實測：

| 方法 | 單次更新耗時 | 更新節點數 |
|---|---|---|
| 全圖重算（傳統） | 703 ms | 19,101,890 |
| 增量（局部推送） | 293 ms | **10,155** |
| 直接相依 首次（flag N→A） | 431 ms | — |
| 直接相依 快取命中（flag A） | **0.41 ms** | — |

增量結果與全圖重算誤差 < 1e-5（PageRank 值 L1）。

## Phase 3：稀疏矩陣乘法優化（計畫書 p.5–6、p.9）

```bash
python gcn/spmm_experiments.py
```

- 乘法順序：Â·(X·W) 比 (Â·X)·W 實測快 2.3×，理論 FLOPs 少 33×（p.5 論點成立）
- 動態剪裁零元素：density 0.001 時稀疏乘法快 22×；density > ~5% 時 dense 反而較快
  （交叉點分析，report 討論用）
- 圖表：`results/spmm_experiments.png`

## Phase 4：GCN+GRU 交通時空預測（計畫書 p.7–8 實驗章節）

```bash
python gcn/traffic/download_m05a.py --start 20260510 --end 20260530
python gcn/traffic/train_traffic.py --model tgcn   # GCN+GRU
python gcn/traffic/train_traffic.py --model gru    # ablation：無 GCN
```

- 資料：高公局 TDCS M05A，國道一號主線門架路段 5 分鐘平均速率，
  21 天 × 148 路段（6048 時間步）
- 圖：路段迄門架 = 下一路段起門架 → 建邊（行車方向鏈）
- 任務：過去 60 分鐘 → 預測未來 5/15/30 分鐘車速；MAE/RMSE
- Baseline：naive（上一時刻）、HA（一週時間槽歷史平均）、GRU（無空間資訊）
- 異常偵測：預測殘差 z-score > 3 標記（precision/recall 待以 TDX 事件標記對齊）
- 結果：`results/traffic_*.json`、示意圖 `results/traffic_pred_*.png`

## 與計畫書的差異／備註

- 計畫書 p.2 範例對邊界鏈歸屬（v8,v10）前後不一致，本實作採一致的
  「上游核心 DFS 認領」規則，詳見 `subgraph_partition.py` docstring
- DDA/DFA 硬體加速器（Verilog）不在本次範圍，以上皆為軟體模擬對應
- 能量量測（p.9）以執行時間為代理指標
