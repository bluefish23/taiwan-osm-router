"""T-GCN 式時空模型（計畫書 p.7：圖卷積層 + 時間序列建模）。

每個時間步先用 GCN 聚合空間鄰居資訊，再把每個節點的隱藏序列
餵進 GRU 捕捉時間相依性，最後預測未來多個時間點的車速。

對照組 NodeGRU：同架構但拿掉 GCN（無空間資訊），
用來證明圖結構的貢獻（報告的 ablation）。
"""
from __future__ import annotations

import sys
from pathlib import Path

import torch
import torch.nn as nn

_GCN_DIR = str(Path(__file__).resolve().parent.parent)
if _GCN_DIR not in sys.path:
    sys.path.insert(0, _GCN_DIR)
from gcn_layer import GCNLayer  # noqa: E402


class TGCN(nn.Module):
    """GCN（空間）+ GRU（時間）→ 多步車速預測。

    輸入 x: (B, T, N, F)；輸出: (B, N, H)，H = len(horizons)。
    空間聚合把批次維攤平成單次稀疏乘法：對每層權重 W，
    A_hat·(X·W) 以 (N, B·T·hidden) 形狀一次算完（數學上等價於
    對 B·T 個樣本逐一做 GCN，但快得多）。
    """

    def __init__(self, num_features: int, hidden: int = 64,
                 num_horizons: int = 3, gcn_layers: int = 2):
        super().__init__()
        dims = [num_features] + [hidden] * gcn_layers
        self.gcn = nn.ModuleList(GCNLayer(dims[i], dims[i + 1])
                                 for i in range(gcn_layers))
        # 殘差路徑：保留節點自身特徵，避免空間聚合模糊各路段的時間模式
        self.skip = nn.Linear(num_features, hidden)
        self.gru = nn.GRU(hidden, hidden, batch_first=True)
        self.head = nn.Linear(hidden, num_horizons)

    def forward(self, a_hat: torch.Tensor, x: torch.Tensor) -> torch.Tensor:
        b, t, n, f = x.shape
        h = x.reshape(b * t, n, f)
        for layer in self.gcn:
            support = h @ layer.weight                        # X·W（計畫書 p.5 順序）
            s2 = support.permute(1, 0, 2).reshape(n, -1)      # (N, B·T·hidden)
            out = torch.sparse.mm(a_hat, s2)                  # A_hat·(XW)
            h = out.reshape(n, b * t, -1).permute(1, 0, 2) + layer.bias
            h = torch.relu(h)
        h = h + self.skip(x.reshape(b * t, n, f))             # 殘差
        h = h.reshape(b, t, n, -1).permute(0, 2, 1, 3).reshape(b * n, t, -1)
        _, h_last = self.gru(h)                               # (1, B·N, hidden)
        out = self.head(h_last.squeeze(0))                    # (B·N, H)
        return out.reshape(b, n, -1)


class NodeGRU(nn.Module):
    """Ablation baseline：無 GCN，各節點獨立 GRU（只看自己的歷史）。"""

    def __init__(self, num_features: int, hidden: int = 64, num_horizons: int = 3):
        super().__init__()
        self.proj = nn.Linear(num_features, hidden)
        self.gru = nn.GRU(hidden, hidden, batch_first=True)
        self.head = nn.Linear(hidden, num_horizons)

    def forward(self, a_hat: torch.Tensor, x: torch.Tensor) -> torch.Tensor:
        b, t, n, f = x.shape
        h = torch.relu(self.proj(x))                          # (B, T, N, hidden)
        h = h.permute(0, 2, 1, 3).reshape(b * n, t, -1)
        _, h_last = self.gru(h)
        return self.head(h_last.squeeze(0)).reshape(b, n, -1)
