"""GCN 基礎實作（純 PyTorch，不依賴 PyTorch Geometric）。

對應計畫書 p.5 的公式：每層計算 A_hat × X × W，其中
A_hat = D^(-1/2) (A + I) D^(-1/2) 為正規化相鄰矩陣。

乘法順序刻意採用計畫書結論：先算 X × W（稠密小矩陣），
再算 A_hat × (XW)（稀疏 × 稠密），避免直接對稀疏大矩陣做兩次乘法。
"""
from __future__ import annotations

import time

import numpy as np
import scipy.sparse as sp
import torch
import torch.nn as nn
import torch.nn.functional as F


def normalize_adjacency(adj: sp.spmatrix) -> sp.coo_matrix:
    """A_hat = D^(-1/2) (A + I) D^(-1/2)，計畫書 p.5 的 normalized adjacency matrix。"""
    adj = sp.coo_matrix(adj)
    # 無向化（引用網路等資料集給的是有向邊，GCN 慣例做對稱化）
    adj = adj + adj.T.multiply(adj.T > adj) - adj.multiply(adj.T > adj)
    adj = adj + sp.eye(adj.shape[0], format="coo")
    deg = np.asarray(adj.sum(axis=1)).flatten()
    d_inv_sqrt = np.power(deg, -0.5, where=deg > 0)
    d_inv_sqrt[deg == 0] = 0.0
    d_mat = sp.diags(d_inv_sqrt)
    return sp.coo_matrix(d_mat @ adj @ d_mat)


def sparse_to_torch(adj: sp.coo_matrix, device: torch.device) -> torch.Tensor:
    adj = adj.tocoo().astype(np.float32)
    indices = torch.from_numpy(np.vstack([adj.row, adj.col])).long()
    values = torch.from_numpy(adj.data)
    return torch.sparse_coo_tensor(indices, values, adj.shape, device=device).coalesce()


class GCNLayer(nn.Module):
    """單層圖卷積：A_hat · (X · W) + b"""

    def __init__(self, in_dim: int, out_dim: int):
        super().__init__()
        self.weight = nn.Parameter(torch.empty(in_dim, out_dim))
        self.bias = nn.Parameter(torch.zeros(out_dim))
        nn.init.xavier_uniform_(self.weight)
        # 累計本層 spmm（稀疏×稠密）耗時，供 Phase 3 稀疏乘法實驗當 baseline
        self.spmm_seconds = 0.0

    def forward(self, a_hat: torch.Tensor, x: torch.Tensor) -> torch.Tensor:
        support = x @ self.weight          # X · W（先算稠密小矩陣，計畫書 p.5 順序）
        t0 = time.perf_counter()
        out = torch.sparse.mm(a_hat, support)  # A_hat · (XW)
        if out.is_cuda:
            torch.cuda.synchronize()
        self.spmm_seconds += time.perf_counter() - t0
        return out + self.bias


class GCN(nn.Module):
    """多層 GCN。計畫書 p.8：層數 2–3、隱藏維度 64、ReLU。"""

    def __init__(self, in_dim: int, hidden_dim: int, out_dim: int,
                 num_layers: int = 2, dropout: float = 0.5):
        super().__init__()
        dims = [in_dim] + [hidden_dim] * (num_layers - 1) + [out_dim]
        self.layers = nn.ModuleList(GCNLayer(dims[i], dims[i + 1]) for i in range(num_layers))
        self.dropout = dropout

    def forward(self, a_hat: torch.Tensor, x: torch.Tensor) -> torch.Tensor:
        for i, layer in enumerate(self.layers):
            x = layer(a_hat, x)
            if i < len(self.layers) - 1:
                x = F.relu(x)
                x = F.dropout(x, p=self.dropout, training=self.training)
        return x

    @property
    def spmm_seconds(self) -> float:
        return sum(layer.spmm_seconds for layer in self.layers)
