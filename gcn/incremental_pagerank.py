"""增量 PageRank 與直接相依關係（計畫書 p.2、p.4–5）。

三種計算模式，供 benchmark 比較：

1. `pagerank_full`：全圖冪迭代（傳統做法，baseline）。
2. `LocalPushPageRank`：增量更新——某節點狀態改變時，只沿受影響的
   傳播鏈局部推送（push）殘差，不重算全圖（對應計畫書「只有有狀態
   更新節點的子圖需要處理」）。
3. `DirectDependencyIndex`：核心節點間的直接相依——因為 PageRank 對
   來源向量是線性的，核心節點 i 的變化量對核心節點 j 的影響可表示為
   線性關係 Δr_j = w_ij · Δr_i（即計畫書 p.5 的 α, β 線性表示）。
   首次需要時才計算（flag N→A，計畫書 p.4 的 Y/N/A 標誌機制），
   之後直接查表，讓下游子圖不必等上游傳播鏈逐節點跑完。
"""
from __future__ import annotations

import numpy as np
import scipy.sparse as sp

DAMPING = 0.85
TOL = 1e-10


def _out_normalized(adj: sp.spmatrix) -> sp.csr_matrix:
    """列正規化的轉移矩陣 P（row-stochastic；dangling 節點列為 0）。"""
    csr = sp.csr_matrix(adj, dtype=np.float64)
    out_deg = np.asarray(csr.sum(axis=1)).flatten()
    inv = np.divide(1.0, out_deg, out=np.zeros_like(out_deg), where=out_deg > 0)
    return sp.diags(inv) @ csr


def pagerank_full(adj: sp.spmatrix, personalization: np.ndarray | None = None,
                  damping: float = DAMPING, tol: float = TOL,
                  max_iter: int = 200) -> tuple[np.ndarray, int]:
    """全圖冪迭代 PageRank。回傳 (r, 實際觸碰的節點更新總數)。"""
    n = adj.shape[0]
    p = _out_normalized(adj)
    s = (np.ones(n) / n) if personalization is None else personalization / personalization.sum()
    r = s.copy()
    touched = 0
    for _ in range(max_iter):
        r_new = (1 - damping) * s + damping * (p.T @ r)
        touched += n
        if np.abs(r_new - r).sum() < tol:
            r = r_new
            break
        r = r_new
    return r, touched


class LocalPushPageRank:
    """殘差推送（Gauss–Southwell）式增量 PageRank。

    維護解 r 與殘差 res，滿足不變式 r + f(res) = 真實 PageRank。
    來源向量在某節點的變化只需把 delta 加進殘差，然後沿傳播鏈
    局部推送直到殘差夠小——更新的節點數正比於受影響範圍，
    而非全圖大小。
    """

    def __init__(self, adj: sp.spmatrix, damping: float = DAMPING, tol: float = TOL):
        self.pt = sp.csr_matrix(_out_normalized(adj).T)   # P^T：row v = 指向 v 的來源
        self.n = adj.shape[0]
        self.damping = damping
        self.tol = tol
        s = np.ones(self.n) / self.n
        self.r = np.zeros(self.n)
        self.res = (1 - damping) * s
        self.touched = 0
        self._push_all()

    def _push_all(self) -> None:
        """批次推送殘差直到收斂（向量化的 Gauss–Jacobi push）。

        每輪只處理殘差超過門檻的「活躍節點」，其餘節點完全不動——
        更新量正比於受影響的傳播鏈範圍，而非全圖大小。
        """
        while True:
            active = np.abs(self.res) > self.tol
            n_active = int(active.sum())
            if n_active == 0:
                break
            delta = np.where(active, self.res, 0.0)
            self.res[active] = 0.0
            self.r += delta
            self.touched += n_active
            # 把 damping·delta 沿出邊分給鄰居（P^T @ delta 一次算完整批）
            self.res += self.damping * (self.pt @ delta)

    def update_source(self, node: int, delta: float) -> None:
        """來源向量（節點狀態）在 node 改變 delta，增量修正 PageRank。"""
        self.res[node] += (1 - self.damping) * delta
        self._push_all()


class DirectDependencyIndex:
    """核心節點間的直接相依索引（計畫書 p.4–5）。

    influence(i, j)：來源在核心 i 的單位變化，對核心 j 的 PageRank 影響係數。
    PageRank 線性 → 係數只需算一次；flags 記錄 'N'（未算）/'A'（已算可用）。
    索引跨查詢重複使用（計畫書 p.3：索引在記憶體中維護重用，不重新生成）。
    """

    def __init__(self, adj: sp.spmatrix, cores: np.ndarray,
                 damping: float = DAMPING, tol: float = TOL):
        self.adj = adj
        self.cores = np.asarray(cores)
        self.damping = damping
        self.tol = tol
        self.index: dict[int, np.ndarray] = {}   # core i -> 對全部節點的影響向量
        self.flags: dict[int, str] = {int(c): "N" for c in self.cores}
        self.compute_count = 0

    def influence_vector(self, core: int) -> np.ndarray:
        """核心節點 core 的單位來源變化對所有節點的影響（含 α,β 線性關係）。"""
        core = int(core)
        if self.flags.get(core) != "A":
            # 解 (I - d·P^T) x = (1-d)·e_core —— 即單位 delta 的完整傳播結果
            if not hasattr(self, "_pt"):
                self._pt = sp.csr_matrix(_out_normalized(self.adj).T)
            n = self.adj.shape[0]
            x = np.zeros(n)
            res = np.zeros(n)
            res[core] = 1.0 - self.damping
            while True:
                active = np.abs(res) > self.tol
                if not active.any():
                    break
                delta = np.where(active, res, 0.0)
                res[active] = 0.0
                x += delta
                res += self.damping * (self._pt @ delta)
            self.index[core] = x
            self.flags[core] = "A"
            self.compute_count += 1
        return self.index[core]

    def apply_update(self, r: np.ndarray, core: int, delta: float) -> np.ndarray:
        """r 直接加上 core 變化 delta 的影響——一步到位，不沿傳播鏈逐點更新。"""
        return r + delta * self.influence_vector(core)


if __name__ == "__main__":
    from subgraph_partition import find_core_nodes, proposal_figure_graph

    adj = proposal_figure_graph()
    r_full, _ = pagerank_full(adj)

    lp = LocalPushPageRank(adj)
    err = np.abs(lp.r - r_full).sum()
    print(f"local push vs full: L1 err = {err:.2e}  (應 < 1e-6)")

    # 增量更新一致性：來源在 v5 改變 0.01
    n = adj.shape[0]
    s = np.ones(n) / n
    s2 = s.copy()
    s2[5] += 0.01
    r_ref, _ = pagerank_full(adj, personalization=s2 * n)
    lp.update_source(5, 0.01)
    # local push 的來源未重新正規化，直接比對縮放後結果
    err2 = np.abs(lp.r / lp.r.sum() - r_ref / r_ref.sum()).sum()
    print(f"incremental vs full-recompute: L1 err = {err2:.2e}  (應 < 1e-6)")

    idx = DirectDependencyIndex(adj, find_core_nodes(adj))
    v = idx.influence_vector(5)
    print(f"influence(v5) 前 8 節點: {np.round(v[:8], 5)}  flag={idx.flags[5]}")
