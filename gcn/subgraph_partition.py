"""計畫書 p.1–2 的子圖分割：以 out-degree > 1 的核心節點（core node）切割圖。

規則（本實作採用的確定性解讀）：
1. 核心節點 = out-degree > 1 的節點（計畫書 p.2）。
2. 每個核心節點各自擁有一個子圖（n 個核心節點 → n 個子圖）。
3. 從每個核心節點做 DFS 往外認領「尚未被認領的非核心節點」，
   走到其他核心節點就停（該核心節點屬於它自己的子圖）。
   多個核心都能到達的節點，由編號較小的核心先認領（確定性）。
4. 沒被任何核心認領的節點（例如源頭鏈）併入它第一個到達的核心的子圖；
   完全碰不到核心的孤立部分自成一個子圖。

註：計畫書 p.2 的範例（圖二）對邊界鏈的歸屬前後不一致——
v9,v12（v7 下游）歸上游的 G2，但 v8,v10（v5 下游）卻歸下游核心 v13 的 G3。
本實作選擇一致的「上游核心認領」規則，邊界鏈歸屬可能與計畫書範例
在 v8,v10 這類節點上不同，其餘分割結果一致。
"""
from __future__ import annotations

from collections import defaultdict

import numpy as np
import scipy.sparse as sp


def find_core_nodes(adj: sp.spmatrix) -> np.ndarray:
    """回傳 out-degree > 1 的節點編號（列 = 來源節點）。"""
    out_deg = np.asarray((sp.csr_matrix(adj) != 0).sum(axis=1)).flatten()
    return np.where(out_deg > 1)[0]


def partition(adj: sp.spmatrix) -> tuple[dict[int, list[int]], np.ndarray]:
    """切割圖，回傳 (subgraphs, owner)。

    subgraphs: {核心節點 id: 該子圖的節點列表（含核心節點自己）}
    owner:     長度 n 的陣列，owner[v] = v 所屬子圖的核心節點 id（-1 = 未指派）
    """
    csr = sp.csr_matrix(adj)
    n = csr.shape[0]
    cores = find_core_nodes(csr)
    core_set = set(cores.tolist())

    owner = np.full(n, -1, dtype=np.int64)
    subgraphs: dict[int, list[int]] = {int(c): [int(c)] for c in cores}
    for c in cores:
        owner[c] = c

    # 步驟 3：每個核心 DFS 認領非核心節點，遇到其他核心即停
    for c in cores:
        stack = list(csr.indices[csr.indptr[c]:csr.indptr[c + 1]])
        while stack:
            v = stack.pop()
            if owner[v] != -1 or v in core_set:
                continue
            owner[v] = c
            subgraphs[int(c)].append(int(v))
            stack.extend(csr.indices[csr.indptr[v]:csr.indptr[v + 1]])

    # 步驟 4：未指派節點（源頭鏈等）→ 沿出邊找到第一個已指派節點，併入其子圖
    for v in np.where(owner == -1)[0]:
        cur, seen = int(v), set()
        chain = []
        while owner[cur] == -1 and cur not in seen:
            seen.add(cur)
            chain.append(cur)
            nxt = csr.indices[csr.indptr[cur]:csr.indptr[cur + 1]]
            if len(nxt) == 0:
                break
            cur = int(nxt[0])
        target = int(owner[cur]) if owner[cur] != -1 else chain[0]
        if target not in subgraphs:          # 孤立部分自成子圖
            subgraphs[target] = []
        for u in chain:
            if owner[u] == -1:
                owner[u] = target
                subgraphs[target].append(u)

    return subgraphs, owner


def proposal_figure_graph() -> sp.coo_matrix:
    """重建計畫書 p.1 圖一的 21 節點範例圖（邊方向依 p.1–2 文字敘述推定）。

    已知傳播鏈（p.1）：v5→v7→v9→v12→v15、v5→v8→v10→v13→v15、
    v16→v18→v19→v20、v16→v17；核心節點（p.2）：v5, v7, v13, v15, v16。
    """
    edges = [
        (5, 1), (5, 2), (5, 3), (5, 6), (5, 7), (5, 8),   # v5 為核心（分支度 6）
        (2, 0),                                            # v0 經 v2 掛在 G1
        (7, 4), (7, 9), (9, 12), (12, 15),                 # v7 核心，G2 鏈
        (8, 10), (10, 13),                                 # v5→v8→v10→v13 傳播鏈
        (13, 11), (13, 14), (13, 15),                      # v13 核心
        (15, 16), (15, 13),                                # v15 核心（含回邊使 out-deg>1）
        (16, 17), (16, 18), (18, 19), (19, 20),            # v16 核心，G5
    ]
    row, col = zip(*edges)
    return sp.coo_matrix((np.ones(len(edges)), (row, col)), shape=(21, 21))


if __name__ == "__main__":
    adj = proposal_figure_graph()
    cores = find_core_nodes(adj)
    subgraphs, owner = partition(adj)
    print(f"核心節點（out-degree > 1）: {sorted(cores.tolist())}")
    print("（計畫書 p.2 預期：[5, 7, 13, 15, 16]）")
    for c in sorted(subgraphs):
        print(f"  G(core={c}): {sorted(subgraphs[c])}")
