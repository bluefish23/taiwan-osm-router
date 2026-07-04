"""載入計畫書表二的圖資料集。

- Pubmed（19,717 節點 / 99K 邊）：Planetoid 格式，自動從 GitHub 下載。
- ogbn-arxiv（169K 節點 / 1.17M 邊）：透過 ogb 套件下載（表二的 Arxiv）。

回傳統一格式：(adj: scipy.sparse, features: np.ndarray, labels: np.ndarray,
              idx_train, idx_val, idx_test)
"""
from __future__ import annotations

import os
import pickle
import sys
import urllib.request
from pathlib import Path

import numpy as np
import scipy.sparse as sp

DATA_DIR = Path(__file__).parent / "data"

PLANETOID_URL = "https://github.com/kimiyoung/planetoid/raw/master/data"
PLANETOID_FILES = ["x", "y", "tx", "ty", "allx", "ally", "graph", "test.index"]


def _download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.exists():
        print(f"downloading {url}")
        urllib.request.urlretrieve(url, dest)


def load_pubmed():
    """Planetoid 格式 Pubmed，切分沿用 Kipf & Welling (2016) 的標準切分。"""
    name = "pubmed"
    objects = {}
    for suffix in PLANETOID_FILES:
        dest = DATA_DIR / "planetoid" / f"ind.{name}.{suffix}"
        _download(f"{PLANETOID_URL}/ind.{name}.{suffix}", dest)
        if suffix == "test.index":
            objects[suffix] = np.loadtxt(dest, dtype=int)
        else:
            with open(dest, "rb") as f:
                objects[suffix] = pickle.load(f, encoding="latin1")

    x, y, tx, ty, allx, ally, graph = (objects[k] for k in
                                       ["x", "y", "tx", "ty", "allx", "ally", "graph"])
    test_idx = objects["test.index"]
    test_idx_sorted = np.sort(test_idx)

    features = sp.vstack((allx, tx)).tolil()
    features[test_idx, :] = features[test_idx_sorted, :]
    features = np.asarray(features.todense(), dtype=np.float32)
    # row-normalize features（GCN 慣例）
    rowsum = features.sum(axis=1, keepdims=True)
    rowsum[rowsum == 0] = 1.0
    features = features / rowsum

    labels_onehot = np.vstack((ally, ty))
    labels_onehot[test_idx, :] = labels_onehot[test_idx_sorted, :]
    labels = labels_onehot.argmax(axis=1)

    n = labels.shape[0]
    row, col = [], []
    for src, neighbors in graph.items():
        for dst in neighbors:
            row.append(src)
            col.append(dst)
    adj = sp.coo_matrix((np.ones(len(row)), (row, col)), shape=(n, n))

    idx_train = np.arange(len(y))
    idx_val = np.arange(len(y), len(y) + 500)
    idx_test = test_idx_sorted
    return adj, features, labels, idx_train, idx_val, idx_test


def load_ogbn_arxiv():
    """表二的 Arxiv（ogbn-arxiv），用 ogb 的框架無關載入器。"""
    import torch
    from ogb.nodeproppred import NodePropPredDataset

    # ogb 的預處理快取用舊式 pickle，PyTorch>=2.6 預設 weights_only=True 會拒載；
    # 快取是本機自己產生的，暫時放寬後還原
    _orig_load = torch.load
    torch.load = lambda *a, **k: _orig_load(*a, **{**k, "weights_only": False})
    try:
        dataset = NodePropPredDataset(name="ogbn-arxiv", root=str(DATA_DIR / "ogb"))
    finally:
        torch.load = _orig_load
    split = dataset.get_idx_split()
    graph, labels = dataset[0]
    n = graph["num_nodes"]
    src, dst = graph["edge_index"]
    adj = sp.coo_matrix((np.ones(src.shape[0]), (src, dst)), shape=(n, n))
    features = graph["node_feat"].astype(np.float32)
    labels = labels.flatten()
    return (adj, features, labels,
            split["train"], split["valid"], split["test"])


LOADERS = {
    "pubmed": load_pubmed,
    "ogbn-arxiv": load_ogbn_arxiv,
}


def load(name: str):
    if name not in LOADERS:
        sys.exit(f"unknown dataset {name!r}; available: {list(LOADERS)}")
    return LOADERS[name]()
