"""Phase 1：在計畫書表二資料集上訓練 GCN 節點分類，驗證實作正確性。

超參數預設照計畫書 p.8：hidden 64、lr 0.001、Adam、epochs 50、early stopping。
（註：文獻上 Pubmed 常用 lr 0.01 / 200 epochs；可用 --lr/--epochs 覆寫比較。）

用法：
    python gcn/train_benchmark.py --dataset pubmed
    python gcn/train_benchmark.py --dataset ogbn-arxiv --layers 3
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np
import torch
import torch.nn.functional as F

from datasets import load
from gcn_layer import GCN, normalize_adjacency, sparse_to_torch

RESULTS_DIR = Path(__file__).parent / "results"


def accuracy(logits: torch.Tensor, labels: torch.Tensor, idx: torch.Tensor) -> float:
    pred = logits[idx].argmax(dim=1)
    return (pred == labels[idx]).float().mean().item()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="pubmed")
    ap.add_argument("--hidden", type=int, default=64)      # 計畫書 p.8
    ap.add_argument("--layers", type=int, default=2)        # 計畫書 p.8：2–3 層
    ap.add_argument("--lr", type=float, default=0.001)      # 計畫書 p.8
    ap.add_argument("--epochs", type=int, default=50)       # 計畫書 p.8
    ap.add_argument("--patience", type=int, default=10, help="early stopping 耐心值")
    ap.add_argument("--dropout", type=float, default=0.5)
    ap.add_argument("--weight-decay", type=float, default=5e-4)
    ap.add_argument("--cpu", action="store_true")
    args = ap.parse_args()

    device = torch.device("cpu" if args.cpu or not torch.cuda.is_available() else "cuda")
    print(f"device: {device}")

    adj, features, labels, idx_train, idx_val, idx_test = load(args.dataset)
    print(f"{args.dataset}: {adj.shape[0]:,} nodes, {adj.nnz:,} edges, "
          f"{features.shape[1]} features, {labels.max() + 1} classes")

    a_hat = sparse_to_torch(normalize_adjacency(adj), device)
    x = torch.from_numpy(features).to(device)
    y = torch.from_numpy(labels.astype(np.int64)).to(device)
    idx_train = torch.from_numpy(np.asarray(idx_train)).long().to(device)
    idx_val = torch.from_numpy(np.asarray(idx_val)).long().to(device)
    idx_test = torch.from_numpy(np.asarray(idx_test)).long().to(device)

    model = GCN(features.shape[1], args.hidden, int(labels.max()) + 1,
                num_layers=args.layers, dropout=args.dropout).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=args.lr,
                                 weight_decay=args.weight_decay)

    best_val, best_state, best_epoch, epochs_no_improve = 0.0, None, 0, 0
    epoch_times = []
    t_start = time.perf_counter()

    for epoch in range(1, args.epochs + 1):
        t0 = time.perf_counter()
        model.train()
        optimizer.zero_grad()
        logits = model(a_hat, x)
        loss = F.cross_entropy(logits[idx_train], y[idx_train])
        loss.backward()
        optimizer.step()
        epoch_times.append(time.perf_counter() - t0)

        model.eval()
        with torch.no_grad():
            logits = model(a_hat, x)
            val_acc = accuracy(logits, y, idx_val)

        if val_acc > best_val:
            best_val, best_epoch, epochs_no_improve = val_acc, epoch, 0
            best_state = {k: v.detach().clone() for k, v in model.state_dict().items()}
        else:
            epochs_no_improve += 1

        if epoch % 10 == 0 or epoch == 1:
            print(f"epoch {epoch:3d}  loss {loss.item():.4f}  val_acc {val_acc:.4f}")
        if epochs_no_improve >= args.patience:
            print(f"early stopping at epoch {epoch} (best val at {best_epoch})")
            break

    model.load_state_dict(best_state)
    model.eval()
    with torch.no_grad():
        test_acc = accuracy(model(a_hat, x), y, idx_test)

    total = time.perf_counter() - t_start
    result = {
        "dataset": args.dataset,
        "config": vars(args) | {"device": str(device)},
        "test_accuracy": round(test_acc, 4),
        "best_val_accuracy": round(best_val, 4),
        "best_epoch": best_epoch,
        "total_train_seconds": round(total, 2),
        "mean_epoch_seconds": round(float(np.mean(epoch_times)), 4),
        "spmm_seconds_total": round(model.spmm_seconds, 4),
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))

    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"benchmark_{args.dataset}_L{args.layers}_lr{args.lr}.json"
    out.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"saved -> {out}")


if __name__ == "__main__":
    main()
