"""Phase 3：稀疏矩陣乘法優化實驗（計畫書 p.5–6、p.9）。

實驗一（p.5 乘法順序）：比較 (A_hat·X)·W 與 A_hat·(X·W) 的理論 FLOPs 與實測時間。
    A_hat: n×n 稀疏、X: n×f 稀疏（GCN 初始特徵多為零）、W: f×h 稠密小矩陣。
    先算 X·W 可把後續運算縮到 n×h（h << f），大幅減少計算量。

實驗二（p.6 動態剪裁零元素）：dense 乘法 vs CSR 稀疏乘法（只算非零），
    在不同稀疏度（zero 比例）與不同矩陣大小下實測執行時間。

實驗三（p.9）：相同稀疏度下，矩陣大小對執行時間的影響。

輸出：results/spmm_results.json 與 results/spmm_*.png 圖表。
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import numpy as np
import scipy.sparse as sp

RESULTS_DIR = Path(__file__).parent / "results"
RNG = np.random.default_rng(0)


def timed(fn, repeat: int = 5) -> float:
    fn()  # warmup
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return float(np.median(times))


def rand_sparse(n_rows: int, n_cols: int, density: float) -> sp.csr_matrix:
    return sp.random(n_rows, n_cols, density=density, format="csr",
                     random_state=42, dtype=np.float64)


def experiment_order(n: int = 20000, f: int = 500, h: int = 64,
                     adj_density: float = 5e-4, x_density: float = 0.01) -> dict:
    """實驗一：乘法順序。"""
    a_hat = rand_sparse(n, n, adj_density)
    x = rand_sparse(n, f, x_density)
    w = RNG.standard_normal((f, h))

    t_ax_w = timed(lambda: (a_hat @ x) @ w)     # (A·X)·W
    t_a_xw = timed(lambda: a_hat @ (x @ w))     # A·(X·W)

    # 理論 FLOPs（稀疏乘法以非零元素計）
    nnz_a, nnz_x = a_hat.nnz, x.nnz
    flops_ax = 2 * nnz_a * (nnz_x / n)          # A·X 每列平均非零
    flops_axw = flops_ax + 2 * n * f * h        # 再乘 W（AX 已趨稠密, n×f）
    flops_xw = 2 * nnz_x * h                    # X·W（稀疏×稠密）
    flops_a_xw = flops_xw + 2 * nnz_a * h       # 再 A·(XW)

    r = {"n": n, "f": f, "h": h,
         "time_(AX)W": round(t_ax_w, 4), "time_A(XW)": round(t_a_xw, 4),
         "speedup": round(t_ax_w / t_a_xw, 2),
         "flops_(AX)W": int(flops_axw), "flops_A(XW)": int(flops_a_xw)}
    print("實驗一 乘法順序:", json.dumps(r, ensure_ascii=False))
    return r


def experiment_sparsity(n: int = 4000) -> list[dict]:
    """實驗二：固定大小，掃描稀疏度，dense vs sparse（動態剪裁零元素）。"""
    rows = []
    dense_b = RNG.standard_normal((n, 64))
    for density in [0.5, 0.2, 0.1, 0.05, 0.01, 0.005, 0.001]:
        a_sp = rand_sparse(n, n, density)
        a_dn = a_sp.toarray()
        t_dense = timed(lambda: a_dn @ dense_b, repeat=3)
        t_sparse = timed(lambda: a_sp @ dense_b, repeat=3)
        rows.append({"n": n, "density": density,
                     "time_dense": round(t_dense, 5),
                     "time_sparse_pruned": round(t_sparse, 5),
                     "speedup": round(t_dense / t_sparse, 2)})
        print(f"實驗二 density={density:<6} dense={t_dense:.4f}s "
              f"sparse={t_sparse:.4f}s speedup={t_dense/t_sparse:.1f}x")
    return rows


def experiment_size(density: float = 0.01) -> list[dict]:
    """實驗三：固定稀疏度，掃描矩陣大小。"""
    rows = []
    for n in [1000, 2000, 4000, 8000, 16000]:
        a_sp = rand_sparse(n, n, density)
        b = RNG.standard_normal((n, 64))
        a_dn = a_sp.toarray()
        t_dense = timed(lambda: a_dn @ b, repeat=3)
        t_sparse = timed(lambda: a_sp @ b, repeat=3)
        rows.append({"density": density, "n": n,
                     "time_dense": round(t_dense, 5),
                     "time_sparse_pruned": round(t_sparse, 5)})
        print(f"實驗三 n={n:<6} dense={t_dense:.4f}s sparse={t_sparse:.4f}s")
    return rows


def plot(results: dict) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    r2 = results["sparsity_sweep"]
    d = [row["density"] for row in r2]
    axes[0].loglog(d, [row["time_dense"] for row in r2], "o-", label="dense")
    axes[0].loglog(d, [row["time_sparse_pruned"] for row in r2], "s-",
                   label="sparse (zero-pruned)")
    axes[0].set_xlabel("density (nnz ratio)")
    axes[0].set_ylabel("time (s)")
    axes[0].set_title(f"Sparsity sweep (n={r2[0]['n']})")
    axes[0].legend(); axes[0].grid(True, which="both", alpha=0.3)

    r3 = results["size_sweep"]
    ns = [row["n"] for row in r3]
    axes[1].loglog(ns, [row["time_dense"] for row in r3], "o-", label="dense")
    axes[1].loglog(ns, [row["time_sparse_pruned"] for row in r3], "s-",
                   label="sparse (zero-pruned)")
    axes[1].set_xlabel("matrix size n")
    axes[1].set_ylabel("time (s)")
    axes[1].set_title(f"Size sweep (density={r3[0]['density']})")
    axes[1].legend(); axes[1].grid(True, which="both", alpha=0.3)

    fig.tight_layout()
    out = RESULTS_DIR / "spmm_experiments.png"
    fig.savefig(out, dpi=150)
    print(f"plot -> {out}")


if __name__ == "__main__":
    RESULTS_DIR.mkdir(exist_ok=True)
    results = {
        "order_experiment": experiment_order(),
        "sparsity_sweep": experiment_sparsity(),
        "size_sweep": experiment_size(),
    }
    out = RESULTS_DIR / "spmm_results.json"
    out.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"saved -> {out}")
    plot(results)
