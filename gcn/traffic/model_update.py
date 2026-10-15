"""持續更新預測模型：累積新資料 → 重新訓練 → 用最新幾天驗證 → 比原本準才換上。

流程（每次執行）：
  1. 補齊歷史資料：從最後一天之後，逐日下載高公局 M05A（與原始訓練資料同來源、同前處理）
  2. 以全部歷史資料重新訓練一個候選模型（架構、超參數與原始模型相同）
     依時間切分：最新 test_days 天當測試、再往前 val_days 天當驗證（提前停止）、其餘訓練
  3. 在「最新 test_days 天」上比較候選模型與目前啟用的模型——兩者都沒看過這幾天
  4. 候選模型平均誤差至少降低 min_gain，且 30 分鐘誤差沒有變差，才登記並啟用；
     否則保留目前的模型（候選模型仍登記為 rejected，可人工檢視或手動啟用）

原始模型 results/traffic_tgcn.ckpt 永遠不會被覆寫（見 model_registry）。

    python gcn/traffic/model_update.py                    # 下載到昨天、重訓、驗證、決定是否換上
    python gcn/traffic/model_update.py --until 20260620   # 只補到指定日期
    python gcn/traffic/model_update.py --no-download --dry-run
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import torch
import torch.nn.functional as F

GCN_DIR = Path(__file__).resolve().parent.parent
if str(GCN_DIR) not in sys.path:
    sys.path.insert(0, str(GCN_DIR))
from gcn_layer import normalize_adjacency, sparse_to_torch                   # noqa: E402
from traffic import model_registry as reg                                    # noqa: E402
from traffic import traffic_history as hist                                  # noqa: E402
from traffic.tgcn_model import TGCN                                          # noqa: E402
from traffic.train_traffic import HORIZONS, IN_STEPS, build_adjacency, make_windows  # noqa: E402

UPDATE_LOG = reg.MODELS_DIR / "update_log.jsonl"
MAX_DOWNLOAD_DAYS = 31        # 單次最多補幾天（第一次執行時避免一口氣下載數月）
_RUN_LOCK = threading.Lock()


# ── 資料 ──────────────────────────────────────────────

def _valid_index(speed: np.ndarray, volume: np.ndarray) -> np.ndarray:
    """可用的視窗起點 t：輸入 [t-12, t) 與目標 [t, t+max_h) 都沒有缺值。"""
    bad = ~(np.isfinite(speed).all(axis=1) & np.isfinite(volume).all(axis=1))
    cum = np.concatenate([[0], np.cumsum(bad)])
    max_h = max(HORIZONS)
    idx = np.arange(IN_STEPS, len(speed) - max_h + 1)
    ok = (cum[idx + max_h] - cum[idx - IN_STEPS]) == 0
    return idx[ok]


def _norm_stats(speed: np.ndarray, volume: np.ndarray, rows: np.ndarray) -> dict:
    sp, vo = speed[rows], volume[rows]
    sp, vo = sp[np.isfinite(sp)], vo[np.isfinite(vo)]
    return {"sp_mean": float(sp.mean()), "sp_std": float(max(sp.std(), 1e-6)),
            "vo_mean": float(vo.mean()), "vo_std": float(max(vo.std(), 1e-6))}


def _features(speed, volume, norm) -> np.ndarray:
    return np.stack([(speed - norm["sp_mean"]) / norm["sp_std"],
                     (volume - norm["vo_mean"]) / norm["vo_std"]], axis=-1).astype(np.float32)


# ── 模型 ──────────────────────────────────────────────

def _predict(model, a_hat, feats, idx, device, bs=64) -> np.ndarray:
    model.eval()
    out = []
    with torch.no_grad():
        for i in range(0, len(idx), bs):
            x, _ = make_windows(feats, idx[i:i + bs])
            out.append(model(a_hat, torch.from_numpy(x).to(device)).cpu().numpy())
    return np.concatenate(out)


def fit(feats, idx_train, idx_val, a_hat, device, *, epochs, lr, hidden, batch, patience,
        log=print, seed=0):
    """與 train_traffic 相同的訓練迴圈（L1 損失、Adam、驗證集提前停止），
    但每個批次才從特徵矩陣切出視窗，資料累積到數個月也不會吃光記憶體。"""
    torch.manual_seed(seed)
    model = TGCN(num_features=2, hidden=hidden, num_horizons=len(HORIZONS)).to(device)
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    _, y_val = make_windows(feats, idx_val)
    best_val, best_state, no_improve, ran = np.inf, None, 0, 0
    for epoch in range(1, epochs + 1):
        model.train()
        perm = np.random.default_rng(epoch).permutation(len(idx_train))
        losses = []
        for i in range(0, len(perm), batch):
            x, y = make_windows(feats, idx_train[perm[i:i + batch]])
            opt.zero_grad()
            loss = F.l1_loss(model(a_hat, torch.from_numpy(x).to(device)),
                             torch.from_numpy(y).to(device))
            loss.backward()
            opt.step()
            losses.append(loss.item())
        val_mae = float(np.abs(_predict(model, a_hat, feats, idx_val, device) - y_val).mean())
        ran = epoch
        if val_mae < best_val - 1e-5:
            best_val, no_improve = val_mae, 0
            best_state = {k: v.detach().clone() for k, v in model.state_dict().items()}
        else:
            no_improve += 1
        log(f"  epoch {epoch:2d}  train_l1 {np.mean(losses):.4f}  val_l1 {val_mae:.4f}")
        if no_improve >= patience:
            break
    return best_state, ran


def _metrics(pred_kmh: np.ndarray, speed: np.ndarray, idx: np.ndarray) -> dict:
    out = {}
    for k, h in enumerate(HORIZONS):
        out[f"MAE@{h * 5}min"] = round(float(np.abs(pred_kmh[..., k] - speed[idx + h - 1]).mean()), 3)
    out["MAE_mean"] = round(float(np.mean([out[f"MAE@{h * 5}min"] for h in HORIZONS])), 3)
    return out


def evaluate_checkpoint(ckpt_path: Path, speed, volume, idx, device) -> dict:
    """以模型檔自己的正規化參數，在 idx 視窗上算各預測時距的平均誤差（km/h）。"""
    ck = torch.load(ckpt_path, map_location="cpu", weights_only=False)
    model = TGCN(num_features=2, hidden=ck["hidden"], num_horizons=len(ck["horizons"])).to(device)
    model.load_state_dict(ck["state_dict"])
    a_hat = sparse_to_torch(normalize_adjacency(build_adjacency(np.array(ck["sections"]))), device)
    n = ck["norm"]
    pred = _predict(model, a_hat, _features(speed, volume, n), idx, device)
    return _metrics(pred * n["sp_std"] + n["sp_mean"], speed, idx)


def naive_metrics(speed, idx) -> dict:
    """沿用最後觀測車速當所有時距的預測（比較基準）。"""
    pred = np.repeat(speed[idx - 1][..., None], len(HORIZONS), axis=-1)
    return _metrics(pred, speed, idx)


# ── 主流程 ────────────────────────────────────────────

def _yesterday_tw() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=8) - timedelta(days=1)).strftime("%Y%m%d")


def collect(sections, until: str, max_days: int = MAX_DOWNLOAD_DAYS, log=print) -> dict:
    """補齊歷史：先把原始訓練矩陣拆成每日檔，再由舊到新下載缺少的日子（含先前失敗的缺口）。"""
    seeded = hist.seed_from_matrix()
    got, failed = [], []
    for day in hist.missing_days(until)[:max_days]:
        try:
            info = hist.import_m05a_day(day, sections)
            got.append(info)
            log(f"  匯入 {day}：覆蓋率 {info['coverage']:.1%}")
        except Exception as e:                                   # 尚未發布或下載失敗：下次再補
            failed.append({"day": day, "error": str(e)})
            log(f"  略過 {day}：{e}")
    return {"seeded_days": seeded, "imported": got, "failed": failed}


def run_update(until: str | None = None, download: bool = True, test_days: int = 3,
               val_days: int = 2, min_gain: float = 0.01, max_days: int = 0,
               epochs: int = 50, lr: float = 1e-3, hidden: int = 64, batch: int = 32,
               patience: int = 8, dry_run: bool = False, max_download: int = MAX_DOWNLOAD_DAYS,
               log=print) -> dict:
    if not _RUN_LOCK.acquire(blocking=False):
        raise RuntimeError("已有一個模型更新正在執行")
    try:
        return _run(until, download, test_days, val_days, min_gain, max_days, epochs, lr,
                    hidden, batch, patience, dry_run, max_download, log)
    finally:
        _RUN_LOCK.release()


def _run(until, download, test_days, val_days, min_gain, max_days, epochs, lr, hidden, batch,
         patience, dry_run, max_download, log) -> dict:
    t0 = time.perf_counter()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    registry = reg.load_registry()
    active_id, active_ckpt = registry["active"], reg.active_checkpoint()
    sections = torch.load(active_ckpt, map_location="cpu", weights_only=False)["sections"]
    until = until or _yesterday_tw()
    log(f"[1/4] 補齊歷史資料（到 {until}）")
    collected = (collect(sections, until, max_download, log=log) if download
                 else {"seeded_days": hist.seed_from_matrix()})

    days = hist.available_days()
    if max_days:
        days = days[-max_days:]
    speed, volume, stamps, day_of = hist.load_range(sections, days[0], days[-1])
    idx = _valid_index(speed, volume)
    all_days = sorted(set(day_of))
    test_from = all_days[-test_days]
    val_from = all_days[-(test_days + val_days)]
    tgt_day = day_of[np.minimum(idx + max(HORIZONS) - 1, len(day_of) - 1)]
    in_day = day_of[idx]
    idx_test = idx[in_day >= test_from]
    idx_val = idx[(in_day >= val_from) & (in_day < test_from) & (tgt_day < test_from)]
    idx_train = idx[tgt_day < val_from]
    if min(len(idx_train), len(idx_val), len(idx_test)) == 0:
        raise RuntimeError(f"資料不足：訓練 {len(idx_train)}、驗證 {len(idx_val)}、測試 {len(idx_test)} 個視窗")
    train_span = [str(day_of[idx_train[0] - IN_STEPS]), str(day_of[idx_train[-1]])]
    test_span = [test_from, all_days[-1]]
    log(f"[2/4] 重新訓練：{len(days)} 天資料，訓練 {train_span[0]}～{train_span[1]}"
        f"（{len(idx_train):,} 個視窗），測試 {test_span[0]}～{test_span[1]}")

    norm = _norm_stats(speed, volume, np.arange(idx_train[-1]))
    feats = _features(speed, volume, norm)
    a_hat = sparse_to_torch(normalize_adjacency(build_adjacency(np.array(sections))), device)
    state, epochs_run = fit(feats, idx_train, idx_val, a_hat, device, epochs=epochs, lr=lr,
                            hidden=hidden, batch=batch, patience=patience, log=log)

    log(f"[3/4] 在最新 {test_days} 天上比較（{active_id} 與候選模型都沒看過這幾天）")
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    reg.MODELS_DIR.mkdir(parents=True, exist_ok=True)
    cand_path = reg.MODELS_DIR / f"tgcn_{stamp}.ckpt"
    torch.save({"model": "tgcn", "state_dict": state, "hidden": hidden, "in_steps": IN_STEPS,
                "horizons": HORIZONS, "sections": list(sections), "norm": norm,
                "train_span": train_span, "data_days": len(days)}, cand_path)
    cand = evaluate_checkpoint(cand_path, speed, volume, idx_test, device)
    current = evaluate_checkpoint(active_ckpt, speed, volume, idx_test, device)
    naive = naive_metrics(speed, idx_test)
    gain = (current["MAE_mean"] - cand["MAE_mean"]) / current["MAE_mean"]
    promote = gain >= min_gain and cand["MAE@30min"] <= current["MAE@30min"]
    log(f"  目前 {active_id}：{current}")
    log(f"  候選模型：{cand}（平均誤差 {'降低' if gain >= 0 else '增加'} {abs(gain):.1%}）")

    summary = {"time": datetime.now().isoformat(timespec="seconds"), "device": str(device),
               "collected": collected, "data_days": len(days), "train_span": train_span,
               "test_span": test_span, "windows": {"train": len(idx_train), "val": len(idx_val),
                                                   "test": len(idx_test)},
               "epochs_run": epochs_run, "compared_with": active_id,
               "current_metrics": current, "candidate_metrics": cand, "naive_metrics": naive,
               "gain": round(gain, 4), "min_gain": min_gain, "promoted": bool(promote and not dry_run),
               "dry_run": dry_run}
    log("[4/4] " + ("換上新模型" if promote else "保留目前的模型") + ("（試跑，不登記）" if dry_run else ""))
    if dry_run:
        cand_path.unlink(missing_ok=True)
    else:
        summary["version"] = reg.register(cand_path, {
            "train_span": train_span, "test_span": test_span, "data_days": len(days),
            "metrics": cand, "compared_with": active_id, "compared_metrics": current,
            "gain": round(gain, 4)}, activate=promote)
        summary["active"] = reg.load_registry()["active"]
    summary["seconds"] = round(time.perf_counter() - t0, 1)
    with UPDATE_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(summary, ensure_ascii=False) + "\n")
    return summary


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--until", help="補資料到哪一天（YYYYMMDD，預設昨天）")
    ap.add_argument("--no-download", action="store_true", help="只用已有的歷史資料")
    ap.add_argument("--test-days", type=int, default=3)
    ap.add_argument("--val-days", type=int, default=2)
    ap.add_argument("--min-gain", type=float, default=0.01, help="平均誤差至少要降低的比例")
    ap.add_argument("--max-days", type=int, default=0, help="只用最近幾天訓練（0 = 全部）")
    ap.add_argument("--epochs", type=int, default=50)
    ap.add_argument("--dry-run", action="store_true", help="完整跑一次但不登記、不換模型")
    ap.add_argument("--max-download", type=int, default=MAX_DOWNLOAD_DAYS,
                    help=f"單次最多補幾天（預設 {MAX_DOWNLOAD_DAYS}）")
    a = ap.parse_args()
    res = run_update(until=a.until, download=not a.no_download, test_days=a.test_days,
                     val_days=a.val_days, min_gain=a.min_gain, max_days=a.max_days,
                     epochs=a.epochs, dry_run=a.dry_run, max_download=a.max_download)
    print(json.dumps({k: v for k, v in res.items() if k != "collected"}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
