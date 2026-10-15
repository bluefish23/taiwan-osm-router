"""模型持續更新的驗證（不訓練、不下載，數秒完成）。

    python gcn/traffic/verify_model_update.py

1. 歷史資料：由原始訓練矩陣拆成每日檔後讀回，與原矩陣逐值相同
2. 視窗：跨過缺值的訓練視窗一律排除
3. 版本切換：換上新版本後預測服務自動改用；可退回原始模型 v0；不存在的版本會被拒絕
   （在暫存資料夾操作，不碰真正的 registry.json）
4. 原始模型檔 results/traffic_tgcn.ckpt 的內容沒有改變
"""
from __future__ import annotations

import hashlib
import shutil
import sys
import tempfile
from pathlib import Path

import numpy as np

GCN_DIR = Path(__file__).resolve().parent.parent
if str(GCN_DIR) not in sys.path:
    sys.path.insert(0, str(GCN_DIR))
from traffic import model_registry as reg            # noqa: E402
from traffic import predict_service as ps            # noqa: E402
from traffic import traffic_history as hist          # noqa: E402
from traffic.model_update import _valid_index        # noqa: E402
from traffic.train_traffic import HORIZONS, IN_STEPS  # noqa: E402

BASE_SHA256 = "ec8a0784eab9356ecd3a129d7383c3cd4c02b5a0bc5e26c0b32866caf22de1f4"


def check(name: str, ok: bool) -> bool:
    print(f"  {'通過' if ok else '失敗'}  {name}")
    return ok


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    results = []

    print("[1] 歷史資料")
    hist.seed_from_matrix()
    z = np.load(hist.SEED_MATRIX, allow_pickle=True)
    days = [d for d in hist.available_days() if d <= "20260530"]
    sp, vo, ts, _ = hist.load_range(z["sections"], days[0], days[-1])
    results.append(check("原始 21 天讀回後與訓練矩陣逐值相同",
                         np.array_equal(sp, z["speed"]) and np.array_equal(vo, z["volume"])))

    print("[2] 訓練視窗")
    s = np.ones((300, 4), dtype=np.float32)
    v = s.copy()
    s[150, 2] = np.nan
    idx = _valid_index(s, v)
    span = set(range(150 - max(HORIZONS) + 1, 150 + IN_STEPS + 1))
    results.append(check("跨過缺值的視窗全部排除", not (set(idx.tolist()) & span)))
    results.append(check("其餘視窗都保留",
                         len(idx) == len(range(IN_STEPS, 300 - max(HORIZONS) + 1)) - len(span)))

    print("[3] 版本切換（暫存資料夾）")
    real_dir, real_reg = reg.MODELS_DIR, reg.REGISTRY
    tmp = Path(tempfile.mkdtemp())
    try:
        reg.MODELS_DIR, reg.REGISTRY = tmp, tmp / "registry.json"
        results.append(check("沒有任何新版本時使用原始模型", reg.active_checkpoint() == reg.BASE_CKPT))
        fake = tmp / "tgcn_test.ckpt"
        shutil.copy(reg.BASE_CKPT, fake)
        vid = reg.register(fake, {"note": "test"}, activate=False)
        results.append(check("未通過驗證的版本不會被啟用", reg.active_checkpoint() == reg.BASE_CKPT))
        reg.activate(vid)
        p = ps.get_predictor()
        results.append(check("啟用新版本後，預測服務自動改用", p.ckpt_path == fake))
        reg.activate("v0")
        results.append(check("可以退回原始模型 v0", ps.get_predictor().ckpt_path == reg.BASE_CKPT))
        try:
            reg.activate("v99")
            results.append(check("不存在的版本會被拒絕", False))
        except KeyError:
            results.append(check("不存在的版本會被拒絕", True))
    finally:
        reg.MODELS_DIR, reg.REGISTRY = real_dir, real_reg
        ps._PREDICTOR = None
        shutil.rmtree(tmp, ignore_errors=True)

    print("[4] 原始模型")
    digest = hashlib.sha256(reg.BASE_CKPT.read_bytes()).hexdigest()
    results.append(check("results/traffic_tgcn.ckpt 內容沒有改變", digest == BASE_SHA256))

    print(f"\n結果：{'全部通過' if all(results) else '有項目未通過'}")
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
