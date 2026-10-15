"""預測模型版本紀錄：保留每一版模型，預測服務一律使用「目前啟用」的那一版。

    gcn/results/models/registry.json
        {"active": "v0", "versions": [{"id": "v0", "path": "results/traffic_tgcn.ckpt", ...}]}

v0 是原始模型（results/traffic_tgcn.ckpt），永遠不會被覆寫或刪除；新版本存到
results/models/，可以隨時切回任何一版（activate）。沒有 registry.json 時等同只有 v0。
"""
from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path

GCN_DIR = Path(__file__).resolve().parent.parent
BASE_CKPT = GCN_DIR / "results" / "traffic_tgcn.ckpt"
MODELS_DIR = GCN_DIR / "results" / "models"
REGISTRY = MODELS_DIR / "registry.json"

_LOCK = threading.Lock()


def _base_entry() -> dict:
    return {"id": "v0", "path": str(BASE_CKPT.relative_to(GCN_DIR)).replace("\\", "/"),
            "created": None, "status": "baseline",
            "note": "原始模型（2026/5/10～5/30 資料訓練）"}


def load_registry() -> dict:
    if REGISTRY.exists():
        return json.loads(REGISTRY.read_text(encoding="utf-8"))
    return {"active": "v0", "versions": [_base_entry()]}


def _save(reg: dict) -> None:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    tmp = REGISTRY.with_suffix(".tmp")
    tmp.write_text(json.dumps(reg, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(REGISTRY)


def _abs(path: str) -> Path:
    p = Path(path)
    return p if p.is_absolute() else GCN_DIR / p


def active_checkpoint() -> Path:
    """目前啟用的模型檔；紀錄指向的檔案不存在時退回原始模型。"""
    reg = load_registry()
    for v in reg["versions"]:
        if v["id"] == reg["active"]:
            p = _abs(v["path"])
            if p.exists():
                return p
    return BASE_CKPT


def register(ckpt_path: Path, info: dict, activate: bool) -> str:
    """登記一個新版本，回傳版本編號。activate=True 時同時改為啟用。"""
    with _LOCK:
        reg = load_registry()
        vid = f"v{len(reg['versions'])}"
        try:
            rel = str(Path(ckpt_path).resolve().relative_to(GCN_DIR)).replace("\\", "/")
        except ValueError:
            rel = str(ckpt_path)
        reg["versions"].append({"id": vid, "path": rel,
                                "created": datetime.now().isoformat(timespec="seconds"),
                                "status": "rejected", **info})
        if activate:
            _set_active(reg, vid)
        _save(reg)
    return vid


def _set_active(reg: dict, vid: str) -> None:
    for v in reg["versions"]:
        if v["status"] == "active":
            v["status"] = "standby"
    for v in reg["versions"]:
        if v["id"] == vid:
            v["status"] = "baseline" if vid == "v0" else "active"
    reg["active"] = vid


def activate(vid: str) -> dict:
    """切換啟用的版本（例如退回原始模型 v0）。"""
    with _LOCK:
        reg = load_registry()
        hit = next((v for v in reg["versions"] if v["id"] == vid), None)
        if hit is None:
            raise KeyError(f"沒有版本 {vid}")
        if not _abs(hit["path"]).exists():
            raise FileNotFoundError(f"版本 {vid} 的模型檔不存在：{hit['path']}")
        _set_active(reg, vid)
        _save(reg)
        return reg
