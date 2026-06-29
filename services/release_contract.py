"""
發行契約：從 manifest 讀取核准快照與發行中繼資料（首頁發行版／新鮮度水位）。
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from services.pipeline_guardian import load_manifest, resolve_manifest_path


def load_release_context(dataset_id: str) -> Dict[str, Any]:
    """
    讀取 dataset manifest 的發行欄位；無 manifest 時回傳空契約（不拋錯）。
    """
    ds = str(dataset_id or "").strip().lower()
    if not ds:
        return _empty_context()

    try:
        path = resolve_manifest_path(ds)
        manifest = load_manifest(path)
    except (FileNotFoundError, ValueError, OSError):
        return _empty_context(dataset_id=ds)

    gold = manifest.get("gold") or {}
    approved = gold.get("approved_snapshot_at")
    processed = gold.get("processed_image_count")
    return {
        "dataset_id": ds,
        "manifest_path": str(path),
        "release_id": str(manifest.get("release_id") or "").strip() or None,
        "approved_snapshot_at": str(approved).strip() if approved else None,
        "processed_image_count": int(processed) if processed is not None else None,
        "release_lexicon_version": str(
            gold.get("release_lexicon_version") or gold.get("lexicon_version") or ""
        ).strip()
        or None,
        "topic_rule_version": str(gold.get("topic_rule_version") or "").strip() or None,
    }


def _empty_context(*, dataset_id: Optional[str] = None) -> Dict[str, Any]:
    return {
        "dataset_id": dataset_id,
        "manifest_path": None,
        "release_id": None,
        "approved_snapshot_at": None,
        "processed_image_count": None,
        "release_lexicon_version": None,
        "topic_rule_version": None,
    }
