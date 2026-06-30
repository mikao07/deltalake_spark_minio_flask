"""
發行契約：從 manifest 讀取核准快照與發行中繼資料（首頁發行版／新鮮度水位）。
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from services.pipeline_guardian import load_manifest, resolve_manifest_path
from services.timezone_policy import format_display_timestamp


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


def format_release_filter_summary(release_context: Dict[str, Any]) -> str:
    """首頁篩選列：正式版一行白話摘要。"""
    approved = release_context.get("approved_snapshot_at")
    if not approved:
        return "尚未建立正式版"
    parts: list[str] = []
    rid = release_context.get("release_id")
    if rid:
        parts.append(f"版本 {rid}")
    parts.append(format_display_timestamp(approved))
    pic = release_context.get("processed_image_count")
    if pic is not None:
        parts.append(f"含 {pic} 張截圖")
    return " · ".join(parts)


def format_release_card_subtitle(release_context: Dict[str, Any]) -> str:
    """痛點卡片標題旁副標。"""
    parts: list[str] = []
    rid = release_context.get("release_id")
    if rid:
        parts.append(f"版本 {rid}")
    pic = release_context.get("processed_image_count")
    if pic is not None:
        parts.append(f"納入 {pic} 張截圖")
    approved = release_context.get("approved_snapshot_at")
    if approved:
        parts.append(f"核准於 {format_display_timestamp(approved)}")
    return " · ".join(parts)


def build_topic_hint(
    *,
    snapshot_mode: str,
    selected_dataset_id: str | None,
    release_context: Dict[str, Any],
    latest_snapshot_at: str | None,
    topic_rows: list,
    approved: str | None,
) -> str | None:
    """首頁痛點區提示文字（白話，不含欄位英文名）。"""
    if snapshot_mode == "preview":
        hint = "試看版：顯示最近一次金層分析的痛點，尚未核准，請勿當作正式報告對外使用。"
        if latest_snapshot_at:
            hint += f" 分析時間：{format_display_timestamp(latest_snapshot_at)}。"
        return hint
    if not selected_dataset_id:
        return "請先在上方選擇資料集（例如 drinks），勿選「全部」，才能查看正式版痛點。"
    if not approved:
        return (
            "尚未建立正式對外版。請先完成金層分析並執行核准；"
            "開發調參時可暫時切換「試看版」。"
        )
    if not topic_rows:
        return (
            f"找不到核准時間（{format_display_timestamp(approved)}）對應的痛點資料，"
            "請重跑金層或重新核准。"
        )
    rid = release_context.get("release_id") or "—"
    hint = (
        f"目前顯示正式對外版（版本 {rid}）。"
        f"結論產生時間：{format_display_timestamp(approved)}。"
    )
    pic = release_context.get("processed_image_count")
    if pic is not None:
        hint += f"當時共納入 {pic} 張截圖的分析結果。"
    else:
        hint += "納入截圖張數尚未登記。"
    return hint


def format_manifest_release_plain(
    *,
    release_id: str,
    approved_snapshot_at: str,
    processed_image_count: int | None = None,
) -> Dict[str, str]:
    """辭典／manifest 面板用白話欄位。"""
    out: Dict[str, str] = {
        "version_label": release_id or "—",
        "approved_label": format_display_timestamp(approved_snapshot_at) if approved_snapshot_at else "尚未核准",
    }
    if processed_image_count is not None:
        out["coverage_label"] = f"納入 {processed_image_count} 張截圖"
    else:
        out["coverage_label"] = ""
    return out
