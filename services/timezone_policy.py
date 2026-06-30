"""時區政策：Delta／指標存 UTC；Web UI 顯示 Asia/Taipei。"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

STORAGE_TZ = timezone.utc


def _load_display_tz():
    try:
        return ZoneInfo("Asia/Taipei")
    except ZoneInfoNotFoundError:
        return timezone(timedelta(hours=8), name="Asia/Taipei")


DISPLAY_TZ = _load_display_tz()
DISPLAY_SUFFIX = "（台北）"

TIMESTAMP_FIELD_NAMES: frozenset[str] = frozenset(
    {
        "ingestion_timestamp",
        "etl_update_timestamp",
        "recorded_at",
        "last_modified",
        "snapshot_at",
        "analyzed_at",
        "quarantined_at",
        "archived_at",
        "evaluated_at",
        "checked_at",
        "started_at",
        "finished_at",
        "silver_batch_ts",
        "manifest_approved_snapshot_at",
        "approved_snapshot_at",
    }
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def is_timestamp_field(name: str) -> bool:
    key = str(name or "").strip()
    if not key or key.endswith("_display"):
        return False
    return key in TIMESTAMP_FIELD_NAMES or key.endswith("_timestamp") or key.endswith("_at")


def coerce_storage_utc(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=STORAGE_TZ)
        return value.astimezone(STORAGE_TZ)
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value, time.min, tzinfo=STORAGE_TZ)
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00") if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=STORAGE_TZ)
    return parsed.astimezone(STORAGE_TZ)


def format_storage_iso(value: Any) -> str:
    dt = coerce_storage_utc(value)
    if dt is None:
        return ""
    return dt.isoformat()


def format_display_timestamp(value: Any) -> str:
    dt = coerce_storage_utc(value)
    if dt is None:
        if value is None or value == "":
            return "—"
        return str(value)
    local = dt.astimezone(DISPLAY_TZ)
    return local.strftime("%Y-%m-%d %H:%M:%S") + f" {DISPLAY_SUFFIX}"


def format_rows_for_display(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        formatted: Dict[str, Any] = {}
        for key, value in row.items():
            if is_timestamp_field(key):
                formatted[key] = format_display_timestamp(value)
            elif isinstance(value, list):
                formatted[key] = list(value)
            elif isinstance(value, dict):
                formatted[key] = dict(value)
            else:
                formatted[key] = value
        out.append(formatted)
    return out


def enrich_timestamps_for_ui(row: Mapping[str, Any]) -> Dict[str, Any]:
    """保留 UTC 原值，另加 ``{field}_display`` 供 UI／人讀 API 使用。"""
    out: Dict[str, Any] = dict(row)
    for key, value in row.items():
        if is_timestamp_field(key) and value is not None and f"{key}_display" not in out:
            out[f"{key}_display"] = format_display_timestamp(value)
    return out


def enrich_rows_timestamps_for_ui(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [enrich_timestamps_for_ui(row) for row in rows]
