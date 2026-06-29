"""
管線新鮮度：條件式滯後偵測（非固定 24h）與外部探針 heartbeat。

規則（預設滯後門檻 FRESHNESS_STALE_HOURS=12）：
- raw_count > silver_count 持續超過門檻 → 上游有圖、銀層未跟上
- silver_count > release_count 持續超過門檻 → 銀層有、發行未跟上（manifest processed_image_count）
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from config import (
    FRESHNESS_STALE_HOURS,
    PIPELINE_FRESHNESS_STATE_FILE,
    PIPELINE_HEARTBEAT_FILE,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_dt(value: str) -> Optional[datetime]:
    s = str(value or "").strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso_now() -> str:
    return _utc_now().isoformat()


def _resolve_path(path: str | Path) -> Path:
    p = Path(path)
    if not p.is_absolute():
        p = _REPO_ROOT / p
    return p


def load_freshness_state(path: str | Path | None = None) -> Dict[str, Any]:
    p = _resolve_path(path or PIPELINE_FRESHNESS_STATE_FILE)
    if not p.is_file():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def save_freshness_state(state: Dict[str, Any], path: str | Path | None = None) -> Path:
    p = _resolve_path(path or PIPELINE_FRESHNESS_STATE_FILE)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return p


def write_heartbeat(payload: Dict[str, Any], path: str | Path | None = None) -> Path:
    p = _resolve_path(path or PIPELINE_HEARTBEAT_FILE)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return p


def _update_lag_since(
    *,
    active: bool,
    prev_since: Optional[str],
    now_iso: str,
) -> tuple[Optional[str], bool]:
    """
    回傳 (lag_since, became_stale)。
    became_stale：本次檢查判定已超過門檻（需 prev_since 存在且持續 active）。
    """
    if not active:
        return None, False
    if not prev_since:
        return now_iso, False
    return prev_since, True


def _hours_since(since_iso: str, now: datetime) -> float:
    since_dt = _parse_iso_dt(since_iso)
    if since_dt is None:
        return 0.0
    return max(0.0, (now - since_dt).total_seconds() / 3600.0)


def collect_freshness_counts(
    dataset_id: str,
    *,
    offline: bool = False,
    spark=None,
) -> Dict[str, Any]:
    from services.minio_upload import count_raw_image_objects_for_dataset
    from services.release_contract import load_release_context

    ds = str(dataset_id or "").strip().lower()
    release_ctx = load_release_context(ds)
    raw_count = count_raw_image_objects_for_dataset(ds)

    silver_count: Optional[int] = None
    if not offline:
        from services.spark_service import SparkManager, count_silver_distinct_image_paths

        if spark is None:
            spark = SparkManager().spark
        silver_count = count_silver_distinct_image_paths(spark, ds)

    release_count = release_ctx.get("processed_image_count")
    if release_count is None and release_ctx.get("approved_snapshot_at"):
        release_count = 0

    return {
        "dataset_id": ds,
        "raw_image_count": raw_count,
        "silver_image_count": silver_count,
        "release_image_count": release_count,
        "approved_snapshot_at": release_ctx.get("approved_snapshot_at"),
        "release_id": release_ctx.get("release_id"),
    }


def evaluate_freshness(
    counts: Dict[str, Any],
    *,
    stale_hours: float = FRESHNESS_STALE_HOURS,
    state: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    依計數與持久化 state 產生 alerts，並回傳更新後的 dataset state 片段。
    """
    now = now or _utc_now()
    now_iso = now.isoformat()
    ds = str(counts.get("dataset_id") or "").strip().lower()
    raw_count = int(counts.get("raw_image_count") or 0)
    silver_count = counts.get("silver_image_count")
    release_count = counts.get("release_image_count")

    prev = (state or {}).get(ds) if isinstance(state, dict) else {}
    if not isinstance(prev, dict):
        prev = {}

    upstream_active = silver_count is not None and raw_count > int(silver_count)
    upstream_since, _ = _update_lag_since(
        active=upstream_active,
        prev_since=prev.get("upstream_lag_since"),
        now_iso=now_iso,
    )

    release_active = False
    if silver_count is not None and release_count is not None:
        release_active = int(silver_count) > int(release_count)
    release_since, _ = _update_lag_since(
        active=release_active,
        prev_since=prev.get("release_lag_since"),
        now_iso=now_iso,
    )

    alerts: List[Dict[str, Any]] = []
    if upstream_active and upstream_since:
        hours = _hours_since(upstream_since, now)
        if hours >= stale_hours:
            alerts.append(
                {
                    "code": "upstream_stale",
                    "message": (
                        f"上游原始圖 ({raw_count}) 多於銀層 distinct image_path ({silver_count})，"
                        f"已持續 {hours:.1f}h（門檻 {stale_hours}h）"
                    ),
                    "since": upstream_since,
                    "hours": round(hours, 2),
                    "raw_image_count": raw_count,
                    "silver_image_count": silver_count,
                }
            )

    if release_active and release_since:
        hours = _hours_since(release_since, now)
        if hours >= stale_hours:
            alerts.append(
                {
                    "code": "release_stale",
                    "message": (
                        f"銀層 ({silver_count}) 多於發行水位 processed_image_count ({release_count})，"
                        f"已持續 {hours:.1f}h（門檻 {stale_hours}h）"
                    ),
                    "since": release_since,
                    "hours": round(hours, 2),
                    "silver_image_count": silver_count,
                    "release_image_count": release_count,
                }
            )

    dataset_state = {
        "upstream_lag_since": upstream_since,
        "release_lag_since": release_since,
        "last_checked_at": now_iso,
    }
    return {
        "alerts": alerts,
        "dataset_state": dataset_state,
        "upstream_active": upstream_active,
        "release_active": release_active,
    }


def run_freshness_check(
    dataset_id: str,
    *,
    offline: bool = False,
    spark=None,
    stale_hours: float = FRESHNESS_STALE_HOURS,
    state_path: str | Path | None = None,
    heartbeat_path: str | Path | None = None,
) -> Dict[str, Any]:
    counts = collect_freshness_counts(dataset_id, offline=offline, spark=spark)
    state = load_freshness_state(state_path)
    evaluation = evaluate_freshness(counts, stale_hours=stale_hours, state=state)

    ds = counts["dataset_id"]
    state[ds] = evaluation["dataset_state"]
    save_freshness_state(state, state_path)

    payload = {
        "checked_at": _iso_now(),
        "dataset_id": ds,
        "counts": counts,
        "stale_hours": stale_hours,
        "alerts": evaluation["alerts"],
        "ok": len(evaluation["alerts"]) == 0,
        "offline": offline,
    }
    write_heartbeat(payload, heartbeat_path)
    return payload
