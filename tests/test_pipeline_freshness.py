"""發行契約與管線新鮮度。"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from services.pipeline_freshness import (
    evaluate_freshness,
    load_freshness_state,
    run_freshness_check,
    save_freshness_state,
)
from services.release_contract import load_release_context


def test_load_release_context_from_repo_manifest():
    ctx = load_release_context("drinks")
    assert ctx["dataset_id"] == "drinks"
    assert ctx["release_id"] == "drinks-gold-v1"
    assert "approved_snapshot_at" in ctx
    assert "processed_image_count" in ctx


def test_load_release_context_missing_manifest():
    ctx = load_release_context("nonexistent_dataset_xyz")
    assert ctx["dataset_id"] == "nonexistent_dataset_xyz"
    assert ctx["approved_snapshot_at"] is None


def test_evaluate_freshness_upstream_alert_after_stale_hours():
    now = datetime(2026, 6, 27, 12, 0, tzinfo=timezone.utc)
    since = (now - timedelta(hours=13)).isoformat()
    state = {"drinks": {"upstream_lag_since": since, "release_lag_since": None}}
    counts = {
        "dataset_id": "drinks",
        "raw_image_count": 10,
        "silver_image_count": 5,
        "release_image_count": 5,
    }
    result = evaluate_freshness(counts, stale_hours=12.0, state=state, now=now)
    assert any(a["code"] == "upstream_stale" for a in result["alerts"])


def test_evaluate_freshness_clears_lag_when_resolved():
    now = datetime(2026, 6, 27, 12, 0, tzinfo=timezone.utc)
    since = (now - timedelta(hours=20)).isoformat()
    state = {"drinks": {"upstream_lag_since": since, "release_lag_since": None}}
    counts = {
        "dataset_id": "drinks",
        "raw_image_count": 5,
        "silver_image_count": 5,
        "release_image_count": 5,
    }
    result = evaluate_freshness(counts, stale_hours=12.0, state=state, now=now)
    assert result["alerts"] == []
    assert result["dataset_state"]["upstream_lag_since"] is None


def test_evaluate_freshness_release_stale_alert():
    now = datetime(2026, 6, 27, 12, 0, tzinfo=timezone.utc)
    since = (now - timedelta(hours=15)).isoformat()
    state = {"drinks": {"upstream_lag_since": None, "release_lag_since": since}}
    counts = {
        "dataset_id": "drinks",
        "raw_image_count": 8,
        "silver_image_count": 8,
        "release_image_count": 5,
    }
    result = evaluate_freshness(counts, stale_hours=12.0, state=state, now=now)
    assert any(a["code"] == "release_stale" for a in result["alerts"])


def test_freshness_state_roundtrip(tmp_path: Path):
    path = tmp_path / "state.json"
    save_freshness_state({"drinks": {"upstream_lag_since": "2026-06-27T00:00:00+00:00"}}, path)
    loaded = load_freshness_state(path)
    assert loaded["drinks"]["upstream_lag_since"].startswith("2026-06-27")


@patch("services.pipeline_freshness.collect_freshness_counts")
def test_run_freshness_check_writes_heartbeat(mock_collect, tmp_path: Path):
    mock_collect.return_value = {
        "dataset_id": "drinks",
        "raw_image_count": 3,
        "silver_image_count": 3,
        "release_image_count": 3,
        "approved_snapshot_at": None,
        "release_id": "drinks-gold-v1",
    }
    hb = tmp_path / "heartbeat.json"
    state = tmp_path / "state.json"
    payload = run_freshness_check(
        "drinks",
        offline=True,
        state_path=state,
        heartbeat_path=hb,
    )
    assert payload["ok"] is True
    assert hb.is_file()
    data = json.loads(hb.read_text(encoding="utf-8"))
    assert data["dataset_id"] == "drinks"


def test_stamp_approved_snapshot_sets_processed_image_count(tmp_path: Path):
    from services.pipeline_guardian import load_manifest, save_manifest, stamp_approved_snapshot

    path = tmp_path / "drinks.json"
    save_manifest(
        path,
        {
            "dataset_id": "drinks",
            "gold": {
                "release_lexicon_version": "v1.0.0",
                "lexicon_content_hash": "abc123",
                "topic_rule_version": "v1",
            },
        },
    )
    spark = MagicMock()
    with patch(
        "services.spark_service.find_latest_topic_snapshot_at_for_release",
        return_value="2026-06-27T10:00:00",
    ), patch(
        "services.spark_service.count_silver_distinct_image_paths",
        return_value=42,
    ):
        result = stamp_approved_snapshot("drinks", manifest_path=path, spark=spark)
    assert result["processed_image_count"] == 42
    loaded = load_manifest(path)
    assert loaded["gold"]["processed_image_count"] == 42
