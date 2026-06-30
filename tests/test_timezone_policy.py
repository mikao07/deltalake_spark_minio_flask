from datetime import datetime, timezone

from services.timezone_policy import (
    DISPLAY_SUFFIX,
    coerce_storage_utc,
    enrich_timestamps_for_ui,
    format_display_timestamp,
    format_rows_for_display,
    format_storage_iso,
    utc_now_iso,
)


def test_utc_now_iso_has_offset():
    iso = utc_now_iso()
    assert "+00:00" in iso or iso.endswith("Z")


def test_naive_storage_treated_as_utc():
    dt = coerce_storage_utc("2026-06-30T14:16:12.500127")
    assert dt is not None
    assert dt.tzinfo == timezone.utc
    assert dt.hour == 14


def test_display_converts_utc_to_taipei():
    shown = format_display_timestamp("2026-06-30T14:16:12+00:00")
    assert "22:16:12" in shown
    assert DISPLAY_SUFFIX in shown


def test_format_storage_iso_keeps_utc():
    assert format_storage_iso("2026-06-30T14:16:12+00:00").startswith("2026-06-30T14:16:12")


def test_format_rows_for_display_replaces_timestamp_columns():
    rows = format_rows_for_display(
        [{"ingestion_timestamp": "2026-06-30T14:16:12+00:00", "image_path": "a.png"}]
    )
    assert DISPLAY_SUFFIX in rows[0]["ingestion_timestamp"]
    assert rows[0]["image_path"] == "a.png"


def test_enrich_timestamps_for_ui_keeps_utc_and_adds_display():
    row = enrich_timestamps_for_ui({"last_modified": "2026-06-30T06:11:43+00:00"})
    assert row["last_modified"] == "2026-06-30T06:11:43+00:00"
    assert DISPLAY_SUFFIX in row["last_modified_display"]
