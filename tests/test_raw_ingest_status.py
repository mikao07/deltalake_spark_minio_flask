"""原圖攝入狀態預覽。"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from services import raw_ingest_status as ris


def test_list_raw_ingest_status_marks_missing_bronze():
    lm_new = datetime(2026, 6, 30, 8, 6, 50, tzinfo=timezone.utc)
    lm_old = datetime(2026, 4, 17, 10, 53, 34, tzinfo=timezone.utc)

    class _Obj:
        def __init__(self, name, lm):
            self.object_name = name
            self.last_modified = lm

    client = MagicMock()
    client.list_objects.return_value = [
        _Obj("raw/images/drinks/2026-06-30_160218.png", lm_new),
        _Obj("raw/images/drinks/old.png", lm_old),
    ]

    bronze_paths = {"s3a://data-lake/raw/images/drinks/old.png"}
    bronze_texts = {"s3a://data-lake/raw/images/drinks/old.png": "舊文"}

    with patch.object(ris, "get_minio_client", return_value=client):
        with patch.object(ris, "ensure_bucket"):
            with patch.object(ris, "_bronze_lookup_for_dataset", return_value=(bronze_paths, bronze_texts)):
                rows = ris.list_raw_ingest_status("drinks", limit=10)

    assert len(rows) == 2
    assert rows[0]["filename"] == "2026-06-30_160218.png"
    assert rows[0]["in_bronze"] is False
    assert rows[0]["bronze_status"] == "已上傳，尚未辨識"
    assert rows[1]["in_bronze"] is True
    assert rows[1]["extracted_text_preview"] == "舊文"


def test_list_raw_ingest_status_only_missing():
    class _Obj:
        def __init__(self, name):
            self.object_name = name
            self.last_modified = datetime(2026, 6, 30, tzinfo=timezone.utc)

    client = MagicMock()
    client.list_objects.return_value = [
        _Obj("raw/images/drinks/new.png"),
        _Obj("raw/images/drinks/old.png"),
    ]
    bronze_paths = {"s3a://data-lake/raw/images/drinks/old.png"}

    with patch.object(ris, "get_minio_client", return_value=client):
        with patch.object(ris, "ensure_bucket"):
            with patch.object(ris, "_bronze_lookup_for_dataset", return_value=(bronze_paths, {})):
                rows = ris.list_raw_ingest_status("drinks", limit=10, only_missing=True)

    assert len(rows) == 1
    assert rows[0]["filename"] == "new.png"


def test_collect_missing_raw_image_paths_returns_all_gaps():
    class _Obj:
        def __init__(self, name):
            self.object_name = name
            self.last_modified = datetime(2026, 6, 30, tzinfo=timezone.utc)

    client = MagicMock()
    client.list_objects.return_value = [
        _Obj("raw/images/drinks/a.png"),
        _Obj("raw/images/drinks/b.png"),
        _Obj("raw/images/drinks/c.png"),
    ]
    bronze_paths = {"s3a://data-lake/raw/images/drinks/b.png"}

    with patch.object(ris, "get_minio_client", return_value=client):
        with patch.object(ris, "ensure_bucket"):
            with patch.object(ris, "_bronze_lookup_for_dataset", return_value=(bronze_paths, {})):
                gap = ris.collect_missing_raw_image_paths("drinks")

    assert gap["missing_count"] == 2
    assert gap["missing_paths"] == [
        "s3a://data-lake/raw/images/drinks/a.png",
        "s3a://data-lake/raw/images/drinks/c.png",
    ]
    assert gap["truncated"] is False


def test_collect_missing_raw_image_paths_not_limited_to_fifty():
    """一鍵金補缺口須全量路徑，不受 list_raw_ingest_status 的 limit=50 限制。"""
    class _Obj:
        def __init__(self, i):
            self.object_name = f"raw/images/drinks/img_{i:03d}.png"
            self.last_modified = datetime(2026, 6, 30, tzinfo=timezone.utc)

    client = MagicMock()
    client.list_objects.return_value = [_Obj(i) for i in range(60)]

    with patch.object(ris, "get_minio_client", return_value=client):
        with patch.object(ris, "ensure_bucket"):
            with patch.object(ris, "_bronze_lookup_for_dataset", return_value=(set(), {})):
                gap = ris.collect_missing_raw_image_paths("drinks")

    assert gap["missing_count"] == 60
