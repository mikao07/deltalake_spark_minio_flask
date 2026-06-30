"""Bronze merge 前 history 歸檔（單元測試：不依賴 Spark）。"""

from services.ocr_spark import resolve_bronze_history_path, should_archive_bronze_before_merge


def test_should_archive_when_enabled_and_path_set():
    assert should_archive_bronze_before_merge(
        enabled=True,
        history_path="s3a://data-lake/bronze/history/",
    )


def test_should_not_archive_when_disabled():
    assert not should_archive_bronze_before_merge(
        enabled=False,
        history_path="s3a://data-lake/bronze/history/",
    )


def test_should_not_archive_when_path_empty():
    assert not should_archive_bronze_before_merge(enabled=True, history_path="")
    assert not should_archive_bronze_before_merge(enabled=True, history_path="   ")


def test_resolve_bronze_history_path_non_empty():
    path = resolve_bronze_history_path()
    assert path
    assert path.startswith("s3a://")
