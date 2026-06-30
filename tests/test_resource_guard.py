"""Resource Guard 三層准入（單元測試；不依賴 Spark / MinIO）。"""

from contextlib import contextmanager
from unittest.mock import patch

import pytest

from services import resource_guard as rg
from services.resource_guard import (
    ResourceGuardError,
    check_bronze_ocr_batch,
    check_request_upload,
    check_runtime_for_etl,
    pipeline_etl_slot,
    resolve_bronze_ocr_image_count,
)


@pytest.fixture(autouse=True)
def reset_active_jobs():
    with rg._lock:
        rg._active_pipeline_jobs = 0
    yield
    with rg._lock:
        rg._active_pipeline_jobs = 0


@contextmanager
def _guard_enabled():
    with patch.object(rg, "ETL_RESOURCE_GUARD_ENABLED", True):
        yield


@contextmanager
def _guard_disabled():
    with patch.object(rg, "ETL_RESOURCE_GUARD_ENABLED", False):
        yield


def test_check_request_upload_rejects_too_many_files():
    with _guard_enabled(), patch.object(rg, "MAX_UPLOAD_FILES_PER_REQUEST", 2):
        check_request_upload(file_count=2)
        with pytest.raises(ResourceGuardError, match="單次上傳檔案數"):
            check_request_upload(file_count=3)


def test_check_request_upload_skips_when_disabled():
    with _guard_disabled(), patch.object(rg, "MAX_UPLOAD_FILES_PER_REQUEST", 1):
        check_request_upload(file_count=99)


def test_check_bronze_ocr_batch_rejects_over_limit():
    with _guard_enabled(), patch.object(rg, "MAX_BRONZE_OCR_IMAGES", 100):
        check_bronze_ocr_batch(100)
        with pytest.raises(ResourceGuardError, match="Bronze OCR"):
            check_bronze_ocr_batch(101)


def test_check_runtime_rejects_high_percent():
    with _guard_enabled(), patch.object(rg, "ETL_MEMORY_MAX_PERCENT", 85.0), patch.object(
        rg, "ETL_MEMORY_MIN_AVAILABLE_MB", 1536
    ), patch.object(
        rg, "memory_snapshot", return_value={"percent": 90.0, "available_mb": 4096, "total_mb": 8192}
    ):
        with pytest.raises(ResourceGuardError, match="記憶體使用率"):
            check_runtime_for_etl("test_etl")


def test_check_runtime_rejects_low_available_mb():
    with _guard_enabled(), patch.object(rg, "ETL_MEMORY_MAX_PERCENT", 85.0), patch.object(
        rg, "ETL_MEMORY_MIN_AVAILABLE_MB", 1536
    ), patch.object(
        rg, "memory_snapshot", return_value={"percent": 50.0, "available_mb": 512, "total_mb": 8192}
    ):
        with pytest.raises(ResourceGuardError, match="可用記憶體"):
            check_runtime_for_etl("test_etl")


def test_pipeline_etl_slot_serializes_concurrent_jobs():
    with _guard_enabled(), patch.object(rg, "ETL_MAX_CONCURRENT_JOBS", 1), patch.object(
        rg, "memory_snapshot", return_value={"percent": 10.0, "available_mb": 4096, "total_mb": 8192}
    ):
        with pipeline_etl_slot(operation="job_a"):
            assert rg.active_pipeline_jobs() == 1
            with pytest.raises(ResourceGuardError, match="管線工作執行中"):
                with pipeline_etl_slot(operation="job_b"):
                    pass
        assert rg.active_pipeline_jobs() == 0


def test_resolve_bronze_ocr_image_count_prefers_image_paths():
    assert resolve_bronze_ocr_image_count(
        dataset_id="drinks",
        image_paths=["a.png", "b.png"],
        raw_images_path="s3a://data-lake/raw/images/drinks/",
    ) == 2


def test_resolve_bronze_ocr_image_count_from_dataset():
    with patch(
        "services.minio_upload.count_raw_image_objects_for_dataset",
        return_value=42,
    ) as counter:
        n = resolve_bronze_ocr_image_count(
            dataset_id="drinks",
            image_paths=None,
            raw_images_path=None,
        )
    assert n == 42
    counter.assert_called_once_with("drinks")


def test_resolve_bronze_ocr_image_count_minio_unavailable_returns_zero():
    with patch(
        "services.minio_upload.count_raw_image_objects_for_dataset",
        side_effect=RuntimeError("缺少 MINIO_ACCESS_KEY"),
    ):
        n = resolve_bronze_ocr_image_count(
            dataset_id="drinks",
            image_paths=None,
            raw_images_path=None,
        )
    assert n == 0
