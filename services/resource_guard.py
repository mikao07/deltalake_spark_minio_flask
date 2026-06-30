"""
Resource Guard（資源保護）：Request / Pipeline / Runtime 三層准入。

防止單次請求過大、ETL 併發堆疊、以及記憶體已緊張時仍啟動重型 Spark 工作。
"""

from __future__ import annotations

import logging
import re
import threading
from contextlib import contextmanager
from typing import Iterator, Optional

import psutil

from config import (
    ETL_MAX_CONCURRENT_JOBS,
    ETL_MEMORY_MAX_PERCENT,
    ETL_MEMORY_MIN_AVAILABLE_MB,
    ETL_RESOURCE_GUARD_ENABLED,
    MAX_BRONZE_OCR_IMAGES,
    MAX_UPLOAD_FILES_PER_REQUEST,
    MAX_UPLOAD_MB,
)


_logger = logging.getLogger(__name__)


class ResourceGuardError(ValueError):
    """資源保護拒絕（API 應回 400）。"""


_lock = threading.Lock()
_active_pipeline_jobs = 0


def resource_guard_enabled() -> bool:
    return bool(ETL_RESOURCE_GUARD_ENABLED)


def memory_snapshot() -> dict[str, float | int]:
    mem = psutil.virtual_memory()
    return {
        "percent": float(mem.percent),
        "available_mb": int(mem.available // (1024 * 1024)),
        "total_mb": int(mem.total // (1024 * 1024)),
    }


def check_request_upload(*, file_count: int, max_upload_mb: int | None = None) -> None:
    """Request Guard：單次 multipart 檔案數（單檔大小由 MAX_UPLOAD_MB 另檢）。"""
    if not resource_guard_enabled():
        return
    n = int(file_count or 0)
    if n <= 0:
        return
    limit = int(MAX_UPLOAD_FILES_PER_REQUEST)
    if n > limit:
        raise ResourceGuardError(
            f"單次上傳檔案數 {n} 超過上限 {limit}。"
            f"請分批上傳（單檔仍須 ≤{int(max_upload_mb or MAX_UPLOAD_MB)} MB）。"
        )


def check_runtime_for_etl(operation: str) -> None:
    """Runtime Guard：記憶體使用率 + 可用 MB 雙條件。"""
    if not resource_guard_enabled():
        return
    snap = memory_snapshot()
    pct = float(snap["percent"])
    avail = int(snap["available_mb"])
    max_pct = float(ETL_MEMORY_MAX_PERCENT)
    min_avail = int(ETL_MEMORY_MIN_AVAILABLE_MB)
    if pct >= max_pct:
        raise ResourceGuardError(
            f"記憶體使用率 {pct:.1f}% 已達上限 {max_pct:.0f}%，"
            f"暫停 {operation}。請關閉其他程式或調整 Docker 記憶體配額後重試。"
        )
    if avail < min_avail:
        raise ResourceGuardError(
            f"可用記憶體僅 {avail} MB（下限 {min_avail} MB），"
            f"暫停 {operation}。請釋放記憶體或調低 SPARK_DRIVER_MEMORY 後重試。"
        )


def check_bronze_ocr_batch(image_count: int) -> None:
    """Pipeline Guard：單次 Bronze OCR 處理圖片數。"""
    if not resource_guard_enabled():
        return
    n = int(image_count or 0)
    if n <= 0:
        return
    limit = int(MAX_BRONZE_OCR_IMAGES)
    if n > limit:
        raise ResourceGuardError(
            f"本次 Bronze OCR 將處理 {n} 張圖，超過上限 {limit}。"
            f"請分批上傳、使用 write_mode=merge 子集，或調高 MAX_BRONZE_OCR_IMAGES。"
        )


def resolve_bronze_ocr_image_count(
    *,
    dataset_id: str | None,
    image_paths: list[str] | None,
    raw_images_path: str | None = None,
) -> int:
    """估算本次 Bronze OCR 將觸及的圖片數。"""
    if image_paths:
        return len([p for p in image_paths if str(p).strip()])
    ds = str(dataset_id or "").strip()
    if not ds and raw_images_path:
        m = re.search(r"/raw/images/([^/]+)/?", str(raw_images_path).replace("\\", "/"))
        if m:
            ds = m.group(1).strip()
    if ds:
        from services.minio_upload import count_raw_image_objects_for_dataset, normalize_dataset_id

        try:
            return count_raw_image_objects_for_dataset(normalize_dataset_id(ds))
        except (RuntimeError, OSError) as exc:
            _logger.warning(
                "bronze_ocr_image_count_unavailable dataset=%s: %s",
                ds,
                exc,
            )
            return 0
    return 0


def _acquire_pipeline_slot() -> None:
    global _active_pipeline_jobs
    if not resource_guard_enabled():
        return
    max_jobs = max(1, int(ETL_MAX_CONCURRENT_JOBS))
    with _lock:
        if _active_pipeline_jobs >= max_jobs:
            raise ResourceGuardError(
                f"已有 {_active_pipeline_jobs} 個管線工作執行中（上限 {max_jobs}）。"
                "請待完成後再啟動新的 Bronze／Silver／Gold ETL。"
            )
        _active_pipeline_jobs += 1


def _release_pipeline_slot() -> None:
    global _active_pipeline_jobs
    with _lock:
        if _active_pipeline_jobs > 0:
            _active_pipeline_jobs -= 1


@contextmanager
def pipeline_etl_slot(*, operation: str = "ETL") -> Iterator[None]:
    """Pipeline Guard：併發槽 + Runtime 檢查（進入前）。"""
    check_runtime_for_etl(operation)
    _acquire_pipeline_slot()
    try:
        yield
    finally:
        _release_pipeline_slot()


def active_pipeline_jobs() -> int:
    with _lock:
        return int(_active_pipeline_jobs)
