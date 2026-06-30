"""
P4 執行期節流：Spark job 逾時、Bronze OCR repartition。

預設行為不改變現況：
- OCR_REPARTITION=0 → 不 repartition
- OCR_TIMEOUT_SECONDS=0 或 SPARK_JOB_TIMEOUT_SECONDS=0 → 不啟用對應逾時
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from contextlib import contextmanager
from typing import Any, Callable, Iterator, TypeVar

from pyspark.sql import DataFrame, SparkSession

from config import OCR_REPARTITION, OCR_TIMEOUT_SECONDS, SPARK_JOB_TIMEOUT_SECONDS

_logger = logging.getLogger(__name__)

T = TypeVar("T")


class SparkJobTimeoutError(TimeoutError):
    """Spark 管線工作超過 SPARK_JOB_TIMEOUT_SECONDS。"""


def resolve_ocr_repartition(n: int | None = None) -> int:
    """回傳生效的 repartition 數；0 表示不強制。"""
    if n is not None:
        return max(0, int(n))
    return max(0, int(OCR_REPARTITION or 0))


def apply_ocr_repartition(df: DataFrame, n: int | None = None) -> tuple[DataFrame, int]:
    """Bronze OCR 前可選 repartition；回傳 (df, 實際 N，0=未套用)。"""
    parts = resolve_ocr_repartition(n)
    if parts <= 0:
        return df, 0
    return df.repartition(parts), parts


@contextmanager
def spark_job_timeout_guard(
    spark: SparkSession | None,
    *,
    operation: str = "ETL",
    timeout_seconds: int | None = None,
) -> Iterator[None]:
    """
    逾時後呼叫 sparkContext.cancelAllJobs()。
    timeout_seconds 為 None 時讀 SPARK_JOB_TIMEOUT_SECONDS；≤0 則不啟用。
    """
    limit = int(timeout_seconds if timeout_seconds is not None else SPARK_JOB_TIMEOUT_SECONDS)
    if limit <= 0 or spark is None:
        yield
        return

    done = threading.Event()
    timed_out = {"value": False}

    def _watch() -> None:
        if done.wait(limit):
            return
        timed_out["value"] = True
        _logger.error(
            "spark_job_timeout: operation=%s limit=%ss — cancelling Spark jobs",
            operation,
            limit,
        )
        try:
            spark.sparkContext.cancelAllJobs()
        except Exception as e:
            _logger.warning("spark_job_timeout_cancel_failed: %s", e)

    watcher = threading.Thread(target=_watch, name=f"spark-timeout-{operation}", daemon=True)
    watcher.start()
    try:
        yield
    finally:
        done.set()
        watcher.join(timeout=2.0)
    if timed_out["value"]:
        raise SparkJobTimeoutError(
            f"{operation} 超過 {limit} 秒已強制取消 Spark 工作。"
            "請縮小批次或調高 SPARK_JOB_TIMEOUT_SECONDS。"
        )


def run_callable_with_timeout(
    fn: Callable[[], T],
    *,
    timeout_seconds: int,
    operation: str = "task",
) -> T:
    """通用 callable 逾時（本機 driver 用）。"""
    limit = int(timeout_seconds or 0)
    if limit <= 0:
        return fn()
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"timeout-{operation}") as pool:
        fut = pool.submit(fn)
        try:
            return fut.result(timeout=limit)
        except FuturesTimeout as e:
            raise TimeoutError(f"{operation} 超過 {limit} 秒。") from e


def run_ocr_with_timeout(
    fn: Callable[[], dict[str, str] | None],
    *,
    timeout_seconds: int | None = None,
) -> dict[str, str] | None:
    """單張 OCR 逾時；逾時回 OCR_ERROR_TIMEOUT 列（不拋出）。"""
    limit = int(timeout_seconds if timeout_seconds is not None else OCR_TIMEOUT_SECONDS)
    if limit <= 0:
        return fn()
    try:
        return run_callable_with_timeout(fn, timeout_seconds=limit, operation="ocr_image")
    except TimeoutError:
        _logger.warning("ocr_image_timeout: limit=%ss", limit)
        return None
