"""P4 執行期節流：逾時守衛與 OCR repartition 接口。"""

import time
from unittest.mock import MagicMock

import pytest

from services import etl_runtime_limits as limits
from services.etl_runtime_limits import (
    SparkJobTimeoutError,
    apply_ocr_repartition,
    resolve_ocr_repartition,
    run_ocr_with_timeout,
    spark_job_timeout_guard,
)
from services.ocr_spark import _bronze_p4_runtime_meta


def test_resolve_ocr_repartition_default_zero(monkeypatch):
    monkeypatch.delenv("OCR_REPARTITION", raising=False)
    assert resolve_ocr_repartition(None) == max(0, int(limits.OCR_REPARTITION or 0))


def test_resolve_ocr_repartition_from_env(monkeypatch):
    monkeypatch.setattr(limits, "OCR_REPARTITION", 4)
    assert resolve_ocr_repartition() == 4
    assert resolve_ocr_repartition(8) == 8


def test_apply_ocr_repartition_zero_skips(monkeypatch):
    monkeypatch.setenv("OCR_REPARTITION", "0")
    df = MagicMock()
    out, n = apply_ocr_repartition(df)
    assert out is df
    assert n == 0
    df.repartition.assert_not_called()


def test_apply_ocr_repartition_positive(monkeypatch):
    monkeypatch.setenv("OCR_REPARTITION", "0")
    df = MagicMock()
    repartitioned = MagicMock()
    df.repartition.return_value = repartitioned
    out, n = apply_ocr_repartition(df, n=3)
    df.repartition.assert_called_once_with(3)
    assert out is repartitioned
    assert n == 3


def test_spark_job_timeout_guard_disabled_when_zero():
    spark = MagicMock()
    with spark_job_timeout_guard(spark, operation="test", timeout_seconds=0):
        pass
    spark.sparkContext.cancelAllJobs.assert_not_called()


def test_spark_job_timeout_guard_raises_on_timeout():
    spark = MagicMock()
    with pytest.raises(SparkJobTimeoutError, match="test"):
        with spark_job_timeout_guard(spark, operation="test", timeout_seconds=1):
            time.sleep(1.5)
    spark.sparkContext.cancelAllJobs.assert_called()


def test_run_ocr_with_timeout_returns_none_on_slow_call(monkeypatch):
    monkeypatch.setenv("OCR_TIMEOUT_SECONDS", "1")

    def slow():
        time.sleep(1.5)
        return {"extracted_text": "ok", "ocr_signature": "sig"}

    result = run_ocr_with_timeout(slow, timeout_seconds=1)
    assert result is None


def test_bronze_p4_runtime_meta_shape(monkeypatch):
    monkeypatch.setenv("OCR_TIMEOUT_SECONDS", "30")
    monkeypatch.setenv("SPARK_JOB_TIMEOUT_SECONDS", "300")
    monkeypatch.setenv("OCR_REPARTITION", "0")
    meta = _bronze_p4_runtime_meta(ocr_repartition_applied=0)
    assert meta["ocr_timeout_seconds"] == 30
    assert meta["spark_job_timeout_seconds"] == 300
    assert meta["ocr_repartition_config"] == 0
    assert meta["ocr_repartition_applied"] == 0
