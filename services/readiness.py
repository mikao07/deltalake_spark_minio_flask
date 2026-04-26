"""
即時就緒／依賴檢查（供 GET /ready），不寫 log 檔；細節除錯仍靠應用程式 logging。

設計要點：
- MinIO：預設一定檢查（bucket 存在、可連線）。
- Spark：預設不檢查（避免 JVM 冷啟動拖慢探針）；可透過查詢參數或環境變數啟用。
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

from config import BUCKET_NAME
from services.minio_upload import get_minio_client


def resolve_include_spark(request_arg: str | None) -> bool:
    """
    查詢參數 `spark` 有給值時優先於環境變數 READY_CHECK_INCLUDE_SPARK。
    空字串視為「未指定」，改看環境變數。
    """
    if request_arg is not None and str(request_arg).strip() != "":
        v = str(request_arg).strip().lower()
        if v in ("0", "false", "no", "off"):
            return False
        if v in ("1", "true", "yes", "on"):
            return True
        return False
    return os.getenv("READY_CHECK_INCLUDE_SPARK", "false").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def check_minio_readiness() -> dict[str, Any]:
    t0 = time.perf_counter()
    ms = lambda: round((time.perf_counter() - t0) * 1000.0, 2)

    if not (os.getenv("MINIO_ACCESS_KEY") or "").strip() or not (
        os.getenv("MINIO_SECRET_KEY") or ""
    ).strip():
        return {
            "status": "error",
            "error": "缺少 MINIO_ACCESS_KEY 或 MINIO_SECRET_KEY",
            "latency_ms": ms(),
        }

    try:
        client = get_minio_client()
        if not client.bucket_exists(BUCKET_NAME):
            return {
                "status": "error",
                "error": f"bucket 不存在: {BUCKET_NAME}",
                "bucket": BUCKET_NAME,
                "latency_ms": ms(),
            }
        return {"status": "ok", "bucket": BUCKET_NAME, "latency_ms": ms()}
    except Exception as e:
        return {"status": "error", "error": str(e), "latency_ms": ms()}


def check_spark_readiness(spark) -> dict[str, Any]:
    t0 = time.perf_counter()
    ms = lambda: round((time.perf_counter() - t0) * 1000.0, 2)
    try:
        n = int(spark.range(1).count())
        if n != 1:
            return {
                "status": "error",
                "error": f"預期 count=1，實際為 {n}",
                "latency_ms": ms(),
            }
        return {"status": "ok", "latency_ms": ms()}
    except Exception as e:
        return {"status": "error", "error": str(e), "latency_ms": ms()}


def build_ready_payload(
    *,
    include_spark: bool,
    get_spark,
) -> tuple[str, dict[str, Any]]:
    """
    回傳 (overall_status, body_dict)。
    overall_status: ok | down
    """
    checked_at = datetime.now(timezone.utc).isoformat()
    checks: dict[str, Any] = {
        "minio": check_minio_readiness(),
    }

    minio_ok = checks["minio"].get("status") == "ok"
    overall = "ok" if minio_ok else "down"

    if include_spark:
        if not minio_ok:
            checks["spark"] = {
                "status": "skipped",
                "reason": "minio 未通過，略過 Spark 檢查",
            }
        else:
            try:
                spark = get_spark()
                checks["spark"] = check_spark_readiness(spark)
            except Exception as e:
                checks["spark"] = {
                    "status": "error",
                    "error": str(e),
                }
            if checks["spark"].get("status") != "ok":
                overall = "down"
    else:
        checks["spark"] = {"status": "skipped", "reason": "未啟用（見 ?spark= 或 READY_CHECK_INCLUDE_SPARK）"}

    body: dict[str, Any] = {
        "status": overall,
        "checked_at": checked_at,
        "checks": checks,
    }
    return overall, body
