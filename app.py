from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Callable, Dict, List, Optional

from flask import Flask, jsonify, redirect, render_template, request, url_for, Response
from werkzeug.exceptions import HTTPException

# 本機用 `python app.py` 啟動時，Python 不會自動載入 `.env`（Docker Compose 才會）。
# 若有安裝 python-dotenv，則在啟動時自動讀取專案根目錄的 `.env`。
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    pass

from config import (
    BUCKET_NAME,
    BRONZE_QUARANTINE_PATH,
    BRONZE_TABLE_PATH,
    GOLD_TOPIC_SNAPSHOT_PATH,
    GOLD_TFIDF_KEYWORDS_PATH,
    GOLD_PHRASE_CANDIDATES_PATH,
    MINIO_ENDPOINT,
    OCR_AB_MAX_SAMPLE_SIZE,
    OCR_AB_RESULTS_PATH,
    OCR_AB_SAMPLE_SIZE,
    RAW_IMAGE_PREFIX,
    RAW_IMAGES_PATH,
    SILVER_OCR_TABLE_PATH,
)
from services.minio_upload import (
    count_raw_image_objects_for_dataset,
    ensure_bucket,
    get_minio_client,
    list_dataset_ids,
    normalize_dataset_id,
    upload_file_bytes,
)
from services.readiness import build_ready_payload, resolve_include_spark
from services.release_contract import load_release_context
from services.async_jobs import job_registry, job_to_public_dict
from services.etl_metrics import append_etl_metric, read_etl_metrics
from services.ocr_spark import preview_raw_images_sample, run_bronze_ocr_ingest, normalize_psm
from services.ocr_psm_ab import (
    default_keyword_hints,
    delete_test_results_prefix,
    load_latest_ab_results,
    list_sample_image_paths,
    read_raw_image_bytes,
    run_ocr_psm_ab,
)
from services.bronze_quarantine import BronzeQuarantineError
from services.silver_quality import SilverQualityError
from services.spark_service import (
    SparkManager,
    analyze_bronze_duplicates,
    add_etl_timestamp,
    deduplicate_bronze_table,
    delete_older_than_latest_batch,
    get_bronze_data,
    get_bronze_quarantine_data,
    get_dictionary_usage_status,
    get_gold_delta_table_preview,
    get_gold_topic_snapshot_comparison,
    list_gold_topic_snapshots,
    get_gold_topic_snapshot_at_data,
    get_gold_topic_snapshot_latest_data,
    get_gold_tfidf_keywords_data,
    get_gold_phrase_candidates_data,
    get_silver_ocr_data,
    get_system_status,
    merge_upsert_by_key,
    records_to_df,
    read_delta_table,
    run_silver_ocr_etl,
    run_gold_etl,
    run_gold_corpus_analytics_etl,
    run_gold_topic_snapshot_rebuild_etl,
    delete_gold_topic_snapshot_rows,
    count_silver_distinct_image_paths,
)

app = Flask(__name__)

# 僅使用此目錄下單一檔名，避免 Windows 路徑別名造成「兩份 index」誤改。
TEMPLATE_INDEX = "index.html"
TEMPLATE_LAYERS = "layers.html"
TEMPLATE_PIPELINE_BRONZE = "pipeline_bronze.html"
TEMPLATE_TEST_OCR_PSM = "test_ocr_psm.html"
TEMPLATE_PIPELINE_SILVER = "pipeline_silver.html"
TEMPLATE_PIPELINE_GOLD = "pipeline_gold.html"

_logger = logging.getLogger("car_rental_flask_spark_delta")
if not _logger.handlers:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

_spark_manager: Optional[SparkManager] = None


def _record_etl_metric(payload: Dict[str, Any]) -> None:
    """
    ETL 指標落地：失敗只記 warning，不影響主流程。
    """
    try:
        append_etl_metric(payload)
    except Exception as e:
        _logger.warning("etl_metric_write_failed: %s", e)


def _json_error(message: str, status_code: int = 400, **extra: Any):
    payload: Dict[str, Any] = {"error": message, **extra}
    return jsonify(payload), status_code


def _default_allowed_delta_prefixes() -> List[str]:
    # 以 bucket 作為最低限度的預設白名單（避免任意路徑讀寫刪）
    return [f"s3a://{BUCKET_NAME.strip('/')}/"]


def _get_allowed_delta_prefixes() -> List[str]:
    raw = os.getenv("ALLOWED_DELTA_PATH_PREFIXES", "").strip()
    if not raw:
        return _default_allowed_delta_prefixes()
    prefixes = [p.strip() for p in raw.split(",") if p.strip()]
    return prefixes or _default_allowed_delta_prefixes()


def _validate_delta_path(path: str):
    prefixes = _get_allowed_delta_prefixes()
    if not any(path.startswith(p) for p in prefixes):
        return _json_error(
            "不允許的 table_path/target_path（不在白名單 prefix 內）。",
            403,
            allowed_prefixes=prefixes,
        )
    return None


def _get_admin_token_required() -> Optional[str]:
    val = os.getenv("ADMIN_TOKEN")
    return val.strip() if val and val.strip() else None


def _require_admin_token_if_configured():
    required = _get_admin_token_required()
    if not required:
        return None
    provided = request.headers.get("X-Admin-Token", "").strip()
    if provided != required:
        return _json_error("未授權：缺少或錯誤的管理 token。", 401)
    return None


def _parse_request_json_object() -> Dict[str, Any]:
    """
    解析 JSON body。
    若客戶端未設 Content-Type: application/json，get_json 常得到空 dict；
    此時改讀原始字串再 json.loads（方便 curl / 某些腳本）。
    """
    data = request.get_json(silent=True)
    if isinstance(data, dict) and data:
        return data
    raw = request.get_data(as_text=True)
    if raw and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass
    return {}


def _merge_missing_from_query(body: Dict[str, Any], keys: tuple[str, ...]) -> Dict[str, Any]:
    """body 缺欄位時，從 URL 查詢字串補上（與 JSON 合併，方便 PowerShell / curl）。"""
    out = dict(body)
    for k in keys:
        cur = out.get(k)
        empty = cur is None or (isinstance(cur, str) and not str(cur).strip())
        if not empty:
            continue
        q = request.args.get(k)
        if q is not None and str(q).strip() != "":
            out[k] = q.strip()
    return out


def _parse_bool_loose(val: Any) -> Optional[bool]:
    if isinstance(val, bool):
        return val
    if val is None:
        return None
    s = str(val).strip().lower()
    if s in ("true", "1", "yes", "y", "on"):
        return True
    if s in ("false", "0", "no", "n", "off"):
        return False
    return None


def _body_get_bool(body: Dict[str, Any], key: str, default: bool) -> bool:
    if key not in body:
        return default
    v = body.get(key)
    if isinstance(v, bool):
        return v
    p = _parse_bool_loose(v)
    return default if p is None else p


def _body_get_int(body: Dict[str, Any], key: str, default: int) -> int:
    if key not in body:
        return default
    v = body.get(key)
    if isinstance(v, bool):
        raise ValueError(f"{key} 必須是整數。")
    if isinstance(v, int):
        return v
    return int(str(v).strip())


def _get_spark_manager() -> SparkManager:
    """
    延遲初始化 Spark（避免 /health、/api/status 等不需要 Spark 的路由也強制啟動）。
    注意：每個 process 仍會各自持有一個 SparkSession。
    """
    global _spark_manager
    if _spark_manager is None:
        _spark_manager = SparkManager()
    return _spark_manager


def _noop_progress(_step: int, _total: int, _msg: str) -> None:
    return None


def _execute_pipeline_to_gold_inner(
    *,
    dataset_id: str,
    raw_images_path: str,
    write_mode: str,
    coalesce_partitions: int,
    skip_gold_if_no_new_ocr: bool,
    progress: Callable[[int, int, str], None],
) -> Dict[str, Any]:
    started = time.perf_counter()
    spark = _get_spark_manager().spark
    try:
        progress(1, 3, "銅層 Bronze OCR…")
        bronze_result = run_bronze_ocr_ingest(
            spark,
            raw_images_path=raw_images_path,
            bronze_path=BRONZE_TABLE_PATH,
            write_mode=write_mode,
            dataset_id=dataset_id,
        )
        bronze_processed_rows = int(bronze_result.get("processed_rows") or 0)
        if skip_gold_if_no_new_ocr and bronze_processed_rows <= 0:
            out = {
                "status": "ok",
                "dataset_id": dataset_id,
                "steps": ["bronze_ocr"],
                "raw_images_path": raw_images_path,
                "bronze_path": BRONZE_TABLE_PATH,
                "silver_ocr_path": SILVER_OCR_TABLE_PATH,
                "tfidf_path": GOLD_TFIDF_KEYWORDS_PATH,
                "write_mode": write_mode,
                "coalesce_partitions": coalesce_partitions,
                "skip_gold_if_no_new_ocr": skip_gold_if_no_new_ocr,
                "is_incremental_short_circuit": True,
                "summary": "本次沒有新增 OCR 資料，已跳過 Silver/Gold 重算。",
                "bronze_result": bronze_result,
                "silver_result": {"updated_rows": 0, "skipped": True},
                "gold_result": {
                    "tfidf_output_rows": 0,
                    "is_gold_written": False,
                    "skipped": True,
                    "summary": "本次沒有新增 OCR 資料，已跳過 Gold 重算。",
                },
            }
            _record_etl_metric(
                {
                    "etl_name": "pipeline_to_gold",
                    "dataset_id": dataset_id,
                    "status": "ok",
                    "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                    "bronze_processed_rows": bronze_processed_rows,
                    "silver_updated_rows": 0,
                    "gold_output_rows": 0,
                    "tfidf_output_rows": 0,
                    "is_gold_written": False,
                    "is_incremental_short_circuit": True,
                }
            )
            return out
        progress(2, 3, "銀層 Silver ETL…")
        silver_result = run_silver_ocr_etl(
            bronze_path=BRONZE_TABLE_PATH,
            silver_ocr_path=SILVER_OCR_TABLE_PATH,
            dataset_id=dataset_id,
        )
        progress(3, 3, "金層 Gold ETL…")
        gold_result = run_gold_etl(
            silver_ocr_path=SILVER_OCR_TABLE_PATH,
            dataset_id=dataset_id,
            coalesce_partitions=coalesce_partitions,
            silver_batch_ts=silver_result.get("silver_batch_ts"),
            prefer_incremental=bool(silver_result.get("inserted_rows", 0) > 0),
            force_full_recompute=bool(silver_result.get("updated_existing_rows", 0) > 0),
        )
        out = {
            "status": "ok",
            "dataset_id": dataset_id,
            "steps": ["bronze_ocr", "silver_ocr", "gold_etl"],
            "raw_images_path": raw_images_path,
            "bronze_path": BRONZE_TABLE_PATH,
            "silver_ocr_path": SILVER_OCR_TABLE_PATH,
            "tfidf_path": GOLD_TFIDF_KEYWORDS_PATH,
            "write_mode": write_mode,
            "coalesce_partitions": coalesce_partitions,
            "skip_gold_if_no_new_ocr": skip_gold_if_no_new_ocr,
            "bronze_result": bronze_result,
            "silver_result": silver_result,
            "gold_result": gold_result,
        }
        _record_etl_metric(
            {
                "etl_name": "pipeline_to_gold",
                "dataset_id": dataset_id,
                "status": "ok",
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                "bronze_processed_rows": bronze_result.get("processed_rows"),
                "silver_updated_rows": silver_result.get("updated_rows"),
                "gold_output_rows": gold_result.get("tfidf_output_rows"),
                "tfidf_output_rows": gold_result.get("tfidf_output_rows"),
                "is_gold_written": gold_result.get("is_gold_written"),
                "topic_output_rows": gold_result.get("topic_output_rows"),
                "topic_frequency_top": gold_result.get("topic_frequency_top"),
                "gold_recompute_mode": gold_result.get("gold_recompute_mode"),
            }
        )
        return out
    except Exception as e:
        _record_etl_metric(
            {
                "etl_name": "pipeline_to_gold",
                "dataset_id": dataset_id,
                "status": "failed",
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                "error": str(e),
            }
        )
        raise


def _execute_bronze_ocr_inner(
    *,
    dataset_id: Optional[str],
    raw_images_path: str,
    bronze_path: str,
    write_mode: str,
    image_paths: Optional[List[str]] = None,
    progress: Callable[[int, int, str], None],
) -> Dict[str, Any]:
    started = time.perf_counter()
    spark = _get_spark_manager().spark
    try:
        progress(1, 1, "Bronze OCR（讀圖、Tesseract）…")
        bronze_result = run_bronze_ocr_ingest(
            spark,
            raw_images_path=raw_images_path,
            bronze_path=bronze_path,
            write_mode=write_mode,
            dataset_id=dataset_id,
            image_paths=image_paths,
        )
        out = {
            "status": "ok",
            "dataset_id": dataset_id,
            "raw_images_path": raw_images_path,
            "bronze_path": bronze_path,
            "write_mode": write_mode,
            "bronze_result": bronze_result,
        }
        _record_etl_metric(
            {
                "etl_name": "bronze_ocr",
                "dataset_id": dataset_id,
                "status": "ok",
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                "input_rows": bronze_result.get("input_rows"),
                "output_rows": bronze_result.get("processed_rows"),
            }
        )
        return out
    except Exception as e:
        _record_etl_metric(
            {
                "etl_name": "bronze_ocr",
                "dataset_id": dataset_id,
                "status": "failed",
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                "error": str(e),
            }
        )
        raise


@app.before_request
def _start_timer():
    request._start_time = time.perf_counter()


@app.after_request
def _log_request(response):
    try:
        dur_ms = (time.perf_counter() - getattr(request, "_start_time", time.perf_counter())) * 1000.0
        _logger.info(
            "request method=%s path=%s status=%s dur_ms=%.2f",
            request.method,
            request.path,
            response.status_code,
            dur_ms,
        )
    except Exception:
        # 不要因 logging 失敗影響回應
        pass
    return response


@app.errorhandler(Exception)
def _handle_unexpected_error(e: Exception):
    if isinstance(e, HTTPException):
        return e

    _logger.exception("unhandled_error path=%s", request.path)
    # API 以 JSON 回傳，頁面路由則保留預設行為（讓 Flask 顯示 500 頁）
    if (
        request.path.startswith("/api/")
        or request.path.startswith("/delta/")
    ):
        return _json_error("伺服器內部錯誤。", 500)
    return "伺服器內部錯誤。", 500


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ready")
def ready():
    """
    依賴就緒檢查（JSON）。預設只驗證 MinIO；Spark 需 ?spark=true 或 READY_CHECK_INCLUDE_SPARK。
    失敗時 HTTP 503，成功 200。（與輕量 GET /health 分離，避免探針每秒打重檢查。）
    """
    include_spark = resolve_include_spark(request.args.get("spark"))
    overall, body = build_ready_payload(
        include_spark=include_spark,
        get_spark=lambda: _get_spark_manager().spark,
    )
    code = 200 if overall == "ok" else 503
    return jsonify(body), code


def _safe_bronze_quarantine_preview(
    dataset_id: str | None = None,
    *,
    limit: int = 15,
    newest_first: bool = True,
):
    try:
        return get_bronze_quarantine_data(limit=limit, dataset_id=dataset_id, newest_first=newest_first), None
    except Exception as e:
        _logger.warning("bronze_quarantine_preview_failed: %s", e)
        return [], str(e)


def _safe_bronze_preview(
    dataset_id: str | None = None,
    *,
    limit: int = 10,
    newest_first: bool = True,
):
    try:
        return get_bronze_data(limit=limit, dataset_id=dataset_id, newest_first=newest_first), None
    except Exception as e:
        _logger.warning("bronze_preview_failed: %s", e)
        return [], str(e)


def _safe_tfidf_preview(limit: int = 15, dataset_id: str | None = None):
    try:
        return get_gold_tfidf_keywords_data(limit=limit, dataset_id=dataset_id), None
    except Exception as e:
        _logger.warning("tfidf_preview_failed: %s", e)
        return [], str(e)


def _safe_phrase_preview(limit: int = 15, dataset_id: str | None = None):
    try:
        return get_gold_phrase_candidates_data(limit=limit, dataset_id=dataset_id), None
    except Exception as e:
        _logger.warning("phrase_preview_failed: %s", e)
        return [], str(e)


def _safe_silver_preview(
    limit: int = 30,
    dataset_id: str | None = None,
    *,
    newest_first: bool = True,
):
    try:
        return get_silver_ocr_data(limit=limit, dataset_id=dataset_id, newest_first=newest_first), None
    except Exception as e:
        _logger.warning("silver_preview_failed: %s", e)
        return [], str(e)


def _safe_gold_disk_preview(
    limit: int = 50,
    dataset_id: str | None = None,
    *,
    newest_first: bool = True,
):
    try:
        return get_gold_delta_table_preview(
            limit=limit,
            dataset_id=dataset_id,
            newest_first=newest_first,
        ), None
    except Exception as e:
        _logger.warning("gold_disk_preview_failed: %s", e)
        return [], str(e)


def _parse_pipeline_dataset_context(step: str) -> Dict[str, Any]:
    """管線分頁共用：dataset 選項與目前 pipeline_step。"""
    dataset_raw = request.args.get("dataset_id", "").strip().lower()
    selected_dataset_id: str | None = None
    if dataset_raw:
        try:
            selected_dataset_id = normalize_dataset_id(dataset_raw)
        except ValueError:
            selected_dataset_id = None

    dataset_options: list[str] = []
    try:
        dataset_options = list_dataset_ids()
    except Exception as e:
        _logger.warning("list_dataset_ids_for_pipeline_failed: %s", e)

    return {
        "pipeline_step": step,
        "dataset_options": dataset_options,
        "selected_dataset_id": selected_dataset_id,
    }


def _latest_etl_metric_for(etl_name: str, dataset_id: str | None) -> Dict[str, Any] | None:
    rows = read_etl_metrics(limit=20, dataset_id=dataset_id or None)
    for row in rows:
        if row.get("etl_name") == etl_name:
            return row
    return None


def _bronze_quarantine_for_metric(payload: Any) -> Dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    if payload.get("skipped"):
        return {
            "skipped": True,
            "reason": payload.get("reason"),
            "passed": True,
        }
    return {
        "passed": bool(payload.get("passed")),
        "reject_rate": payload.get("reject_rate"),
        "reject_rows": payload.get("reject_rows"),
        "total_rows": payload.get("total_rows"),
        "ok_rows": payload.get("ok_rows"),
        "analyzed_rows": payload.get("analyzed_rows"),
        "by_status": dict(payload.get("by_status") or {}),
        "quarantined_rows_written": payload.get("quarantined_rows_written"),
        "quarantine_path": payload.get("quarantine_path"),
        "melted": payload.get("melted"),
        "melt_action": payload.get("melt_action"),
        "melt_reason": payload.get("melt_reason"),
        "message": payload.get("message"),
        "high_reject_rate": payload.get("high_reject_rate"),
        "approve_blocked": payload.get("approve_blocked"),
    }


def _corpus_coverage_for_dataset(dataset_id: str | None) -> Dict[str, Any] | None:
    """MinIO 原圖數 vs Silver 已分析圖數（母體完整度）。"""
    if not dataset_id:
        return None
    try:
        raw_count = count_raw_image_objects_for_dataset(dataset_id)
        silver_count = count_silver_distinct_image_paths(_get_spark_manager().spark, dataset_id)
        return {
            "raw_image_count": raw_count,
            "analyzed_image_count": silver_count,
            "coverage_label": f"有效 OCR：{silver_count}/{raw_count}",
        }
    except Exception as e:
        _logger.warning("corpus_coverage_failed: %s", e)
        return {"error": str(e), "dataset_id": dataset_id}


def _silver_quality_for_metric(quality: Any) -> Dict[str, Any] | None:
    """精簡 silver_quality 供 JSONL／頁面顯示。"""
    if not isinstance(quality, dict):
        return None
    if quality.get("skipped"):
        return {
            "skipped": True,
            "reason": quality.get("reason"),
            "passed": True,
        }
    return {
        "passed": bool(quality.get("passed")),
        "transform_version": quality.get("transform_version"),
        "total_rows": quality.get("total_rows"),
        "warnings": list(quality.get("warnings") or []),
        "hard_failures": list(quality.get("hard_failures") or []),
        "checks": list(quality.get("checks") or []),
    }


def _latest_silver_quality_for_dataset(dataset_id: str | None) -> Dict[str, Any] | None:
    metric = _latest_etl_metric_for("silver_ocr_etl", dataset_id)
    if not metric:
        return None
    q = metric.get("silver_quality")
    return q if isinstance(q, dict) else None


@app.get("/upload")
def upload_page():
    """相容舊連結：導向銅層管線頁。"""
    qs = request.query_string.decode()
    target = url_for("pipeline_bronze_page")
    if qs:
        return redirect(f"{target}?{qs}")
    return redirect(target)


@app.get("/pipeline/bronze")
def pipeline_bronze_page():
    ctx = _parse_pipeline_dataset_context("bronze")
    bronze_rows, bronze_error = _safe_bronze_preview(
        limit=10,
        dataset_id=ctx.get("selected_dataset_id"),
        newest_first=True,
    )
    return render_template(
        TEMPLATE_PIPELINE_BRONZE,
        bronze_path=BRONZE_TABLE_PATH,
        bronze_rows=bronze_rows,
        bronze_error=bronze_error,
        **ctx,
    )


@app.get("/pipeline/silver")
def pipeline_silver_page():
    ctx = _parse_pipeline_dataset_context("silver")
    selected = ctx.get("selected_dataset_id")
    preview_limit = 15
    silver_rows, silver_error = _safe_silver_preview(
        limit=preview_limit,
        dataset_id=selected,
        newest_first=True,
    )
    bronze_rows, _ = _safe_bronze_preview(limit=1, dataset_id=selected, newest_first=True)
    quarantine_rows, quarantine_error = _safe_bronze_quarantine_preview(
        limit=preview_limit,
        dataset_id=selected,
        newest_first=True,
    )
    dictionary_status: Dict[str, Any] = {}
    try:
        dictionary_status = get_dictionary_usage_status(_get_spark_manager().spark, selected)
    except Exception as e:
        _logger.warning("dictionary_status_check_failed: %s", e)
        dictionary_status = {
            "dataset_id": selected,
            "jieba_userdict_used": False,
            "stopwords_used": False,
            "builtin_stopwords_count": 0,
            "error": str(e),
        }
    return render_template(
        TEMPLATE_PIPELINE_SILVER,
        silver_path=SILVER_OCR_TABLE_PATH,
        silver_rows=silver_rows,
        silver_error=silver_error,
        preview_limit=preview_limit,
        bronze_has_rows=bool(bronze_rows) if selected else None,
        dictionary_status=dictionary_status,
        latest_silver_metric=_latest_etl_metric_for("silver_ocr_etl", selected),
        latest_silver_quality=_latest_silver_quality_for_dataset(selected),
        bronze_quarantine_path=BRONZE_QUARANTINE_PATH,
        quarantine_rows=quarantine_rows,
        quarantine_error=quarantine_error,
        **ctx,
    )


@app.get("/pipeline/gold")
def pipeline_gold_page():
    ctx = _parse_pipeline_dataset_context("gold")
    selected = ctx.get("selected_dataset_id")
    preview_limit = 15
    gold_disk_rows, gold_disk_error = _safe_gold_disk_preview(
        limit=preview_limit,
        dataset_id=selected,
        newest_first=True,
    )
    gold_disk_hint = None
    if not gold_disk_error and not gold_disk_rows and selected:
        gold_disk_hint = "已選 dataset 但 Gold 表無對應列；請確認已執行金層 ETL 且 dataset_id 欄位一致。"
    return render_template(
        TEMPLATE_PIPELINE_GOLD,
        gold_path=GOLD_TFIDF_KEYWORDS_PATH,
        gold_disk_rows=gold_disk_rows,
        gold_disk_error=gold_disk_error,
        gold_disk_hint=gold_disk_hint,
        preview_limit=preview_limit,
        latest_gold_metric=_latest_etl_metric_for("gold_etl", selected),
        **ctx,
    )


def _guess_image_mimetype(image_path: str) -> str:
    p = str(image_path or "").lower()
    if p.endswith(".png"):
        return "image/png"
    if p.endswith(".gif"):
        return "image/gif"
    if p.endswith(".webp"):
        return "image/webp"
    if p.endswith(".bmp"):
        return "image/bmp"
    if p.endswith((".tif", ".tiff")):
        return "image/tiff"
    return "image/jpeg"


def _validate_raw_image_path(image_path: str):
    err = _validate_delta_path(image_path)
    if err:
        return err
    norm = str(image_path).replace("\\", "/").lower()
    raw_prefix = RAW_IMAGES_PATH.replace("\\", "/").lower().rstrip("/") + "/"
    if not norm.startswith(raw_prefix):
        return _json_error("image_path 必須位於 RAW_IMAGES_PATH 底下。", 403)
    return None


@app.get("/test/ocr-psm")
def test_ocr_psm_page():
    ctx = _parse_pipeline_dataset_context("ocr_ab")
    selected = ctx.get("selected_dataset_id")
    latest_result: Dict[str, Any] | None = None
    sample_paths: List[Dict[str, Any]] = []
    sample_error: str | None = None
    if selected:
        try:
            latest_result = load_latest_ab_results(selected)
        except Exception as e:
            _logger.warning("ocr_ab_latest_load_failed: %s", e)
        try:
            raw_path = f"{RAW_IMAGES_PATH.rstrip('/')}/{selected}/"
            err = _validate_delta_path(raw_path)
            if err:
                sample_error = "raw 路徑不在白名單"
            else:
                sample_paths = list_sample_image_paths(
                    _get_spark_manager().spark,
                    raw_path,
                    limit=OCR_AB_SAMPLE_SIZE,
                )
        except Exception as e:
            _logger.warning("ocr_ab_sample_list_failed: %s", e)
            sample_error = str(e)

    return render_template(
        TEMPLATE_TEST_OCR_PSM,
        results_path=OCR_AB_RESULTS_PATH,
        sample_size_default=OCR_AB_SAMPLE_SIZE,
        sample_size_max=OCR_AB_MAX_SAMPLE_SIZE,
        default_psm_a="11",
        default_psm_b="6",
        psm_options=["3", "4", "6", "7", "11", "13"],
        default_keywords=default_keyword_hints(),
        raw_images_hint=f"{RAW_IMAGES_PATH.rstrip('/')}/{{dataset_id}}/",
        latest_result=latest_result,
        sample_paths=sample_paths,
        sample_error=sample_error,
        **ctx,
    )


@app.post("/api/test/ocr-psm/run")
def api_test_ocr_psm_run():
    """固定樣本影像 PSM A/B；結果寫入 test 路徑，不寫 Bronze。"""
    auth_err = _require_admin_token_if_configured()
    if auth_err:
        return auth_err
    body = _merge_missing_from_query(
        _parse_request_json_object(),
        ("dataset_id", "psm_a", "psm_b", "sample_size"),
    )
    dataset_raw = str(body.get("dataset_id") or "").strip()
    if not dataset_raw:
        return _json_error("dataset_id 必填。", 400)
    try:
        dataset_id = normalize_dataset_id(dataset_raw)
    except ValueError as e:
        return _json_error(str(e), 400)
    try:
        psm_a = normalize_psm(str(body.get("psm_a") or "11"), default="11")
        psm_b = normalize_psm(str(body.get("psm_b") or "6"), default="6")
    except ValueError as e:
        return _json_error(str(e), 400)
    sample_size = OCR_AB_SAMPLE_SIZE
    if "sample_size" in body and body.get("sample_size") is not None:
        try:
            sample_size = _body_get_int(body, "sample_size", OCR_AB_SAMPLE_SIZE)
        except ValueError as e:
            return _json_error(str(e), 400)
    keywords_raw = body.get("keywords")
    keywords = None
    if isinstance(keywords_raw, list):
        keywords = [str(k).strip() for k in keywords_raw if str(k).strip()]
    elif isinstance(keywords_raw, str) and keywords_raw.strip():
        keywords = [ln.strip() for ln in keywords_raw.splitlines() if ln.strip()]

    raw_images_path = f"{RAW_IMAGES_PATH.rstrip('/')}/{dataset_id}/"
    err = _validate_delta_path(raw_images_path)
    if err:
        return err
    err = _validate_delta_path(OCR_AB_RESULTS_PATH)
    if err:
        return err

    try:
        payload = run_ocr_psm_ab(
            _get_spark_manager().spark,
            dataset_id=dataset_id,
            raw_images_path=raw_images_path,
            psm_a=psm_a,
            psm_b=psm_b,
            sample_size=sample_size,
            keywords=keywords,
            save_results=True,
        )
    except ValueError as e:
        return _json_error(str(e), 400)
    except Exception as e:
        _logger.exception("ocr_psm_ab_run_failed")
        return _json_error(f"OCR PSM A/B 失敗：{e}", 500)

    if int(payload.get("sample_size_actual") or 0) <= 0:
        return _json_error("找不到可 OCR 的樣本影像。", 404, **payload)
    return jsonify(payload), 200


@app.get("/api/test/ocr-psm/latest")
def api_test_ocr_psm_latest():
    dataset_raw = request.args.get("dataset_id", "").strip()
    if not dataset_raw:
        return _json_error("dataset_id 必填。", 400)
    try:
        dataset_id = normalize_dataset_id(dataset_raw)
    except ValueError as e:
        return _json_error(str(e), 400)
    try:
        payload = load_latest_ab_results(dataset_id)
    except Exception as e:
        _logger.exception("ocr_psm_ab_latest_failed")
        return _json_error(f"讀取失敗：{e}", 500)
    if not payload:
        return _json_error("尚無儲存的 A/B 結果。", 404, dataset_id=dataset_id)
    return jsonify(payload), 200


@app.delete("/api/test/ocr-psm/results")
def api_test_ocr_psm_delete():
    auth_err = _require_admin_token_if_configured()
    if auth_err:
        return auth_err
    dataset_raw = request.args.get("dataset_id", "").strip()
    dataset_id = None
    if dataset_raw:
        try:
            dataset_id = normalize_dataset_id(dataset_raw)
        except ValueError as e:
            return _json_error(str(e), 400)
    err = _validate_delta_path(OCR_AB_RESULTS_PATH)
    if err:
        return err
    try:
        result = delete_test_results_prefix(dataset_id=dataset_id)
    except Exception as e:
        _logger.exception("ocr_psm_ab_delete_failed")
        return _json_error(f"刪除失敗：{e}", 500)
    return jsonify(result), 200


@app.get("/api/test/ocr-psm/image")
def api_test_ocr_psm_image():
    image_path = (request.args.get("image_path") or "").strip()
    if not image_path:
        return _json_error("image_path 必填。", 400)
    err = _validate_raw_image_path(image_path)
    if err:
        return err
    try:
        data = read_raw_image_bytes(image_path)
    except ValueError as e:
        return _json_error(str(e), 400)
    except Exception as e:
        _logger.exception("ocr_psm_ab_image_failed")
        return _json_error(f"讀取影像失敗：{e}", 500)
    return Response(data, mimetype=_guess_image_mimetype(image_path))


@app.get("/")
def index():
    dataset_raw = request.args.get("dataset_id", "").strip().lower()
    selected_dataset_id: str | None = None
    if dataset_raw:
        try:
            selected_dataset_id = normalize_dataset_id(dataset_raw)
        except ValueError:
            selected_dataset_id = None

    dataset_options: list[str] = []
    try:
        dataset_options = list_dataset_ids()
    except Exception as e:
        _logger.warning("list_dataset_ids_for_index_failed: %s", e)

    sys_status = get_system_status()
    tfidf_rows, tfidf_error = _safe_tfidf_preview(limit=15, dataset_id=selected_dataset_id)
    phrase_rows, phrase_error = _safe_phrase_preview(limit=15, dataset_id=selected_dataset_id)
    topic_rows: List[Dict[str, Any]] = []
    topic_compare_rows: List[Dict[str, Any]] = []
    topic_snapshot_options: List[str] = []
    selected_topic_snapshots = [s.strip() for s in request.args.getlist("topic_snapshot") if s and s.strip()]
    topic_hint: str | None = None
    snapshot_mode_raw = request.args.get("snapshot_mode", "release").strip().lower()
    snapshot_mode = "preview" if snapshot_mode_raw == "preview" else "release"
    release_context: Dict[str, Any] = {}
    latest_snapshot_at: str | None = None

    if selected_dataset_id:
        try:
            release_context = load_release_context(selected_dataset_id)
        except Exception as e:
            _logger.warning("release_context_load_failed: %s", e)

    try:
        topic_snapshot_options = list_gold_topic_snapshots(dataset_id=selected_dataset_id, limit=30)
        if topic_snapshot_options:
            latest_snapshot_at = topic_snapshot_options[0]
        if selected_topic_snapshots:
            topic_compare_rows = get_gold_topic_snapshot_comparison(
                dataset_id=selected_dataset_id,
                snapshots=selected_topic_snapshots,
            )
        if snapshot_mode == "preview":
            topic_rows = get_gold_topic_snapshot_latest_data(limit=15, dataset_id=selected_dataset_id)
            topic_hint = "最新預覽：每次 Gold ETL 的最新 snapshot_at，未經核准，不可作為對外發行版。"
            if latest_snapshot_at:
                topic_hint += f" 快照時間：{latest_snapshot_at}"
        elif not selected_dataset_id:
            topic_rows = []
            topic_hint = "發行版需選擇 dataset_id（勿選「全部資料」）。"
        else:
            approved = release_context.get("approved_snapshot_at")
            if not approved:
                topic_rows = []
                topic_hint = (
                    "尚無發行版：manifest 未設定 approved_snapshot_at。"
                    "請執行 pipeline_guardian --approve-snapshot，或切換至「最新預覽」調參。"
                )
            else:
                topic_rows = get_gold_topic_snapshot_at_data(
                    approved,
                    limit=15,
                    dataset_id=selected_dataset_id,
                )
                if not topic_rows:
                    topic_hint = (
                        f"找不到核准快照資料（approved_snapshot_at={approved}），"
                        "請重跑 Gold 或重新核准。"
                    )
                else:
                    rid = release_context.get("release_id") or "-"
                    topic_hint = f"發行版 release_id={rid}，核准快照 {approved}"
                    pic = release_context.get("processed_image_count")
                    if pic is not None:
                        topic_hint += f"，發行水位 processed_image_count={pic}"
    except Exception as e:
        _logger.warning("topic_snapshot_preview_failed: %s", e)
        topic_rows = []
        topic_compare_rows = []

    corpus_coverage = _corpus_coverage_for_dataset(selected_dataset_id)

    return render_template(
        TEMPLATE_INDEX,
        cpu_percent=sys_status.get("cpu_percent"),
        memory_percent=sys_status.get("memory_percent"),
        dataset_options=dataset_options,
        selected_dataset_id=selected_dataset_id,
        tfidf_rows=tfidf_rows,
        tfidf_error=tfidf_error,
        phrase_rows=phrase_rows,
        phrase_error=phrase_error,
        topic_rows=topic_rows,
        topic_compare_rows=topic_compare_rows,
        topic_snapshot_options=topic_snapshot_options,
        selected_topic_snapshots=selected_topic_snapshots,
        topic_hint=topic_hint,
        snapshot_mode=snapshot_mode,
        release_context=release_context,
        latest_snapshot_at=latest_snapshot_at,
        corpus_coverage=corpus_coverage,
        tfidf_table_path=GOLD_TFIDF_KEYWORDS_PATH,
        phrase_table_path=GOLD_PHRASE_CANDIDATES_PATH,
        topic_snapshot_path=GOLD_TOPIC_SNAPSHOT_PATH,
        silver_ocr_table_path=SILVER_OCR_TABLE_PATH,
    )


@app.get("/layers")
def layers_preview_page():
    """
    獨立頁：預覽銅／銀／金層表格內容，方便對照 OCR → 銀層 → TF-IDF 何處異常（避免首頁過擠）。
    """
    dataset_raw = request.args.get("dataset_id", "").strip().lower()
    selected_dataset_id: str | None = None
    if dataset_raw:
        try:
            selected_dataset_id = normalize_dataset_id(dataset_raw)
        except ValueError:
            selected_dataset_id = None

    limit_raw = request.args.get("limit", "30")
    try:
        preview_limit = max(5, min(int(limit_raw), 100))
    except (TypeError, ValueError):
        preview_limit = 30
    sort_by_time = (request.args.get("sort_time", "desc") or "desc").strip().lower()
    newest_first = sort_by_time != "asc"

    dataset_options: list[str] = []
    try:
        dataset_options = list_dataset_ids()
    except Exception as e:
        _logger.warning("list_dataset_ids_for_layers_failed: %s", e)

    # 首次進入（網址未帶 dataset_id）預設 drinks，避免辭典／manifest 區塊空白。
    if "dataset_id" not in request.args and not selected_dataset_id:
        if "drinks" in dataset_options:
            selected_dataset_id = "drinks"

    bronze_rows, bronze_error = _safe_bronze_preview(
        limit=preview_limit,
        dataset_id=selected_dataset_id,
        newest_first=newest_first,
    )
    silver_rows, silver_error = _safe_silver_preview(
        limit=preview_limit,
        dataset_id=selected_dataset_id,
        newest_first=newest_first,
    )
    gold_disk_rows, gold_disk_error = _safe_gold_disk_preview(
        limit=preview_limit,
        dataset_id=selected_dataset_id,
        newest_first=newest_first,
    )
    etl_metrics_rows = read_etl_metrics(limit=12, dataset_id=selected_dataset_id or None)
    latest_etl_metric = etl_metrics_rows[0] if etl_metrics_rows else None
    dictionary_status: Dict[str, Any] = {}
    try:
        dictionary_status = get_dictionary_usage_status(_get_spark_manager().spark, selected_dataset_id)
    except Exception as e:
        _logger.warning("dictionary_status_check_failed: %s", e)
        dictionary_status = {
            "dataset_id": selected_dataset_id,
            "jieba_userdict_used": False,
            "jieba_userdict_path": "",
            "stopwords_used": False,
            "stopwords_path": "",
            "stopwords_count": 0,
            "builtin_stopwords_count": 0,
            "silver_tokenization": "jieba_with_builtin_stopwords",
            "error": str(e),
        }

    gold_disk_hint = None
    if not gold_disk_error and not gold_disk_rows:
        if selected_dataset_id:
            gold_disk_hint = (
                "已選定 dataset_id：落盤預覽只顯示 Gold 表內 dataset_id 與所選相符的列；若不符會空白。"
                "請改選「全部」後按更新，或確認該 id 已執行金層 ETL 且寫入欄位一致。"
            )
        else:
            gold_disk_hint = (
                "MinIO 若有很小的 part 檔仍可能 0 列（空 partition）。"
                "請用 GET /api/gold/tfidf-keywords?limit=20 確認，或確認金層 ETL 是否產出有效 TF-IDF 列。"
            )

    return render_template(
        TEMPLATE_LAYERS,
        dataset_options=dataset_options,
        selected_dataset_id=selected_dataset_id,
        preview_limit=preview_limit,
        sort_time=sort_by_time,
        bronze_path=BRONZE_TABLE_PATH,
        silver_path=SILVER_OCR_TABLE_PATH,
        gold_path=GOLD_TFIDF_KEYWORDS_PATH,
        bronze_rows=bronze_rows,
        bronze_error=bronze_error,
        silver_rows=silver_rows,
        silver_error=silver_error,
        gold_disk_rows=gold_disk_rows,
        gold_disk_error=gold_disk_error,
        gold_disk_hint=gold_disk_hint,
        latest_etl_metric=latest_etl_metric,
        etl_metrics_rows=etl_metrics_rows,
        dictionary_status=dictionary_status,
    )


@app.get("/api/status")
def api_status():
    return jsonify(get_system_status())


@app.get("/api/datasets")
def api_datasets():
    """列出 MinIO raw/images 下已存在的 dataset_id。"""
    err = _require_admin_token_if_configured()
    if err:
        return err
    try:
        datasets = list_dataset_ids()
    except Exception as e:
        _logger.warning("list_dataset_ids_failed: %s", e)
        return _json_error(f"讀取 dataset_id 清單失敗：{e}", 503)
    return jsonify({"datasets": datasets, "count": len(datasets)})


@app.get("/api/jobs/<job_id>")
def api_job_status(job_id: str):
    """查詢背景任務狀態（pipeline_to_gold / bronze_ocr 等）。須與建立任務時相同的管理憑證。"""
    err = _require_admin_token_if_configured()
    if err:
        return err
    r = job_registry.get(job_id)
    if not r:
        return _json_error(
            "找不到此任務（可能已過期，或若有多個 web worker 請求落到其他程序）。",
            404,
        )
    return jsonify(job_to_public_dict(r))


@app.get("/api/metrics/etl")
def api_etl_metrics():
    """
    查詢 ETL 指標（最新優先）。
    query:
      - limit: 預設 100，最大 1000
      - dataset_id: 可選
      - etl_name: 可選（bronze_ocr / silver_ocr_etl / gold_etl / pipeline_to_gold）
    """
    err = _require_admin_token_if_configured()
    if err:
        return err

    raw = request.args.get("limit", "100")
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        return _json_error("limit 必須是整數。", 400)
    limit = max(1, min(limit, 1000))

    dataset_raw = (request.args.get("dataset_id", "") or "").strip().lower()
    etl_name_raw = (request.args.get("etl_name", "") or "").strip()
    dataset_id = normalize_dataset_id(dataset_raw) if dataset_raw else None
    etl_name = etl_name_raw if etl_name_raw else None

    rows = read_etl_metrics(limit=limit, dataset_id=dataset_id, etl_name=etl_name)
    return jsonify(
        {
            "status": "ok",
            "count": len(rows),
            "limit": limit,
            "dataset_id": dataset_id,
            "etl_name": etl_name,
            "rows": rows,
        }
    )


@app.get("/api/debug/storage-check")
def api_debug_storage_check():
    """
    對比 MinIO SDK 與 Spark binaryFile 對同一路徑的可見檔案，快速定位環境不一致問題。
    query:
      - dataset_id（可選）
      - limit（可選，預設 10，最大 50）
    """
    err = _require_admin_token_if_configured()
    if err:
        return err

    raw = request.args.get("dataset_id", "").strip().lower()
    dataset_id: str | None = None
    if raw:
        try:
            dataset_id = normalize_dataset_id(raw)
        except ValueError as e:
            return _json_error(str(e), 400)

    lim_raw = request.args.get("limit", "10")
    try:
        limit = int(lim_raw)
    except (TypeError, ValueError):
        return _json_error("limit 必須是整數。", 400)
    limit = max(1, min(limit, 50))

    # 同步產生 Spark 端要讀的 s3a path
    spark_raw_path = RAW_IMAGES_PATH
    if dataset_id:
        spark_raw_path = f"{spark_raw_path.rstrip('/')}/{dataset_id}/"

    # MinIO SDK 端 prefix
    prefix = RAW_IMAGE_PREFIX.strip("/").strip()
    if dataset_id:
        prefix = f"{prefix}/{dataset_id}/"
    else:
        prefix = f"{prefix}/"

    minio_items: list[str] = []
    minio_err: str | None = None
    try:
        client = get_minio_client()
        ensure_bucket(client, BUCKET_NAME)
        for obj in client.list_objects(BUCKET_NAME, prefix=prefix, recursive=True):
            name = getattr(obj, "object_name", "") or ""
            if name:
                minio_items.append(name)
            if len(minio_items) >= limit:
                break
    except Exception as e:
        minio_err = str(e)

    spark_items: list[dict] = []
    spark_err: str | None = None
    try:
        spark = _get_spark_manager().spark
        spark_items = preview_raw_images_sample(spark, spark_raw_path, limit=limit)
    except Exception as e:
        spark_err = str(e)

    return jsonify(
        {
            "dataset_id": dataset_id,
            "config": {
                "MINIO_ENDPOINT": MINIO_ENDPOINT,
                "BUCKET_NAME": BUCKET_NAME,
                "RAW_IMAGE_PREFIX": RAW_IMAGE_PREFIX,
                "RAW_IMAGES_PATH": RAW_IMAGES_PATH,
            },
            "resolved": {
                "sdk_prefix": prefix,
                "spark_raw_images_path": spark_raw_path,
            },
            "minio_sdk": {
                "count": len(minio_items),
                "items": minio_items,
                "error": minio_err,
            },
            "spark_binaryfile": {
                "count": len(spark_items),
                "items": spark_items,
                "error": spark_err,
            },
        }
    )


@app.get("/api/health/storage")
def api_health_storage():
    """
    Storage 健康檢查（MinIO SDK vs Spark S3A 可見性）：
    - ok: 兩邊都能看到資料（或皆可連線且無錯）
    - degraded: MinIO SDK 有資料但 Spark S3A 看不到（通常需走 fallback）
    - down: 至少一邊發生連線/讀取錯誤且無法提供可用結果
    """
    err = _require_admin_token_if_configured()
    if err:
        return err

    raw = request.args.get("dataset_id", "").strip().lower()
    dataset_id: str | None = None
    if raw:
        try:
            dataset_id = normalize_dataset_id(raw)
        except ValueError as e:
            return _json_error(str(e), 400)

    lim_raw = request.args.get("limit", "10")
    try:
        limit = int(lim_raw)
    except (TypeError, ValueError):
        return _json_error("limit 必須是整數。", 400)
    limit = max(1, min(limit, 50))

    spark_raw_path = RAW_IMAGES_PATH
    if dataset_id:
        spark_raw_path = f"{spark_raw_path.rstrip('/')}/{dataset_id}/"

    prefix = RAW_IMAGE_PREFIX.strip("/").strip()
    if dataset_id:
        prefix = f"{prefix}/{dataset_id}/"
    else:
        prefix = f"{prefix}/"

    sdk_count = 0
    sdk_error: str | None = None
    try:
        client = get_minio_client()
        ensure_bucket(client, BUCKET_NAME)
        for _ in client.list_objects(BUCKET_NAME, prefix=prefix, recursive=True):
            sdk_count += 1
            if sdk_count >= limit:
                break
    except Exception as e:
        sdk_error = str(e)

    spark_count = 0
    spark_error: str | None = None
    try:
        spark = _get_spark_manager().spark
        spark_count = len(preview_raw_images_sample(spark, spark_raw_path, limit=limit))
    except Exception as e:
        spark_error = str(e)

    status = "ok"
    hint = "storage looks healthy"
    if sdk_error or spark_error:
        if sdk_error and spark_error:
            status = "down"
            hint = "both MinIO SDK and Spark S3A checks failed"
        else:
            status = "degraded"
            hint = "one check failed; OCR may rely on fallback path"
    elif sdk_count > 0 and spark_count == 0:
        status = "degraded"
        hint = "MinIO SDK can list objects but Spark S3A cannot; using fallback is recommended"

    http_code = 200 if status == "ok" else 503 if status == "down" else 200
    return (
        jsonify(
            {
                "status": status,
                "hint": hint,
                "dataset_id": dataset_id,
                "resolved": {
                    "sdk_prefix": prefix,
                    "spark_raw_images_path": spark_raw_path,
                },
                "minio_sdk": {"count": sdk_count, "error": sdk_error},
                "spark_binaryfile": {"count": spark_count, "error": spark_error},
            }
        ),
        http_code,
    )


@app.post("/delta/gold/run")
def delta_gold_run():
    """
    執行金層 ETL：Silver OCR → 痛點主題 + TF-IDF / PMI（run_gold_etl）。

    body（皆可選）:
      {
        "dataset_id": "invoice_ocr",
        "silver_ocr_path": "s3a://.../silver/ocr_features/",
        "coalesce_partitions": 1,
        "dry_run": false
      }

    亦可用查詢字串補齊欄位（與 body 合併），例如：
    POST /delta/gold/run?dataset_id=drinks&dry_run=false
    """

    err = _require_admin_token_if_configured()
    if err:
        return err

    body = _merge_missing_from_query(
        _parse_request_json_object(),
        (
            "dataset_id",
            "silver_ocr_path",
            "coalesce_partitions",
            "dry_run",
        ),
    )
    if not isinstance(body, dict):
        return _json_error("body 必須是 JSON object。", 400)

    dataset_raw = body.get("dataset_id")
    if isinstance(dataset_raw, str) and not dataset_raw.strip():
        dataset_raw = None
    dataset_id: str | None = None
    if dataset_raw is not None:
        if not isinstance(dataset_raw, str):
            return _json_error("dataset_id 必須是字串。", 400)
        try:
            dataset_id = normalize_dataset_id(dataset_raw)
        except ValueError as e:
            return _json_error(str(e), 400)

    silver_raw = body.get("silver_ocr_path")
    silver_ocr_path = (
        silver_raw.strip()
        if isinstance(silver_raw, str) and silver_raw.strip()
        else SILVER_OCR_TABLE_PATH
    )

    err = _validate_delta_path(silver_ocr_path)
    if err:
        return err

    try:
        coalesce_partitions = _body_get_int(body, "coalesce_partitions", 1)
    except (TypeError, ValueError):
        return _json_error("coalesce_partitions 必須是整數。", 400)

    dry_run = _body_get_bool(body, "dry_run", False)
    if dry_run:
        return jsonify(
            {
                "status": "dry_run",
                "dataset_id": dataset_id,
                "silver_ocr_path": silver_ocr_path,
                "coalesce_partitions": coalesce_partitions,
                "tfidf_path": GOLD_TFIDF_KEYWORDS_PATH,
            }
        )

    _get_spark_manager()
    started = time.perf_counter()
    try:
        gold_result = run_gold_etl(
            silver_ocr_path=silver_ocr_path,
            dataset_id=dataset_id,
            coalesce_partitions=coalesce_partitions,
        )
        _record_etl_metric(
            {
                "etl_name": "gold_etl",
                "dataset_id": dataset_id,
                "status": "ok",
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                "silver_ocr_path": silver_ocr_path,
                "tfidf_path": GOLD_TFIDF_KEYWORDS_PATH,
                "output_rows": gold_result.get("tfidf_output_rows"),
                "silver_filtered_rows": gold_result.get("silver_filtered_rows"),
                "is_gold_written": gold_result.get("is_gold_written"),
                "topic_output_rows": gold_result.get("topic_output_rows"),
                "topic_frequency_top": gold_result.get("topic_frequency_top"),
            }
        )
    except Exception as e:
        _record_etl_metric(
            {
                "etl_name": "gold_etl",
                "dataset_id": dataset_id,
                "status": "failed",
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                "silver_ocr_path": silver_ocr_path,
                "tfidf_path": GOLD_TFIDF_KEYWORDS_PATH,
                "error": str(e),
            }
        )
        _logger.exception("gold_run_failed")
        return _json_error(f"Gold ETL 失敗：{e}", 500)
    return jsonify(
        {
            "status": "ok",
            "dataset_id": dataset_id,
            "silver_ocr_path": silver_ocr_path,
            "tfidf_path": GOLD_TFIDF_KEYWORDS_PATH,
            "coalesce_partitions": coalesce_partitions,
            "gold_result": gold_result,
            "是否成功寫入金層": "是" if gold_result.get("is_gold_written") else "否",
            "白話說明": gold_result.get("summary"),
        }
    )


@app.get("/api/gold/tfidf-keywords")
def api_gold_tfidf_keywords():
    """
    Phase A：讀取 TF-IDF 痛點候選詞（依 tfidf_score 降序）。
    query: limit（預設 20，最大 200）、dataset_id（可選）
    """

    raw = request.args.get("limit", "20")
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        return _json_error("limit 必須是整數。", 400)
    limit = max(1, min(limit, 200))

    dataset_id, err = _parse_optional_dataset_id()
    if err:
        return err

    rows = get_gold_tfidf_keywords_data(limit=limit, dataset_id=dataset_id)
    return jsonify(
        {
            "path": GOLD_TFIDF_KEYWORDS_PATH,
            "dataset_id": dataset_id,
            "rows": rows,
            "count": len(rows),
        }
    )


@app.get("/api/gold/phrase-candidates")
def api_gold_phrase_candidates():
    """
    Phase B：讀取 PMI 片語候選（依 pmi_score 降序）。
    query: limit（預設 20，最大 200）、dataset_id（可選）
    """

    raw = request.args.get("limit", "20")
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        return _json_error("limit 必須是整數。", 400)
    limit = max(1, min(limit, 200))

    dataset_id, err = _parse_optional_dataset_id()
    if err:
        return err

    rows = get_gold_phrase_candidates_data(limit=limit, dataset_id=dataset_id)
    return jsonify(
        {
            "path": GOLD_PHRASE_CANDIDATES_PATH,
            "dataset_id": dataset_id,
            "rows": rows,
            "count": len(rows),
        }
    )


def _parse_limit_arg(raw: str, *, max_val: int = 200) -> tuple[int | None, Any]:
    """回傳 (limit, error_response)；error_response 非 None 時應直接 return。"""
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        return None, _json_error("limit 必須是整數。", 400)
    return max(1, min(limit, max_val)), None


def _parse_optional_dataset_id() -> tuple[str | None, Any]:
    """回傳 (dataset_id, error_response)。"""
    dataset_raw = request.args.get("dataset_id", "").strip().lower()
    if not dataset_raw:
        return None, None
    try:
        return normalize_dataset_id(dataset_raw), None
    except ValueError as e:
        return None, _json_error(str(e), 400)


@app.get("/api/silver")
@app.get("/api/silver/ocr")
def api_silver_ocr():
    """
    query: limit（預設 30，最大 200）、dataset_id（可選）
    讀取 config 的 SILVER_OCR_TABLE_PATH，與 /layers 銀層預覽同源。
    """

    raw = request.args.get("limit", "30")
    limit, err = _parse_limit_arg(raw)
    if err is not None:
        return err
    assert limit is not None

    dataset_id, err = _parse_optional_dataset_id()
    if err is not None:
        return err

    rows = get_silver_ocr_data(limit=limit, dataset_id=dataset_id)
    return jsonify(
        {
            "path": SILVER_OCR_TABLE_PATH,
            "dataset_id": dataset_id,
            "rows": rows,
            "count": len(rows),
        }
    )


@app.post("/delta/read")
def delta_read_preview():
    """
    body:
      {
        "table_path": "s3a://.../some_table/",
        "limit": 20
      }
    """

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        return _json_error("body 必須是 JSON object。", 400)

    table_path = body.get("table_path")
    if not isinstance(table_path, str) or not table_path.strip():
        return _json_error("table_path 必須是非空字串。", 400)
    err = _validate_delta_path(table_path)
    if err:
        return err

    limit_raw = body.get("limit", 20)
    try:
        limit = int(limit_raw)
    except (TypeError, ValueError):
        return _json_error("limit 必須是整數。", 400)

    limit = max(1, min(limit, 200))

    spark = _get_spark_manager().spark
    df = read_delta_table(spark, table_path).limit(limit)
    rows = [row.asDict(recursive=True) for row in df.collect()]
    return jsonify({"rows": rows, "count": len(rows)})


@app.post("/delta/upsert")
def delta_upsert():
    """
    body:
      {
        "target_path": "s3a://.../silver/cleaned_features/",
        "key_col": "item_id",
        "records": [ { ... }, { ... } ]
      }
    """

    err = _require_admin_token_if_configured()
    if err:
        return err

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        return _json_error("body 必須是 JSON object。", 400)

    target_path = body.get("target_path")
    if not isinstance(target_path, str) or not target_path.strip():
        return _json_error("target_path 必須是非空字串。", 400)
    err = _validate_delta_path(target_path)
    if err:
        return err

    key_col = body.get("key_col")
    if not isinstance(key_col, str) or not key_col.strip():
        return _json_error("key_col 必須是非空字串。", 400)

    records_raw = body.get("records", [])
    if not isinstance(records_raw, list):
        return _json_error("records 必須是 array。", 400)
    records: List[Dict[str, Any]] = records_raw  # 型別在 records_to_df 之前先視為 dict list

    if not records:
        return _json_error("records 不能是空的。", 400)

    spark = _get_spark_manager().spark
    try:
        source_df = records_to_df(spark, records)
    except ValueError as e:
        return _json_error(str(e), 400)
    source_df = add_etl_timestamp(source_df, col_name="etl_update_timestamp")
    merge_upsert_by_key(spark, source_df, target_path, key_col=key_col)
    return jsonify({"status": "ok"})


@app.post("/delta/cleanup-latest-only")
def delta_cleanup_latest_only():
    """
    body:
      {
        "target_path": "s3a://.../silver/ocr_features/",
        "timestamp_col": "ingestion_timestamp"
      }
    """

    err = _require_admin_token_if_configured()
    if err:
        return err

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        return _json_error("body 必須是 JSON object。", 400)

    target_path = body.get("target_path")
    if not isinstance(target_path, str) or not target_path.strip():
        return _json_error("target_path 必須是非空字串。", 400)
    err = _validate_delta_path(target_path)
    if err:
        return err

    timestamp_col = body.get("timestamp_col", "ingestion_timestamp")
    if not isinstance(timestamp_col, str) or not timestamp_col.strip():
        return _json_error("timestamp_col 必須是非空字串。", 400)

    dry_run = bool(body.get("dry_run", False))
    if dry_run:
        return jsonify({"status": "dry_run", "target_path": target_path, "timestamp_col": timestamp_col})

    spark = _get_spark_manager().spark
    delete_older_than_latest_batch(spark, target_path, timestamp_col=timestamp_col)
    return jsonify({"status": "ok", "target_path": target_path, "timestamp_col": timestamp_col})


@app.get("/api/debug/bronze-duplicates")
def api_debug_bronze_duplicates():
    """檢查 Bronze 重複資料概況（image_path 與 skip-key）。"""
    err = _require_admin_token_if_configured()
    if err:
        return err

    bronze_path = (request.args.get("bronze_path") or "").strip() or BRONZE_TABLE_PATH
    err = _validate_delta_path(bronze_path)
    if err:
        return err

    spark = _get_spark_manager().spark
    try:
        result = analyze_bronze_duplicates(spark, bronze_path)
    except Exception as e:
        _logger.exception("bronze_duplicates_check_failed")
        return _json_error(f"Bronze 查重失敗：{e}", 500, bronze_path=bronze_path)
    return jsonify({"status": "ok", **result})


@app.post("/delta/bronze/deduplicate")
def delta_bronze_deduplicate():
    """
    Bronze 去重（預設 dry_run=true）。
    body:
      {
        "bronze_path": "s3a://.../bronze/raw_features/",
        "strategy": "skipkey",
        "dry_run": true
      }
    """
    err = _require_admin_token_if_configured()
    if err:
        return err

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        return _json_error("body 必須是 JSON object。", 400)

    bronze_raw = body.get("bronze_path")
    bronze_path = bronze_raw.strip() if isinstance(bronze_raw, str) and bronze_raw.strip() else BRONZE_TABLE_PATH
    err = _validate_delta_path(bronze_path)
    if err:
        return err

    strategy = body.get("strategy", "skipkey")
    if not isinstance(strategy, str) or strategy not in ("skipkey", "image_path"):
        return _json_error('strategy 必須是 "skipkey" 或 "image_path"。', 400)

    dry_run = bool(body.get("dry_run", True))
    spark = _get_spark_manager().spark
    try:
        result = deduplicate_bronze_table(
            spark,
            bronze_path,
            strategy=strategy,
            dry_run=dry_run,
        )
    except ValueError as e:
        return _json_error(str(e), 400, bronze_path=bronze_path)
    except Exception as e:
        _logger.exception("bronze_deduplicate_failed")
        return _json_error(f"Bronze 去重失敗：{e}", 500, bronze_path=bronze_path)

    return jsonify({"status": "dry_run" if dry_run else "ok", **result})


@app.post("/delta/silver/ocr/run")
def delta_silver_ocr_run():
    """
    執行 Silver OCR ETL：Bronze OCR -> 清洗/去重 -> MERGE 至 Silver OCR。

    body（皆可選）:
      {
        "dataset_id": "invoice_ocr",
        "bronze_path": "s3a://.../bronze/raw_features/",
        "silver_ocr_path": "s3a://.../silver/ocr_features/",
        "dry_run": false
      }
    """
    err = _require_admin_token_if_configured()
    if err:
        return err

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        return _json_error("body 必須是 JSON object。", 400)

    dataset_raw = body.get("dataset_id")
    dataset_id: str | None = None
    if dataset_raw is not None:
        if not isinstance(dataset_raw, str):
            return _json_error("dataset_id 必須是字串。", 400)
        try:
            dataset_id = normalize_dataset_id(dataset_raw)
        except ValueError as e:
            return _json_error(str(e), 400)

    bronze_raw = body.get("bronze_path")
    silver_raw = body.get("silver_ocr_path")
    bronze_path = bronze_raw.strip() if isinstance(bronze_raw, str) and bronze_raw.strip() else BRONZE_TABLE_PATH
    silver_ocr_path = (
        silver_raw.strip() if isinstance(silver_raw, str) and silver_raw.strip() else SILVER_OCR_TABLE_PATH
    )

    err = _validate_delta_path(bronze_path)
    if err:
        return err
    err = _validate_delta_path(silver_ocr_path)
    if err:
        return err

    dry_run = bool(body.get("dry_run", False))
    if dry_run:
        return jsonify(
            {
                "status": "dry_run",
                "dataset_id": dataset_id,
                "bronze_path": bronze_path,
                "silver_ocr_path": silver_ocr_path,
            }
        )

    started = time.perf_counter()
    try:
        result = run_silver_ocr_etl(
            bronze_path=bronze_path,
            silver_ocr_path=silver_ocr_path,
            dataset_id=dataset_id,
        )
        _record_etl_metric(
            {
                "etl_name": "silver_ocr_etl",
                "dataset_id": dataset_id,
                "status": "ok",
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                "output_rows": result.get("updated_rows"),
                "bronze_path": bronze_path,
                "silver_ocr_path": silver_ocr_path,
                "silver_transform_version": result.get("silver_transform_version"),
                "silver_quality": _silver_quality_for_metric(result.get("silver_quality")),
                "bronze_quarantine": _bronze_quarantine_for_metric(result.get("bronze_quarantine")),
            }
        )
        bq = _bronze_quarantine_for_metric(result.get("bronze_quarantine"))
        response: Dict[str, Any] = {"status": "ok", **result}
        if bq and bq.get("melt_action") == "soft":
            response["status"] = "warning"
            response["warning"] = bq.get("message") or (
                "Bronze 軟熔斷：隔離占比過高，好列已進 Silver，但不可核准發行版。"
            )
        return jsonify(response)
    except BronzeQuarantineError as e:
        quarantine_payload = _bronze_quarantine_for_metric(getattr(e, "report", None)) or {
            "passed": False,
            "melted": True,
            "reject_rate": None,
        }
        quarantine_payload["hard_failures"] = [str(e)]
        _record_etl_metric(
            {
                "etl_name": "silver_ocr_etl",
                "dataset_id": dataset_id,
                "status": "bronze_quarantine_failed",
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                "error": str(e),
                "bronze_path": bronze_path,
                "silver_ocr_path": silver_ocr_path,
                "bronze_quarantine": quarantine_payload,
            }
        )
        _logger.error("bronze_quarantine_failed: %s", e)
        return _json_error(
            f"Bronze 隔離熔斷：{e}",
            422,
            status="bronze_quarantine_failed",
            bronze_quarantine=quarantine_payload,
        )
    except SilverQualityError as e:
        quality_payload = _silver_quality_for_metric(getattr(e, "report", None)) or {
            "passed": False,
            "hard_failures": [str(e)],
            "warnings": [],
            "checks": [],
        }
        _record_etl_metric(
            {
                "etl_name": "silver_ocr_etl",
                "dataset_id": dataset_id,
                "status": "quality_failed",
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                "error": str(e),
                "bronze_path": bronze_path,
                "silver_ocr_path": silver_ocr_path,
                "silver_quality": quality_payload,
            }
        )
        _logger.error("silver_ocr_quality_failed: %s", e)
        return _json_error(
            f"Silver 品質閘門未通過：{e}",
            422,
            status="quality_failed",
            silver_quality=quality_payload,
        )
    except Exception as e:
        _record_etl_metric(
            {
                "etl_name": "silver_ocr_etl",
                "dataset_id": dataset_id,
                "status": "failed",
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                "error": str(e),
                "bronze_path": bronze_path,
                "silver_ocr_path": silver_ocr_path,
            }
        )
        _logger.exception("silver_ocr_run_failed")
        return _json_error(f"Silver OCR ETL 失敗：{e}", 500)


@app.post("/delta/gold/topic-snapshot/rebuild")
def delta_gold_topic_snapshot_rebuild():
    """
    僅重建痛點主題快照（append 至 topic_snapshot）。
    適用：手動清空 MinIO 上 topic_snapshot 目錄後，依 Silver 補寫快照。

    body 或查詢參數（dataset_id 必填）:
      { "dataset_id": "drinks", "silver_ocr_path": "s3a://.../", "dry_run": false }

    查詢字串範例：POST /delta/gold/topic-snapshot/rebuild?dataset_id=drinks
    """
    err = _require_admin_token_if_configured()
    if err:
        return err

    body = _merge_missing_from_query(
        _parse_request_json_object(),
        ("dataset_id", "silver_ocr_path", "dry_run"),
    )
    if not isinstance(body, dict):
        return _json_error("body 必須是 JSON object。", 400)

    dataset_raw = body.get("dataset_id")
    if not isinstance(dataset_raw, str) or not dataset_raw.strip():
        return _json_error(
            "dataset_id 必填。可在 JSON body 或查詢參數提供，例如 ?dataset_id=drinks",
            400,
        )
    try:
        dataset_id = normalize_dataset_id(dataset_raw)
    except ValueError as e:
        return _json_error(str(e), 400)

    silver_raw = body.get("silver_ocr_path")
    silver_ocr_path = (
        silver_raw.strip()
        if isinstance(silver_raw, str) and silver_raw.strip()
        else SILVER_OCR_TABLE_PATH
    )
    err = _validate_delta_path(silver_ocr_path)
    if err:
        return err
    err = _validate_delta_path(GOLD_TOPIC_SNAPSHOT_PATH)
    if err:
        return err

    dry_run = _body_get_bool(body, "dry_run", False)
    if dry_run:
        return jsonify(
            {
                "status": "dry_run",
                "dataset_id": dataset_id,
                "silver_ocr_path": silver_ocr_path,
                "topic_snapshot_path": GOLD_TOPIC_SNAPSHOT_PATH,
            }
        )

    _get_spark_manager()
    started = time.perf_counter()
    try:
        result = run_gold_topic_snapshot_rebuild_etl(
            silver_ocr_path=silver_ocr_path,
            dataset_id=dataset_id,
        )
        _record_etl_metric(
            {
                "etl_name": "gold_topic_snapshot_rebuild",
                "dataset_id": dataset_id,
                "status": "ok",
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                "silver_ocr_path": silver_ocr_path,
                "topic_snapshot_rows": result.get("topic_snapshot_rows"),
            }
        )
    except ValueError as e:
        return _json_error(str(e), 400)
    except Exception as e:
        _record_etl_metric(
            {
                "etl_name": "gold_topic_snapshot_rebuild",
                "dataset_id": dataset_id,
                "status": "failed",
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                "error": str(e),
            }
        )
        _logger.exception("gold_topic_snapshot_rebuild_failed")
        return _json_error(f"痛點快照重建失敗：{e}", 500)
    return jsonify({"status": "ok", **result})


@app.post("/delta/gold/topic-snapshot/delete")
def delta_gold_topic_snapshot_delete():
    """
    依 dataset_id + snapshot_at 刪除痛點快照列（Delta DELETE，請勿在 MinIO 手動刪 parquet）。

    body 或查詢參數:
      { "dataset_id": "drinks", "snapshot_at": "2026-04-17T12:33:00.123456", "dry_run": false }

    snapshot_at 請使用首頁／layers 對照或 list_gold_topic_snapshots 回傳的 ISO 字串。
    """
    err = _require_admin_token_if_configured()
    if err:
        return err

    body = _merge_missing_from_query(
        _parse_request_json_object(),
        ("dataset_id", "snapshot_at", "dry_run"),
    )
    if not isinstance(body, dict):
        return _json_error("body 必須是 JSON object。", 400)

    dataset_raw = body.get("dataset_id")
    if not isinstance(dataset_raw, str) or not dataset_raw.strip():
        return _json_error(
            "dataset_id 必填。可在 JSON body 或查詢參數提供，例如 ?dataset_id=drinks",
            400,
        )
    try:
        dataset_id = normalize_dataset_id(dataset_raw)
    except ValueError as e:
        return _json_error(str(e), 400)

    snap_raw = body.get("snapshot_at")
    if not isinstance(snap_raw, str) or not str(snap_raw).strip():
        return _json_error("snapshot_at 必填（ISO 時間字串）。", 400)

    err = _validate_delta_path(GOLD_TOPIC_SNAPSHOT_PATH)
    if err:
        return err

    dry_run = _body_get_bool(body, "dry_run", False)

    _get_spark_manager()
    started = time.perf_counter()
    try:
        result = delete_gold_topic_snapshot_rows(
            dataset_id=dataset_id,
            snapshot_at_iso=str(snap_raw).strip(),
            dry_run=dry_run,
        )
        _record_etl_metric(
            {
                "etl_name": "gold_topic_snapshot_delete",
                "dataset_id": dataset_id,
                "status": "ok",
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                "deleted_rows": result.get("deleted_rows"),
                "dry_run": dry_run,
            }
        )
    except ValueError as e:
        return _json_error(str(e), 400)
    except Exception as e:
        _record_etl_metric(
            {
                "etl_name": "gold_topic_snapshot_delete",
                "dataset_id": dataset_id,
                "status": "failed",
                "duration_ms": round((time.perf_counter() - started) * 1000.0, 2),
                "error": str(e),
            }
        )
        _logger.exception("gold_topic_snapshot_delete_failed")
        return _json_error(f"痛點快照刪除失敗：{e}", 500)
    code = 404 if result.get("status") == "not_found" else 200
    return jsonify(result), code


@app.post("/delta/pipeline/to-gold/run")
def delta_pipeline_to_gold_run():
    """
    一鍵執行完整流程：Bronze OCR -> Silver OCR -> Gold ETL（同一個 dataset_id）。

    body:
      {
        "dataset_id": "invoice_ocr",
        "write_mode": "append",
        "coalesce_partitions": 1,
        "skip_gold_if_no_new_ocr": true,
        "dry_run": false,
        "async": false
      }

    - async: true 時立即回傳 job_id（HTTP 202），以 GET /api/jobs/<job_id> 輪詢；
      預檢（來源路徑至少有 1 張圖）仍同步執行，失敗則不回傳 job。

    查詢字串可補齊 dataset_id 等欄位，例如：POST /delta/pipeline/to-gold/run?dataset_id=drinks
    """
    err = _require_admin_token_if_configured()
    if err:
        return err

    body = _merge_missing_from_query(
        _parse_request_json_object(),
        (
            "dataset_id",
            "write_mode",
            "coalesce_partitions",
            "skip_gold_if_no_new_ocr",
            "dry_run",
            "async",
        ),
    )
    if not isinstance(body, dict):
        return _json_error("body 必須是 JSON object。", 400)

    dataset_raw = body.get("dataset_id")
    if not isinstance(dataset_raw, str) or not dataset_raw.strip():
        return _json_error(
            "dataset_id 必填。可在 JSON body 或查詢參數提供，例如 ?dataset_id=drinks",
            400,
        )
    try:
        dataset_id = normalize_dataset_id(dataset_raw)
    except ValueError as e:
        return _json_error(str(e), 400)

    write_mode = body.get("write_mode", "append")
    if not isinstance(write_mode, str) or write_mode not in ("overwrite", "append"):
        return _json_error('write_mode 必須是 "overwrite" 或 "append"。', 400)

    try:
        coalesce_partitions = _body_get_int(body, "coalesce_partitions", 1)
    except (TypeError, ValueError):
        return _json_error("coalesce_partitions 必須是整數。", 400)

    skip_gold_if_no_new_ocr = _body_get_bool(body, "skip_gold_if_no_new_ocr", True)

    raw_images_path = f"{RAW_IMAGES_PATH.rstrip('/')}/{dataset_id}/"
    err = _validate_delta_path(raw_images_path)
    if err:
        return err
    err = _validate_delta_path(BRONZE_TABLE_PATH)
    if err:
        return err
    err = _validate_delta_path(SILVER_OCR_TABLE_PATH)
    if err:
        return err
    err = _validate_delta_path(GOLD_TFIDF_KEYWORDS_PATH)
    if err:
        return err

    dry_run = _body_get_bool(body, "dry_run", False)
    if dry_run:
        return jsonify(
            {
                "status": "dry_run",
                "dataset_id": dataset_id,
                "steps": ["bronze_ocr", "silver_ocr", "gold_etl"],
                "raw_images_path": raw_images_path,
                "bronze_path": BRONZE_TABLE_PATH,
                "silver_ocr_path": SILVER_OCR_TABLE_PATH,
                "tfidf_path": GOLD_TFIDF_KEYWORDS_PATH,
                "write_mode": write_mode,
                "coalesce_partitions": coalesce_partitions,
                "skip_gold_if_no_new_ocr": skip_gold_if_no_new_ocr,
            }
        )

    spark = _get_spark_manager().spark
    # 前置檢查：來源路徑至少有 1 張圖
    try:
        sample = preview_raw_images_sample(spark, raw_images_path, limit=1)
    except Exception as e:
        return _json_error(f"檢查來源路徑失敗：{e}", 400)
    if not sample:
        return _json_error(
            "來源 dataset_id 沒有可處理圖片。",
            400,
            dataset_id=dataset_id,
            raw_images_path=raw_images_path,
        )

    if _body_get_bool(body, "async", False):
        jid = job_registry.create("pipeline_to_gold", step_total=3)

        def work(progress: Callable[[int, int, str], None]) -> Dict[str, Any]:
            return _execute_pipeline_to_gold_inner(
                dataset_id=dataset_id,
                raw_images_path=raw_images_path,
                write_mode=write_mode,
                coalesce_partitions=coalesce_partitions,
                skip_gold_if_no_new_ocr=skip_gold_if_no_new_ocr,
                progress=progress,
            )

        job_registry.run_async(jid, work)
        return (
            jsonify(
                {
                    "status": "accepted",
                    "job_id": jid,
                    "poll_path": f"/api/jobs/{jid}",
                    "dataset_id": dataset_id,
                }
            ),
            202,
        )

    try:
        payload = _execute_pipeline_to_gold_inner(
            dataset_id=dataset_id,
            raw_images_path=raw_images_path,
            write_mode=write_mode,
            coalesce_partitions=coalesce_partitions,
            skip_gold_if_no_new_ocr=skip_gold_if_no_new_ocr,
            progress=_noop_progress,
        )
        return jsonify(payload)
    except ValueError as e:
        return _json_error(str(e), 400)
    except Exception as e:
        _logger.exception("pipeline_to_gold_failed")
        return _json_error(f"一鍵 ETL 失敗：{e}", 500)


@app.post("/delta/ocr/bronze/run")
def delta_ocr_bronze_run():
    """
    執行 Bronze OCR 攝入：從 RAW_IMAGES_PATH（binaryFile）讀圖 → Tesseract → 寫入 BRONZE_TABLE_PATH。
    對齊 MinIO_DeltaLake_Spark_1.1.ipynb 之流程。

    body（欄位皆可選，路徑須符合 ALLOWED_DELTA_PATH_PREFIXES）:
      {
        "dataset_id": "invoice_ocr",
        "raw_images_path": "s3a://data-lake/raw/images/",
        "bronze_path": "s3a://data-lake/bronze/raw_features/",
        "write_mode": "overwrite",
        "dry_run": false,
        "async": false,
        "include_sample": false,
        "preview_limit": 5
      }

    - write_mode: \"overwrite\"（全表覆寫）、\"append\"（追加）或 \"merge\"（依 image_path 子集 Upsert）
    - image_paths: merge 時必填；可為 s3a 全路徑或檔名（相對於 raw_images_path）
    - dry_run: 僅驗證路徑；若 include_sample 為 true 會啟動 Spark 從 MinIO 取少量檔案預覽（需憑證）
    - async: true 時回傳 job_id（HTTP 202），以 GET /api/jobs/<job_id> 輪詢（來源預檢仍同步）
    """

    err = _require_admin_token_if_configured()
    if err:
        return err

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        return _json_error("body 必須是 JSON object。", 400)

    dataset_raw = body.get("dataset_id")
    dataset_id: str | None = None
    if dataset_raw is not None:
        if not isinstance(dataset_raw, str):
            return _json_error("dataset_id 必須是字串。", 400)
        try:
            dataset_id = normalize_dataset_id(dataset_raw)
        except ValueError as e:
            return _json_error(str(e), 400)

    raw_raw = body.get("raw_images_path")
    bronze_raw = body.get("bronze_path")
    if isinstance(raw_raw, str) and raw_raw.strip():
        raw_images_path = raw_raw.strip()
    else:
        raw_images_path = RAW_IMAGES_PATH
        if dataset_id:
            raw_images_path = f"{raw_images_path.rstrip('/')}/{dataset_id}/"
    bronze_path = (
        bronze_raw.strip()
        if isinstance(bronze_raw, str) and bronze_raw.strip()
        else BRONZE_TABLE_PATH
    )

    err = _validate_delta_path(raw_images_path)
    if err:
        return err
    err = _validate_delta_path(bronze_path)
    if err:
        return err

    write_mode = body.get("write_mode", "overwrite")
    if not isinstance(write_mode, str) or write_mode not in ("overwrite", "append", "merge"):
        return _json_error('write_mode 必須是 \"overwrite\"、\"append\" 或 \"merge\"。', 400)

    image_paths_raw = body.get("image_paths")
    image_paths: list[str] | None = None
    if image_paths_raw is not None:
        if not isinstance(image_paths_raw, list) or not all(isinstance(p, str) for p in image_paths_raw):
            return _json_error("image_paths 必須是字串陣列。", 400)
        image_paths = [p.strip() for p in image_paths_raw if str(p).strip()]
    if write_mode == "merge" and not image_paths:
        return _json_error('write_mode=\"merge\" 時必須提供 image_paths（至少一筆）。', 400)

    dry_run = bool(body.get("dry_run", False))
    if dry_run:
        payload: Dict[str, Any] = {
            "status": "dry_run",
            "dataset_id": dataset_id,
            "raw_images_path": raw_images_path,
            "bronze_path": bronze_path,
            "write_mode": write_mode,
            "image_paths": image_paths,
        }
        if bool(body.get("include_sample", False)):
            plim = body.get("preview_limit", 5)
            try:
                preview_limit = int(plim)
            except (TypeError, ValueError):
                return _json_error("preview_limit 必須是整數。", 400)
            preview_limit = max(1, min(preview_limit, 50))
            spark = _get_spark_manager().spark
            try:
                payload["sample_files"] = preview_raw_images_sample(
                    spark, raw_images_path, limit=preview_limit
                )
            except Exception as e:
                _logger.warning("ocr_bronze_preview_failed: %s", e)
                return _json_error(f"預覽 raw 影像路徑失敗：{e}", 400)
        return jsonify(payload)

    spark = _get_spark_manager().spark
    # 避免「看似成功但其實無資料」：正式執行前先確認至少有 1 筆可處理影像
    try:
        sample_files = preview_raw_images_sample(spark, raw_images_path, limit=1)
    except Exception as e:
        _logger.warning("ocr_bronze_precheck_failed: %s", e)
        return _json_error(f"檢查 OCR 來源路徑失敗：{e}", 400)
    if not sample_files:
        return _json_error(
            "OCR 來源路徑沒有可處理圖片，請先確認上傳位置與 dataset_id 是否一致。",
            400,
            dataset_id=dataset_id,
            raw_images_path=raw_images_path,
        )

    if bool(body.get("async", False)):
        jid = job_registry.create("bronze_ocr", step_total=1)

        def work(progress: Callable[[int, int, str], None]) -> Dict[str, Any]:
            return _execute_bronze_ocr_inner(
                dataset_id=dataset_id,
                raw_images_path=raw_images_path,
                bronze_path=bronze_path,
                write_mode=write_mode,
                image_paths=image_paths,
                progress=progress,
            )

        job_registry.run_async(jid, work)
        return (
            jsonify(
                {
                    "status": "accepted",
                    "job_id": jid,
                    "poll_path": f"/api/jobs/{jid}",
                    "dataset_id": dataset_id,
                }
            ),
            202,
        )

    try:
        out = _execute_bronze_ocr_inner(
            dataset_id=dataset_id,
            raw_images_path=raw_images_path,
            bronze_path=bronze_path,
            write_mode=write_mode,
            image_paths=image_paths,
            progress=_noop_progress,
        )
        return jsonify(out)
    except ValueError as e:
        return _json_error(str(e), 400)
    except Exception as e:
        _logger.exception("ocr_bronze_run_failed")
        return _json_error(f"OCR 攝入失敗：{e}", 500)


@app.post("/api/upload/images")
def api_upload_images():
    """
    multipart/form-data 上傳圖片至 MinIO（bucket=BUCKET_NAME，前綴 RAW_IMAGE_PREFIX）。
    僅接受靜態圖片；影片副檔名、video/* MIME、影片檔頭或假圖片一律拒絕（寫入前驗證）。

    表單欄位：
    - file：單檔，或
    - files：多檔（可重複欄位名 files）
    - dataset_id（必填）：資料分類代碼（英數、-、_），會寫入 raw/images/{dataset_id}/...
    - subfolder（可選）：寫在 raw/images/{dataset_id}/ 底下的子目錄名（僅允許英數、-、_、/）
    - run_ocr（可選）：true / 1 時，上傳完成後呼叫既有 Bronze OCR（Spark）
    - write_mode（可選，僅當 run_ocr 時）：overwrite 或 append，預設 append（避免覆寫整張 Bronze 表）
    - on_duplicate（可選）：suffix（預設，同名已存在則改為 檔名_時間戳.ext）或 overwrite（覆寫）

    需設定 MINIO_ACCESS_KEY / MINIO_SECRET_KEY；若設定 ADMIN_TOKEN 則需 header X-Admin-Token。
    """

    err = _require_admin_token_if_configured()
    if err:
        return err

    dataset_id = (request.form.get("dataset_id") or "").strip().lower()
    if not dataset_id:
        return _json_error("dataset_id 必填。", 400)

    subfolder = request.form.get("subfolder") or None
    if subfolder is not None:
        subfolder = subfolder.strip() or None

    max_mb = int(os.getenv("MAX_UPLOAD_MB", "15"))
    max_bytes = max(1, max_mb) * 1024 * 1024

    file_list = request.files.getlist("files")
    if not file_list or all(f.filename in (None, "") for f in file_list):
        single = request.files.get("file")
        file_list = [single] if single and single.filename else []

    if not file_list:
        return _json_error("請提供檔案：欄位名 file 或 files。", 400)

    dup_policy = request.form.get("on_duplicate", "").strip().lower()
    if dup_policy and dup_policy not in ("suffix", "overwrite"):
        return _json_error('on_duplicate 必須是 \"suffix\" 或 \"overwrite\"。', 400)
    on_duplicate_arg = dup_policy if dup_policy else None

    uploaded: List[Dict[str, Any]] = []
    for f in file_list:
        if not f or not f.filename:
            continue
        name = f.filename

        data = f.read(max_bytes + 1)
        if len(data) > max_bytes:
            return _json_error(f"單檔超過上限 {max_mb} MB。", 413)

        try:
            info = upload_file_bytes(
                filename=name,
                dataset_id=dataset_id,
                data=data,
                content_type=f.mimetype or None,
                subfolder=subfolder,
                on_duplicate=on_duplicate_arg,
            )
        except RuntimeError as e:
            return _json_error(str(e), 503)
        except ValueError as e:
            return _json_error(str(e), 400)
        uploaded.append(info)

    if not uploaded:
        return _json_error("沒有有效檔案。", 400)

    run_ocr = request.form.get("run_ocr", "").strip().lower() in ("1", "true", "yes", "on")
    ocr_payload: Dict[str, Any] | None = None
    if run_ocr:
        wm = request.form.get("write_mode", "append").strip().lower()
        if wm not in ("overwrite", "append"):
            return _json_error('write_mode 必須是 \"overwrite\" 或 \"append\"。', 400)
        spark = _get_spark_manager().spark
        try:
            bronze_result = run_bronze_ocr_ingest(
                spark,
                raw_images_path=f"{RAW_IMAGES_PATH.rstrip('/')}/{normalize_dataset_id(dataset_id)}/",
                bronze_path=BRONZE_TABLE_PATH,
                write_mode=wm,
            )
        except ValueError as e:
            return _json_error(str(e), 400)
        except Exception as e:
            _logger.exception("upload_then_ocr_failed")
            return _json_error(f"OCR 執行失敗：{e}", 500)
        ocr_payload = {
            "dataset_id": normalize_dataset_id(dataset_id),
            "raw_images_path": f"{RAW_IMAGES_PATH.rstrip('/')}/{normalize_dataset_id(dataset_id)}/",
            "bronze_path": BRONZE_TABLE_PATH,
            "write_mode": wm,
            "bronze_result": bronze_result,
        }

    out: Dict[str, Any] = {
        "status": "ok",
        "count": len(uploaded),
        "dataset_id": dataset_id,
        "uploaded": uploaded,
        "raw_images_prefix": RAW_IMAGES_PATH,
    }
    if ocr_payload:
        out["ocr"] = ocr_payload
    return jsonify(out)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "").strip().lower() in {"1", "true", "yes", "y", "on"}
    # 預設用 0.0.0.0，方便在容器/遠端直接呼叫
    app.run(host="0.0.0.0", port=port, debug=debug)

