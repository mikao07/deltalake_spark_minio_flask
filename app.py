from __future__ import annotations

import logging
import os
import time
from typing import Any, Callable, Dict, List, Optional

from flask import Flask, jsonify, render_template, request
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
    BRONZE_TABLE_PATH,
    GOLD_WORD_COUNT_PATH,
    MINIO_ENDPOINT,
    RAW_IMAGE_PREFIX,
    RAW_IMAGES_PATH,
    SILVER_OCR_TABLE_PATH,
)
from services.minio_upload import (
    ensure_bucket,
    get_minio_client,
    list_dataset_ids,
    normalize_dataset_id,
    upload_file_bytes,
)
from services.async_jobs import job_registry, job_to_public_dict
from services.ocr_spark import preview_raw_images_sample, run_bronze_ocr_ingest
from services.spark_service import (
    SparkManager,
    analyze_bronze_duplicates,
    add_etl_timestamp,
    deduplicate_bronze_table,
    delete_older_than_latest_batch,
    get_bronze_data,
    get_gold_delta_table_preview,
    get_gold_word_frequency_data,
    get_silver_ocr_data,
    get_system_status,
    merge_upsert_by_key,
    records_to_df,
    read_delta_table,
    run_silver_ocr_etl,
    run_gold_word_frequency_etl,
)

app = Flask(__name__)

_logger = logging.getLogger("car_rental_flask_spark_delta")
if not _logger.handlers:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

_spark_manager: Optional[SparkManager] = None


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
    apply_noise_filter: bool,
    coalesce_partitions: int,
    progress: Callable[[int, int, str], None],
) -> Dict[str, Any]:
    spark = _get_spark_manager().spark
    progress(1, 3, "銅層 Bronze OCR…")
    bronze_result = run_bronze_ocr_ingest(
        spark,
        raw_images_path=raw_images_path,
        bronze_path=BRONZE_TABLE_PATH,
        write_mode=write_mode,
    )
    progress(2, 3, "銀層 Silver ETL…")
    silver_result = run_silver_ocr_etl(
        bronze_path=BRONZE_TABLE_PATH,
        silver_ocr_path=SILVER_OCR_TABLE_PATH,
        dataset_id=dataset_id,
    )
    progress(3, 3, "金層 Gold 詞頻…")
    gold_result = run_gold_word_frequency_etl(
        silver_ocr_path=SILVER_OCR_TABLE_PATH,
        gold_path=GOLD_WORD_COUNT_PATH,
        dataset_id=dataset_id,
        apply_noise_filter=apply_noise_filter,
        coalesce_partitions=coalesce_partitions,
    )
    return {
        "status": "ok",
        "dataset_id": dataset_id,
        "steps": ["bronze_ocr", "silver_ocr", "gold_word_frequency"],
        "raw_images_path": raw_images_path,
        "bronze_path": BRONZE_TABLE_PATH,
        "silver_ocr_path": SILVER_OCR_TABLE_PATH,
        "gold_path": GOLD_WORD_COUNT_PATH,
        "write_mode": write_mode,
        "apply_noise_filter": apply_noise_filter,
        "coalesce_partitions": coalesce_partitions,
        "bronze_result": bronze_result,
        "silver_result": silver_result,
        "gold_result": gold_result,
    }


def _execute_bronze_ocr_inner(
    *,
    dataset_id: Optional[str],
    raw_images_path: str,
    bronze_path: str,
    write_mode: str,
    progress: Callable[[int, int, str], None],
) -> Dict[str, Any]:
    spark = _get_spark_manager().spark
    progress(1, 1, "Bronze OCR（讀圖、Tesseract）…")
    bronze_result = run_bronze_ocr_ingest(
        spark,
        raw_images_path=raw_images_path,
        bronze_path=bronze_path,
        write_mode=write_mode,
    )
    return {
        "status": "ok",
        "dataset_id": dataset_id,
        "raw_images_path": raw_images_path,
        "bronze_path": bronze_path,
        "write_mode": write_mode,
        "bronze_result": bronze_result,
    }


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


def _safe_gold_preview(limit: int = 15, dataset_id: str | None = None):
    try:
        return get_gold_word_frequency_data(limit=limit, dataset_id=dataset_id), None
    except Exception as e:
        _logger.warning("gold_preview_failed: %s", e)
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


@app.get("/upload")
def upload_page():
    """瀏覽器上傳圖片至 MinIO（表單 POST 改由前端 fetch 呼叫 /api/upload/images）。"""
    return render_template("upload.html")


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
    bronze_rows, bronze_error = _safe_bronze_preview(dataset_id=selected_dataset_id)
    gold_rows, gold_error = _safe_gold_preview(limit=15, dataset_id=selected_dataset_id)
    return render_template(
        "index.html",
        cpu_percent=sys_status.get("cpu_percent"),
        memory_percent=sys_status.get("memory_percent"),
        dataset_options=dataset_options,
        selected_dataset_id=selected_dataset_id,
        bronze_rows=bronze_rows,
        bronze_error=bronze_error,
        gold_rows=gold_rows,
        gold_error=gold_error,
        gold_table_path=GOLD_WORD_COUNT_PATH,
        silver_ocr_table_path=SILVER_OCR_TABLE_PATH,
    )


@app.get("/layers")
def layers_preview_page():
    """
    獨立頁：預覽銅／銀／金層表格內容，方便對照 OCR → 銀層 → 詞頻何處異常（避免首頁過擠）。
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
    gold_live_rows, gold_live_error = _safe_gold_preview(limit=preview_limit, dataset_id=selected_dataset_id)

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
                "請用 GET /api/gold/word-frequency?limit=20 看 count，或確認金層 ETL 是否產出有效詞頻列。"
            )

    return render_template(
        "layers.html",
        dataset_options=dataset_options,
        selected_dataset_id=selected_dataset_id,
        preview_limit=preview_limit,
        sort_time=sort_by_time,
        bronze_path=BRONZE_TABLE_PATH,
        silver_path=SILVER_OCR_TABLE_PATH,
        gold_path=GOLD_WORD_COUNT_PATH,
        bronze_rows=bronze_rows,
        bronze_error=bronze_error,
        silver_rows=silver_rows,
        silver_error=silver_error,
        gold_disk_rows=gold_disk_rows,
        gold_disk_error=gold_disk_error,
        gold_disk_hint=gold_disk_hint,
        gold_live_rows=gold_live_rows,
        gold_live_error=gold_live_error,
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


@app.get("/api/gold/word-frequency")
def api_gold_word_frequency():
    """
    query: limit（預設 20，最大 200）
    讀取 config 的 GOLD_WORD_COUNT_PATH（依 frequency 降序）。
    """

    raw = request.args.get("limit", "20")
    try:
        limit = int(raw)
    except (TypeError, ValueError):
        return _json_error("limit 必須是整數。", 400)
    limit = max(1, min(limit, 200))

    dataset_raw = request.args.get("dataset_id", "").strip().lower()
    dataset_id: str | None = None
    if dataset_raw:
        try:
            dataset_id = normalize_dataset_id(dataset_raw)
        except ValueError as e:
            return _json_error(str(e), 400)

    rows = get_gold_word_frequency_data(limit=limit, dataset_id=dataset_id)
    return jsonify(
        {
            "path": GOLD_WORD_COUNT_PATH,
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

    try:
        result = run_silver_ocr_etl(
            bronze_path=bronze_path,
            silver_ocr_path=silver_ocr_path,
            dataset_id=dataset_id,
        )
    except Exception as e:
        _logger.exception("silver_ocr_run_failed")
        return _json_error(f"Silver OCR ETL 失敗：{e}", 500)
    return jsonify({"status": "ok", **result})


@app.post("/delta/gold/word-frequency/run")
def delta_gold_word_frequency_run():
    """
    執行金層詞頻 ETL：Silver OCR → Jieba → Gold（run_gold_word_frequency_etl）。

    body（皆可選）:
      {
        "dataset_id": "invoice_ocr",
        "silver_ocr_path": "s3a://.../silver/ocr_features/",
        "gold_path": "s3a://.../gold/word_frequency/",
        "apply_noise_filter": true,
        "coalesce_partitions": 1,
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

    silver_raw = body.get("silver_ocr_path")
    gold_raw = body.get("gold_path")
    silver_ocr_path = (
        silver_raw.strip()
        if isinstance(silver_raw, str) and silver_raw.strip()
        else SILVER_OCR_TABLE_PATH
    )
    gold_path = gold_raw.strip() if isinstance(gold_raw, str) and gold_raw.strip() else GOLD_WORD_COUNT_PATH

    err = _validate_delta_path(silver_ocr_path)
    if err:
        return err
    err = _validate_delta_path(gold_path)
    if err:
        return err

    apply_noise_filter = body.get("apply_noise_filter", True)
    if not isinstance(apply_noise_filter, bool):
        return _json_error("apply_noise_filter 必須是布林值。", 400)

    coalesce_raw = body.get("coalesce_partitions", 1)
    try:
        coalesce_partitions = int(coalesce_raw)
    except (TypeError, ValueError):
        return _json_error("coalesce_partitions 必須是整數。", 400)

    dry_run = bool(body.get("dry_run", False))
    if dry_run:
        return jsonify(
            {
                "status": "dry_run",
                "dataset_id": dataset_id,
                "silver_ocr_path": silver_ocr_path,
                "gold_path": gold_path,
                "apply_noise_filter": apply_noise_filter,
                "coalesce_partitions": coalesce_partitions,
            }
        )

    _get_spark_manager()
    gold_result = run_gold_word_frequency_etl(
        silver_ocr_path=silver_ocr_path,
        gold_path=gold_path,
        dataset_id=dataset_id,
        apply_noise_filter=apply_noise_filter,
        coalesce_partitions=coalesce_partitions,
    )
    return jsonify(
        {
            "status": "ok",
            "dataset_id": dataset_id,
            "silver_ocr_path": silver_ocr_path,
            "gold_path": gold_path,
            "apply_noise_filter": apply_noise_filter,
            "coalesce_partitions": coalesce_partitions,
            "gold_result": gold_result,
            "是否成功寫入金層": "是" if gold_result.get("is_gold_written") else "否",
            "白話說明": gold_result.get("summary"),
        }
    )


@app.post("/delta/pipeline/to-gold/run")
def delta_pipeline_to_gold_run():
    """
    一鍵執行完整流程：Bronze OCR -> Silver OCR -> Gold 詞頻（同一個 dataset_id）。

    body:
      {
        "dataset_id": "invoice_ocr",
        "write_mode": "append",
        "apply_noise_filter": true,
        "coalesce_partitions": 1,
        "dry_run": false,
        "async": false
      }

    - async: true 時立即回傳 job_id（HTTP 202），以 GET /api/jobs/<job_id> 輪詢；
      預檢（來源路徑至少有 1 張圖）仍同步執行，失敗則不回傳 job。
    """
    err = _require_admin_token_if_configured()
    if err:
        return err

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        return _json_error("body 必須是 JSON object。", 400)

    dataset_raw = body.get("dataset_id")
    if not isinstance(dataset_raw, str) or not dataset_raw.strip():
        return _json_error("dataset_id 必填。", 400)
    try:
        dataset_id = normalize_dataset_id(dataset_raw)
    except ValueError as e:
        return _json_error(str(e), 400)

    write_mode = body.get("write_mode", "append")
    if not isinstance(write_mode, str) or write_mode not in ("overwrite", "append"):
        return _json_error('write_mode 必須是 "overwrite" 或 "append"。', 400)

    apply_noise_filter = body.get("apply_noise_filter", True)
    if not isinstance(apply_noise_filter, bool):
        return _json_error("apply_noise_filter 必須是布林值。", 400)

    coalesce_raw = body.get("coalesce_partitions", 1)
    try:
        coalesce_partitions = int(coalesce_raw)
    except (TypeError, ValueError):
        return _json_error("coalesce_partitions 必須是整數。", 400)

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
    err = _validate_delta_path(GOLD_WORD_COUNT_PATH)
    if err:
        return err

    dry_run = bool(body.get("dry_run", False))
    if dry_run:
        return jsonify(
            {
                "status": "dry_run",
                "dataset_id": dataset_id,
                "steps": ["bronze_ocr", "silver_ocr", "gold_word_frequency"],
                "raw_images_path": raw_images_path,
                "bronze_path": BRONZE_TABLE_PATH,
                "silver_ocr_path": SILVER_OCR_TABLE_PATH,
                "gold_path": GOLD_WORD_COUNT_PATH,
                "write_mode": write_mode,
                "apply_noise_filter": apply_noise_filter,
                "coalesce_partitions": coalesce_partitions,
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

    if bool(body.get("async", False)):
        jid = job_registry.create("pipeline_to_gold", step_total=3)

        def work(progress: Callable[[int, int, str], None]) -> Dict[str, Any]:
            return _execute_pipeline_to_gold_inner(
                dataset_id=dataset_id,
                raw_images_path=raw_images_path,
                write_mode=write_mode,
                apply_noise_filter=apply_noise_filter,
                coalesce_partitions=coalesce_partitions,
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
            apply_noise_filter=apply_noise_filter,
            coalesce_partitions=coalesce_partitions,
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

    - write_mode: \"overwrite\"（同 Notebook 全表覆寫）或 \"append\"
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
    if not isinstance(write_mode, str) or write_mode not in ("overwrite", "append"):
        return _json_error('write_mode 必須是 \"overwrite\" 或 \"append\"。', 400)

    dry_run = bool(body.get("dry_run", False))
    if dry_run:
        payload: Dict[str, Any] = {
            "status": "dry_run",
            "dataset_id": dataset_id,
            "raw_images_path": raw_images_path,
            "bronze_path": bronze_path,
            "write_mode": write_mode,
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
            progress=_noop_progress,
        )
        return jsonify(out)
    except ValueError as e:
        return _json_error(str(e), 400)
    except Exception as e:
        _logger.exception("ocr_bronze_run_failed")
        return _json_error(f"OCR 攝入失敗：{e}", 500)


_ALLOWED_IMAGE_EXT = frozenset({".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tif", ".tiff"})


@app.post("/api/upload/images")
def api_upload_images():
    """
    multipart/form-data 上傳圖片至 MinIO（bucket=BUCKET_NAME，前綴 RAW_IMAGE_PREFIX）。

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

    from pathlib import Path

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
        ext = Path(name).suffix.lower()
        if ext not in _ALLOWED_IMAGE_EXT:
            return _json_error(f"不支援的副檔名：{ext}（允許：{sorted(_ALLOWED_IMAGE_EXT)}）", 400)

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

