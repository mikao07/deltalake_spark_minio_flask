from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, render_template, request
from werkzeug.exceptions import HTTPException

from config import (
    BUCKET_NAME,
    BRONZE_TABLE_PATH,
    GOLD_WORD_COUNT_PATH,
    RAW_IMAGES_PATH,
    SILVER_OCR_TABLE_PATH,
)
from services.minio_upload import list_dataset_ids, normalize_dataset_id, upload_file_bytes
from services.ocr_spark import preview_raw_images_sample, run_bronze_ocr_ingest
from services.spark_service import (
    SparkManager,
    add_etl_timestamp,
    delete_older_than_latest_batch,
    get_bronze_data,
    get_gold_word_frequency_data,
    get_system_status,
    merge_upsert_by_key,
    records_to_df,
    read_delta_table,
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


def _safe_bronze_preview(dataset_id: str | None = None):
    try:
        return get_bronze_data(limit=10, dataset_id=dataset_id), None
    except Exception as e:
        _logger.warning("bronze_preview_failed: %s", e)
        return [], str(e)


def _safe_gold_preview(limit: int = 15, dataset_id: str | None = None):
    try:
        return get_gold_word_frequency_data(limit=limit, dataset_id=dataset_id), None
    except Exception as e:
        _logger.warning("gold_preview_failed: %s", e)
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


@app.post("/delta/gold/word-frequency/run")
def delta_gold_word_frequency_run():
    """
    執行金層詞頻 ETL：Silver OCR → Jieba → Gold（run_gold_word_frequency_etl）。

    body（皆可選）:
      {
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
                "silver_ocr_path": silver_ocr_path,
                "gold_path": gold_path,
                "apply_noise_filter": apply_noise_filter,
                "coalesce_partitions": coalesce_partitions,
            }
        )

    _get_spark_manager()
    run_gold_word_frequency_etl(
        silver_ocr_path=silver_ocr_path,
        gold_path=gold_path,
        apply_noise_filter=apply_noise_filter,
        coalesce_partitions=coalesce_partitions,
    )
    return jsonify(
        {
            "status": "ok",
            "silver_ocr_path": silver_ocr_path,
            "gold_path": gold_path,
            "apply_noise_filter": apply_noise_filter,
            "coalesce_partitions": coalesce_partitions,
        }
    )


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
        "include_sample": false,
        "preview_limit": 5
      }

    - write_mode: \"overwrite\"（同 Notebook 全表覆寫）或 \"append\"
    - dry_run: 僅驗證路徑；若 include_sample 為 true 會啟動 Spark 從 MinIO 取少量檔案預覽（需憑證）
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
    try:
        run_bronze_ocr_ingest(
            spark,
            raw_images_path=raw_images_path,
            bronze_path=bronze_path,
            write_mode=write_mode,
        )
    except ValueError as e:
        return _json_error(str(e), 400)
    except Exception as e:
        _logger.exception("ocr_bronze_run_failed")
        return _json_error(f"OCR 攝入失敗：{e}", 500)

    return jsonify(
        {
            "status": "ok",
            "dataset_id": dataset_id,
            "raw_images_path": raw_images_path,
            "bronze_path": bronze_path,
            "write_mode": write_mode,
        }
    )


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
            run_bronze_ocr_ingest(
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

