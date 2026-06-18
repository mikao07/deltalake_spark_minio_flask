"""
Bronze 層 OCR 攝入（對齊 MinIO_DeltaLake_Spark_1.1.ipynb）：
從 MinIO（S3A）以 binaryFile 讀取影像 → Tesseract（pytesseract）→ 寫入 Delta Bronze。

須安裝系統套件：Tesseract OCR 與語言包（例如 chi_tra、eng）。
"""

from __future__ import annotations

import os
from typing import Optional
from urllib.parse import urlparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, length, lit, lower, regexp_extract, sha2, udf
from pyspark.sql.types import StringType
from minio.error import S3Error

from config import BUCKET_NAME, RAW_IMAGE_PREFIX
from services.minio_upload import ensure_bucket, get_minio_client

_SUPPORTED_IMAGE_EXTS = (".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp", ".tif", ".tiff")


def _has_supported_image_extension(path: str) -> bool:
    p = (path or "").strip().lower()
    return any(p.endswith(ext) for ext in _SUPPORTED_IMAGE_EXTS)


def _looks_like_image_bytes(data: bytes) -> bool:
    """
    以常見檔頭判斷是否為圖片，避免非圖片檔混入 OCR。
    """
    if not data:
        return False
    sig = bytes(data[:16])
    return (
        sig.startswith(b"\x89PNG\r\n\x1a\n")
        or sig.startswith(b"\xff\xd8\xff")  # JPEG
        or sig.startswith(b"GIF87a")
        or sig.startswith(b"GIF89a")
        or sig.startswith(b"BM")  # BMP
        or (len(sig) >= 12 and sig[0:4] == b"RIFF" and sig[8:12] == b"WEBP")
        or sig.startswith(b"II*\x00")  # TIFF little-endian
        or sig.startswith(b"MM\x00*")  # TIFF big-endian
    )


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def preprocess_image_for_ocr(img):
    """
    Bronze OCR 前處理（可由 .env 調校）：
    - OCR_SCALE_MIN_SIDE：短邊低於此值時等比放大（0=不放大）
    - OCR_CONTRAST：對比度倍率（灰階後）
    - OCR_SHARPNESS：銳利度倍率（1.0=不變）
    """
    from PIL import Image, ImageEnhance

    scale_min = max(0, _env_int("OCR_SCALE_MIN_SIDE", 0))
    if scale_min > 0:
        w, h = img.size
        short = min(w, h)
        if 0 < short < scale_min:
            factor = scale_min / short
            new_size = (max(1, int(w * factor)), max(1, int(h * factor)))
            img = img.resize(new_size, Image.Resampling.LANCZOS)

    img = img.convert("L")

    contrast = _env_float("OCR_CONTRAST", 1.5)
    if contrast != 1.0:
        img = ImageEnhance.Contrast(img).enhance(contrast)

    sharpness = _env_float("OCR_SHARPNESS", 1.0)
    if sharpness != 1.0:
        img = ImageEnhance.Sharpness(img).enhance(sharpness)

    return img


def _ocr_binary_to_text(image_content) -> Optional[str]:
    """
    將影像二進位內容轉成文字（於 Spark Python UDF 內執行，需在 worker 上能呼叫 tesseract）。
    """
    try:
        import pytesseract
        from io import BytesIO

        from PIL import Image

        cmd = os.getenv("TESSERACT_CMD", "").strip()
        if cmd:
            pytesseract.pytesseract.tesseract_cmd = cmd

        ocr_lang = os.getenv("OCR_LANG", "chi_tra+eng")
        ocr_psm = os.getenv("OCR_PSM", "11").strip() or "11"

        if image_content is None:
            return None

        if isinstance(image_content, memoryview):
            data = image_content.tobytes()
        elif isinstance(image_content, bytearray):
            data = bytes(image_content)
        else:
            data = bytes(image_content)

        buf = BytesIO(data)
        buf.seek(0)
        img = Image.open(buf)
        img = preprocess_image_for_ocr(img)

        text = pytesseract.image_to_string(img, lang=ocr_lang, config=f"--psm {ocr_psm}")
        result = text.strip() or "OCR_EMPTY_RESULT"
        return result

    except ImportError as ie:
        return f"OCR_ERROR_IMPORT: {ie}"
    except Exception as e:
        return f"OCR_ERROR_REAL: {e}"


_ocr_udf = udf(_ocr_binary_to_text, StringType())


def _get_ocr_signature() -> str:
    # 可用環境變數覆寫，方便升級 OCR 流程後區分版本
    sig = os.getenv("OCR_SIGNATURE", "").strip()
    if sig:
        return sig
    lang = os.getenv("OCR_LANG", "chi_tra+eng").strip() or "chi_tra+eng"
    psm = os.getenv("OCR_PSM", "11").strip() or "11"
    pre = os.getenv("OCR_PREPROCESS_VERSION", "v1").strip() or "v1"
    scale = str(max(0, _env_int("OCR_SCALE_MIN_SIDE", 0)))
    contrast = str(_env_float("OCR_CONTRAST", 1.5))
    sharp = str(_env_float("OCR_SHARPNESS", 1.0))
    return f"tesseract|lang={lang}|psm={psm}|pre={pre}|scale={scale}|ctr={contrast}|shp={sharp}"


def _extract_bucket_and_prefix(raw_images_path: str) -> tuple[str, str]:
    path = (raw_images_path or "").strip()
    if not path.startswith("s3a://"):
        raise ValueError("raw_images_path 必須是 s3a://bucket/prefix 形式。")
    u = urlparse(path)
    bucket = (u.netloc or "").strip()
    prefix = (u.path or "").lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    if not bucket:
        raise ValueError("raw_images_path 缺少 bucket。")
    return bucket, prefix


def _list_and_read_via_minio(raw_images_path: str, limit: int | None = None) -> list[dict]:
    """
    使用 MinIO SDK 列檔並讀取 bytes。回傳 list[{"image_path","image_content"}]。
    """
    bucket, prefix = _extract_bucket_and_prefix(raw_images_path)
    client = get_minio_client()
    ensure_bucket(client, bucket)

    rows: list[dict] = []
    max_n = None if limit is None else max(1, int(limit))
    try:
        for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
            name = getattr(obj, "object_name", "") or ""
            if not name or name.endswith("/"):
                continue
            if not _has_supported_image_extension(name):
                continue
            if max_n is not None and len(rows) >= max_n:
                break
            resp = client.get_object(bucket, name)
            try:
                data = resp.read()
            finally:
                resp.close()
                resp.release_conn()
            if not _looks_like_image_bytes(data):
                continue
            rows.append(
                {
                    "image_path": f"s3a://{bucket}/{name}",
                    "image_content": data,
                }
            )
    except S3Error as e:
        raise RuntimeError(f"MinIO SDK 讀取影像失敗：{e}") from e
    return rows


def _build_df_paths(spark: SparkSession, raw_images_path: str):
    """
    優先使用 Spark binaryFile；若為 0 筆則 fallback 到 MinIO SDK。
    """
    df_paths = (
        spark.read.format("binaryFile")
        .load(raw_images_path)
        .filter(
            lower(col("path")).rlike(r".*\.(png|jpg|jpeg|bmp|gif|webp|tif|tiff)$")
        )
        .select(
            col("path").alias("image_path"),
            col("content").alias("image_content"),
        )
    )
    try:
        cnt = df_paths.limit(1).count()
    except Exception:
        cnt = 0
    if cnt > 0:
        return df_paths

    # Spark binaryFile 讀不到時 fallback（常見於特定 MinIO/S3A 相容性）
    rows = _list_and_read_via_minio(raw_images_path, limit=None)
    if not rows:
        return spark.createDataFrame([], "image_path string, image_content binary")
    return spark.createDataFrame(rows)


def run_bronze_ocr_ingest(
    spark: SparkSession,
    *,
    raw_images_path: str,
    bronze_path: str,
    write_mode: str = "overwrite",
) -> dict:
    """
    從 raw_images_path（s3a://.../ 目錄，內含圖檔）讀取 binaryFile，執行 OCR 後寫入 bronze_path。

    write_mode: \"overwrite\" 與 Notebook 全量覆寫一致；\"append\" 為追加（可能產生重複 image_path，請自行評估）。
    """

    if write_mode not in ("overwrite", "append"):
        raise ValueError('write_mode 必須是 \"overwrite\" 或 \"append\"。')

    df_paths = _build_df_paths(spark, raw_images_path)
    sig = _get_ocr_signature()

    df_base = (
        df_paths.withColumn("file_hash", sha2(col("image_content"), 256))
        .withColumn("dataset_id", regexp_extract(col("image_path"), r"/raw/images/([^/]+)/", 1))
        .withColumn("ocr_signature", lit(sig))
    )

    total_input = int(df_base.count())
    if total_input == 0:
        return {"input_rows": 0, "processed_rows": 0, "skipped_rows": 0, "ocr_signature": sig}

    # append 模式下，嘗試跳過「同 dataset + 同檔案內容 hash + 同 OCR signature」已處理資料
    if write_mode == "append":
        try:
            df_existing = spark.read.format("delta").load(bronze_path)
            cols = set(df_existing.columns)
            if {"dataset_id", "file_hash", "ocr_signature"}.issubset(cols):
                key_cols = ["dataset_id", "file_hash", "ocr_signature"]
                existing_keys = df_existing.select(*key_cols).dropDuplicates()
                df_base = df_base.join(existing_keys, on=key_cols, how="left_anti")
            elif "image_path" in cols:
                existing_keys = df_existing.select("image_path").dropDuplicates()
                df_base = df_base.join(existing_keys, on=["image_path"], how="left_anti")
        except Exception:
            # 目標表不存在或 schema 無法讀取時，直接繼續寫入
            pass

    processed_rows = int(df_base.count())
    skipped_rows = max(0, total_input - processed_rows)
    if processed_rows == 0:
        return {
            "input_rows": total_input,
            "processed_rows": 0,
            "skipped_rows": skipped_rows,
            "ocr_signature": sig,
        }

    df_ocr = (
        df_base.withColumn("extracted_text", _ocr_udf(col("image_content")))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_bucket", lit("raw_images"))
        .drop("image_content")
    )
    # OCR 執行失敗（OCR_ERROR_*）的列不寫入 Bronze，避免髒資料擴散到 Silver/Gold。
    ocr_error_rows = int(df_ocr.filter(col("extracted_text").startswith("OCR_ERROR_")).count())
    df_ocr = df_ocr.filter(~col("extracted_text").startswith("OCR_ERROR_"))
    write_rows = int(df_ocr.count())
    if write_rows == 0:
        return {
            "input_rows": total_input,
            "processed_rows": processed_rows,
            "skipped_rows": skipped_rows,
            "ocr_error_rows_dropped": ocr_error_rows,
            "ocr_signature": sig,
        }

    writer = df_ocr.write.format("delta").mode(write_mode)
    if write_mode == "append":
        # 舊 Bronze 僅有 image_path 等欄位時，追加寫入需合併 schema（file_hash / dataset_id / ocr_signature）
        writer = writer.option("mergeSchema", "true")
    else:
        writer = writer.option("overwriteSchema", "true")
    writer.save(bronze_path)
    return {
        "input_rows": total_input,
        "processed_rows": write_rows,
        "skipped_rows": skipped_rows,
        "ocr_error_rows_dropped": ocr_error_rows,
        "ocr_signature": sig,
    }


def preview_raw_images_sample(
    spark: SparkSession,
    raw_images_path: str,
    *,
    limit: int = 5,
) -> list[dict]:
    """回傳即將送 OCR 的檔案路徑與內容長度（不執行 Tesseract，供 dry_run 用）。"""

    lim = max(1, min(int(limit), 50))

    # 先走 Spark binaryFile
    df = (
        spark.read.format("binaryFile")
        .load(raw_images_path)
        .filter(
            lower(col("path")).rlike(r".*\.(png|jpg|jpeg|bmp|gif|webp|tif|tiff)$")
        )
        .select(
            col("path").alias("image_path"),
            length(col("content")).alias("content_length"),
        )
        .limit(lim)
    )
    rows = [row.asDict(recursive=True) for row in df.collect()]
    if rows:
        return rows

    # Spark 看不到時 fallback 到 MinIO SDK
    sdk_rows = _list_and_read_via_minio(raw_images_path, limit=lim)
    return [
        {"image_path": r["image_path"], "content_length": len(r["image_content"])}
        for r in sdk_rows
    ]
