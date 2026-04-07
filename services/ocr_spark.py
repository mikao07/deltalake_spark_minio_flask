"""
Bronze 層 OCR 攝入（對齊 MinIO_DeltaLake_Spark_1.1.ipynb）：
從 MinIO（S3A）以 binaryFile 讀取影像 → Tesseract（pytesseract）→ 寫入 Delta Bronze。

須安裝系統套件：Tesseract OCR 與語言包（例如 chi_tra、eng）。
"""

from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, length, lit, udf
from pyspark.sql.types import StringType


def _ocr_binary_to_text(image_content) -> Optional[str]:
    """
    將影像二進位內容轉成文字（於 Spark Python UDF 內執行，需在 worker 上能呼叫 tesseract）。
    """
    try:
        import pytesseract
        from io import BytesIO

        from PIL import Image, ImageEnhance

        cmd = os.getenv("TESSERACT_CMD", "").strip()
        if cmd:
            pytesseract.pytesseract.tesseract_cmd = cmd

        ocr_lang = os.getenv("OCR_LANG", "chi_tra+eng")

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
        img = img.convert("L")
        enhancer = ImageEnhance.Contrast(img)
        img = enhancer.enhance(1.5)

        text = pytesseract.image_to_string(img, lang=ocr_lang, config="--psm 6")
        result = text.strip() or "OCR_EMPTY_RESULT"
        return result

    except ImportError as ie:
        return f"OCR_ERROR_IMPORT: {ie}"
    except Exception as e:
        return f"OCR_ERROR_REAL: {e}"


_ocr_udf = udf(_ocr_binary_to_text, StringType())


def run_bronze_ocr_ingest(
    spark: SparkSession,
    *,
    raw_images_path: str,
    bronze_path: str,
    write_mode: str = "overwrite",
) -> None:
    """
    從 raw_images_path（s3a://.../ 目錄，內含圖檔）讀取 binaryFile，執行 OCR 後寫入 bronze_path。

    write_mode: \"overwrite\" 與 Notebook 全量覆寫一致；\"append\" 為追加（可能產生重複 image_path，請自行評估）。
    """

    if write_mode not in ("overwrite", "append"):
        raise ValueError('write_mode 必須是 \"overwrite\" 或 \"append\"。')

    df_paths = (
        spark.read.format("binaryFile")
        .load(raw_images_path)
        .select(
            col("path").alias("image_path"),
            col("content").alias("image_content"),
        )
    )

    df_ocr = (
        df_paths.withColumn("extracted_text", _ocr_udf(col("image_content")))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_bucket", lit("raw_images"))
        .drop("image_content")
    )

    df_ocr.write.format("delta").mode(write_mode).save(bronze_path)


def preview_raw_images_sample(
    spark: SparkSession,
    raw_images_path: str,
    *,
    limit: int = 5,
) -> list[dict]:
    """回傳即將送 OCR 的檔案路徑與內容長度（不執行 Tesseract，供 dry_run 用）。"""

    lim = max(1, min(int(limit), 50))
    df = (
        spark.read.format("binaryFile")
        .load(raw_images_path)
        .select(
            col("path").alias("image_path"),
            length(col("content")).alias("content_length"),
        )
        .limit(lim)
    )
    return [row.asDict(recursive=True) for row in df.collect()]
