"""
MinIO / S3A 與 Delta Lake 路徑設定

內容參考：`MinIO_DeltaLake_Spark_1.1.ipynb` 的 Cell 1

建議：不要把此檔提交到 Git（因為包含憑證）。
"""

from __future__ import annotations

import os

# -------------------------
# MinIO / S3A 連線設定
# -------------------------

# Notebook：MINIO_ENDPOINT
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000")

# Notebook：MINIO_ENDPOINT_CLIENT
MINIO_ENDPOINT_CLIENT = os.getenv("MINIO_ENDPOINT_CLIENT", "127.0.0.1:9000")

# Notebook：MINIO_ACCESS_KEY
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")

# Notebook：MINIO_SECRET_KEY
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

# Notebook：Spark S3A 其他設定
S3A_PATH_STYLE_ACCESS = os.getenv("S3A_PATH_STYLE_ACCESS", "true")
S3A_IMPL = os.getenv("S3A_IMPL", "org.apache.hadoop.fs.s3a.S3AFileSystem")
S3A_ENDPOINT_REGION = os.getenv("S3A_ENDPOINT_REGION", "us-east-1")
S3A_CONNECTION_SSL_ENABLED = os.getenv("S3A_CONNECTION_SSL_ENABLED", "false")

# -------------------------
# Delta Lake 表格路徑
# -------------------------

BUCKET_NAME = os.getenv("BUCKET_NAME", "data-lake")

# Notebook：BRONZE_TABLE_PATH
BRONZE_TABLE_PATH = os.getenv("BRONZE_TABLE_PATH", "s3a://data-lake/bronze/raw_features/")

# Notebook：SILVER_TABLE_PATH
SILVER_TABLE_PATH = os.getenv("SILVER_TABLE_PATH", "s3a://data-lake/silver/cleaned_features/")

# Notebook：SILVER_OCR_TABLE_PATH
SILVER_OCR_TABLE_PATH = os.getenv("SILVER_OCR_TABLE_PATH", "s3a://data-lake/silver/ocr_features/")

# Notebook：GOLD_WORD_COUNT_PATH（詞頻分析 Gold 層）
GOLD_WORD_COUNT_PATH = os.getenv(
    "GOLD_WORD_COUNT_PATH",
    f"s3a://{BUCKET_NAME}/gold/word_frequency/",
)

# Notebook：JIEBA_ZIP_PATH（上傳至 MinIO 的 jieba.zip，供 executors addPyFile）
JIEBA_ZIP_PATH = os.getenv(
    "JIEBA_ZIP_PATH",
    "",
)

# 可選：Jieba 自訂字典（支援本機路徑或 s3a://；Spark executors 會以 addFile 分發）
JIEBA_USERDICT_PATH = os.getenv("JIEBA_USERDICT_PATH", "")
# 可選：依 dataset_id 綁定字典路徑模板，例：s3a://bucket/jieba_dicts/{dataset_id}.txt
JIEBA_USERDICT_DATASET_PATTERN = os.getenv("JIEBA_USERDICT_DATASET_PATTERN", "")

# 可選：停用詞表（每行一詞；# 開頭為註解；與 jieba 詞典分開）
STOPWORDS_PATH = os.getenv("STOPWORDS_PATH", "")
STOPWORDS_DATASET_PATTERN = os.getenv("STOPWORDS_DATASET_PATTERN", "")

# Notebook：RAW_IMAGE_PREFIX（用於拼 RAW_IMAGES_PATH）
RAW_IMAGE_PREFIX = os.getenv("RAW_IMAGE_PREFIX", "raw/images/")

# Notebook：RAW_IMAGES_PATH（由 BUCKET_NAME 與 RAW_IMAGE_PREFIX 拼出）
RAW_IMAGES_PATH = os.getenv("RAW_IMAGES_PATH", f"s3a://{BUCKET_NAME}/{RAW_IMAGE_PREFIX}")

# OCR（Tesseract / pytesseract）：語言包需已安裝於系統；Windows 可設 TESSERACT_CMD 指向 tesseract.exe
OCR_LANG = os.getenv("OCR_LANG", "chi_tra+eng")
TESSERACT_CMD = os.getenv("TESSERACT_CMD", "")

# 上傳至 MinIO 時若物件已存在：suffix=自動改檔名加時間戳；overwrite=直接覆寫
UPLOAD_ON_DUPLICATE = os.getenv("UPLOAD_ON_DUPLICATE", "suffix").strip().lower()

