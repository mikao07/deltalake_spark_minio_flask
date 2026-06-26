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

# Gold：痛點主題快照（topic / frequency）
GOLD_TOPIC_SNAPSHOT_PATH = os.getenv(
    "GOLD_TOPIC_SNAPSHOT_PATH",
    f"s3a://{BUCKET_NAME}/gold/topic_snapshot/",
)

# Gold：TF-IDF 痛點候選詞（Phase A）
GOLD_TFIDF_KEYWORDS_PATH = os.getenv(
    "GOLD_TFIDF_KEYWORDS_PATH",
    f"s3a://{BUCKET_NAME}/gold/tfidf_keywords/",
)

# Gold：PMI 片語候選（Phase B）
GOLD_PHRASE_CANDIDATES_PATH = os.getenv(
    "GOLD_PHRASE_CANDIDATES_PATH",
    f"s3a://{BUCKET_NAME}/gold/phrase_candidates/",
)

# 寫入痛點快照表後是否強制讀取驗證（不啟用 ignoreMissingFiles，避免元資料與 parquet 不一致仍回報成功）
_GOLD_TOPIC_SNAPSHOT_VERIFY_RAW = os.getenv("GOLD_TOPIC_SNAPSHOT_VERIFY_AFTER_WRITE", "true").strip().lower()
GOLD_TOPIC_SNAPSHOT_VERIFY_AFTER_WRITE = _GOLD_TOPIC_SNAPSHOT_VERIFY_RAW in (
    "1",
    "true",
    "yes",
    "on",
)

# Notebook：JIEBA_ZIP_PATH（上傳至 MinIO 的 jieba.zip，供 executors addPyFile）
JIEBA_ZIP_PATH = os.getenv(
    "JIEBA_ZIP_PATH",
    "",
)

# Jieba 自訂字典（支援本機路徑或 s3a://；Spark executors 會以 addFile 分發）
JIEBA_USERDICT_PATH = os.getenv("JIEBA_USERDICT_PATH", "")
# 可選：依 dataset_id 綁定字典路徑模板，例：s3a://bucket/dic/jieba_dicts/{dataset_id}.txt
JIEBA_USERDICT_DATASET_PATTERN = os.getenv("JIEBA_USERDICT_DATASET_PATTERN", "")

# 可選：停用詞表（每行一詞；# 開頭為註解；與 jieba 詞典分開；僅 Gold 分析使用）
STOPWORDS_PATH = os.getenv("STOPWORDS_PATH", "")
STOPWORDS_DATASET_PATTERN = os.getenv("STOPWORDS_DATASET_PATTERN", "")
# 黃金發行停用詞版本（analytics_tokens／痛點快照；僅發版時 bump）
STOPWORDS_LEXICON_VERSION = os.getenv("STOPWORDS_LEXICON_VERSION", "v1.0.0")
# 探索／測試停用詞版本（tfidf_exploration_tokens；日常可改 dic/stop_words/dev/）
STOPWORDS_EXPLORATION_LEXICON_VERSION = os.getenv("STOPWORDS_EXPLORATION_LEXICON_VERSION", "dev")

# 銀層轉換版本（冪等性：同版本 + 同 Bronze → 同 Silver）
SILVER_TRANSFORM_VERSION = os.getenv("SILVER_TRANSFORM_VERSION", "v2.1.0")

def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "true" if default else "false").strip().lower()
    return raw in ("1", "true", "yes", "on")


SILVER_QUALITY_ENABLED = _env_bool("SILVER_QUALITY_ENABLED", True)
SILVER_QUALITY_FAIL_ON_HARD = _env_bool("SILVER_QUALITY_FAIL_ON_HARD", True)
SILVER_QUALITY_MAX_NOISE_ROW_RATIO = _env_float("SILVER_QUALITY_MAX_NOISE_ROW_RATIO", 0.001)
SILVER_QUALITY_MAX_LEN1_TOKEN_RATIO = _env_float("SILVER_QUALITY_MAX_LEN1_TOKEN_RATIO", 0.01)
SILVER_QUALITY_MAX_LONG_TOKEN_RATIO = _env_float("SILVER_QUALITY_MAX_LONG_TOKEN_RATIO", 0.005)
SILVER_QUALITY_MAX_EMPTY_CLEANED_RATIO = _env_float("SILVER_QUALITY_MAX_EMPTY_CLEANED_RATIO", 0.5)
SILVER_QUALITY_MIN_NONEMPTY_TOKENS_RATIO = _env_float("SILVER_QUALITY_MIN_NONEMPTY_TOKENS_RATIO", 0.3)
SILVER_QUALITY_MIN_CHAR_RETENTION_RATIO = _env_float("SILVER_QUALITY_MIN_CHAR_RETENTION_RATIO", 0.15)
SILVER_QUALITY_TOP_N = int(os.getenv("SILVER_QUALITY_TOP_N", "50"))
SILVER_TOP_TOKEN_DENYLIST = tuple(
    w.strip()
    for w in os.getenv("SILVER_TOP_TOKEN_DENYLIST", "").split(",")
    if w.strip()
)

# Notebook：RAW_IMAGE_PREFIX（用於拼 RAW_IMAGES_PATH）
RAW_IMAGE_PREFIX = os.getenv("RAW_IMAGE_PREFIX", "raw/images/")

# Notebook：RAW_IMAGES_PATH（由 BUCKET_NAME 與 RAW_IMAGE_PREFIX 拼出）
RAW_IMAGES_PATH = os.getenv("RAW_IMAGES_PATH", f"s3a://{BUCKET_NAME}/{RAW_IMAGE_PREFIX}")

# OCR（Tesseract / pytesseract）：語言包需已安裝於系統；Windows 可設 TESSERACT_CMD 指向 tesseract.exe
OCR_LANG = os.getenv("OCR_LANG", "chi_tra+eng")
OCR_PSM = os.getenv("OCR_PSM", "6")
TESSERACT_CMD = os.getenv("TESSERACT_CMD", "")
# 前處理：短邊放大（0=不放大）、對比度、銳利度
OCR_SCALE_MIN_SIDE = os.getenv("OCR_SCALE_MIN_SIDE", "0")
OCR_CONTRAST = os.getenv("OCR_CONTRAST", "1.5")
OCR_SHARPNESS = os.getenv("OCR_SHARPNESS", "1.0")
OCR_BINARIZE = os.getenv("OCR_BINARIZE", "off")
OCR_PREPROCESS_VERSION = os.getenv("OCR_PREPROCESS_VERSION", "v1")
OCR_SIGNATURE = os.getenv("OCR_SIGNATURE", "").strip()
# 前處理 preset 分流（預設關閉；開啟後於單次 Bronze OCR 內依圖選 profile）
OCR_PRESET_ROUTER_ENABLED = _env_bool("OCR_PRESET_ROUTER_ENABLED", False)
OCR_LOW_RES_SHORT_SIDE = int(os.getenv("OCR_LOW_RES_SHORT_SIDE", "720"))
OCR_LOW_RES_TARGET_SIDE = int(os.getenv("OCR_LOW_RES_TARGET_SIDE", "1080"))
OCR_LIGHT_DOC_MEAN_LUMA = _env_float("OCR_LIGHT_DOC_MEAN_LUMA", 180.0)
# Tesseract user-words（本機路徑或 s3a://；Bronze OCR 前 addFile 分發）
OCR_USER_WORDS_PATH = os.getenv("OCR_USER_WORDS_PATH", "")
OCR_USER_WORDS_DATASET_PATTERN = os.getenv("OCR_USER_WORDS_DATASET_PATTERN", "")

# 痛點主題模糊匹配（Gold label_pain_topics）
PAIN_FUZZY_ENABLED = os.getenv("PAIN_FUZZY_ENABLED", "true")
PAIN_FUZZY_MIN_RATIO = os.getenv("PAIN_FUZZY_MIN_RATIO", "0.78")
PAIN_FUZZY_ANCHOR_RATIO = os.getenv("PAIN_FUZZY_ANCHOR_RATIO", "0.88")
PAIN_FUZZY_MIN_CHARS = os.getenv("PAIN_FUZZY_MIN_CHARS", "3")

# OCR PSM A/B 測試（獨立 test 路徑，不寫入正式 Bronze）
OCR_AB_SAMPLE_SIZE = int(os.getenv("OCR_AB_SAMPLE_SIZE", "20"))
OCR_AB_MAX_SAMPLE_SIZE = int(os.getenv("OCR_AB_MAX_SAMPLE_SIZE", "50"))
OCR_AB_RESULTS_PATH = os.getenv(
    "OCR_AB_RESULTS_PATH",
    f"s3a://{BUCKET_NAME}/test/ocr_psm_ab/",
)

# 上傳至 MinIO 時若物件已存在：suffix=自動改檔名加時間戳；overwrite=直接覆寫
UPLOAD_ON_DUPLICATE = os.getenv("UPLOAD_ON_DUPLICATE", "suffix").strip().lower()

