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

# Bronze 列級隔離（Silver ETL 前；寫入 quarantine Delta）
BRONZE_QUARANTINE_ENABLED = _env_bool("BRONZE_QUARANTINE_ENABLED", True)
BRONZE_QUARANTINE_PATH = os.getenv(
    "BRONZE_QUARANTINE_PATH",
    "s3a://data-lake/bronze/quarantine/",
)
BRONZE_QUARANTINE_MAX_REJECT_RATE = _env_float("BRONZE_QUARANTINE_MAX_REJECT_RATE", 0.10)
BRONZE_QUARANTINE_HARD_REJECT_RATE = _env_float("BRONZE_QUARANTINE_HARD_REJECT_RATE", 0.30)
BRONZE_QUARANTINE_MIN_TEXT_LEN = int(os.getenv("BRONZE_QUARANTINE_MIN_TEXT_LEN", "4"))
# soft：隔離占比高時好列仍進 Silver、擋核准；hard：>軟門檻即整批不進 Silver
BRONZE_QUARANTINE_MELT_MODE = os.getenv("BRONZE_QUARANTINE_MELT_MODE", "soft").strip().lower()
if BRONZE_QUARANTINE_MELT_MODE not in ("soft", "hard"):
    BRONZE_QUARANTINE_MELT_MODE = "soft"

# Bronze merge 前自動歸檔舊列（懶建立：第一次 merge 才 append 至 history Delta）
BRONZE_HISTORY_ON_MERGE = _env_bool("BRONZE_HISTORY_ON_MERGE", True)
BRONZE_HISTORY_PATH = os.getenv(
    "BRONZE_HISTORY_PATH",
    f"s3a://{BUCKET_NAME}/bronze/history/",
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

# ---------------------------------------------------------------------------
# Resource Guard（P3：Request / Pipeline / Runtime 三層資源保護）
# ---------------------------------------------------------------------------
MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", "15"))
MAX_UPLOAD_FILES_PER_REQUEST = int(os.getenv("MAX_UPLOAD_FILES_PER_REQUEST", "20"))
MAX_BRONZE_OCR_IMAGES = int(os.getenv("MAX_BRONZE_OCR_IMAGES", "100"))
# P4：MinIO SDK fallback 分批讀圖（每批最多 N 張進 driver；不超過 MAX_BRONZE_OCR_IMAGES）
OCR_MINIO_BATCH_SIZE = int(os.getenv("OCR_MINIO_BATCH_SIZE", "20"))
# P4：Bronze OCR 前 repartition（0=不強制；擴量時建議設為 CPU 核心數）
OCR_REPARTITION = int(os.getenv("OCR_REPARTITION", "0"))
ETL_MAX_CONCURRENT_JOBS = int(os.getenv("ETL_MAX_CONCURRENT_JOBS", "1"))
ETL_MEMORY_MAX_PERCENT = _env_float("ETL_MEMORY_MAX_PERCENT", 85.0)
ETL_MEMORY_MIN_AVAILABLE_MB = int(os.getenv("ETL_MEMORY_MIN_AVAILABLE_MB", "1536"))
ETL_RESOURCE_GUARD_ENABLED = _env_bool("ETL_RESOURCE_GUARD_ENABLED", True)
# Spark local 模式（driver 為主；executor 設定供對齊／未來叢集）
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g").strip() or "2g"
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "2g").strip() or "2g"
SPARK_DRIVER_MAX_RESULT_SIZE = os.getenv("SPARK_DRIVER_MAX_RESULT_SIZE", "512m").strip() or "512m"
# P4 執行期節流（0=關閉該項限制）
OCR_TIMEOUT_SECONDS = int(os.getenv("OCR_TIMEOUT_SECONDS", "30"))
SPARK_JOB_TIMEOUT_SECONDS = int(os.getenv("SPARK_JOB_TIMEOUT_SECONDS", "300"))

# 管線新鮮度／外部探針（cron：pipeline_freshness_check.py）
PIPELINE_HEARTBEAT_FILE = os.getenv("PIPELINE_HEARTBEAT_FILE", "var/pipeline_heartbeat.json")
PIPELINE_FRESHNESS_STATE_FILE = os.getenv(
    "PIPELINE_FRESHNESS_STATE_FILE",
    "var/pipeline_freshness_state.json",
)
FRESHNESS_STALE_HOURS = _env_float("FRESHNESS_STALE_HOURS", 12.0)

# 管線探針／告警（cron：pipeline_probe.py）
PIPELINE_PROBE_LAST_FILE = os.getenv("PIPELINE_PROBE_LAST_FILE", "var/pipeline_probe_last.json")
PIPELINE_PROBE_READY_URL = os.getenv("PIPELINE_PROBE_READY_URL", "").strip()
# none | discord | line_notify
PIPELINE_NOTIFY_BACKEND = os.getenv("PIPELINE_NOTIFY_BACKEND", "none").strip().lower()
PIPELINE_NOTIFY_WEBHOOK_URL = os.getenv("PIPELINE_NOTIFY_WEBHOOK_URL", "").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
# LINE Messaging API（Notify 已停服；Bot push 至指定 User ID）
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
LINE_PUSH_USER_ID = os.getenv("LINE_PUSH_USER_ID", "").strip()

# -------------------------
# 時區政策（Delta／指標存 UTC；UI 顯示台北）
# -------------------------
STORAGE_TIMEZONE = os.getenv("STORAGE_TIMEZONE", "UTC")
DISPLAY_TIMEZONE = os.getenv("DISPLAY_TIMEZONE", "Asia/Taipei")

