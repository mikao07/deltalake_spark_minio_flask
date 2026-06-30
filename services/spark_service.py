from __future__ import annotations

import os
import re
import threading
import logging
from datetime import datetime
from urllib.parse import urlparse
from typing import Any, Dict, Iterable, List, Optional

import psutil
from pyspark import SparkFiles

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    collect_set,
    concat,
    count,
    countDistinct,
    current_timestamp,
    explode,
    expr,
    length,
    lit,
    log as spark_log,
    lower,
    max as spark_max,
    regexp_extract,
    regexp_replace,
    row_number,
    size,
    sum as spark_sum,
    trim,
    to_timestamp,
    udf,
)
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from config import (
    BRONZE_QUARANTINE_PATH,
    BRONZE_TABLE_PATH,
    GOLD_TFIDF_KEYWORDS_PATH,
    GOLD_PHRASE_CANDIDATES_PATH,
    GOLD_TOPIC_SNAPSHOT_PATH,
    GOLD_TOPIC_SNAPSHOT_VERIFY_AFTER_WRITE,
    JIEBA_USERDICT_DATASET_PATTERN,
    JIEBA_USERDICT_PATH,
    JIEBA_ZIP_PATH,
    STOPWORDS_DATASET_PATTERN,
    STOPWORDS_PATH,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    OCR_BINARIZE,
    OCR_CONTRAST,
    OCR_LANG,
    OCR_LIGHT_DOC_MEAN_LUMA,
    OCR_LOW_RES_SHORT_SIDE,
    OCR_LOW_RES_TARGET_SIDE,
    OCR_PREPROCESS_VERSION,
    OCR_PRESET_ROUTER_ENABLED,
    OCR_PSM,
    OCR_SCALE_MIN_SIDE,
    OCR_SHARPNESS,
    S3A_CONNECTION_SSL_ENABLED,
    S3A_ENDPOINT_REGION,
    S3A_IMPL,
    S3A_PATH_STYLE_ACCESS,
    SILVER_OCR_TABLE_PATH,
    SILVER_TRANSFORM_VERSION,
    STOPWORDS_EXPLORATION_LEXICON_VERSION,
    STOPWORDS_LEXICON_VERSION,
    TESSERACT_CMD,
)
from services.domain_lexicons import resolve_local_jieba_userdict_path
from services.lexicon import (
    collect_gold_dual_lexicon,
    collect_gold_lexicon,
    filter_tokens_for_analytics,
    filter_tokens_for_tfidf_exploration,
    parse_stopwords_lines,
)
from services.pain_topic_rules import TOPIC_RULE_VERSION, label_pain_topics
from services.bronze_quarantine import BronzeQuarantineError, apply_bronze_quarantine_gate
from services.silver_quality import evaluate_gold_downstream_quality, run_silver_quality_gate
from services.text_tokens import BUILTIN_STOPWORDS, SILVER_CLEAN_TEXT_SPARK_PATTERN

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 依照 Notebook 的 SparkSession.builder 與 Delta Lake 設定來建立 Spark
# ---------------------------------------------------------------------------
# 與 VM / requirements：Spark 3.5 + Delta 3.0（Maven 使用 delta-spark_2.12，非舊版 delta-core）
PACKAGES = (
    "io.delta:delta-spark_2.12:3.0.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

S3A_CONNECTION_SSL_ENABLED_VALUE = S3A_CONNECTION_SSL_ENABLED


def _normalize_s3a_endpoint(raw_endpoint: str) -> tuple[str, str]:
    """
    將 MINIO_ENDPOINT 正規化給 Spark S3A 使用：
    - endpoint: host:port（不含 scheme）
    - ssl_enabled: "true" / "false"
    """
    raw = (raw_endpoint or "").strip()
    if not raw:
        return "127.0.0.1:9000", "false"

    if "://" in raw:
        u = urlparse(raw)
        host = u.hostname or "127.0.0.1"
        if u.port:
            endpoint = f"{host}:{u.port}"
        else:
            endpoint = f"{host}:443" if u.scheme == "https" else f"{host}:9000"
        ssl_enabled = "true" if u.scheme == "https" else "false"
        return endpoint, ssl_enabled

    # 已是 host[:port] 形式，ssl 跟隨現有設定
    return raw, str(S3A_CONNECTION_SSL_ENABLED_VALUE).strip().lower()


class SparkManager:
    """
    SparkSession 單例管理器，確保整個 App 運行期間只會被建立一次。
    """

    _instance: "SparkManager | None" = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, app_name: str = "Jupyter_SilverLayer_Merge_ETL"):
        # singleton：避免 __init__ 重入造成重複啟動
        if getattr(self, "_initialized", False):
            return

        if not MINIO_ACCESS_KEY or not str(MINIO_ACCESS_KEY).strip():
            raise ValueError("缺少 `MINIO_ACCESS_KEY`（請在環境變數中設定）。")
        if not MINIO_SECRET_KEY or not str(MINIO_SECRET_KEY).strip():
            raise ValueError("缺少 `MINIO_SECRET_KEY`（請在環境變數中設定）。")

        s3a_endpoint, s3a_ssl_enabled = _normalize_s3a_endpoint(MINIO_ENDPOINT)

        ocr_executor_env = {
            "OCR_LANG": OCR_LANG,
            "OCR_PSM": OCR_PSM,
            "OCR_SCALE_MIN_SIDE": OCR_SCALE_MIN_SIDE,
            "OCR_CONTRAST": OCR_CONTRAST,
            "OCR_SHARPNESS": OCR_SHARPNESS,
            "OCR_BINARIZE": OCR_BINARIZE,
            "OCR_PREPROCESS_VERSION": OCR_PREPROCESS_VERSION,
            "OCR_PRESET_ROUTER_ENABLED": str(OCR_PRESET_ROUTER_ENABLED).lower(),
            "OCR_LOW_RES_SHORT_SIDE": OCR_LOW_RES_SHORT_SIDE,
            "OCR_LOW_RES_TARGET_SIDE": OCR_LOW_RES_TARGET_SIDE,
            "OCR_LIGHT_DOC_MEAN_LUMA": OCR_LIGHT_DOC_MEAN_LUMA,
            "TESSERACT_CMD": TESSERACT_CMD or "",
        }

        builder = (
            SparkSession.builder.appName(app_name)
            # Maven 套件載入（hadoop-aws / aws-java-sdk-bundle / delta-spark）
            .config("spark.jars.packages", PACKAGES)
            # Delta Lake
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # MinIO S3A（完全參考 config.py）
            .config("spark.hadoop.fs.s3a.endpoint", s3a_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", S3A_PATH_STYLE_ACCESS)
            .config("spark.hadoop.fs.s3a.impl", S3A_IMPL)
            .config("spark.hadoop.fs.s3a.endpoint.region", S3A_ENDPOINT_REGION)
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", s3a_ssl_enabled)
            # 中文 / UTF-8 編碼修正（對齊 Notebook）
            .config("spark.executorEnv.LANG", "en_US.UTF-8")
            .config("spark.executorEnv.LC_ALL", "en_US.UTF-8")
            .config("spark.executorEnv.PYTHONIOENCODING", "UTF-8")
            .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8")
            .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8")
            .config("spark.sql.execution.python.udf.inPandas.parent.env", "PYTHONIOENCODING=UTF-8")
        )
        for key, value in ocr_executor_env.items():
            builder = builder.config(f"spark.executorEnv.{key}", str(value))

        self.spark = builder.getOrCreate()

        # 對齊 Notebook：後續呼叫時再確保一致行為
        self.spark.conf.set("spark.executorEnv.PYTHONIOENCODING", "UTF-8")
        self.spark.conf.set("spark.sql.execution.python.udf.inPandas.parent.env", "PYTHONIOENCODING=UTF-8")
        self._initialized = True


def create_spark_session(app_name: str = "Jupyter_SilverLayer_Merge_ETL") -> SparkSession:
    """
    舊介面：由 SparkManager（單例）建立 SparkSession。
    """

    return SparkManager(app_name=app_name).spark


# ---------------------------------------------------------------------------
# Delta Lake 讀寫封裝
# ---------------------------------------------------------------------------
def read_delta_table(spark: SparkSession, table_path: str) -> DataFrame:
    return spark.read.format("delta").load(table_path)


def write_delta_table(df: DataFrame, table_path: str, mode: str = "append") -> None:
    df.write.format("delta").mode(mode).save(table_path)


def delta_table_exists(spark: SparkSession, table_path: str) -> bool:
    """
    Notebook 有使用 spark._jsparkSession.catalog().tableExists("delta.`{}`".format(path))，
    這裡先沿用該作法；若失敗則回退到 DeltaTable.isDeltaTable。
    """

    try:
        return bool(spark._jsparkSession.catalog().tableExists(f"delta.`{table_path}`"))
    except Exception:
        return bool(DeltaTable.isDeltaTable(spark, table_path))


def _ensure_delta_columns(
    spark: SparkSession,
    table_path: str,
    columns: dict[str, str],
    *,
    existing_cols: set[str] | None = None,
) -> set[str]:
    """既有 Delta 表補齊缺少欄位（schema evolution），回傳更新後欄位集合。"""
    cols = set(existing_cols) if existing_cols is not None else set(read_delta_table(spark, table_path).columns)
    for name, dtype in columns.items():
        if name in cols:
            continue
        spark.sql(f"ALTER TABLE delta.`{table_path}` ADD COLUMNS ({name} {dtype})")
        cols.add(name)
    return cols


def _hadoop_path_exists(spark: SparkSession, path: str) -> bool:
    """使用 Hadoop FileSystem 檢查路徑是否存在（支援 s3a:// 與本機路徑）。"""
    if not path or not str(path).strip():
        return False
    try:
        jvm = spark._jvm
        hconf = spark._jsc.hadoopConfiguration()
        jpath = jvm.org.apache.hadoop.fs.Path(str(path).strip())
        fs = jpath.getFileSystem(hconf)
        return bool(fs.exists(jpath))
    except Exception:
        return False


def merge_upsert_by_key(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    key_col: str,
) -> None:
    """
    依照 Notebook 的 MERGE 邏輯：
    - Silver 表存在：用 key_col 做 upsert（whenMatchedUpdateAll / whenNotMatchedInsertAll）
    - Silver 表不存在：直接 overwrite 寫入建立資料表
    """

    if delta_table_exists(spark, target_path):
        delta_table = DeltaTable.forPath(spark, target_path)

        delta_table.alias("target").merge(
            source=source_df.alias("source"),
            condition=expr(f"target.{key_col} = source.{key_col}"),
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        write_delta_table(source_df, target_path, mode="overwrite")


def delete_by_condition(spark: SparkSession, target_path: str, condition_sql: str) -> None:
    """
    依照 Notebook 的 deltaTable.delete(condition=...)。
    condition_sql 需要是可被 Spark SQL 解析的條件字串。
    """

    delta_table = DeltaTable.forPath(spark, target_path)
    delta_table.delete(condition=condition_sql)


def delete_older_than_latest_batch(
    spark: SparkSession,
    target_path: str,
    timestamp_col: str = "ingestion_timestamp",
) -> None:
    """
    對齊 Notebook 的作法：
    - 找出 timestamp_col 的最大值 latest_time
    - delete：to_timestamp(timestamp_col) < to_timestamp(latest_time)
    """

    df = read_delta_table(spark, target_path)

    latest_ts = (
        df.select(to_timestamp(col(timestamp_col)).alias("ts_col"))
        .agg(spark_max("ts_col").alias("latest_time"))
        .collect()[0]["latest_time"]
    )

    if latest_ts is None:
        return

    # Spark to_timestamp('...') 需要字串；用無時區字串格式較穩
    latest_ts_str = str(latest_ts)
    delete_by_condition(
        spark,
        target_path,
        condition_sql=f"to_timestamp({timestamp_col}) < to_timestamp('{latest_ts_str}')",
    )


def _extract_dataset_id_col(df: DataFrame) -> DataFrame:
    # raw/images/{dataset_id}/...
    return df.withColumn("dataset_id", regexp_extract(col("image_path"), r"/raw/images/([^/]+)/", 1))


_silver_tokens_udf_cache: dict[str, Any] = {}


def _silver_cleaned_text_expr(source_col: str = "extracted_text"):
    """
    銀層 Spark 欄位運算（僅去標點）；正式 cleaned_text 以 _silver_cleaned_text_udf 為準（含剝純數字）。
    與 text_tokens.clean_text_for_segmentation 前半段語意對齊。
    """
    src = trim(col(source_col))
    return lower(
        trim(
            regexp_replace(
                regexp_replace(src, SILVER_CLEAN_TEXT_SPARK_PATTERN, " "),
                r"\s+",
                " ",
            )
        )
    )


def prepare_bronze_deduped_for_silver(
    df_bronze: DataFrame,
    *,
    dataset_id: str | None = None,
) -> DataFrame:
    """Bronze 去重（每 image_path 取最新 ingestion）並 trim extracted_text。"""
    ds = _normalize_dataset_id_or_none(dataset_id)
    df = df_bronze.filter(col("extracted_text").isNotNull())
    if ds:
        df = _filter_df_by_dataset_id(df, ds)

    w = Window.partitionBy(col("image_path")).orderBy(col("ingestion_timestamp").desc())
    df = df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")
    df = df.withColumn("extracted_text", trim(col("extracted_text")))
    df = _extract_dataset_id_col(df)
    if ds:
        df = df.withColumn("dataset_id", lit(ds))
    return df.withColumnRenamed("ingestion_timestamp", "latest_ingestion_timestamp")


def build_silver_ocr_updates_from_bronze(
    df_bronze: DataFrame,
    *,
    dataset_id: str | None = None,
) -> DataFrame:
    """
    Bronze OCR -> Silver 更新集（去重、保留原文）。
    標點清理與分詞在 enrich_silver_dataframe_with_tokens 產出 cleaned_text / tokens。
    """
    df = prepare_bronze_deduped_for_silver(df_bronze, dataset_id=dataset_id)
    return df.select(
        "image_path",
        "extracted_text",
        "source_bucket",
        col("latest_ingestion_timestamp"),
        "dataset_id",
    )


def _make_silver_tokens_udf(
    jieba_userdict_path: str | None = None,
    dataset_id_for_log: str | None = None,
):
    userdict_basename = ""
    if jieba_userdict_path and str(jieba_userdict_path).strip():
        userdict_basename = os.path.basename(str(jieba_userdict_path).strip())

    def tokens_from_text(text):
        if not hasattr(tokens_from_text, "_jieba_initialized"):
            tokens_from_text._jieba_initialized = False
            tokens_from_text._userdict_loaded = False
            tokens_from_text._userdict_error_logged = False

        try:
            import jieba
            from services.text_tokens import segment_text_to_tokens

            if not tokens_from_text._jieba_initialized:
                jieba.initialize()
                tokens_from_text._jieba_initialized = True

            userdict_local = None
            if userdict_basename and not tokens_from_text._userdict_loaded:
                try:
                    userdict_local = SparkFiles.get(userdict_basename)
                    jieba.load_userdict(userdict_local)
                    tokens_from_text._userdict_loaded = True
                except Exception as e:
                    if not tokens_from_text._userdict_error_logged:
                        _logger.warning(
                            "silver_tokens_userdict_load_failed_once: dataset_id=%s path=%s error=%s",
                            dataset_id_for_log,
                            jieba_userdict_path,
                            e,
                        )
                        tokens_from_text._userdict_error_logged = True

            return segment_text_to_tokens(
                text,
                userdict_local_path=None,
                apply_noise_filter=True,
                already_cleaned=True,
            )
        except Exception as e:
            _logger.warning("silver_tokens_udf_failed_once: %s", e)
            return []

    return udf(tokens_from_text, ArrayType(StringType()))


def _get_silver_tokens_udf(
    jieba_userdict_path: str | None = None,
    dataset_id_for_log: str | None = None,
):
    cache_key = f"{str(jieba_userdict_path or '').strip()}|{str(dataset_id_for_log or '').strip()}"
    existing = _silver_tokens_udf_cache.get(cache_key)
    if existing is not None:
        return existing
    created = _make_silver_tokens_udf(
        jieba_userdict_path if str(jieba_userdict_path or "").strip() else None,
        dataset_id_for_log=dataset_id_for_log,
    )
    _silver_tokens_udf_cache[cache_key] = created
    return created


def _make_silver_cleaned_text_udf():
    def cleaned_from_raw(text):
        from services.text_tokens import clean_text_for_segmentation

        if text is None:
            return ""
        return clean_text_for_segmentation(text)

    return udf(cleaned_from_raw, StringType())


_silver_cleaned_text_udf = _make_silver_cleaned_text_udf()


def enrich_silver_dataframe_with_tokens(
    df: DataFrame,
    *,
    jieba_userdict_path: str | None = None,
    dataset_id_for_log: str | None = None,
) -> DataFrame:
    """Silver：cleaned_text（物理清洗）→ Jieba 分詞 + 內建虛詞停用詞 → tokens（冪等）。"""
    tokens_udf = _get_silver_tokens_udf(
        jieba_userdict_path,
        dataset_id_for_log=dataset_id_for_log,
    )
    return (
        df.withColumn("cleaned_text", _silver_cleaned_text_udf(col("extracted_text")))
        .withColumn("tokens", tokens_udf(col("cleaned_text")))
    )


def run_silver_ocr_etl(
    *,
    bronze_path: str | None = None,
    silver_ocr_path: str | None = None,
    dataset_id: str | None = None,
) -> dict[str, Any]:
    """
    Bronze OCR -> Silver OCR（去重、cleaned_text、分詞 tokens、MERGE）。
    """
    spark = SparkManager().spark
    bronze = bronze_path or BRONZE_TABLE_PATH
    silver = silver_ocr_path or SILVER_OCR_TABLE_PATH
    ds = _normalize_dataset_id_or_none(dataset_id)

    df_bronze = read_delta_table(spark, bronze)
    df_prep = prepare_bronze_deduped_for_silver(df_bronze, dataset_id=ds)
    df_prep, bronze_quarantine = apply_bronze_quarantine_gate(
        spark,
        df_prep,
        quarantine_path=BRONZE_QUARANTINE_PATH,
        bronze_path=bronze,
        dataset_id=ds,
    )
    df_updates = df_prep.select(
        "image_path",
        "extracted_text",
        "source_bucket",
        col("latest_ingestion_timestamp"),
        "dataset_id",
    )

    active_userdict_path = _resolve_existing_jieba_userdict_path(spark, ds)
    register_jieba_pyfile_if_needed(
        spark,
        JIEBA_ZIP_PATH,
        active_userdict_path,
        dataset_id=ds,
    )

    df_updates = enrich_silver_dataframe_with_tokens(
        df_updates,
        jieba_userdict_path=active_userdict_path,
        dataset_id_for_log=ds,
    )
    df_updates = df_updates.withColumn("silver_transform_version", lit(SILVER_TRANSFORM_VERSION))

    update_count_raw = int(df_updates.count())
    if update_count_raw == 0:
        return {
            "updated_rows": 0,
            "inserted_rows": 0,
            "updated_existing_rows": 0,
            "silver_batch_ts": "",
            "dataset_id": ds,
            "silver_ocr_path": silver,
            "bronze_path": bronze,
            "tokens_column_written": True,
            "cleaned_text_column_written": True,
            "builtin_stopwords_count": len(BUILTIN_STOPWORDS),
            "silver_transform_version": SILVER_TRANSFORM_VERSION,
            "bronze_quarantine": bronze_quarantine,
        }

    batch_ts = datetime.utcnow()
    batch_ts_lit = lit(batch_ts)

    if delta_table_exists(spark, silver):
        # 先補 schema，再建立 DeltaTable（否則 MERGE 仍用舊欄位清單）
        _ensure_delta_columns(
            spark,
            silver,
            {
                "cleaned_text": "STRING",
                "tokens": "ARRAY<STRING>",
                "dataset_id": "STRING",
                "silver_transform_version": "STRING",
            },
        )
        delta_table = DeltaTable.forPath(spark, silver)
        # ALTER 後必須重讀，否則 df_target 仍為舊 schema（無 dataset_id / tokens / cleaned_text）
        df_target = read_delta_table(spark, silver)
        target_cols = set(df_target.columns)
        df_target_cmp = df_target.select(
            col("image_path").alias("_t_image_path"),
            col("extracted_text").alias("_t_extracted_text"),
            col("source_bucket").alias("_t_source_bucket"),
            col("ingestion_timestamp").alias("_t_ingestion_timestamp"),
            col("dataset_id").alias("_t_dataset_id") if "dataset_id" in target_cols else lit(None).alias("_t_dataset_id"),
            col("cleaned_text").alias("_t_cleaned_text")
            if "cleaned_text" in target_cols
            else lit(None).cast(StringType()).alias("_t_cleaned_text"),
            col("tokens").alias("_t_tokens")
            if "tokens" in target_cols
            else lit(None).cast(ArrayType(StringType())).alias("_t_tokens"),
            col("silver_transform_version").alias("_t_silver_transform_version")
            if "silver_transform_version" in target_cols
            else lit(None).cast(StringType()).alias("_t_silver_transform_version"),
        )
        df_cmp = df_updates.alias("u").join(
            df_target_cmp.alias("t"),
            col("u.image_path") == col("t._t_image_path"),
            how="left",
        )
        same_dataset_expr = (
            trim(lower(col("u.dataset_id"))) == trim(lower(col("t._t_dataset_id")))
            if "dataset_id" in target_cols
            else lit(True)
        )
        unchanged_expr = (
            col("t._t_image_path").isNotNull()
            & (col("u.extracted_text") == col("t._t_extracted_text"))
            & (col("u.cleaned_text") == col("t._t_cleaned_text"))
            & (col("u.source_bucket") == col("t._t_source_bucket"))
            & (col("u.latest_ingestion_timestamp") == col("t._t_ingestion_timestamp"))
            & same_dataset_expr
        )
        if "cleaned_text" in target_cols:
            cleaned_stale = col("t._t_cleaned_text").isNull() | (length(trim(col("t._t_cleaned_text"))) == 0)
            unchanged_expr = unchanged_expr & (~cleaned_stale)
        if "tokens" in target_cols:
            tokens_stale = col("t._t_tokens").isNull() | (size(col("t._t_tokens")) == 0)
            unchanged_expr = unchanged_expr & (~tokens_stale)
        else:
            # 舊 Silver 表尚無 tokens 欄位：本次 MERGE 需寫入分詞結果
            unchanged_expr = lit(False)
        # 轉換版本不一致（NULL 或舊版）→ 需以目前規則重算並覆寫
        if "silver_transform_version" in target_cols:
            transform_current = col("t._t_silver_transform_version").isNotNull() & (
                trim(col("t._t_silver_transform_version")) == lit(SILVER_TRANSFORM_VERSION)
            )
            unchanged_expr = unchanged_expr & transform_current
        else:
            unchanged_expr = lit(False)
        df_changes = df_cmp.filter(~unchanged_expr).select("u.*")
        update_count = int(df_changes.count())
        if update_count == 0:
            silver_quality: Dict[str, Any] = {"skipped": True, "reason": "merge_unchanged"}
            if delta_table_exists(spark, silver):
                df_quality = read_delta_table(spark, silver)
                if ds:
                    df_quality = _filter_df_by_dataset_id(df_quality, ds)
                if int(df_quality.limit(1).count()) > 0:
                    silver_quality = run_silver_quality_gate(df_quality)
            return {
                "updated_rows": 0,
                "inserted_rows": 0,
                "updated_existing_rows": 0,
                "silver_batch_ts": "",
                "dataset_id": ds,
                "silver_ocr_path": silver,
                "bronze_path": bronze,
                "silver_transform_version": SILVER_TRANSFORM_VERSION,
                "silver_quality": silver_quality,
                "merge_note": "所有列與目前轉換版本一致，未寫入",
                "bronze_quarantine": bronze_quarantine,
            }
        inserted_rows = int(df_cmp.filter(col("t._t_image_path").isNull()).count())
        updated_existing_rows = max(0, update_count - inserted_rows)
        update_set = {
            "extracted_text": col("source.extracted_text"),
            "source_bucket": col("source.source_bucket"),
            "ingestion_timestamp": col("source.latest_ingestion_timestamp"),
            "etl_update_timestamp": batch_ts_lit,
            "tokens": col("source.tokens"),
            "silver_transform_version": col("source.silver_transform_version"),
        }
        insert_values = {
            "image_path": col("source.image_path"),
            "extracted_text": col("source.extracted_text"),
            "source_bucket": col("source.source_bucket"),
            "ingestion_timestamp": col("source.latest_ingestion_timestamp"),
            "etl_update_timestamp": batch_ts_lit,
            "tokens": col("source.tokens"),
            "silver_transform_version": col("source.silver_transform_version"),
        }
        if "cleaned_text" in target_cols:
            update_set["cleaned_text"] = col("source.cleaned_text")
            insert_values["cleaned_text"] = col("source.cleaned_text")
        else:
            _logger.warning(
                "silver_merge_missing_cleaned_text_column: path=%s — 略過寫入 cleaned_text",
                silver,
            )
        # 舊 Silver 表可能尚未有 dataset_id，避免 MERGE 直接失敗
        if "dataset_id" in target_cols:
            update_set["dataset_id"] = col("source.dataset_id")
            insert_values["dataset_id"] = col("source.dataset_id")
        (
            delta_table.alias("target")
            .merge(df_changes.alias("source"), "target.image_path = source.image_path")
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_values)
            .execute()
        )
    else:
        inserted_rows = update_count_raw
        updated_existing_rows = 0
        update_count = update_count_raw
        (
            df_updates.withColumnRenamed("latest_ingestion_timestamp", "ingestion_timestamp")
            .withColumn("etl_update_timestamp", batch_ts_lit)
            .write.format("delta")
            .mode("overwrite")
            .save(silver)
        )

    silver_quality: Dict[str, Any] = {"skipped": True, "reason": "no_rows_written"}
    if update_count > 0:
        df_quality = read_delta_table(spark, silver)
        if ds:
            df_quality = _filter_df_by_dataset_id(df_quality, ds)
        silver_quality = run_silver_quality_gate(df_quality)

    return {
        "updated_rows": update_count,
        "inserted_rows": inserted_rows,
        "updated_existing_rows": updated_existing_rows,
        "silver_batch_ts": batch_ts.isoformat(),
        "dataset_id": ds,
        "silver_ocr_path": silver,
        "bronze_path": bronze,
        "tokens_column_written": True,
        "cleaned_text_column_written": True,
        "builtin_stopwords_count": len(BUILTIN_STOPWORDS),
        "silver_transform_version": SILVER_TRANSFORM_VERSION,
        "jieba_userdict_used": bool(active_userdict_path),
        "jieba_userdict_path": active_userdict_path or "",
        "silver_quality": silver_quality,
        "bronze_quarantine": bronze_quarantine,
    }


# ---------------------------------------------------------------------------
# Gold 層：讀銀層 tokens → 套用版本化 lexicon → 痛點主題／TF-IDF／PMI
# ---------------------------------------------------------------------------
_jieba_pyfile_registered: str | None = None
_jieba_userdict_registered: str | None = None
_topic_rule_version = TOPIC_RULE_VERSION
_TOPIC_RULE_VERSION = TOPIC_RULE_VERSION
_gold_filter_tokens_udf_cache: Dict[str, Any] = {}
_gold_tfidf_filter_tokens_udf_cache: Dict[str, Any] = {}


def _make_gold_filter_tokens_udf(effective_stopwords: List[str]):
    stop_set = frozenset(effective_stopwords)

    def _filter_tokens(tokens):
        return filter_tokens_for_analytics(tokens, stop_set)

    return udf(_filter_tokens, ArrayType(StringType()))


def _make_gold_tfidf_filter_tokens_udf(tfidf_stopwords: List[str]):
    stop_set = frozenset(tfidf_stopwords)

    def _filter_tokens(tokens):
        return filter_tokens_for_tfidf_exploration(tokens, stop_set)

    return udf(_filter_tokens, ArrayType(StringType()))


def _get_gold_filter_tokens_udf(effective_stopwords: List[str]):
    key = ",".join(sorted(effective_stopwords))
    existing = _gold_filter_tokens_udf_cache.get(key)
    if existing is not None:
        return existing
    created = _make_gold_filter_tokens_udf(effective_stopwords)
    _gold_filter_tokens_udf_cache[key] = created
    return created


def _get_gold_tfidf_filter_tokens_udf(tfidf_stopwords: List[str]):
    key = ",".join(sorted(tfidf_stopwords))
    existing = _gold_tfidf_filter_tokens_udf_cache.get(key)
    if existing is not None:
        return existing
    created = _make_gold_tfidf_filter_tokens_udf(tfidf_stopwords)
    _gold_tfidf_filter_tokens_udf_cache[key] = created
    return created


def _with_gold_analytics_tokens(
    df_silver: DataFrame,
    spark: SparkSession,
    dataset_id: str | None,
) -> tuple[DataFrame, Dict[str, Any]]:
    """Gold：release lexicon → analytics_tokens；exploration lexicon → tfidf_exploration_tokens。"""
    bundle = collect_gold_dual_lexicon(spark, dataset_id)
    release = bundle.get("release") or {}
    exploration = bundle.get("exploration") or {}
    effective = release.get("effective_stopwords") or []
    tfidf_stop = exploration.get("tfidf_exploration_stopwords") or []
    if "tokens" not in df_silver.columns:
        return df_silver, bundle
    analytics_udf = _get_gold_filter_tokens_udf(list(effective))
    tfidf_udf = _get_gold_tfidf_filter_tokens_udf(list(tfidf_stop))
    return (
        df_silver.withColumn("analytics_tokens", analytics_udf(col("tokens")))
        .withColumn("tfidf_exploration_tokens", tfidf_udf(col("tokens"))),
        bundle,
    )


def _silver_token_column(df: DataFrame) -> str:
    return "analytics_tokens" if "analytics_tokens" in df.columns else "tokens"


def _tfidf_token_column(df: DataFrame) -> str:
    if "tfidf_exploration_tokens" in df.columns:
        return "tfidf_exploration_tokens"
    return _silver_token_column(df)


def _normalize_dataset_id_or_none(dataset_id: str | None) -> str | None:
    if dataset_id is None:
        return None
    raw = str(dataset_id).strip().lower()
    if not raw:
        return None
    safe = re.sub(r"[^a-z0-9_-]", "", raw)
    return safe or None


def _filter_df_by_dataset_id(df: DataFrame, dataset_id: str | None) -> DataFrame:
    ds = _normalize_dataset_id_or_none(dataset_id)
    if not ds:
        return df
    cols = set(df.columns)

    # 優先用欄位 dataset_id 過濾（最穩），避免依賴路徑格式
    if "dataset_id" in cols:
        return df.filter(trim(lower(col("dataset_id"))) == lit(ds))

    # 退回用 image_path 推導（兼容 s3a://... 與 Windows 反斜線）
    if "image_path" in cols:
        normalized_path = lower(regexp_replace(col("image_path"), r"\\\\", "/"))
        return df.filter(normalized_path.contains(f"/{ds}/"))

    # 無可用欄位時不過濾（避免整段變空）
    return df


def _resolve_existing_jieba_userdict_path(
    spark: SparkSession,
    dataset_id: str | None,
) -> str | None:
    ds = _normalize_dataset_id_or_none(dataset_id)
    pattern = str(JIEBA_USERDICT_DATASET_PATTERN or "").strip()
    fallback = str(JIEBA_USERDICT_PATH or "").strip()

    dataset_candidate = ""
    if ds and pattern:
        try:
            dataset_candidate = pattern.format(dataset_id=ds).strip()
        except Exception as e:
            _logger.warning("invalid_jieba_userdict_dataset_pattern: %s", e)
            dataset_candidate = ""

    if dataset_candidate:
        if _hadoop_path_exists(spark, dataset_candidate):
            return dataset_candidate
        _logger.warning(
            "jieba_userdict_missing_dataset_candidate: dataset_id=%s path=%s",
            ds,
            dataset_candidate,
        )

    if fallback:
        if _hadoop_path_exists(spark, fallback):
            if dataset_candidate:
                _logger.warning(
                    "jieba_userdict_fallback_used: dataset_id=%s fallback_path=%s",
                    ds,
                    fallback,
                )
            return fallback
        _logger.warning(
            "jieba_userdict_missing_fallback: dataset_id=%s path=%s",
            ds,
            fallback,
        )

    local_path = resolve_local_jieba_userdict_path(ds)
    if local_path:
        _logger.info(
            "jieba_userdict_local_used: dataset_id=%s path=%s",
            ds,
            local_path,
        )
        return local_path

    return None


def _parse_stopwords_lines(lines: Iterable[str]) -> List[str]:
    return parse_stopwords_lines(lines)


def _load_stopwords_from_path(spark: SparkSession, path: str) -> List[str]:
    try:
        rows = spark.read.text(str(path).strip()).select("value").collect()
    except Exception as e:
        _logger.warning("stopwords_read_failed: path=%s error=%s", path, e)
        return []
    lines = [r[0] for r in rows if r[0] is not None]
    return _parse_stopwords_lines(lines)


def _resolve_existing_stopwords_path(
    spark: SparkSession,
    dataset_id: str | None,
) -> str | None:
    ds = _normalize_dataset_id_or_none(dataset_id)
    pattern = str(STOPWORDS_DATASET_PATTERN or "").strip()
    fallback = str(STOPWORDS_PATH or "").strip()

    dataset_candidate = ""
    if ds and pattern:
        try:
            dataset_candidate = pattern.format(dataset_id=ds).strip()
        except Exception as e:
            _logger.warning("invalid_stopwords_dataset_pattern: %s", e)
            dataset_candidate = ""

    if dataset_candidate:
        if _hadoop_path_exists(spark, dataset_candidate):
            return dataset_candidate
        _logger.warning(
            "stopwords_missing_dataset_candidate: dataset_id=%s path=%s",
            ds,
            dataset_candidate,
        )

    if fallback:
        if _hadoop_path_exists(spark, fallback):
            if dataset_candidate:
                _logger.warning(
                    "stopwords_fallback_used: dataset_id=%s fallback_path=%s",
                    ds,
                    fallback,
                )
            return fallback
        _logger.warning(
            "stopwords_missing_fallback: dataset_id=%s path=%s",
            ds,
            fallback,
        )

    return None


def get_dictionary_usage_status(
    spark: SparkSession,
    dataset_id: str | None = None,
) -> Dict[str, Any]:
    """
    回傳目前 dataset 的辭典實際套用狀態（供 API/頁面顯示）。
    Silver 僅內建虛詞；領域停用詞在 Gold lexicon。
    """
    ds = _normalize_dataset_id_or_none(dataset_id)
    userdict_path = _resolve_existing_jieba_userdict_path(spark, ds)
    lexicon = collect_gold_dual_lexicon(spark, ds)
    release = lexicon.get("release") or {}
    exploration = lexicon.get("exploration") or {}
    manifest_release_id = ""
    manifest_approved_snapshot_at = ""
    manifest_lexicon_content_hash = ""
    manifest_dataset_id = ""
    try:
        from pathlib import Path

        from services.pipeline_guardian import load_manifest, resolve_manifest_path

        manifest_ds = ds or str(lexicon.get("dataset_id") or "").strip()
        if manifest_ds:
            manifest_dataset_id = manifest_ds
            mpath = resolve_manifest_path(manifest_ds)
            if mpath.is_file():
                manifest = load_manifest(Path(mpath))
                manifest_release_id = str(manifest.get("release_id") or "").strip()
                gold_m = manifest.get("gold") or {}
                manifest_approved_snapshot_at = str(gold_m.get("approved_snapshot_at") or "").strip()
                if manifest_approved_snapshot_at.lower() == "none":
                    manifest_approved_snapshot_at = ""
                manifest_lexicon_content_hash = str(gold_m.get("lexicon_content_hash") or "").strip()
    except Exception as e:
        _logger.warning("manifest_status_load_failed: dataset_id=%s error=%s", ds, e)
    return {
        "dataset_id": ds,
        "jieba_userdict_used": bool(userdict_path),
        "jieba_userdict_path": userdict_path or "",
        "silver_tokenization": "jieba_builtin_stopwords_only",
        "silver_transform_version": SILVER_TRANSFORM_VERSION,
        "builtin_stopwords_count": len(BUILTIN_STOPWORDS),
        "topic_rule_version": _TOPIC_RULE_VERSION,
        "gold_release_lexicon_version": release.get("lexicon_version") or STOPWORDS_LEXICON_VERSION,
        "gold_release_stopwords_path": release.get("stopwords_path") or "",
        "gold_release_lexicon_content_hash": release.get("lexicon_content_hash") or "",
        "gold_exploration_lexicon_version": exploration.get("lexicon_version")
        or STOPWORDS_EXPLORATION_LEXICON_VERSION,
        "gold_exploration_stopwords_path": exploration.get("stopwords_path") or "",
        "gold_exploration_lexicon_content_hash": exploration.get("lexicon_content_hash") or "",
        "gold_exploration_merged_count": exploration.get("stopwords_merged_count") or 0,
        "manifest_release_id": manifest_release_id,
        "manifest_dataset_id": manifest_dataset_id,
        "manifest_approved_snapshot_at": manifest_approved_snapshot_at,
        "manifest_lexicon_content_hash": manifest_lexicon_content_hash,
        "gold_lexicon_version": release.get("lexicon_version") or STOPWORDS_LEXICON_VERSION,
        "gold_stopwords_path": release.get("stopwords_path") or "",
        "gold_stopwords_merged_count": release.get("stopwords_merged_count") or 0,
        "gold_effective_stopwords_count": release.get("effective_stopwords_count") or 0,
        "gold_protected_terms_count": release.get("protected_terms_count") or 0,
        # 相容舊模板欄位
        "stopwords_used": bool(release.get("effective_stopwords_count")),
        "stopwords_path": release.get("stopwords_path") or "",
        "stopwords_count": release.get("effective_stopwords_count") or 0,
        "domain_stopwords_count": release.get("domain_stopwords_count") or 0,
    }


def register_jieba_pyfile_if_needed(
    spark: SparkSession,
    jieba_zip_path: str | None,
    jieba_userdict_path: str | None = None,
    dataset_id: str | None = None,
) -> None:
    """
    將 MinIO 上的 jieba.zip 分發給 executors（與 Notebook 的 spark.sparkContext.addPyFile 相同）。
    若 jieba_zip_path 為空，則假設執行環境已 pip 安裝 jieba，不分發 zip。
    若 jieba_userdict_path 有值，會透過 addFile 分發自訂字典。
    """

    global _jieba_pyfile_registered
    global _jieba_userdict_registered
    if jieba_zip_path and str(jieba_zip_path).strip():
        path = str(jieba_zip_path).strip()
        if _jieba_pyfile_registered != path:
            spark.sparkContext.addPyFile(path)
            _jieba_pyfile_registered = path

    if jieba_userdict_path and str(jieba_userdict_path).strip():
        userdict_path = str(jieba_userdict_path).strip()
        if _jieba_userdict_registered != userdict_path:
            try:
                spark.sparkContext.addFile(userdict_path)
                _jieba_userdict_registered = userdict_path
            except Exception as e:
                _logger.warning(
                    "jieba_userdict_distribute_failed: dataset_id=%s path=%s error=%s",
                    dataset_id,
                    userdict_path,
                    e,
                )


def _explode_silver_tokens_to_keywords(
    df_silver_ocr: DataFrame,
    *,
    dataset_id_for_log: str | None = None,
    token_col: str | None = None,
) -> DataFrame:
    """產出 (image_path, keyword) 列：explode 指定 token 欄（預設 TF-IDF 探索欄）。"""
    col_name = token_col or _tfidf_token_column(df_silver_ocr)
    if col_name not in df_silver_ocr.columns:
        _logger.warning(
            "gold_keywords_missing_silver_tokens: dataset_id=%s — 請重跑 Silver ETL 以產出 cleaned_text / tokens",
            dataset_id_for_log,
        )
        return (
            df_silver_ocr.select(
                lit(None).cast(StringType()).alias("image_path"),
                lit(None).cast(StringType()).alias("keyword"),
            ).filter(lit(False))
        )

    return (
        df_silver_ocr.select("image_path", explode(col(col_name)).alias("keyword"))
        .filter(col("keyword").isNotNull() & (col("keyword") != ""))
    )


_BIGRAM_PAIR_SCHEMA = ArrayType(
    StructType(
        [
            StructField("word1", StringType(), False),
            StructField("word2", StringType(), False),
        ]
    )
)


def _bigrams_from_token_list(tokens) -> List[Dict[str, str]]:
    if not tokens:
        return []
    safe = [str(t).strip().lower() for t in tokens if str(t).strip()]
    return [{"word1": safe[i], "word2": safe[i + 1]} for i in range(len(safe) - 1)]


_bigram_from_tokens_udf = udf(_bigrams_from_token_list, _BIGRAM_PAIR_SCHEMA)


def build_gold_tfidf_keywords_dataframe(df_exploded: DataFrame) -> DataFrame:
    """
    Phase A：由 (image_path, keyword) 計算語料級 TF-IDF，找出具區分力的痛點候選詞。
    """
    empty_schema = (
        "keyword STRING, total_tf LONG, doc_frequency LONG, corpus_doc_count LONG, "
        "idf DOUBLE, tfidf_score DOUBLE"
    )
    spark = df_exploded.sparkSession
    if df_exploded.limit(1).count() == 0:
        return spark.createDataFrame([], empty_schema)

    corpus_doc_count = int(df_exploded.select("image_path").distinct().count())
    if corpus_doc_count <= 0:
        return spark.createDataFrame([], empty_schema)

    df_tf = df_exploded.groupBy("image_path", "keyword").agg(count("*").alias("tf"))
    df_stats = df_tf.groupBy("keyword").agg(
        spark_sum("tf").alias("total_tf"),
        countDistinct("image_path").alias("doc_frequency"),
    )
    n_lit = lit(float(corpus_doc_count))
    return (
        df_stats.withColumn("corpus_doc_count", lit(corpus_doc_count))
        .withColumn("idf", spark_log((n_lit + lit(1.0)) / (col("doc_frequency").cast("double") + lit(1.0))))
        .withColumn("tfidf_score", col("total_tf").cast("double") * col("idf"))
        .orderBy(col("tfidf_score").desc(), col("total_tf").desc())
    )


def build_gold_phrase_candidates_dataframe(
    df_silver: DataFrame,
    *,
    min_bigram_count: int = 2,
) -> DataFrame:
    """
    Phase B：由銀層 tokens 相鄰 bigram 計算 PMI，發現應視為片語的候選（如「珍珠 奶茶」）。
    """
    spark = df_silver.sparkSession
    empty_schema = (
        "word1 STRING, word2 STRING, phrase STRING, bigram_count LONG, "
        "pmi_score DOUBLE, total_bigrams LONG"
    )
    if "tokens" not in df_silver.columns and "analytics_tokens" not in df_silver.columns:
        _logger.warning("phrase_pmi_skipped: silver_missing_tokens_column")
        return spark.createDataFrame([], empty_schema)

    token_col = _silver_token_column(df_silver)
    df_pairs = (
        df_silver.filter(col(token_col).isNotNull() & (size(col(token_col)) >= lit(2)))
        .select(explode(_bigram_from_tokens_udf(col(token_col))).alias("pair"))
        .select(col("pair.word1").alias("word1"), col("pair.word2").alias("word2"))
        .filter(col("word1") != "")
        .filter(col("word2") != "")
    )
    if not df_pairs.limit(1).count():
        return spark.createDataFrame([], empty_schema)

    df_counts = df_pairs.groupBy("word1", "word2").agg(count("*").alias("bigram_count"))
    total_row = df_counts.agg(spark_sum("bigram_count").alias("total")).collect()
    total_bigrams = int(total_row[0]["total"] or 0) if total_row else 0
    if total_bigrams <= 0:
        return spark.createDataFrame([], empty_schema)

    w1 = df_counts.groupBy("word1").agg(spark_sum("bigram_count").alias("w1_count"))
    w2 = df_counts.groupBy("word2").agg(spark_sum("bigram_count").alias("w2_count"))
    joined = (
        df_counts.join(w1, "word1")
        .join(w2, "word2")
        .withColumn("total_bigrams", lit(total_bigrams))
        .filter(col("bigram_count") >= lit(max(1, int(min_bigram_count))))
    )
    total_lit = col("total_bigrams").cast("double")
    p_xy = col("bigram_count").cast("double") / total_lit
    p_x = col("w1_count").cast("double") / total_lit
    p_y = col("w2_count").cast("double") / total_lit
    return (
        joined.withColumn("phrase", concat(col("word1"), lit(" "), col("word2")))
        .withColumn("pmi_score", spark_log(p_xy / (p_x * p_y)))
        .select("word1", "word2", "phrase", "bigram_count", "pmi_score", "total_bigrams")
        .orderBy(col("pmi_score").desc(), col("bigram_count").desc())
    )


def _write_gold_dataset_scoped_table(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    dataset_id: str | None,
    *,
    coalesce_partitions: int = 1,
) -> int:
    """依 dataset_id 刪除舊列後 append；無 dataset_id 時整表 overwrite。"""
    out = df.withColumn("analyzed_at", current_timestamp())
    if dataset_id:
        out = out.withColumn("dataset_id", lit(dataset_id))
    out = out if coalesce_partitions <= 0 else out.coalesce(coalesce_partitions)
    row_count = int(out.count())
    if row_count <= 0:
        return 0
    if dataset_id and delta_table_exists(spark, path):
        DeltaTable.forPath(spark, path).delete(condition=f"dataset_id = '{dataset_id}'")
        out.write.format("delta").mode("append").save(path)
    elif dataset_id:
        out.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(path)
    else:
        out.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(path)
    return row_count


def run_gold_corpus_analytics_etl(
    df_silver: DataFrame,
    *,
    dataset_id: str | None = None,
    coalesce_partitions: int = 1,
    min_bigram_count: int = 2,
    tfidf_path: str | None = None,
    phrase_path: str | None = None,
    lexicon_bundle: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    Phase A（TF-IDF）+ Phase B（PMI 片語）並寫入 Gold Delta。
    df_silver 應已含 analytics_tokens（或僅 tokens）。
    """
    spark = df_silver.sparkSession
    ds = _normalize_dataset_id_or_none(dataset_id)
    tfidf_out = tfidf_path or GOLD_TFIDF_KEYWORDS_PATH
    phrase_out = phrase_path or GOLD_PHRASE_CANDIDATES_PATH

    if "analytics_tokens" not in df_silver.columns and "tokens" in df_silver.columns:
        df_silver, lexicon_bundle = _with_gold_analytics_tokens(df_silver, spark, ds)
    bundle = lexicon_bundle or {}

    df_exploded = _explode_silver_tokens_to_keywords(
        df_silver,
        dataset_id_for_log=ds,
        token_col=_tfidf_token_column(df_silver),
    )
    df_tfidf = build_gold_tfidf_keywords_dataframe(df_exploded)
    df_phrases = build_gold_phrase_candidates_dataframe(df_silver, min_bigram_count=min_bigram_count)

    tfidf_rows = _write_gold_dataset_scoped_table(
        spark, df_tfidf, tfidf_out, ds, coalesce_partitions=coalesce_partitions
    )
    phrase_rows = _write_gold_dataset_scoped_table(
        spark, df_phrases, phrase_out, ds, coalesce_partitions=coalesce_partitions
    )
    tfidf_top = [row.asDict() for row in df_tfidf.limit(10).collect()]
    phrase_top = [row.asDict() for row in df_phrases.limit(10).collect()]

    return {
        "tfidf_output_rows": tfidf_rows,
        "phrase_candidate_rows": phrase_rows,
        "tfidf_path": tfidf_out,
        "phrase_candidates_path": phrase_out,
        "tfidf_top": tfidf_top,
        "phrase_top": phrase_top,
        "corpus_doc_count": int(df_silver.select("image_path").distinct().count()) if df_silver.head(1) else 0,
        "gold_lexicon_version": bundle.get("lexicon_version") or STOPWORDS_LEXICON_VERSION,
        "gold_effective_stopwords_count": bundle.get("effective_stopwords_count") or 0,
    }


def get_gold_tfidf_keywords_data(
    limit: int = 15,
    dataset_id: str | None = None,
) -> List[Dict[str, Any]]:
    spark = SparkManager().spark
    lim = max(1, min(int(limit), 200))
    ds = _normalize_dataset_id_or_none(dataset_id)
    path = GOLD_TFIDF_KEYWORDS_PATH
    if not delta_table_exists(spark, path):
        return []
    df = read_delta_table(spark, path)
    if ds and "dataset_id" in df.columns:
        df = df.filter(trim(lower(col("dataset_id"))) == lit(ds))
    return [
        row.asDict(recursive=True)
        for row in df.orderBy(col("tfidf_score").desc_nulls_last()).limit(lim).collect()
    ]


def get_gold_phrase_candidates_data(
    limit: int = 15,
    dataset_id: str | None = None,
) -> List[Dict[str, Any]]:
    spark = SparkManager().spark
    lim = max(1, min(int(limit), 200))
    ds = _normalize_dataset_id_or_none(dataset_id)
    path = GOLD_PHRASE_CANDIDATES_PATH
    if not delta_table_exists(spark, path):
        return []
    df = read_delta_table(spark, path)
    if ds and "dataset_id" in df.columns:
        df = df.filter(trim(lower(col("dataset_id"))) == lit(ds))
    return [
        row.asDict(recursive=True)
        for row in df.orderBy(col("pmi_score").desc_nulls_last()).limit(lim).collect()
    ]


def _make_topic_label_udf():
    def label_topics(words):
        return label_pain_topics(words)

    return udf(label_topics, ArrayType(StringType()))


_topic_label_udf = _make_topic_label_udf()


def _topic_snapshot_lexicon_kwargs(lexicon_bundle: Dict[str, Any] | None) -> Dict[str, str]:
    bundle = lexicon_bundle or {}
    return {
        "release_lexicon_version": str(
            bundle.get("release_lexicon_version")
            or bundle.get("lexicon_version")
            or STOPWORDS_LEXICON_VERSION
        ),
        "lexicon_content_hash": str(
            bundle.get("release_lexicon_content_hash") or bundle.get("lexicon_content_hash") or ""
        ),
    }


def build_gold_pain_topic_frequency_dataframe(
    df_silver_ocr: DataFrame,
    *,
    dataset_id_for_log: str | None = None,
) -> DataFrame:
    """
    從同一批評論計算痛點主題頻率（以 image_path 視為單一評論文件，單文件內同主題只計一次）。
    使用銀層 tokens 跑痛點漏斗。
    """
    if "tokens" not in df_silver_ocr.columns and "analytics_tokens" not in df_silver_ocr.columns:
        _logger.warning(
            "gold_pain_topics_missing_tokens: dataset_id=%s",
            dataset_id_for_log,
        )
        spark = df_silver_ocr.sparkSession
        return spark.createDataFrame([], "topic string, frequency long")

    token_col = _silver_token_column(df_silver_ocr)
    df_docs = df_silver_ocr.filter(col(token_col).isNotNull() & (size(col(token_col)) > 0)).select(
        "image_path",
        col(token_col).alias("tokens"),
    )
    df_topics = (
        df_docs.withColumn("topics", _topic_label_udf(col("tokens")))
        .select(explode(col("topics")).alias("topic"))
        .filter(col("topic").isNotNull() & (col("topic") != ""))
    )
    return (
        df_topics.groupBy("topic")
        .agg(count("*").alias("frequency"))
        .orderBy(col("frequency").desc())
    )


def write_gold_topic_snapshot_delta(
    df_topic_count: DataFrame,
    *,
    gold_topic_snapshot_path: str,
    dataset_id: str | None = None,
    rule_version: str = _TOPIC_RULE_VERSION,
    release_lexicon_version: str | None = None,
    lexicon_content_hash: str | None = None,
) -> int:
    """
    將痛點主題頻率以 append 方式寫入 Gold 快照表，供歷史對照。
    release_lexicon_version／lexicon_content_hash 記錄黃金發行停用詞版次。
    """
    ds = _normalize_dataset_id_or_none(dataset_id)
    rel_ver = str(release_lexicon_version or STOPWORDS_LEXICON_VERSION).strip()
    lex_hash = str(lexicon_content_hash or "").strip()
    out = (
        df_topic_count.withColumn(
            "dataset_id",
            lit(ds).cast(StringType()) if ds else lit(None).cast(StringType()),
        )
        .withColumn("rule_version", lit(str(rule_version).strip() or _TOPIC_RULE_VERSION))
        .withColumn("release_lexicon_version", lit(rel_ver))
        .withColumn("lexicon_content_hash", lit(lex_hash) if lex_hash else lit(None).cast(StringType()))
        .withColumn("snapshot_at", current_timestamp())
    )
    out.write.format("delta").mode("append").save(gold_topic_snapshot_path)
    if GOLD_TOPIC_SNAPSHOT_VERIFY_AFTER_WRITE:
        spark = out.sparkSession
        _verify_topic_snapshot_delta_readable_strict(spark, gold_topic_snapshot_path)
    return int(out.count())


def _verify_topic_snapshot_delta_readable_strict(spark: SparkSession, path: str) -> None:
    """
    寫入後驗證：以不忽略缺檔的方式讀取並 count，若 Delta 與實體檔不一致會失敗。
    topic_snapshot 列數通常不大；若表極大請改為維護窗執行或關閉 GOLD_TOPIC_SNAPSHOT_VERIFY_AFTER_WRITE。
    """
    key = "spark.sql.files.ignoreMissingFiles"
    prev = spark.conf.get(key, None)
    try:
        spark.conf.set(key, "false")
        n = spark.read.format("delta").load(path).count()
        _logger.info("topic_snapshot_verify_ok: path=%s rows=%s", path, n)
    except Exception as e:
        _logger.error("topic_snapshot_verify_failed: path=%s error=%s", path, e)
        raise RuntimeError(
            f"痛點快照表寫入後讀取驗證失敗（表可能不一致或缺檔）：{path}"
        ) from e
    finally:
        if prev is not None:
            spark.conf.set(key, prev)


def _read_delta_ignore_missing(spark: SparkSession, path: str) -> DataFrame:
    """
    盡量容忍 Delta 所引用的 parquet 遺失，避免首頁預覽整段失敗。
    """
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
    return (
        spark.read
        .option("ignoreMissingFiles", "true")
        .format("delta")
        .load(path)
    )


def get_gold_topic_snapshot_latest_data(
    limit: int = 10,
    dataset_id: str | None = None,
) -> List[Dict[str, Any]]:
    """
    讀取 Gold topic 快照表中「最新一個 snapshot_at」的主題頻率。
    """
    spark = SparkManager().spark
    lim = max(1, min(int(limit), 200))
    ds = _normalize_dataset_id_or_none(dataset_id)
    path = GOLD_TOPIC_SNAPSHOT_PATH
    if not delta_table_exists(spark, path):
        return []
    try:
        df = _read_delta_ignore_missing(spark, path)
    except Exception as e:
        _logger.warning("topic_snapshot_read_failed_latest: %s", e)
        return []
    try:
        if ds and "dataset_id" in df.columns:
            df = df.filter(trim(lower(col("dataset_id"))) == lit(ds))
        if "snapshot_at" not in df.columns:
            return [row.asDict(recursive=True) for row in df.orderBy(col("frequency").desc()).limit(lim).collect()]
        max_snapshot_at = df.agg(spark_max(col("snapshot_at")).alias("max_snapshot_at")).collect()[0]["max_snapshot_at"]
        if max_snapshot_at is None:
            return []
        df_latest = df.filter(col("snapshot_at") == lit(max_snapshot_at)).orderBy(col("frequency").desc()).limit(lim)
        return [row.asDict(recursive=True) for row in df_latest.collect()]
    except Exception as e:
        _logger.warning("topic_snapshot_collect_failed_latest: %s", e)
        return []


def get_gold_topic_snapshot_at_data(
    snapshot_at_iso: str,
    limit: int = 10,
    dataset_id: str | None = None,
) -> List[Dict[str, Any]]:
    """
    讀取 Gold topic 快照表中指定 snapshot_at 的主題頻率。
    snapshot_at_iso 須與 list_gold_topic_snapshots 回傳格式相容。
    """
    user_iso = str(snapshot_at_iso or "").strip()
    if not user_iso:
        return []

    spark = SparkManager().spark
    lim = max(1, min(int(limit), 200))
    ds = _normalize_dataset_id_or_none(dataset_id)
    path = GOLD_TOPIC_SNAPSHOT_PATH
    if not delta_table_exists(spark, path):
        return []
    try:
        df = _read_delta_ignore_missing(spark, path)
    except Exception as e:
        _logger.warning("topic_snapshot_read_failed_at: %s", e)
        return []
    try:
        if ds and "dataset_id" in df.columns:
            df = df.filter(trim(lower(col("dataset_id"))) == lit(ds))
        if "snapshot_at" not in df.columns:
            return [row.asDict(recursive=True) for row in df.orderBy(col("frequency").desc()).limit(lim).collect()]
        if not ds:
            return []
        resolved = _resolve_topic_snapshot_timestamp(df, ds, user_iso)
        if resolved is None:
            return []
        df_at = (
            df.filter(col("snapshot_at") == lit(resolved))
            .orderBy(col("frequency").desc())
            .limit(lim)
        )
        return [row.asDict(recursive=True) for row in df_at.collect()]
    except Exception as e:
        _logger.warning("topic_snapshot_collect_failed_at: %s", e)
        return []


def count_silver_distinct_image_paths(
    spark,
    dataset_id: str,
) -> int:
    """銀層 OCR 表內 distinct image_path 數（核准水位 processed_image_count 來源）。"""
    ds = _normalize_dataset_id_or_none(dataset_id)
    if not ds:
        return 0
    if not delta_table_exists(spark, SILVER_OCR_TABLE_PATH):
        return 0
    try:
        df = read_delta_table(spark, SILVER_OCR_TABLE_PATH)
        df = _filter_df_by_dataset_id(df, ds)
        if not df.head(1):
            return 0
        return int(df.select("image_path").distinct().count())
    except Exception as e:
        _logger.warning("silver_distinct_image_count_failed: %s", e)
        return 0


def list_gold_topic_snapshots(
    *,
    dataset_id: str | None = None,
    limit: int = 30,
) -> List[str]:
    """
    列出可供對照的 snapshot_at（ISO 字串，最新在前）。
    """
    spark = SparkManager().spark
    lim = max(1, min(int(limit), 200))
    ds = _normalize_dataset_id_or_none(dataset_id)
    path = GOLD_TOPIC_SNAPSHOT_PATH
    if not delta_table_exists(spark, path):
        return []
    try:
        df = _read_delta_ignore_missing(spark, path)
    except Exception as e:
        _logger.warning("topic_snapshot_read_failed_list: %s", e)
        return []
    try:
        if ds and "dataset_id" in df.columns:
            df = df.filter(trim(lower(col("dataset_id"))) == lit(ds))
        if "snapshot_at" not in df.columns:
            return []
        rows = (
            df.select("snapshot_at")
            .distinct()
            .orderBy(col("snapshot_at").desc())
            .limit(lim)
            .collect()
        )
        out: List[str] = []
        for r in rows:
            v = r["snapshot_at"]
            if v is None:
                continue
            out.append(v.isoformat() if hasattr(v, "isoformat") else str(v))
        return out
    except Exception as e:
        _logger.warning("topic_snapshot_collect_failed_list: %s", e)
        return []


def _topic_snapshot_iso_from_cell(v: Any) -> str:
    if v is None:
        return ""
    if hasattr(v, "isoformat"):
        return str(v.isoformat())
    return str(v)


def _filter_topic_snapshot_by_release(
    df: DataFrame,
    *,
    dataset_id: str | None,
    release_lexicon_version: str,
    lexicon_content_hash: str,
) -> DataFrame:
    ds = _normalize_dataset_id_or_none(dataset_id)
    if ds and "dataset_id" in df.columns:
        df = df.filter(trim(lower(col("dataset_id"))) == lit(ds))
    rel_ver = str(release_lexicon_version or "").strip()
    lex_hash = str(lexicon_content_hash or "").strip()
    if rel_ver and "release_lexicon_version" in df.columns:
        df = df.filter(trim(col("release_lexicon_version")) == lit(rel_ver))
    if lex_hash and "lexicon_content_hash" in df.columns:
        df = df.filter(trim(col("lexicon_content_hash")) == lit(lex_hash))
    return df


def find_latest_topic_snapshot_at_for_release(
    spark: SparkSession,
    *,
    dataset_id: str | None = None,
    release_lexicon_version: str,
    lexicon_content_hash: str,
) -> str | None:
    """找出符合黃金發行 lexicon 的最新 topic_snapshot snapshot_at（ISO）。"""
    path = GOLD_TOPIC_SNAPSHOT_PATH
    if not delta_table_exists(spark, path):
        return None
    try:
        df = _read_delta_ignore_missing(spark, path)
    except Exception as e:
        _logger.warning("topic_snapshot_find_latest_failed: %s", e)
        return None
    df = _filter_topic_snapshot_by_release(
        df,
        dataset_id=dataset_id,
        release_lexicon_version=release_lexicon_version,
        lexicon_content_hash=lexicon_content_hash,
    )
    if "snapshot_at" not in df.columns:
        return None
    try:
        max_row = df.agg(spark_max(col("snapshot_at")).alias("max_snapshot_at")).collect()
        max_snap = max_row[0]["max_snapshot_at"] if max_row else None
        if max_snap is None:
            return None
        iso = _topic_snapshot_iso_from_cell(max_snap)
        return iso or None
    except Exception as e:
        _logger.warning("topic_snapshot_find_latest_collect_failed: %s", e)
        return None


def verify_topic_snapshot_at_for_release(
    spark: SparkSession,
    *,
    dataset_id: str | None,
    snapshot_at_iso: str,
    release_lexicon_version: str,
    lexicon_content_hash: str,
) -> bool:
    """確認指定 snapshot_at 存在且 lexicon 欄位與 manifest 一致。"""
    path = GOLD_TOPIC_SNAPSHOT_PATH
    if not delta_table_exists(spark, path):
        return False
    user_iso = str(snapshot_at_iso or "").strip()
    if not user_iso:
        return False
    try:
        df = _read_delta_ignore_missing(spark, path)
    except Exception as e:
        _logger.warning("topic_snapshot_verify_read_failed: %s", e)
        return False
    df = _filter_topic_snapshot_by_release(
        df,
        dataset_id=dataset_id,
        release_lexicon_version=release_lexicon_version,
        lexicon_content_hash=lexicon_content_hash,
    )
    if "snapshot_at" not in df.columns:
        return False
    try:
        for row in df.select("snapshot_at").distinct().collect():
            if _topic_snapshot_iso_from_cell(row["snapshot_at"]) == user_iso:
                return True
        return False
    except Exception as e:
        _logger.warning("topic_snapshot_verify_collect_failed: %s", e)
        return False


def _parse_user_snapshot_at_iso(user_iso: str) -> Optional[datetime]:
    s = (user_iso or "").strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _resolve_topic_snapshot_timestamp(
    df: DataFrame,
    ds: str,
    user_iso: str,
) -> Any:
    """
    由使用者提供的 ISO 字串（與 list_gold_topic_snapshots 回傳格式相容）對應到表內實際 snapshot_at 值。
    """
    df_f = df.filter(trim(lower(col("dataset_id"))) == lit(ds))
    rows = df_f.select("snapshot_at").distinct().collect()
    user_stripped = user_iso.strip()
    for r in rows:
        v = r["snapshot_at"]
        if v is None:
            continue
        if _topic_snapshot_iso_from_cell(v) == user_stripped:
            return v
    udt = _parse_user_snapshot_at_iso(user_stripped)
    if udt is None:
        return None
    u_naive = udt.replace(tzinfo=None) if udt.tzinfo else udt
    for r in rows:
        v = r["snapshot_at"]
        if v is None:
            continue
        if not isinstance(v, datetime):
            continue
        v_naive = v.replace(tzinfo=None) if v.tzinfo else v
        if abs((v_naive - u_naive).total_seconds()) < 1.0:
            return v
    return None


def delete_gold_topic_snapshot_rows(
    *,
    dataset_id: str,
    snapshot_at_iso: str,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    依 dataset_id + snapshot_at 刪除痛點快照列（Delta DELETE，勿手動刪 MinIO 檔案）。
    snapshot_at_iso 請使用 list_gold_topic_snapshots 或首頁對照所顯示的 ISO 字串。
    """
    spark = SparkManager().spark
    path = GOLD_TOPIC_SNAPSHOT_PATH
    ds = _normalize_dataset_id_or_none(dataset_id)
    if not ds:
        raise ValueError("dataset_id 必填")
    raw_iso = (snapshot_at_iso or "").strip()
    if not raw_iso:
        raise ValueError("snapshot_at 必填（請使用列表 API 回傳的 ISO 時間字串）")

    if not delta_table_exists(spark, path):
        return {
            "status": "noop",
            "deleted_rows": 0,
            "message": "痛點快照表不存在",
            "dataset_id": ds,
            "topic_snapshot_path": path,
        }

    df = read_delta_table(spark, path)
    if "dataset_id" not in df.columns or "snapshot_at" not in df.columns:
        raise ValueError("痛點快照表缺少 dataset_id 或 snapshot_at 欄位")

    target_ts = _resolve_topic_snapshot_timestamp(df, ds, raw_iso)
    if target_ts is None:
        return {
            "status": "not_found",
            "deleted_rows": 0,
            "message": "找不到符合的 snapshot_at（請使用列表 API 或首頁對照所顯示的 ISO 字串）",
            "dataset_id": ds,
            "topic_snapshot_path": path,
        }

    match_df = df.filter(trim(lower(col("dataset_id"))) == lit(ds)).filter(col("snapshot_at") == lit(target_ts))
    to_delete = int(match_df.count())
    resolved_iso = _topic_snapshot_iso_from_cell(target_ts)

    if dry_run:
        return {
            "status": "dry_run",
            "deleted_rows": to_delete,
            "dataset_id": ds,
            "snapshot_at": resolved_iso,
            "topic_snapshot_path": path,
        }

    if to_delete == 0:
        return {
            "status": "ok",
            "deleted_rows": 0,
            "message": "無列可刪",
            "dataset_id": ds,
            "snapshot_at": resolved_iso,
            "topic_snapshot_path": path,
        }

    ds_esc = ds.replace("'", "''")
    if isinstance(target_ts, datetime):
        ts_sql = target_ts.strftime("%Y-%m-%d %H:%M:%S.%f")
    else:
        ts_sql = str(target_ts)
    condition = (
        f"trim(lower(cast(dataset_id as string))) = lower('{ds_esc}') "
        f"AND snapshot_at = cast('{ts_sql}' as timestamp)"
    )
    DeltaTable.forPath(spark, path).delete(condition)
    _logger.info(
        "topic_snapshot_deleted: dataset_id=%s snapshot_at=%s rows=%s path=%s",
        ds,
        resolved_iso,
        to_delete,
        path,
    )

    return {
        "status": "ok",
        "deleted_rows": to_delete,
        "dataset_id": ds,
        "snapshot_at": resolved_iso,
        "topic_snapshot_path": path,
        "message": f"已刪除 {to_delete} 列",
    }


def get_gold_topic_snapshot_comparison(
    *,
    dataset_id: str | None = None,
    snapshots: Iterable[str],
) -> List[Dict[str, Any]]:
    """
    讀取指定多個快照的 topic 頻率，用於對照（每列含 snapshot_at/topic/frequency）。
    """
    spark = SparkManager().spark
    ds = _normalize_dataset_id_or_none(dataset_id)
    keys = [str(s).strip() for s in snapshots if str(s).strip()]
    if not keys:
        return []
    path = GOLD_TOPIC_SNAPSHOT_PATH
    if not delta_table_exists(spark, path):
        return []
    try:
        df = _read_delta_ignore_missing(spark, path)
    except Exception as e:
        _logger.warning("topic_snapshot_read_failed_compare: %s", e)
        return []
    try:
        if ds and "dataset_id" in df.columns:
            df = df.filter(trim(lower(col("dataset_id"))) == lit(ds))
        if "snapshot_at" not in df.columns:
            return []
        rows = (
            df.select("snapshot_at", "topic", "frequency")
            .collect()
        )
        wanted = set(keys)
        out: List[Dict[str, Any]] = []
        for r in rows:
            snap = r["snapshot_at"]
            snap_iso = snap.isoformat() if hasattr(snap, "isoformat") else str(snap)
            if snap_iso not in wanted:
                continue
            out.append(
                {
                    "snapshot_at": snap_iso,
                    "topic": r["topic"],
                    "frequency": int(r["frequency"] or 0),
                }
            )
        out.sort(key=lambda x: (x["snapshot_at"], -x["frequency"], x["topic"]))
        return out
    except Exception as e:
        _logger.warning("topic_snapshot_collect_failed_compare: %s", e)
        return []


def run_gold_topic_snapshot_rebuild_etl(
    *,
    silver_ocr_path: str | None = None,
    dataset_id: str | None = None,
) -> Dict[str, Any]:
    """
    僅依 Silver 重算痛點主題並 append 至 topic_snapshot。
    適用：手動刪除 topic_snapshot 後補寫快照，或只需更新痛點快照。
    """
    spark = SparkManager().spark
    silver = silver_ocr_path or SILVER_OCR_TABLE_PATH
    ds = _normalize_dataset_id_or_none(dataset_id)
    if not ds:
        raise ValueError("dataset_id 必填")

    df_silver = read_delta_table(spark, silver)
    df_silver = _filter_df_by_dataset_id(df_silver, ds)
    silver_filtered_rows = int(df_silver.count())

    lexicon_bundle: Dict[str, Any] = {}
    df_for_topic = df_silver
    if silver_filtered_rows > 0 and "tokens" in df_silver.columns:
        df_for_topic, lexicon_bundle = _with_gold_analytics_tokens(df_silver, spark, ds)

    df_topic = build_gold_pain_topic_frequency_dataframe(
        df_for_topic,
        dataset_id_for_log=ds,
    )
    topic_snapshot_rows = write_gold_topic_snapshot_delta(
        df_topic,
        gold_topic_snapshot_path=GOLD_TOPIC_SNAPSHOT_PATH,
        dataset_id=ds,
        rule_version=_TOPIC_RULE_VERSION,
        **_topic_snapshot_lexicon_kwargs(lexicon_bundle),
    )
    topic_output_rows = int(df_topic.count())
    topic_frequency_top = [row.asDict() for row in df_topic.limit(10).collect()]

    if topic_output_rows > 0:
        summary = f"痛點快照已寫入 {topic_snapshot_rows} 列（主題列 {topic_output_rows} 筆）。"
    elif silver_filtered_rows > 0:
        summary = "Silver 有資料，但未匹配到任何痛點主題。"
    else:
        summary = "Silver 篩選後為 0 筆，無可寫入的主題資料。"

    return {
        "dataset_id": ds,
        "silver_filtered_rows": silver_filtered_rows,
        "topic_output_rows": topic_output_rows,
        "topic_snapshot_rows": topic_snapshot_rows,
        "topic_snapshot_path": GOLD_TOPIC_SNAPSHOT_PATH,
        "topic_rule_version": _TOPIC_RULE_VERSION,
        "topic_frequency_top": topic_frequency_top,
        "summary": summary,
    }


def run_gold_etl(
    *,
    silver_ocr_path: str | None = None,
    dataset_id: str | None = None,
    coalesce_partitions: int = 1,
    silver_batch_ts: str | None = None,
    prefer_incremental: bool = False,
    force_full_recompute: bool = False,
) -> Dict[str, Any]:
    """
    讀取 Silver OCR Delta → 痛點主題快照 + TF-IDF / PMI（完整金層 ETL）。
    """

    spark = SparkManager().spark
    silver = silver_ocr_path or SILVER_OCR_TABLE_PATH
    ds = _normalize_dataset_id_or_none(dataset_id)
    df_silver = read_delta_table(spark, silver)
    if ds:
        df_silver = _filter_df_by_dataset_id(df_silver, ds)
    silver_filtered_rows = int(df_silver.count())

    lexicon_bundle: Dict[str, Any] = {}
    df_gold = df_silver
    if silver_filtered_rows > 0 and "tokens" in df_silver.columns:
        df_gold, lexicon_bundle = _with_gold_analytics_tokens(df_silver, spark, ds)

    topic_output_rows = 0
    topic_snapshot_rows = 0
    topic_frequency_top: List[Dict[str, Any]] = []
    mode_used = "full_recompute"
    topic_snapshot_done = False

    incremental_mode = (
        bool(prefer_incremental)
        and not bool(force_full_recompute)
        and bool(ds)
        and bool(silver_batch_ts)
        and "etl_update_timestamp" in df_silver.columns
    )
    if incremental_mode:
        df_silver_delta = df_gold.filter(
            col("etl_update_timestamp") == to_timestamp(lit(str(silver_batch_ts)))
        )
        if int(df_silver_delta.count()) > 0:
            df_topic = build_gold_pain_topic_frequency_dataframe(
                df_silver_delta,
                dataset_id_for_log=ds,
            )
            topic_snapshot_rows = write_gold_topic_snapshot_delta(
                df_topic,
                gold_topic_snapshot_path=GOLD_TOPIC_SNAPSHOT_PATH,
                dataset_id=ds,
                rule_version=_TOPIC_RULE_VERSION,
                **_topic_snapshot_lexicon_kwargs(lexicon_bundle),
            )
            topic_output_rows = int(df_topic.count())
            topic_frequency_top = [row.asDict() for row in df_topic.limit(10).collect()]
            mode_used = "incremental_topic_snapshot"
            topic_snapshot_done = True

    if not topic_snapshot_done:
        df_topic = build_gold_pain_topic_frequency_dataframe(
            df_gold,
            dataset_id_for_log=ds,
        )
        topic_snapshot_rows = write_gold_topic_snapshot_delta(
            df_topic,
            gold_topic_snapshot_path=GOLD_TOPIC_SNAPSHOT_PATH,
            dataset_id=ds,
            rule_version=_TOPIC_RULE_VERSION,
            **_topic_snapshot_lexicon_kwargs(lexicon_bundle),
        )
        topic_output_rows = int(df_topic.count())
        topic_frequency_top = [row.asDict() for row in df_topic.limit(10).collect()]
        mode_used = "full_recompute"

    corpus_analytics: Dict[str, Any] = {
        "tfidf_output_rows": 0,
        "phrase_candidate_rows": 0,
        "tfidf_path": GOLD_TFIDF_KEYWORDS_PATH,
        "phrase_candidates_path": GOLD_PHRASE_CANDIDATES_PATH,
        "tfidf_top": [],
        "phrase_top": [],
        "corpus_doc_count": 0,
    }
    if silver_filtered_rows > 0:
        try:
            corpus_analytics = run_gold_corpus_analytics_etl(
                df_gold,
                dataset_id=ds,
                coalesce_partitions=coalesce_partitions,
                lexicon_bundle=lexicon_bundle,
            )
        except Exception as e:
            _logger.exception("gold_corpus_analytics_failed")
            corpus_analytics["error"] = str(e)

    gold_downstream_quality = evaluate_gold_downstream_quality(
        corpus_doc_count=int(corpus_analytics.get("corpus_doc_count") or 0),
        tfidf_output_rows=int(corpus_analytics.get("tfidf_output_rows") or 0),
        topic_output_rows=topic_output_rows,
        tfidf_top=corpus_analytics.get("tfidf_top"),
    )

    tfidf_output_rows = int(corpus_analytics.get("tfidf_output_rows") or 0)
    is_gold_written = bool(tfidf_output_rows > 0 or topic_output_rows > 0)

    if is_gold_written:
        summary = (
            f"金層已完成：痛點主題 {topic_output_rows} 類、"
            f"TF-IDF {tfidf_output_rows} 詞、"
            f"PMI {int(corpus_analytics.get('phrase_candidate_rows') or 0)} 片語。"
        )
    elif silver_filtered_rows > 0:
        summary = (
            f"金層流程有執行，但 TF-IDF／痛點主題產出為 0（Silver 篩選後有 {silver_filtered_rows} 筆）。"
            "通常是銀層 tokens 為空或 Gold lexicon 過濾後 analytics_tokens 為空。"
        )
    else:
        summary = "金層流程有執行，但 Silver 篩選後為 0 筆，所以沒有可寫入的金層資料。"

    return {
        "silver_filtered_rows": silver_filtered_rows,
        "tfidf_output_rows": tfidf_output_rows,
        "silver_tokens_source": (
            "silver_tokens_column"
            if "tokens" in df_silver.columns
            else "silver_tokens_missing"
        ),
        "gold_analytics_tokens_applied": bool(lexicon_bundle),
        "gold_release_lexicon_version": lexicon_bundle.get("release_lexicon_version")
        or lexicon_bundle.get("lexicon_version")
        or STOPWORDS_LEXICON_VERSION,
        "gold_release_lexicon_content_hash": lexicon_bundle.get("release_lexicon_content_hash") or "",
        "gold_exploration_lexicon_version": lexicon_bundle.get("exploration_lexicon_version")
        or STOPWORDS_EXPLORATION_LEXICON_VERSION,
        "gold_exploration_lexicon_content_hash": lexicon_bundle.get("exploration_lexicon_content_hash")
        or "",
        "gold_lexicon_version": lexicon_bundle.get("release_lexicon_version")
        or lexicon_bundle.get("lexicon_version")
        or STOPWORDS_LEXICON_VERSION,
        "gold_effective_stopwords_count": lexicon_bundle.get("effective_stopwords_count") or 0,
        "gold_tfidf_exploration_stopwords_count": lexicon_bundle.get("tfidf_exploration_stopwords_count") or 0,
        "gold_protected_terms_count": lexicon_bundle.get("protected_terms_count") or 0,
        "topic_output_rows": topic_output_rows,
        "topic_snapshot_rows": topic_snapshot_rows,
        "topic_snapshot_path": GOLD_TOPIC_SNAPSHOT_PATH,
        "topic_rule_version": _TOPIC_RULE_VERSION,
        "topic_frequency_top": topic_frequency_top,
        "is_gold_written": is_gold_written,
        "gold_recompute_mode": mode_used,
        "gold_downstream_quality": gold_downstream_quality,
        "summary": summary,
        **corpus_analytics,
    }


def _order_df_by_time_if_present(df: DataFrame, *, newest_first: bool = True) -> DataFrame:
    """
    若 DataFrame 含時間欄位，依時間排序；否則維持原順序。
    常見時間欄位優先序：ingestion_timestamp > etl_update_timestamp。
    """
    for c in ("ingestion_timestamp", "etl_update_timestamp"):
        if c in df.columns:
            return df.orderBy(col(c).desc_nulls_last() if newest_first else col(c).asc_nulls_last())
    return df


# ---------------------------------------------------------------------------
# 方便 API/測試使用的 DataFrame 轉換工具
# ---------------------------------------------------------------------------
def records_to_df(spark: SparkSession, records: Iterable[Dict[str, Any]]) -> DataFrame:
    """
    將 list[dict] 轉成 DataFrame（僅適合簡單 JSON 輸入，型別以 Spark 推斷為主）。
    """

    records_list = list(records)
    if not records_list:
        raise ValueError("records 不能是空的。")
    return spark.createDataFrame(records_list)


def add_etl_timestamp(df: DataFrame, col_name: str = "etl_update_timestamp") -> DataFrame:
    """
    對齊 Notebook 常見的 ETL 欄位寫法：.withColumn(col_name, current_timestamp())。
    """

    return df.withColumn(col_name, current_timestamp())


# ---------------------------------------------------------------------------
# 核心功能
# ---------------------------------------------------------------------------
def get_bronze_quarantine_data(
    limit: int = 20,
    dataset_id: str | None = None,
    *,
    newest_first: bool = True,
) -> List[Dict[str, Any]]:
    """讀取 Bronze quarantine Delta 表（隔離列，可稽核查詢）。"""
    spark = SparkManager().spark
    if not delta_table_exists(spark, BRONZE_QUARANTINE_PATH):
        return []
    lim = max(1, min(int(limit), 200))
    df = read_delta_table(spark, BRONZE_QUARANTINE_PATH)
    df = _filter_df_by_dataset_id(df, dataset_id)
    if "quarantined_at" in df.columns:
        df = df.orderBy(
            col("quarantined_at").desc_nulls_last()
            if newest_first
            else col("quarantined_at").asc_nulls_last()
        )
    else:
        df = _order_df_by_time_if_present(df, newest_first=newest_first)
    return [row.asDict(recursive=True) for row in df.limit(lim).collect()]


def get_bronze_data(
    limit: int = 10,
    dataset_id: str | None = None,
    *,
    newest_first: bool = True,
) -> List[Dict[str, Any]]:
    """
    讀取 `BRONZE_TABLE_PATH` 並回傳前 10 筆資料（JSON 可序列化格式）。
    """

    spark = SparkManager().spark
    lim = max(1, min(int(limit), 200))
    df = read_delta_table(spark, BRONZE_TABLE_PATH)
    df = _filter_df_by_dataset_id(df, dataset_id)
    df = _order_df_by_time_if_present(df, newest_first=newest_first).limit(lim)
    return [row.asDict(recursive=True) for row in df.collect()]


def get_silver_ocr_data(
    limit: int = 30,
    dataset_id: str | None = None,
    *,
    newest_first: bool = True,
) -> List[Dict[str, Any]]:
    """
    讀取 `SILVER_OCR_TABLE_PATH`，依 dataset_id（path 片段）過濾後取前 N 筆。
    """

    spark = SparkManager().spark
    lim = max(1, min(int(limit), 200))
    df = read_delta_table(spark, SILVER_OCR_TABLE_PATH)
    df = _filter_df_by_dataset_id(df, dataset_id)
    df = _order_df_by_time_if_present(df, newest_first=newest_first).limit(lim)
    return [row.asDict(recursive=True) for row in df.collect()]


def get_gold_delta_table_preview(
    limit: int = 50,
    dataset_id: str | None = None,
    *,
    newest_first: bool = True,
) -> List[Dict[str, Any]]:
    """
    直接讀取已寫入的 Gold TF-IDF Delta 表（非即時自 Silver 重算），供對照落盤結果。
    若表含 dataset_id 欄位且指定 dataset_id，會過濾該分類。
    """

    spark = SparkManager().spark
    lim = max(1, min(int(limit), 200))
    ds = _normalize_dataset_id_or_none(dataset_id)
    df = read_delta_table(spark, GOLD_TFIDF_KEYWORDS_PATH)
    if ds and "dataset_id" in df.columns:
        # 與寫入時 lit(ds) 對齊；避免字串前後空白、大小寫導致篩出 0 列
        df = df.filter(trim(lower(col("dataset_id"))) == lit(ds))
    df = _order_df_by_time_if_present(df, newest_first=newest_first)
    if df is not None and all(
        c not in df.columns for c in ("ingestion_timestamp", "etl_update_timestamp")
    ):
        if "tfidf_score" in df.columns:
            df = df.orderBy(col("tfidf_score").desc_nulls_last())
        elif "total_tf" in df.columns:
            df = df.orderBy(col("total_tf").desc_nulls_last())
    df = df.limit(lim)
    return [row.asDict(recursive=True) for row in df.collect()]


def get_system_status() -> Dict[str, Any]:
    """
    回傳主機 CPU / 記憶體使用率（使用 psutil）。
    """

    cpu_percent = psutil.cpu_percent(interval=0)
    mem = psutil.virtual_memory()
    return {
        "cpu_percent": cpu_percent,
        "memory_percent": mem.percent,
    }


def analyze_bronze_duplicates(spark: SparkSession, bronze_path: str) -> Dict[str, Any]:
    """
    分析 Bronze 重複資料概況。
    - image_path 重複
    - skip-key（dataset_id + file_hash + ocr_signature）重複（若欄位存在）
    """
    df = read_delta_table(spark, bronze_path)
    total_rows = int(df.count())
    cols = set(df.columns)

    out: Dict[str, Any] = {
        "bronze_path": bronze_path,
        "total_rows": total_rows,
        "has_image_path": "image_path" in cols,
        "has_skip_key_columns": {"dataset_id", "file_hash", "ocr_signature"}.issubset(cols),
    }

    if "image_path" in cols:
        g_path = df.groupBy("image_path").agg(count("*").alias("c")).filter(col("c") > 1)
        dup_groups = int(g_path.count())
        dup_rows = g_path.selectExpr("coalesce(sum(c - 1), 0) as d").collect()[0]["d"]
        out["duplicate_image_path_groups"] = dup_groups
        out["duplicate_image_path_rows"] = int(dup_rows or 0)
    else:
        out["duplicate_image_path_groups"] = -1
        out["duplicate_image_path_rows"] = -1

    if out["has_skip_key_columns"]:
        key_cols = ["dataset_id", "file_hash", "ocr_signature"]
        g_key = df.groupBy(*key_cols).agg(count("*").alias("c")).filter(col("c") > 1)
        dup_groups = int(g_key.count())
        dup_rows = g_key.selectExpr("coalesce(sum(c - 1), 0) as d").collect()[0]["d"]
        out["duplicate_skipkey_groups"] = dup_groups
        out["duplicate_skipkey_rows"] = int(dup_rows or 0)
    else:
        out["duplicate_skipkey_groups"] = -1
        out["duplicate_skipkey_rows"] = -1

    return out


def deduplicate_bronze_table(
    spark: SparkSession,
    bronze_path: str,
    *,
    strategy: str = "skipkey",
    dry_run: bool = True,
) -> Dict[str, Any]:
    """
    Bronze 去重（保留每組最新 ingestion_timestamp 一筆）。
    strategy:
      - skipkey: dataset_id + file_hash + ocr_signature
      - image_path: image_path
    """
    if strategy not in ("skipkey", "image_path"):
        raise ValueError('strategy 必須是 "skipkey" 或 "image_path"。')

    df = read_delta_table(spark, bronze_path)
    cols = set(df.columns)
    total_before = int(df.count())

    if strategy == "skipkey":
        req = {"dataset_id", "file_hash", "ocr_signature"}
        if not req.issubset(cols):
            raise ValueError("Bronze 缺少 skipkey 欄位（dataset_id/file_hash/ocr_signature）。")
        keys = ["dataset_id", "file_hash", "ocr_signature"]
    else:
        if "image_path" not in cols:
            raise ValueError("Bronze 缺少 image_path 欄位。")
        keys = ["image_path"]

    order_col = col("ingestion_timestamp").desc() if "ingestion_timestamp" in cols else col(keys[0]).desc()
    w = Window.partitionBy(*[col(k) for k in keys]).orderBy(order_col)
    dedup_df = df.withColumn("_rn_keep", row_number().over(w)).filter(col("_rn_keep") == 1).drop("_rn_keep")

    total_after = int(dedup_df.count())
    deleted_rows = max(0, total_before - total_after)
    result = {
        "bronze_path": bronze_path,
        "strategy": strategy,
        "group_keys": keys,
        "total_before": total_before,
        "total_after": total_after,
        "deleted_rows": deleted_rows,
        "dry_run": bool(dry_run),
    }

    if dry_run or deleted_rows == 0:
        return result

    dedup_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(bronze_path)
    result["status"] = "ok"
    return result

