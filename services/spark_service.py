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
    BRONZE_TABLE_PATH,
    GOLD_TFIDF_KEYWORDS_PATH,
    GOLD_PHRASE_CANDIDATES_PATH,
    GOLD_TOPIC_SNAPSHOT_PATH,
    GOLD_TOPIC_SNAPSHOT_VERIFY_AFTER_WRITE,
    GOLD_WORD_COUNT_PATH,
    JIEBA_USERDICT_DATASET_PATTERN,
    JIEBA_USERDICT_PATH,
    JIEBA_ZIP_PATH,
    STOPWORDS_DATASET_PATTERN,
    STOPWORDS_PATH,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    S3A_CONNECTION_SSL_ENABLED,
    S3A_ENDPOINT_REGION,
    S3A_IMPL,
    S3A_PATH_STYLE_ACCESS,
    SILVER_OCR_TABLE_PATH,
)
from services.text_tokens import BUILTIN_STOPWORDS

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

        # 依照 Notebook Cell 1：SparkSession 配置（Delta + S3A/MinIO）
        self.spark = (
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
            .getOrCreate()
        )

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


def build_silver_ocr_updates_from_bronze(
    df_bronze: DataFrame,
    *,
    dataset_id: str | None = None,
) -> DataFrame:
    """
    Bronze OCR -> Silver 更新集（去重、清洗）。
    清洗僅做 trim：勿使用 Java \\W 剝除首尾，否則中文正文會被當成「非文字」而整段清空。
    分詞與停用詞在 enrich_silver_dataframe_with_tokens 產出 tokens 欄位。
    """
    ds = _normalize_dataset_id_or_none(dataset_id)
    df = df_bronze.filter(col("extracted_text").isNotNull())
    if ds:
        df = _filter_df_by_dataset_id(df, ds)

    w = Window.partitionBy(col("image_path")).orderBy(col("ingestion_timestamp").desc())
    df = (
        df.withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
        .select(
            "image_path",
            "extracted_text",
            "source_bucket",
            col("ingestion_timestamp").alias("latest_ingestion_timestamp"),
        )
    )

    # 注意：Spark/Java 預設的 \W「非文字」不含 CJK，会把整段中文當成 \W 從首尾剝掉，導致銀層 extracted_text 變空。
    # 這裡只削掉首尾「空白」，不再用 [\s\W_]+（與 Notebook 舊式 regex 在中文語料下行為不同，但可避免誤刪正文）。
    df = df.withColumn("extracted_text", trim(col("extracted_text")))
    df = _extract_dataset_id_col(df)
    if ds:
        df = df.withColumn("dataset_id", lit(ds))
    return df


def _make_silver_tokens_udf(
    jieba_userdict_path: str | None = None,
    extra_stopwords: Iterable[str] | None = None,
    dataset_id_for_log: str | None = None,
):
    userdict_basename = ""
    if jieba_userdict_path and str(jieba_userdict_path).strip():
        userdict_basename = os.path.basename(str(jieba_userdict_path).strip())
    extra_list = sorted(
        {str(w).strip().lower() for w in (extra_stopwords or []) if str(w).strip()}
    )

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
                extra_stopwords=extra_list,
                apply_noise_filter=True,
            )
        except Exception as e:
            _logger.warning("silver_tokens_udf_failed_once: %s", e)
            return []

    return udf(tokens_from_text, ArrayType(StringType()))


def _get_silver_tokens_udf(
    jieba_userdict_path: str | None = None,
    extra_stopwords: Iterable[str] | None = None,
    dataset_id_for_log: str | None = None,
):
    extra_key = ",".join(sorted({str(w).strip().lower() for w in (extra_stopwords or []) if str(w).strip()}))
    cache_key = f"{str(jieba_userdict_path or '').strip()}|{extra_key}|{str(dataset_id_for_log or '').strip()}"
    existing = _silver_tokens_udf_cache.get(cache_key)
    if existing is not None:
        return existing
    created = _make_silver_tokens_udf(
        jieba_userdict_path if str(jieba_userdict_path or "").strip() else None,
        extra_stopwords=extra_stopwords,
        dataset_id_for_log=dataset_id_for_log,
    )
    _silver_tokens_udf_cache[cache_key] = created
    return created


def enrich_silver_dataframe_with_tokens(
    df: DataFrame,
    *,
    jieba_userdict_path: str | None = None,
    extra_stopwords: Iterable[str] | None = None,
    dataset_id_for_log: str | None = None,
) -> DataFrame:
    """Silver：Jieba 分詞 + 內建／外部停用詞，寫入 tokens 陣列欄位。"""
    tokens_udf = _get_silver_tokens_udf(
        jieba_userdict_path,
        extra_stopwords=extra_stopwords,
        dataset_id_for_log=dataset_id_for_log,
    )
    return df.withColumn("tokens", tokens_udf(col("extracted_text")))


def run_silver_ocr_etl(
    *,
    bronze_path: str | None = None,
    silver_ocr_path: str | None = None,
    dataset_id: str | None = None,
) -> dict[str, Any]:
    """
    Bronze OCR -> Silver OCR（去重、清洗、分詞 tokens、MERGE）。
    """
    spark = SparkManager().spark
    bronze = bronze_path or BRONZE_TABLE_PATH
    silver = silver_ocr_path or SILVER_OCR_TABLE_PATH
    ds = _normalize_dataset_id_or_none(dataset_id)

    df_bronze = read_delta_table(spark, bronze)
    df_updates = build_silver_ocr_updates_from_bronze(df_bronze, dataset_id=ds)

    active_userdict_path = _resolve_existing_jieba_userdict_path(spark, ds)
    register_jieba_pyfile_if_needed(
        spark,
        JIEBA_ZIP_PATH,
        active_userdict_path,
        dataset_id=ds,
    )
    active_stopwords_path = _resolve_existing_stopwords_path(spark, ds)
    extra_stopwords: List[str] = []
    if active_stopwords_path:
        extra_stopwords = _load_stopwords_from_path(spark, active_stopwords_path)

    df_updates = enrich_silver_dataframe_with_tokens(
        df_updates,
        jieba_userdict_path=active_userdict_path,
        extra_stopwords=extra_stopwords,
        dataset_id_for_log=ds,
    )

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
            "builtin_stopwords_count": len(BUILTIN_STOPWORDS),
        }

    batch_ts = datetime.utcnow()
    batch_ts_lit = lit(batch_ts)

    if delta_table_exists(spark, silver):
        delta_table = DeltaTable.forPath(spark, silver)
        df_target = read_delta_table(spark, silver)
        target_cols = set(df_target.columns)
        df_target_cmp = df_target.select(
            col("image_path").alias("_t_image_path"),
            col("extracted_text").alias("_t_extracted_text"),
            col("source_bucket").alias("_t_source_bucket"),
            col("ingestion_timestamp").alias("_t_ingestion_timestamp"),
            col("dataset_id").alias("_t_dataset_id") if "dataset_id" in target_cols else lit(None).alias("_t_dataset_id"),
            col("tokens").alias("_t_tokens")
            if "tokens" in target_cols
            else lit(None).cast(ArrayType(StringType())).alias("_t_tokens"),
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
            & (col("u.source_bucket") == col("t._t_source_bucket"))
            & (col("u.latest_ingestion_timestamp") == col("t._t_ingestion_timestamp"))
            & same_dataset_expr
        )
        if "tokens" in target_cols:
            tokens_stale = col("t._t_tokens").isNull() | (size(col("t._t_tokens")) == 0)
            unchanged_expr = unchanged_expr & (~tokens_stale)
        else:
            # 舊 Silver 表尚無 tokens 欄位：本次 MERGE 需寫入分詞結果
            unchanged_expr = lit(False)
        df_changes = df_cmp.filter(~unchanged_expr).select("u.*")
        update_count = int(df_changes.count())
        if update_count == 0:
            return {
                "updated_rows": 0,
                "inserted_rows": 0,
                "updated_existing_rows": 0,
                "silver_batch_ts": "",
                "dataset_id": ds,
                "silver_ocr_path": silver,
                "bronze_path": bronze,
            }
        inserted_rows = int(df_cmp.filter(col("t._t_image_path").isNull()).count())
        updated_existing_rows = max(0, update_count - inserted_rows)
        update_set = {
            "extracted_text": col("source.extracted_text"),
            "source_bucket": col("source.source_bucket"),
            "ingestion_timestamp": col("source.latest_ingestion_timestamp"),
            "etl_update_timestamp": batch_ts_lit,
            "tokens": col("source.tokens"),
        }
        insert_values = {
            "image_path": col("source.image_path"),
            "extracted_text": col("source.extracted_text"),
            "source_bucket": col("source.source_bucket"),
            "ingestion_timestamp": col("source.latest_ingestion_timestamp"),
            "etl_update_timestamp": batch_ts_lit,
            "tokens": col("source.tokens"),
        }
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

    return {
        "updated_rows": update_count,
        "inserted_rows": inserted_rows,
        "updated_existing_rows": updated_existing_rows,
        "silver_batch_ts": batch_ts.isoformat(),
        "dataset_id": ds,
        "silver_ocr_path": silver,
        "bronze_path": bronze,
        "tokens_column_written": True,
        "builtin_stopwords_count": len(BUILTIN_STOPWORDS),
        "jieba_userdict_used": bool(active_userdict_path),
        "jieba_userdict_path": active_userdict_path or "",
        "stopwords_used": bool(active_stopwords_path),
        "stopwords_path": active_stopwords_path or "",
        "stopwords_count": len(extra_stopwords),
    }


# ---------------------------------------------------------------------------
# Gold 層：優先讀銀層 tokens → 詞頻／痛點主題；無 tokens 時退回 Jieba 分詞
# ---------------------------------------------------------------------------
_jieba_pyfile_registered: str | None = None
_jieba_userdict_registered: str | None = None
_jieba_segment_udf_cache: dict[str, Any] = {}

# 與 services.text_tokens.BUILTIN_STOPWORDS 對齊（金層 fallback 分詞時使用）
_COMMON_NOISE = sorted(BUILTIN_STOPWORDS)
_MIN_WORD_LENGTH = 2
_TOPIC_RULE_VERSION = "v1.1"

# 痛點主題規則（MVP）：可先用於熱門痛點監控，後續再抽成外部設定檔
_PAIN_TOPIC_RULES: Dict[str, List[str]] = {
    "等待時間": ["等很久", "等超久", "久等", "排隊", "等待", "慢", "太久", "時間管理"],
    "服務態度": ["態度差", "不耐煩", "兇", "服務差", "白眼", "口氣差", "親切", "貼心", "爛", "教育訓練"],
    "出錯重做": ["做錯", "弄錯", "漏單", "少做", "重做", "做錯了", "漏掉", "沒做", "點錯", "做不出來", "忘記"],
    "品質口感": ["太甜", "太淡", "沒味道", "難喝", "走味", "稀", "不新鮮", "口感", "硬", "不好喝"],
    "安全衛生": ["不安全", "危險", "衛生", "髒", "地板黏", "蟲", "異物"],
    "載具發票":["載具","發票","收據","小票"],
}

# 進階規則：片語 + 極性詞 + 距離容忍（優先判斷）
_PAIN_TOPIC_POLARITY_RULES: Dict[str, Dict[str, Any]] = {
    "服務態度": {
        "anchors": ["服務態度", "態度", "店員", "服務人員", "員工", "無視"],
        "negatives": ["差", "不好", "爛", "糟", "差勁", "不耐煩", "兇", "口氣差", "白眼", ""],
        "max_word_gap": 3,
        "max_char_gap": 8,
    },
    "等待時間": {
        "anchors": ["等", "等待", "排隊", "出餐", "速度", "等候"],
        "negatives": ["久", "慢", "太久", "很久", "超久", "超慢", "過久"],
        "max_word_gap": 3,
        "max_char_gap": 8,
    },
       "品質口感": {
        "anchors": ["珍珠", ],
        "negatives": ["硬", "超硬"],
        "max_word_gap": 3,
        "max_char_gap": 8,
    },
}


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

    return None


def _parse_stopwords_lines(lines: Iterable[str]) -> List[str]:
    """每行一詞；空白行與 # 開頭行略過；行內 # 之後視為註解。詞彙會轉成小寫以配合 keyword 欄位。"""
    out: List[str] = []
    seen: set[str] = set()
    for raw in lines:
        if raw is None:
            continue
        line = str(raw).strip()
        if not line or line.startswith("#"):
            continue
        if "#" in line:
            line = line.split("#", 1)[0].strip()
        if not line:
            continue
        w = line.lower()
        if w not in seen:
            seen.add(w)
            out.append(w)
    return out


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
    回傳目前 dataset 的辭典/停用詞實際套用狀態（供 API/頁面顯示）。
    """
    ds = _normalize_dataset_id_or_none(dataset_id)
    userdict_path = _resolve_existing_jieba_userdict_path(spark, ds)
    stopwords_path = _resolve_existing_stopwords_path(spark, ds)
    stopwords_count = 0
    if stopwords_path:
        stopwords_count = len(_load_stopwords_from_path(spark, stopwords_path))
    return {
        "dataset_id": ds,
        "jieba_userdict_used": bool(userdict_path),
        "jieba_userdict_path": userdict_path or "",
        "stopwords_used": bool(stopwords_path),
        "stopwords_path": stopwords_path or "",
        "stopwords_count": stopwords_count,
        "builtin_stopwords_count": len(BUILTIN_STOPWORDS),
        "silver_tokenization": "jieba_with_builtin_stopwords",
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


def _make_jieba_segment_udf(
    jieba_userdict_path: str | None = None,
    dataset_id_for_log: str | None = None,
):
    userdict_basename = ""
    if jieba_userdict_path and str(jieba_userdict_path).strip():
        userdict_basename = os.path.basename(str(jieba_userdict_path).strip())

    def segment_chinese_jieba_distributed(text):
        if text is None:
            return []
        if not hasattr(segment_chinese_jieba_distributed, "_jieba_initialized"):
            segment_chinese_jieba_distributed._jieba_initialized = False
            segment_chinese_jieba_distributed._jieba_error_logged = False
            segment_chinese_jieba_distributed._userdict_loaded = False
            segment_chinese_jieba_distributed._userdict_error_logged = False
        try:
            import jieba

            if not segment_chinese_jieba_distributed._jieba_initialized:
                jieba.initialize()
                segment_chinese_jieba_distributed._jieba_initialized = True

            if (
                userdict_basename
                and not segment_chinese_jieba_distributed._userdict_loaded
            ):
                try:
                    userdict_local_path = SparkFiles.get(userdict_basename)
                    jieba.load_userdict(userdict_local_path)
                    segment_chinese_jieba_distributed._userdict_loaded = True
                except Exception as e:
                    if not segment_chinese_jieba_distributed._userdict_error_logged:
                        _logger.warning(
                            "jieba_userdict_load_failed_once: dataset_id=%s path=%s error=%s",
                            dataset_id_for_log,
                            jieba_userdict_path,
                            e,
                        )
                        segment_chinese_jieba_distributed._userdict_error_logged = True

            text_cleaned = text
            words = jieba.cut(text_cleaned, cut_all=False)
            return [word.strip().lower() for word in words if len(word.strip()) > 0]
        except Exception as e:
            if not segment_chinese_jieba_distributed._jieba_error_logged:
                _logger.warning("jieba_segment_udf_failed_once: %s", e)
                segment_chinese_jieba_distributed._jieba_error_logged = True
            return []

    return udf(segment_chinese_jieba_distributed, ArrayType(StringType()))


def _get_jieba_segment_udf(
    jieba_userdict_path: str | None = None,
    dataset_id_for_log: str | None = None,
):
    cache_key = f"{str(jieba_userdict_path or '').strip()}|{str(dataset_id_for_log or '').strip()}"
    existing = _jieba_segment_udf_cache.get(cache_key)
    if existing is not None:
        return existing
    created = _make_jieba_segment_udf(
        jieba_userdict_path if str(jieba_userdict_path or "").strip() else None,
        dataset_id_for_log=dataset_id_for_log,
    )
    _jieba_segment_udf_cache[cache_key] = created
    return created


def _build_filtered_keyword_exploded_dataframe(
    df_silver_ocr: DataFrame,
    *,
    dataset_id_for_log: str | None = None,
    jieba_userdict_path: str | None = None,
    apply_noise_filter: bool = True,
    extra_stopwords: Iterable[str] | None = None,
) -> DataFrame:
    """
    產出 (image_path, keyword) 列：優先使用銀層 tokens；舊表無 tokens 時退回金層 Jieba 分詞。
    """
    if "tokens" in df_silver_ocr.columns:
        df_keywords_exploded = (
            df_silver_ocr.select("image_path", explode(col("tokens")).alias("keyword"))
            .filter(col("keyword").isNotNull() & (col("keyword") != ""))
        )
        if not apply_noise_filter:
            return df_keywords_exploded
        noise_terms = sorted(set(_COMMON_NOISE) | set(extra_stopwords or []))
        return df_keywords_exploded.filter(
            (length(col("keyword")) >= _MIN_WORD_LENGTH)
            & (~col("keyword").rlike("^[0-9]+$"))
            & (~col("keyword").rlike("^[a-z]{1,2}$"))
            & (~col("keyword").isin(noise_terms))
        )

    df_keywords = df_silver_ocr.withColumn(
        "cleaned_text",
        lower(regexp_replace(col("extracted_text"), r"[^\p{L}\p{N}\s_]", "")),
    )
    segment_udf = _get_jieba_segment_udf(
        jieba_userdict_path,
        dataset_id_for_log=dataset_id_for_log,
    )
    df_diagnose = df_keywords.withColumn("word_array", segment_udf(col("cleaned_text")))
    df_keywords_exploded = (
        df_diagnose.select("image_path", explode(col("word_array")).alias("keyword"))
        .filter(col("keyword") != "")
    )
    if not apply_noise_filter:
        return df_keywords_exploded

    noise_terms = sorted(set(_COMMON_NOISE) | set(extra_stopwords or []))
    return df_keywords_exploded.filter(
        (length(col("keyword")) >= _MIN_WORD_LENGTH)
        & (~col("keyword").rlike("^[0-9]+$"))
        & (~col("keyword").rlike("^[a-z]{1,2}$"))
        & (~col("keyword").isin(noise_terms))
    )


def build_gold_word_frequency_dataframe(
    df_silver_ocr: DataFrame,
    *,
    dataset_id_for_log: str | None = None,
    jieba_userdict_path: str | None = None,
    apply_noise_filter: bool = True,
    extra_stopwords: Iterable[str] | None = None,
) -> DataFrame:
    """
    從 Silver OCR 表（需含 extracted_text；Notebook 另用到 image_path）產生 keyword / frequency 聚合。
    apply_noise_filter=True 時採用 Notebook 第二段淨化規則（長度、純數字、**極短英文**、雜訊詞）。
    extra_stopwords 為外部停用詞表（與內建 _COMMON_NOISE 合併）。
    英文僅剔除 1～2 個字母（舊版曾剔除 1～4 字，會把 good/food/like 等大量評論常用詞清掉，導致詞頻幾乎為空）。
    """

    df_exploded = _build_filtered_keyword_exploded_dataframe(
        df_silver_ocr,
        dataset_id_for_log=dataset_id_for_log,
        jieba_userdict_path=jieba_userdict_path,
        apply_noise_filter=apply_noise_filter,
        extra_stopwords=extra_stopwords,
    )
    return (
        df_exploded.groupBy("keyword")
        .agg(count("*").alias("frequency"))
        .orderBy(col("frequency").desc())
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
    if "tokens" not in df_silver.columns:
        _logger.warning("phrase_pmi_skipped: silver_missing_tokens_column")
        return spark.createDataFrame([], empty_schema)

    df_pairs = (
        df_silver.filter(col("tokens").isNotNull() & (size(col("tokens")) >= lit(2)))
        .select(explode(_bigram_from_tokens_udf(col("tokens"))).alias("pair"))
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
    jieba_userdict_path: str | None = None,
    apply_noise_filter: bool = True,
    extra_stopwords: Iterable[str] | None = None,
    coalesce_partitions: int = 1,
    min_bigram_count: int = 2,
    tfidf_path: str | None = None,
    phrase_path: str | None = None,
) -> Dict[str, Any]:
    """
    Phase A（TF-IDF）+ Phase B（PMI 片語）並寫入 Gold Delta。
    """
    spark = df_silver.sparkSession
    ds = _normalize_dataset_id_or_none(dataset_id)
    tfidf_out = tfidf_path or GOLD_TFIDF_KEYWORDS_PATH
    phrase_out = phrase_path or GOLD_PHRASE_CANDIDATES_PATH

    df_exploded = _build_filtered_keyword_exploded_dataframe(
        df_silver,
        dataset_id_for_log=ds,
        jieba_userdict_path=jieba_userdict_path,
        apply_noise_filter=apply_noise_filter,
        extra_stopwords=extra_stopwords,
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
    def _contains_hint(word_set, joined_text, joined_no_space, hint: str) -> bool:
        h = str(hint).strip().lower()
        if not h:
            return False
        return h in word_set or h in joined_text or h in joined_no_space

    def _is_near_by_word_gap(words: List[str], anchors: List[str], negatives: List[str], max_gap: int) -> bool:
        anchor_idx = [i for i, w in enumerate(words) if w in anchors]
        neg_idx = [i for i, w in enumerate(words) if w in negatives]
        if not anchor_idx or not neg_idx:
            return False
        return any(abs(ai - ni) <= max_gap for ai in anchor_idx for ni in neg_idx)

    def _is_near_by_char_gap(joined_no_space: str, anchors: List[str], negatives: List[str], max_gap: int) -> bool:
        if not joined_no_space:
            return False
        for a in anchors:
            a0 = str(a).strip().lower()
            if not a0:
                continue
            for n in negatives:
                n0 = str(n).strip().lower()
                if not n0:
                    continue
                if re.search(rf"{re.escape(a0)}.{{0,{max_gap}}}{re.escape(n0)}", joined_no_space):
                    return True
                if re.search(rf"{re.escape(n0)}.{{0,{max_gap}}}{re.escape(a0)}", joined_no_space):
                    return True
        return False

    def label_topics(words):
        if not words:
            return []
        safe_words = [str(w).strip().lower() for w in words if str(w).strip()]
        if not safe_words:
            return []
        word_set = set(safe_words)
        joined_text = " ".join(safe_words)
        joined_no_space = "".join(safe_words)
        hit_topics: List[str] = []

        # 1) 先走進階「片語 + 極性詞」規則（可抓到：服務態度很差 / 非常差）
        for topic, cfg in _PAIN_TOPIC_POLARITY_RULES.items():
            anchors = [str(x).strip().lower() for x in cfg.get("anchors", []) if str(x).strip()]
            negatives = [str(x).strip().lower() for x in cfg.get("negatives", []) if str(x).strip()]
            if not anchors or not negatives:
                continue
            has_anchor = any(_contains_hint(word_set, joined_text, joined_no_space, a) for a in anchors)
            has_negative = any(_contains_hint(word_set, joined_text, joined_no_space, n) for n in negatives)
            if not (has_anchor and has_negative):
                continue

            max_word_gap = int(cfg.get("max_word_gap", 3))
            max_char_gap = int(cfg.get("max_char_gap", 8))
            if _is_near_by_word_gap(safe_words, anchors, negatives, max_word_gap) or _is_near_by_char_gap(
                joined_no_space,
                anchors,
                negatives,
                max_char_gap,
            ):
                hit_topics.append(topic)

        # 2) 再走既有關鍵詞命中規則（補足其他主題與舊資料相容）
        for topic, hints in _PAIN_TOPIC_RULES.items():
            if topic in hit_topics:
                continue
            for hint in hints:
                if _contains_hint(word_set, joined_text, joined_no_space, str(hint)):
                    hit_topics.append(topic)
                    break
        return hit_topics

    return udf(label_topics, ArrayType(StringType()))


_topic_label_udf = _make_topic_label_udf()


def build_gold_pain_topic_frequency_dataframe(
    df_silver_ocr: DataFrame,
    *,
    dataset_id_for_log: str | None = None,
    jieba_userdict_path: str | None = None,
    apply_noise_filter: bool = True,
    extra_stopwords: Iterable[str] | None = None,
) -> DataFrame:
    """
    從同一批評論計算痛點主題頻率（以 image_path 視為單一評論文件，單文件內同主題只計一次）。
    """
    df_exploded = _build_filtered_keyword_exploded_dataframe(
        df_silver_ocr,
        dataset_id_for_log=dataset_id_for_log,
        jieba_userdict_path=jieba_userdict_path,
        apply_noise_filter=apply_noise_filter,
        extra_stopwords=extra_stopwords,
    )
    df_doc_keywords = df_exploded.groupBy("image_path").agg(collect_set("keyword").alias("doc_keywords"))
    df_topics = (
        df_doc_keywords.withColumn("topics", _topic_label_udf(col("doc_keywords")))
        .select(explode(col("topics")).alias("topic"))
    )
    return (
        df_topics.groupBy("topic")
        .agg(count("*").alias("frequency"))
        .orderBy(col("frequency").desc())
    )


def write_gold_word_frequency_delta(
    df_word_count: DataFrame,
    gold_path: str,
    *,
    coalesce_partitions: int = 1,
) -> None:
    """Gold 聚合表通常列數少，Notebook 使用 coalesce(1) 減少小檔案。"""

    out = df_word_count if coalesce_partitions <= 0 else df_word_count.coalesce(coalesce_partitions)
    out.write.format("delta").mode("overwrite").save(gold_path)


def write_gold_topic_snapshot_delta(
    df_topic_count: DataFrame,
    *,
    gold_topic_snapshot_path: str,
    dataset_id: str | None = None,
    rule_version: str = _TOPIC_RULE_VERSION,
) -> int:
    """
    將痛點主題頻率以 append 方式寫入 Gold 快照表，供歷史對照。
    """
    ds = _normalize_dataset_id_or_none(dataset_id)
    out = (
        df_topic_count.withColumn(
            "dataset_id",
            lit(ds).cast(StringType()) if ds else lit(None).cast(StringType()),
        )
        .withColumn("rule_version", lit(str(rule_version).strip() or _TOPIC_RULE_VERSION))
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
    apply_noise_filter: bool = True,
) -> Dict[str, Any]:
    """
    僅依 Silver 重算痛點主題並 append 至 topic_snapshot（不寫入詞頻 Gold 表）。
    適用：手動刪除 topic_snapshot 後補寫快照，或只需更新痛點快照。
    """
    spark = SparkManager().spark
    silver = silver_ocr_path or SILVER_OCR_TABLE_PATH
    ds = _normalize_dataset_id_or_none(dataset_id)
    if not ds:
        raise ValueError("dataset_id 必填")

    active_userdict_path = _resolve_existing_jieba_userdict_path(spark, ds)
    register_jieba_pyfile_if_needed(
        spark,
        JIEBA_ZIP_PATH,
        active_userdict_path,
        dataset_id=ds,
    )
    if active_userdict_path:
        _logger.info("jieba_userdict_selected: dataset_id=%s path=%s", ds, active_userdict_path)
    else:
        _logger.info("jieba_userdict_selected: dataset_id=%s path=<none>", ds)
    active_stopwords_path = _resolve_existing_stopwords_path(spark, ds)
    extra_stopwords: List[str] = []
    if active_stopwords_path:
        extra_stopwords = _load_stopwords_from_path(spark, active_stopwords_path)
        _logger.info(
            "stopwords_selected: dataset_id=%s path=%s count=%s",
            ds,
            active_stopwords_path,
            len(extra_stopwords),
        )
    else:
        _logger.info("stopwords_selected: dataset_id=%s path=<none>", ds)

    df_silver = read_delta_table(spark, silver)
    df_silver = _filter_df_by_dataset_id(df_silver, ds)
    silver_filtered_rows = int(df_silver.count())

    df_topic = build_gold_pain_topic_frequency_dataframe(
        df_silver,
        dataset_id_for_log=ds,
        jieba_userdict_path=active_userdict_path,
        apply_noise_filter=apply_noise_filter,
        extra_stopwords=extra_stopwords if apply_noise_filter else None,
    )
    topic_snapshot_rows = write_gold_topic_snapshot_delta(
        df_topic,
        gold_topic_snapshot_path=GOLD_TOPIC_SNAPSHOT_PATH,
        dataset_id=ds,
        rule_version=_TOPIC_RULE_VERSION,
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
        "jieba_userdict_used": bool(active_userdict_path),
        "jieba_userdict_path": active_userdict_path or "",
        "stopwords_used": bool(active_stopwords_path),
        "stopwords_path": active_stopwords_path or "",
        "stopwords_count": len(extra_stopwords),
        "summary": summary,
    }


def run_gold_word_frequency_etl(
    *,
    silver_ocr_path: str | None = None,
    gold_path: str | None = None,
    dataset_id: str | None = None,
    apply_noise_filter: bool = True,
    coalesce_partitions: int = 1,
    silver_batch_ts: str | None = None,
    prefer_incremental: bool = False,
    force_full_recompute: bool = False,
) -> Dict[str, Any]:
    """
    讀取 Silver OCR Delta → 詞頻 → 覆寫寫入 Gold Delta（完整金層 ETL）。
    """

    spark = SparkManager().spark
    silver = silver_ocr_path or SILVER_OCR_TABLE_PATH
    gold = gold_path or GOLD_WORD_COUNT_PATH
    ds = _normalize_dataset_id_or_none(dataset_id)
    active_userdict_path = _resolve_existing_jieba_userdict_path(spark, ds)
    register_jieba_pyfile_if_needed(
        spark,
        JIEBA_ZIP_PATH,
        active_userdict_path,
        dataset_id=ds,
    )
    if active_userdict_path:
        _logger.info("jieba_userdict_selected: dataset_id=%s path=%s", ds, active_userdict_path)
    else:
        _logger.info("jieba_userdict_selected: dataset_id=%s path=<none>", ds)
    active_stopwords_path = _resolve_existing_stopwords_path(spark, ds)
    extra_stopwords: List[str] = []
    if active_stopwords_path:
        extra_stopwords = _load_stopwords_from_path(spark, active_stopwords_path)
        _logger.info(
            "stopwords_selected: dataset_id=%s path=%s count=%s",
            ds,
            active_stopwords_path,
            len(extra_stopwords),
        )
    else:
        _logger.info("stopwords_selected: dataset_id=%s path=<none>", ds)
    df_silver = read_delta_table(spark, silver)
    if ds:
        df_silver = _filter_df_by_dataset_id(df_silver, ds)
    silver_filtered_rows = int(df_silver.count())
    incremental_mode = (
        bool(prefer_incremental)
        and not bool(force_full_recompute)
        and bool(ds)
        and bool(silver_batch_ts)
        and delta_table_exists(spark, gold)
        and "etl_update_timestamp" in df_silver.columns
    )
    if incremental_mode:
        df_silver_delta = df_silver.filter(col("etl_update_timestamp") == to_timestamp(lit(str(silver_batch_ts))))
        silver_delta_rows = int(df_silver_delta.count())
        if silver_delta_rows > 0:
            df_wc_delta = build_gold_word_frequency_dataframe(
                df_silver_delta,
                dataset_id_for_log=ds,
                jieba_userdict_path=active_userdict_path,
                apply_noise_filter=apply_noise_filter,
                extra_stopwords=extra_stopwords if apply_noise_filter else None,
            )
            df_topic = build_gold_pain_topic_frequency_dataframe(
                df_silver_delta,
                dataset_id_for_log=ds,
                jieba_userdict_path=active_userdict_path,
                apply_noise_filter=apply_noise_filter,
                extra_stopwords=extra_stopwords if apply_noise_filter else None,
            )
            topic_snapshot_rows = write_gold_topic_snapshot_delta(
                df_topic,
                gold_topic_snapshot_path=GOLD_TOPIC_SNAPSHOT_PATH,
                dataset_id=ds,
                rule_version=_TOPIC_RULE_VERSION,
            )
            topic_output_rows = int(df_topic.count())
            topic_frequency_top = [row.asDict() for row in df_topic.limit(10).collect()]

            df_existing_gold = read_delta_table(spark, gold)
            target_cols = set(df_existing_gold.columns)
            if "dataset_id" in target_cols:
                df_existing_gold = df_existing_gold.filter(trim(lower(col("dataset_id"))) == lit(ds))
            df_existing_gold = df_existing_gold.select("keyword", "frequency")
            df_wc_merged = (
                df_existing_gold.unionByName(df_wc_delta.select("keyword", "frequency"))
                .groupBy("keyword")
                .agg(expr("sum(frequency) as frequency"))
            )
            gold_output_rows = int(df_wc_merged.count())
            out = (
                df_wc_merged.withColumn("dataset_id", lit(ds))
                if ds
                else df_wc_merged
            )
            out = out if coalesce_partitions <= 0 else out.coalesce(coalesce_partitions)
            if "dataset_id" in target_cols:
                DeltaTable.forPath(spark, gold).delete(condition=f"dataset_id = '{ds}'")
                out.write.format("delta").mode("append").save(gold)
            else:
                out.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(gold)
            mode_used = "incremental_delta_merge"
        else:
            incremental_mode = False
    if not incremental_mode:
        df_wc = build_gold_word_frequency_dataframe(
            df_silver,
            dataset_id_for_log=ds,
            jieba_userdict_path=active_userdict_path,
            apply_noise_filter=apply_noise_filter,
            extra_stopwords=extra_stopwords if apply_noise_filter else None,
        )
        df_topic = build_gold_pain_topic_frequency_dataframe(
            df_silver,
            dataset_id_for_log=ds,
            jieba_userdict_path=active_userdict_path,
            apply_noise_filter=apply_noise_filter,
            extra_stopwords=extra_stopwords if apply_noise_filter else None,
        )
        topic_snapshot_rows = write_gold_topic_snapshot_delta(
            df_topic,
            gold_topic_snapshot_path=GOLD_TOPIC_SNAPSHOT_PATH,
            dataset_id=ds,
            rule_version=_TOPIC_RULE_VERSION,
        )
        gold_output_rows = int(df_wc.count())
        topic_output_rows = int(df_topic.count())
        topic_frequency_top = [row.asDict() for row in df_topic.limit(10).collect()]
        if ds:
            df_wc = df_wc.withColumn("dataset_id", lit(ds))
            out = df_wc if coalesce_partitions <= 0 else df_wc.coalesce(coalesce_partitions)
            if delta_table_exists(spark, gold):
                target_cols = set(read_delta_table(spark, gold).columns)
                if "dataset_id" in target_cols:
                    DeltaTable.forPath(spark, gold).delete(condition=f"dataset_id = '{ds}'")
                    out.write.format("delta").mode("append").save(gold)
                else:
                    # 舊 Gold 表沒有 dataset_id 欄位時，先用 overwrite 進行欄位遷移
                    out.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(gold)
            else:
                out.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(gold)
        else:
            write_gold_word_frequency_delta(df_wc, gold, coalesce_partitions=coalesce_partitions)
        mode_used = "full_recompute"

    df_gold_after = read_delta_table(spark, gold)
    gold_total_rows_after = int(df_gold_after.count())
    gold_dataset_rows_after: int | None = None
    if ds and "dataset_id" in df_gold_after.columns:
        gold_dataset_rows_after = int(df_gold_after.filter(trim(lower(col("dataset_id"))) == lit(ds)).count())

    if gold_output_rows > 0:
        summary = f"金層已完成，這次產出 {gold_output_rows} 筆詞頻資料。"
    elif silver_filtered_rows > 0:
        summary = (
            f"金層流程有執行，但詞頻產出為 0 筆（Silver 篩選後有 {silver_filtered_rows} 筆）。"
            "通常是分詞後被雜訊規則過濾掉。"
        )
    else:
        summary = "金層流程有執行，但 Silver 篩選後為 0 筆，所以沒有可寫入的金層資料。"

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
                df_silver,
                dataset_id=ds,
                jieba_userdict_path=active_userdict_path,
                apply_noise_filter=apply_noise_filter,
                extra_stopwords=extra_stopwords if apply_noise_filter else None,
                coalesce_partitions=coalesce_partitions,
            )
        except Exception as e:
            _logger.exception("gold_corpus_analytics_failed")
            corpus_analytics["error"] = str(e)

    return {
        "silver_filtered_rows": silver_filtered_rows,
        "gold_output_rows": gold_output_rows,
        "gold_total_rows_after": gold_total_rows_after,
        "gold_dataset_rows_after": gold_dataset_rows_after,
        "jieba_userdict_used": bool(active_userdict_path),
        "jieba_userdict_path": active_userdict_path or "",
        "stopwords_used": bool(active_stopwords_path),
        "stopwords_path": active_stopwords_path or "",
        "stopwords_count": len(extra_stopwords),
        "silver_tokens_source": (
            "silver_tokens_column"
            if "tokens" in df_silver.columns
            else "gold_jieba_fallback"
        ),
        "builtin_stopwords_count": len(BUILTIN_STOPWORDS),
        "topic_output_rows": topic_output_rows,
        "topic_snapshot_rows": topic_snapshot_rows,
        "topic_snapshot_path": GOLD_TOPIC_SNAPSHOT_PATH,
        "topic_rule_version": _TOPIC_RULE_VERSION,
        "topic_frequency_top": topic_frequency_top,
        "is_gold_written": gold_output_rows > 0,
        "gold_recompute_mode": mode_used,
        "summary": summary,
        **corpus_analytics,
    }


def _order_gold_word_count_df(df: DataFrame) -> DataFrame:
    """詞頻表預覽預設依 frequency 降序；若無該欄位則不排序（避免讀到舊 schema 時整段失敗）。"""
    if "frequency" in df.columns:
        return df.orderBy(col("frequency").desc())
    return df


def _order_df_by_time_if_present(df: DataFrame, *, newest_first: bool = True) -> DataFrame:
    """
    若 DataFrame 含時間欄位，依時間排序；否則維持原順序。
    常見時間欄位優先序：ingestion_timestamp > etl_update_timestamp。
    """
    for c in ("ingestion_timestamp", "etl_update_timestamp"):
        if c in df.columns:
            return df.orderBy(col(c).desc_nulls_last() if newest_first else col(c).asc_nulls_last())
    return df


def get_gold_word_frequency_data(limit: int = 10, dataset_id: str | None = None) -> List[Dict[str, Any]]:
    """
    讀取 Gold 詞頻表（GOLD_WORD_COUNT_PATH）前 N 筆。
    指定 dataset_id 時，優先在落盤 Gold 表中以 dataset_id 過濾（符合首頁預期）。
    """

    spark = SparkManager().spark
    lim = max(1, min(int(limit), 200))
    ds = _normalize_dataset_id_or_none(dataset_id)
    df = read_delta_table(spark, GOLD_WORD_COUNT_PATH)
    if ds and "dataset_id" in df.columns:
        df = df.filter(trim(lower(col("dataset_id"))) == lit(ds))
    df = _order_gold_word_count_df(df).limit(lim)
    return [row.asDict(recursive=True) for row in df.collect()]


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
    直接讀取已寫入的 Gold Delta 表（非即時自 Silver 重算），供對照落盤結果。
    若表含 dataset_id 欄位且指定 dataset_id，會過濾該分類。
    """

    spark = SparkManager().spark
    lim = max(1, min(int(limit), 200))
    ds = _normalize_dataset_id_or_none(dataset_id)
    df = read_delta_table(spark, GOLD_WORD_COUNT_PATH)
    if ds and "dataset_id" in df.columns:
        # 與寫入時 lit(ds) 對齊；避免字串前後空白、大小寫導致篩出 0 列
        df = df.filter(trim(lower(col("dataset_id"))) == lit(ds))
    df = _order_df_by_time_if_present(df, newest_first=newest_first)
    if df is not None and all(c not in df.columns for c in ("ingestion_timestamp", "etl_update_timestamp")):
        df = _order_gold_word_count_df(df)
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

