from __future__ import annotations

import os
import re
import threading
from typing import Any, Dict, Iterable, List

import psutil

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    explode,
    expr,
    length,
    lower,
    max as spark_max,
    regexp_replace,
    to_timestamp,
    udf,
)
from pyspark.sql.types import ArrayType, StringType
from delta.tables import DeltaTable

from config import (
    BRONZE_TABLE_PATH,
    GOLD_WORD_COUNT_PATH,
    JIEBA_ZIP_PATH,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    S3A_CONNECTION_SSL_ENABLED,
    S3A_ENDPOINT_REGION,
    S3A_IMPL,
    S3A_PATH_STYLE_ACCESS,
    SILVER_OCR_TABLE_PATH,
)


# ---------------------------------------------------------------------------
# 依照 Notebook 的 SparkSession.builder 與 Delta Lake 設定來建立 Spark
# ---------------------------------------------------------------------------
PACKAGES = (
    "io.delta:delta-core_2.12:2.4.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

S3A_CONNECTION_SSL_ENABLED_VALUE = S3A_CONNECTION_SSL_ENABLED


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

        # 依照 Notebook Cell 1：SparkSession 配置（Delta + S3A/MinIO）
        self.spark = (
            SparkSession.builder.appName(app_name)
            # Maven 套件載入（hadoop-aws / aws-java-sdk-bundle / delta-core）
            .config("spark.jars.packages", PACKAGES)
            # Delta Lake
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # MinIO S3A（完全參考 config.py）
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", S3A_PATH_STYLE_ACCESS)
            .config("spark.hadoop.fs.s3a.impl", S3A_IMPL)
            .config("spark.hadoop.fs.s3a.endpoint.region", S3A_ENDPOINT_REGION)
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", S3A_CONNECTION_SSL_ENABLED_VALUE)
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


# ---------------------------------------------------------------------------
# Gold 層：Silver OCR → Jieba 分詞 → 詞頻 → Delta（對齊 MinIO_DeltaLake_Spark_1.1.ipynb）
# ---------------------------------------------------------------------------
_jieba_pyfile_registered: str | None = None

# Notebook「淨化後詞頻」步驟的雜訊與長度規則
_FINAL_NOISE = [
    "個",
    "月",
    "前",
    "是",
    "的",
    "跟",
    "到",
    "會",
    "有",
    "很",
    "和",
    "也",
    "先生",
    "這",
    "們",
    "並",
]
_COMMON_NOISE = _FINAL_NOISE + [
    "：",
    "，",
    "。",
    "、",
    "(",
    ")",
    "-",
    "+",
    "img",
    "png",
    "html",
    "the",
    "a",
    "b",
    "c",
    "x",
    "ok",
    "aer",
]
_MIN_WORD_LENGTH = 2


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
    # image_path 來源為 raw/images/{dataset_id}/...，用路徑片段過濾
    return df.filter(col("image_path").contains(f"/{ds}/"))


def register_jieba_pyfile_if_needed(spark: SparkSession, jieba_zip_path: str | None) -> None:
    """
    將 MinIO 上的 jieba.zip 分發給 executors（與 Notebook 的 spark.sparkContext.addPyFile 相同）。
    若 jieba_zip_path 為空，則假設執行環境已 pip 安裝 jieba，不分發 zip。
    """

    global _jieba_pyfile_registered
    if not jieba_zip_path or not str(jieba_zip_path).strip():
        return
    path = str(jieba_zip_path).strip()
    if _jieba_pyfile_registered == path:
        return
    spark.sparkContext.addPyFile(path)
    _jieba_pyfile_registered = path


def _make_jieba_segment_udf():
    def segment_chinese_jieba_distributed(text):
        if text is None:
            return []
        try:
            import jieba

            jieba.initialize()
            text_cleaned = text
            words = jieba.cut(text_cleaned, cut_all=False)
            return [word.strip().lower() for word in words if len(word.strip()) > 0]
        except Exception:
            return []

    return udf(segment_chinese_jieba_distributed, ArrayType(StringType()))


_jieba_segment_udf = _make_jieba_segment_udf()


def build_gold_word_frequency_dataframe(
    df_silver_ocr: DataFrame,
    *,
    apply_noise_filter: bool = True,
) -> DataFrame:
    """
    從 Silver OCR 表（需含 extracted_text；Notebook 另用到 image_path）產生 keyword / frequency 聚合。
    apply_noise_filter=True 時採用 Notebook 第二段淨化規則（長度、純數字、短英文、雜訊詞）。
    """

    df_keywords = df_silver_ocr.withColumn(
        "cleaned_text",
        lower(regexp_replace(col("extracted_text"), r"[^\p{L}\p{N}\s_]", "")),
    )
    df_diagnose = df_keywords.withColumn("word_array", _jieba_segment_udf(col("cleaned_text")))
    df_keywords_exploded = (
        df_diagnose.select("image_path", explode(col("word_array")).alias("keyword"))
        .filter(col("keyword") != "")
    )

    if apply_noise_filter:
        df_exploded = df_keywords_exploded.filter(
            (length(col("keyword")) >= _MIN_WORD_LENGTH)
            & (~col("keyword").rlike("^[0-9]+$"))
            & (~col("keyword").rlike("^[a-z]{1,4}$"))
            & (~col("keyword").isin(_COMMON_NOISE))
        )
    else:
        df_exploded = df_keywords_exploded

    return (
        df_exploded.groupBy("keyword")
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


def run_gold_word_frequency_etl(
    *,
    silver_ocr_path: str | None = None,
    gold_path: str | None = None,
    apply_noise_filter: bool = True,
    coalesce_partitions: int = 1,
) -> None:
    """
    讀取 Silver OCR Delta → 詞頻 → 覆寫寫入 Gold Delta（完整金層 ETL）。
    """

    spark = SparkManager().spark
    silver = silver_ocr_path or SILVER_OCR_TABLE_PATH
    gold = gold_path or GOLD_WORD_COUNT_PATH
    register_jieba_pyfile_if_needed(spark, JIEBA_ZIP_PATH)
    df_silver = read_delta_table(spark, silver)
    df_wc = build_gold_word_frequency_dataframe(df_silver, apply_noise_filter=apply_noise_filter)
    write_gold_word_frequency_delta(df_wc, gold, coalesce_partitions=coalesce_partitions)


def get_gold_word_frequency_data(limit: int = 10, dataset_id: str | None = None) -> List[Dict[str, Any]]:
    """
    讀取 Gold 詞頻表（GOLD_WORD_COUNT_PATH）依 frequency 降序前 N 筆。
    """

    spark = SparkManager().spark
    lim = max(1, min(int(limit), 200))
    ds = _normalize_dataset_id_or_none(dataset_id)
    if not ds:
        df = (
            read_delta_table(spark, GOLD_WORD_COUNT_PATH)
            .orderBy(col("frequency").desc())
            .limit(lim)
        )
        return [row.asDict(recursive=True) for row in df.collect()]

    # 指定 dataset_id 時，直接從 Silver OCR 即時計算詞頻，避免混合不同資料集
    register_jieba_pyfile_if_needed(spark, JIEBA_ZIP_PATH)
    df_silver = read_delta_table(spark, SILVER_OCR_TABLE_PATH)
    df_silver = _filter_df_by_dataset_id(df_silver, ds)
    df_wc = build_gold_word_frequency_dataframe(df_silver, apply_noise_filter=True).limit(lim)
    return [row.asDict(recursive=True) for row in df_wc.collect()]


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
def get_bronze_data(limit: int = 10, dataset_id: str | None = None) -> List[Dict[str, Any]]:
    """
    讀取 `BRONZE_TABLE_PATH` 並回傳前 10 筆資料（JSON 可序列化格式）。
    """

    spark = SparkManager().spark
    lim = max(1, min(int(limit), 200))
    df = read_delta_table(spark, BRONZE_TABLE_PATH)
    df = _filter_df_by_dataset_id(df, dataset_id).limit(lim)
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

