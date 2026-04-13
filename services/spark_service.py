from __future__ import annotations

import os
import re
import threading
from urllib.parse import urlparse
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
    lit,
    lower,
    max as spark_max,
    regexp_extract,
    regexp_replace,
    row_number,
    trim,
    to_timestamp,
    udf,
)
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window
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


def build_silver_ocr_updates_from_bronze(
    df_bronze: DataFrame,
    *,
    dataset_id: str | None = None,
) -> DataFrame:
    """
    Bronze OCR -> Silver 更新集（去重、清洗）。
    清洗僅做 trim：勿使用 Java \\W 剝除首尾，否則中文正文會被當成「非文字」而整段清空。
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


def run_silver_ocr_etl(
    *,
    bronze_path: str | None = None,
    silver_ocr_path: str | None = None,
    dataset_id: str | None = None,
) -> dict[str, Any]:
    """
    Bronze OCR -> Silver OCR（去重/清洗/MERGE）.
    """
    spark = SparkManager().spark
    bronze = bronze_path or BRONZE_TABLE_PATH
    silver = silver_ocr_path or SILVER_OCR_TABLE_PATH
    ds = _normalize_dataset_id_or_none(dataset_id)

    df_bronze = read_delta_table(spark, bronze)
    df_updates = build_silver_ocr_updates_from_bronze(df_bronze, dataset_id=ds)
    update_count = int(df_updates.count())
    if update_count == 0:
        return {"updated_rows": 0, "dataset_id": ds, "silver_ocr_path": silver, "bronze_path": bronze}

    if delta_table_exists(spark, silver):
        delta_table = DeltaTable.forPath(spark, silver)
        target_cols = set(read_delta_table(spark, silver).columns)
        update_set = {
            "extracted_text": col("source.extracted_text"),
            "source_bucket": col("source.source_bucket"),
            "ingestion_timestamp": col("source.latest_ingestion_timestamp"),
            "etl_update_timestamp": current_timestamp(),
        }
        insert_values = {
            "image_path": col("source.image_path"),
            "extracted_text": col("source.extracted_text"),
            "source_bucket": col("source.source_bucket"),
            "ingestion_timestamp": col("source.latest_ingestion_timestamp"),
            "etl_update_timestamp": current_timestamp(),
        }
        # 舊 Silver 表可能尚未有 dataset_id，避免 MERGE 直接失敗
        if "dataset_id" in target_cols:
            update_set["dataset_id"] = col("source.dataset_id")
            insert_values["dataset_id"] = col("source.dataset_id")
        (
            delta_table.alias("target")
            .merge(df_updates.alias("source"), "target.image_path = source.image_path")
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_values)
            .execute()
        )
    else:
        (
            df_updates.withColumnRenamed("latest_ingestion_timestamp", "ingestion_timestamp")
            .withColumn("etl_update_timestamp", current_timestamp())
            .write.format("delta")
            .mode("overwrite")
            .save(silver)
        )

    return {"updated_rows": update_count, "dataset_id": ds, "silver_ocr_path": silver, "bronze_path": bronze}


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
    apply_noise_filter=True 時採用 Notebook 第二段淨化規則（長度、純數字、**極短英文**、雜訊詞）。
    英文僅剔除 1～2 個字母（舊版曾剔除 1～4 字，會把 good/food/like 等大量評論常用詞清掉，導致詞頻幾乎為空）。
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
            & (~col("keyword").rlike("^[a-z]{1,2}$"))
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
    dataset_id: str | None = None,
    apply_noise_filter: bool = True,
    coalesce_partitions: int = 1,
) -> Dict[str, Any]:
    """
    讀取 Silver OCR Delta → 詞頻 → 覆寫寫入 Gold Delta（完整金層 ETL）。
    """

    spark = SparkManager().spark
    silver = silver_ocr_path or SILVER_OCR_TABLE_PATH
    gold = gold_path or GOLD_WORD_COUNT_PATH
    ds = _normalize_dataset_id_or_none(dataset_id)
    register_jieba_pyfile_if_needed(spark, JIEBA_ZIP_PATH)
    df_silver = read_delta_table(spark, silver)
    if ds:
        df_silver = _filter_df_by_dataset_id(df_silver, ds)
    silver_filtered_rows = int(df_silver.count())
    df_wc = build_gold_word_frequency_dataframe(df_silver, apply_noise_filter=apply_noise_filter)
    gold_output_rows = int(df_wc.count())
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

    return {
        "silver_filtered_rows": silver_filtered_rows,
        "gold_output_rows": gold_output_rows,
        "gold_total_rows_after": gold_total_rows_after,
        "gold_dataset_rows_after": gold_dataset_rows_after,
        "is_gold_written": gold_output_rows > 0,
        "summary": summary,
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

