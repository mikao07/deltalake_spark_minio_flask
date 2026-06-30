"""MinIO 原圖 vs Bronze 攝入狀態（最新 N 筆，依上傳時間）。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pyspark.sql.functions import col

from config import BRONZE_TABLE_PATH, BUCKET_NAME, RAW_IMAGE_PREFIX
from services.minio_upload import ensure_bucket, get_minio_client, normalize_dataset_id
from services.spark_service import SparkManager, delta_table_exists, read_delta_table

_IMAGE_SUFFIXES = (".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".tif", ".tiff")


def _is_image_object_key(name: str) -> bool:
    return (name or "").lower().endswith(_IMAGE_SUFFIXES)


def _iso_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    return str(value)


def _bronze_lookup_for_dataset(dataset_id: str) -> tuple[set[str], dict[str, str]]:
    """回傳 (image_path 集合, path → extracted_text 前綴)。"""
    spark = SparkManager().spark
    if not delta_table_exists(spark, BRONZE_TABLE_PATH):
        return set(), {}
    df = read_delta_table(spark, BRONZE_TABLE_PATH)
    if "dataset_id" in df.columns:
        df = df.filter(col("dataset_id") == dataset_id)
    cols = set(df.columns)
    select_cols = ["image_path"]
    if "extracted_text" in cols:
        select_cols.append("extracted_text")
    paths: set[str] = set()
    texts: dict[str, str] = {}
    for row in df.select(*select_cols).collect():
        path = str(row.image_path or "").strip()
        if not path:
            continue
        paths.add(path)
        if "extracted_text" in select_cols:
            raw = row.extracted_text
            if raw is not None:
                texts[path] = str(raw)
    return paths, texts


def _enumerate_minio_raw_images(
    dataset_id: str,
    *,
    max_scan: int = 5000,
) -> tuple[list[dict[str, Any]], bool]:
    """
    掃描 MinIO 原圖物件（不含 Bronze 對照）。
    回傳 (列, truncated)；truncated=True 表示未掃完即達 max_scan。
    """
    ds = normalize_dataset_id(dataset_id)
    client = get_minio_client()
    ensure_bucket(client, BUCKET_NAME)
    prefix = f"{RAW_IMAGE_PREFIX.strip().strip('/')}/{ds}/"

    scanned = 0
    truncated = False
    items: list[dict[str, Any]] = []
    for obj in client.list_objects(BUCKET_NAME, prefix=prefix, recursive=True):
        scanned += 1
        if scanned > max_scan:
            truncated = True
            break
        name = getattr(obj, "object_name", "") or ""
        if not name or name.endswith("/") or not _is_image_object_key(name):
            continue
        items.append(
            {
                "object_key": name,
                "s3a_uri": f"s3a://{BUCKET_NAME}/{name}",
                "filename": name.rsplit("/", 1)[-1],
                "last_modified": _iso_datetime(getattr(obj, "last_modified", None)),
            }
        )
    return items, truncated


def collect_missing_raw_image_paths(
    dataset_id: str,
    *,
    max_scan: int = 5000,
) -> dict[str, Any]:
    """
    比對 MinIO 原圖路徑與 Bronze image_path，回傳尚未入庫的 s3a 清單（全量，不受 UI limit 限制）。
    """
    ds = normalize_dataset_id(dataset_id)
    bronze_paths, _ = _bronze_lookup_for_dataset(ds)
    objects, truncated = _enumerate_minio_raw_images(ds, max_scan=max_scan)
    missing = [o["s3a_uri"] for o in objects if o["s3a_uri"] not in bronze_paths]
    return {
        "dataset_id": ds,
        "missing_paths": missing,
        "missing_count": len(missing),
        "max_scan": max_scan,
        "truncated": truncated,
    }


def list_raw_ingest_status(
    dataset_id: str,
    *,
    limit: int = 10,
    only_missing: bool = False,
    max_scan: int = 5000,
) -> list[dict[str, Any]]:
    """
    依 MinIO last_modified 列出最新原圖，標示是否已進 Bronze。
    only_missing=True 時僅回傳尚未進 Bronze 的列（仍依時間排序後取 limit）。
    """
    ds = normalize_dataset_id(dataset_id)
    bronze_paths, bronze_texts = _bronze_lookup_for_dataset(ds)
    objects, _ = _enumerate_minio_raw_images(ds, max_scan=max_scan)

    items: list[dict[str, Any]] = []
    for o in objects:
        s3a = o["s3a_uri"]
        in_bronze = s3a in bronze_paths
        if only_missing and in_bronze:
            continue
        preview = None
        if in_bronze:
            t = bronze_texts.get(s3a, "")
            preview = (t[:160] + "…") if len(t) > 160 else (t or None)
        items.append(
            {
                "object_key": o["object_key"],
                "s3a_uri": s3a,
                "filename": o["filename"],
                "last_modified": o["last_modified"],
                "in_bronze": in_bronze,
                "bronze_status": "已辨識入庫" if in_bronze else "已上傳，尚未辨識",
                "extracted_text_preview": preview,
            }
        )

    items.sort(key=lambda x: str(x.get("last_modified") or ""), reverse=True)
    lim = max(1, min(int(limit), 50))
    return items[:lim]
