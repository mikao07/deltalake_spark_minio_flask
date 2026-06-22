"""
OCR PSM A/B 測試：固定樣本影像、雙 PSM 並跑、結果寫入 test 路徑（不污染正式 Bronze）。
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence
from urllib.parse import urlparse

from minio.deleteobjects import DeleteObject
from minio.error import S3Error
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower

from config import (
    BUCKET_NAME,
    OCR_AB_MAX_SAMPLE_SIZE,
    OCR_AB_RESULTS_PATH,
    OCR_AB_SAMPLE_SIZE,
    RAW_IMAGES_PATH,
)
from services.domain_lexicons import materialize_merged_ocr_user_words_file
from services.minio_upload import ensure_bucket, get_minio_client
from services.ocr_spark import (
    _extract_bucket_and_prefix,
    _has_supported_image_extension,
    _looks_like_image_bytes,
    normalize_psm,
    ocr_image_bytes,
    register_ocr_user_words_if_needed,
)

_DEFAULT_KEYWORDS: tuple[str, ...] = (
    "發票",
    "line pay",
    "不耐煩",
    "收銀",
    "外帶",
    "珍珠",
    "店員態度",
)

_CJK_INTERNAL_SPACE_RE = re.compile(r"(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])")


def default_keyword_hints() -> List[str]:
    return list(_DEFAULT_KEYWORDS)


def count_cjk_internal_spaces(text: str | None) -> int:
    if not text:
        return 0
    return len(_CJK_INTERNAL_SPACE_RE.findall(str(text)))


def keyword_hits(text: str | None, keywords: Sequence[str]) -> List[str]:
    if not text:
        return []
    lower = str(text).lower()
    out: List[str] = []
    seen: set[str] = set()
    for raw in keywords:
        k = str(raw).strip().lower()
        if not k or k in seen:
            continue
        if k in lower:
            out.append(k)
            seen.add(k)
    return out


def summarize_ocr_text(text: str | None, keywords: Sequence[str]) -> Dict[str, Any]:
    raw = str(text or "")
    is_error = raw.startswith("OCR_ERROR_")
    is_empty = raw == "OCR_EMPTY_RESULT"
    return {
        "char_count": len(raw),
        "line_count": len([ln for ln in raw.splitlines() if ln.strip()]),
        "cjk_internal_spaces": count_cjk_internal_spaces(raw),
        "keyword_hits": keyword_hits(raw, keywords),
        "is_error": is_error,
        "is_empty": is_empty,
    }


def _clamp_sample_size(n: int | None) -> int:
    default = max(1, int(OCR_AB_SAMPLE_SIZE or 20))
    cap = max(default, int(OCR_AB_MAX_SAMPLE_SIZE or 50))
    if n is None:
        return default
    try:
        val = int(n)
    except (TypeError, ValueError):
        return default
    return max(1, min(val, cap))


def _parse_s3a(path: str) -> tuple[str, str]:
    p = str(path or "").strip()
    if not p.startswith("s3a://"):
        raise ValueError("路徑必須為 s3a://bucket/key 形式。")
    u = urlparse(p)
    bucket = (u.netloc or "").strip()
    key = (u.path or "").lstrip("/")
    if not bucket:
        raise ValueError("s3a 路徑缺少 bucket。")
    return bucket, key


def results_json_key(dataset_id: str, *, filename: str = "latest.json") -> str:
    base = OCR_AB_RESULTS_PATH.rstrip("/") + "/"
    _, prefix = _parse_s3a(base if base.startswith("s3a://") else f"s3a://{BUCKET_NAME}/{base}")
    ds = str(dataset_id).strip().lower()
    return f"{prefix}{ds}/{filename}"


def results_json_s3a(dataset_id: str, *, filename: str = "latest.json") -> str:
    bucket, _ = _parse_s3a(
        OCR_AB_RESULTS_PATH
        if OCR_AB_RESULTS_PATH.startswith("s3a://")
        else f"s3a://{BUCKET_NAME}/{OCR_AB_RESULTS_PATH.lstrip('/')}"
    )
    return f"s3a://{bucket}/{results_json_key(dataset_id, filename=filename)}"


def put_json_s3a(s3a_path: str, payload: Dict[str, Any]) -> None:
    bucket, key = _parse_s3a(s3a_path)
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    client = get_minio_client()
    ensure_bucket(client, bucket)
    from io import BytesIO

    client.put_object(bucket, key, BytesIO(body), length=len(body), content_type="application/json")


def get_json_s3a(s3a_path: str) -> Dict[str, Any] | None:
    bucket, key = _parse_s3a(s3a_path)
    client = get_minio_client()
    try:
        resp = client.get_object(bucket, key)
    except S3Error as e:
        if getattr(e, "code", "") in ("NoSuchKey", "NoSuchObject"):
            return None
        raise
    try:
        data = resp.read()
    finally:
        resp.close()
        resp.release_conn()
    if not data:
        return None
    parsed = json.loads(data.decode("utf-8"))
    return parsed if isinstance(parsed, dict) else None


def delete_test_results_prefix(*, dataset_id: str | None = None) -> Dict[str, Any]:
    """刪除 test/ocr_psm_ab/ 下指定 dataset 或全部 JSON 結果。"""
    base = OCR_AB_RESULTS_PATH.rstrip("/") + "/"
    if not base.startswith("s3a://"):
        base = f"s3a://{BUCKET_NAME}/{base.lstrip('/')}"
    bucket, prefix = _parse_s3a(base)
    if dataset_id:
        prefix = f"{prefix}{str(dataset_id).strip().lower()}/"
    client = get_minio_client()
    deleted = 0
    try:
        objects = [DeleteObject(obj.object_name) for obj in client.list_objects(bucket, prefix=prefix, recursive=True)]
        if objects:
            errs = list(client.remove_objects(bucket, objects))
            for err in errs:
                raise RuntimeError(f"刪除失敗：{err}")
            deleted = len(objects)
    except S3Error as e:
        raise RuntimeError(f"刪除 test 結果失敗：{e}") from e
    return {"deleted_objects": deleted, "prefix": f"s3a://{bucket}/{prefix}"}


def list_sample_images_with_content(
    spark: SparkSession,
    raw_images_path: str,
    *,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """
    依 image_path 排序後取前 N 筆，含 binary 內容（供 driver 端 OCR）。
    """
    lim = _clamp_sample_size(limit)
    df_paths = (
        spark.read.format("binaryFile")
        .load(raw_images_path)
        .filter(lower(col("path")).rlike(r".*\.(png|jpg|jpeg|bmp|gif|webp|tif|tiff)$"))
        .select(col("path").alias("image_path"), col("content").alias("image_content"))
        .orderBy(col("image_path"))
        .limit(lim)
    )
    try:
        if int(df_paths.limit(1).count()) > 0:
            return [row.asDict(recursive=True) for row in df_paths.collect()]
    except Exception:
        pass

    bucket, prefix = _extract_bucket_and_prefix(raw_images_path)
    client = get_minio_client()
    ensure_bucket(client, bucket)
    paths: List[str] = []
    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        name = getattr(obj, "object_name", "") or ""
        if not name or name.endswith("/"):
            continue
        if not _has_supported_image_extension(name):
            continue
        paths.append(f"s3a://{bucket}/{name}")
    paths.sort()
    rows: List[Dict[str, Any]] = []
    for path in paths[:lim]:
        b, key = _parse_s3a(path)
        resp = client.get_object(b, key)
        try:
            data = resp.read()
        finally:
            resp.close()
            resp.release_conn()
        if not _looks_like_image_bytes(data):
            continue
        rows.append({"image_path": path, "image_content": data})
    return rows


def list_sample_image_paths(
    spark: SparkSession,
    raw_images_path: str,
    *,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """僅路徑與長度（頁面預覽樣本清單）。"""
    lim = _clamp_sample_size(limit)
    rows = list_sample_images_with_content(spark, raw_images_path, limit=lim)
    return [
        {
            "image_path": r.get("image_path"),
            "content_length": len(r.get("image_content") or b""),
        }
        for r in rows
    ]


def _aggregate_side(rows: Sequence[Dict[str, Any]], side_key: str) -> Dict[str, Any]:
    texts = [str(r.get(side_key, {}).get("text") or "") for r in rows]
    metrics = [r.get(side_key, {}).get("metrics") or {} for r in rows]
    n = len(rows) or 1
    return {
        "rows": len(rows),
        "error_rows": sum(1 for t in texts if t.startswith("OCR_ERROR_")),
        "empty_rows": sum(1 for t in texts if t == "OCR_EMPTY_RESULT"),
        "avg_char_count": round(sum(m.get("char_count", 0) for m in metrics) / n, 1),
        "avg_cjk_internal_spaces": round(
            sum(m.get("cjk_internal_spaces", 0) for m in metrics) / n, 2
        ),
        "total_keyword_hits": sum(len(m.get("keyword_hits") or []) for m in metrics),
    }


def run_ocr_psm_ab(
    spark: SparkSession,
    *,
    dataset_id: str,
    raw_images_path: str | None = None,
    psm_a: str = "11",
    psm_b: str = "6",
    sample_size: int | None = None,
    keywords: Sequence[str] | None = None,
    save_results: bool = True,
) -> Dict[str, Any]:
    ds = str(dataset_id).strip().lower()
    if not ds:
        raise ValueError("dataset_id 必填。")
    raw = (raw_images_path or f"{RAW_IMAGES_PATH.rstrip('/')}/{ds}/").strip()
    lim = _clamp_sample_size(sample_size)
    kw = list(keywords) if keywords else default_keyword_hints()
    psm_a_norm = normalize_psm(psm_a, default="11")
    psm_b_norm = normalize_psm(psm_b, default="6")

    register_ocr_user_words_if_needed(spark, dataset_id=ds)
    user_words_path = materialize_merged_ocr_user_words_file(dataset_ids=[ds])

    images = list_sample_images_with_content(spark, raw, limit=lim)
    started = datetime.now(timezone.utc)
    rows_out: List[Dict[str, Any]] = []

    for img in images:
        image_path = str(img.get("image_path") or "")
        content = img.get("image_content")
        text_a = ocr_image_bytes(content, psm=psm_a_norm, user_words_path=user_words_path)
        text_b = ocr_image_bytes(content, psm=psm_b_norm, user_words_path=user_words_path)
        rows_out.append(
            {
                "image_path": image_path,
                "short_name": image_path.rsplit("/", 1)[-1] if image_path else "",
                "a": {
                    "psm": psm_a_norm,
                    "text": text_a,
                    "metrics": summarize_ocr_text(text_a, kw),
                },
                "b": {
                    "psm": psm_b_norm,
                    "text": text_b,
                    "metrics": summarize_ocr_text(text_b, kw),
                },
            }
        )

    finished = datetime.now(timezone.utc)
    payload: Dict[str, Any] = {
        "dataset_id": ds,
        "raw_images_path": raw,
        "sample_size_requested": lim,
        "sample_size_actual": len(rows_out),
        "psm_a": psm_a_norm,
        "psm_b": psm_b_norm,
        "keywords": kw,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "duration_ms": int((finished - started).total_seconds() * 1000),
        "summary": {
            "a": _aggregate_side(rows_out, "a"),
            "b": _aggregate_side(rows_out, "b"),
        },
        "rows": rows_out,
        "results_path": results_json_s3a(ds),
    }

    if save_results and rows_out:
        latest_path = results_json_s3a(ds)
        put_json_s3a(latest_path, payload)
        ts_name = started.strftime("%Y%m%dT%H%M%SZ") + ".json"
        archive_path = results_json_s3a(ds, filename=ts_name)
        put_json_s3a(archive_path, payload)
        payload["results_archive_path"] = archive_path

    return payload


def load_latest_ab_results(dataset_id: str) -> Dict[str, Any] | None:
    return get_json_s3a(results_json_s3a(str(dataset_id).strip().lower()))


def read_raw_image_bytes(image_path: str) -> bytes:
    """從 s3a 路徑讀取原圖 bytes（供縮圖 API）。"""
    path = str(image_path or "").strip()
    if not path.startswith("s3a://"):
        raise ValueError("image_path 必須為 s3a:// 形式。")
    if not _has_supported_image_extension(path):
        raise ValueError("不支援的影像副檔名。")
    bucket, key = _parse_s3a(path)
    client = get_minio_client()
    resp = client.get_object(bucket, key)
    try:
        data = resp.read()
    finally:
        resp.close()
        resp.release_conn()
    if not _looks_like_image_bytes(data):
        raise ValueError("檔案內容不像影像。")
    return data
