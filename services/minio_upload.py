"""
將檔案寫入 MinIO（S3 相容 API），供 OCR 前先把圖放到 RAW_IMAGE_PREFIX。
支援 dataset_id 目錄分群：raw/images/{dataset_id}/...
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import BinaryIO
from urllib.parse import urlparse

from minio import Minio
from minio.error import S3Error

from config import (
    BUCKET_NAME,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    RAW_IMAGE_PREFIX,
    UPLOAD_ON_DUPLICATE,
)


def _parse_minio_endpoint() -> tuple[str, bool]:
    raw = (MINIO_ENDPOINT or "http://127.0.0.1:9000").strip()
    if "://" not in raw:
        raw = "http://" + raw
    u = urlparse(raw)
    host = u.hostname or "127.0.0.1"
    port = u.port
    if port:
        netloc = f"{host}:{port}"
    else:
        netloc = host if u.scheme == "https" else f"{host}:9000"
    secure = u.scheme == "https"
    return netloc, secure


def get_minio_client() -> Minio:
    if not MINIO_ACCESS_KEY or not str(MINIO_ACCESS_KEY).strip():
        raise RuntimeError("缺少 MINIO_ACCESS_KEY")
    if not MINIO_SECRET_KEY or not str(MINIO_SECRET_KEY).strip():
        raise RuntimeError("缺少 MINIO_SECRET_KEY")
    endpoint, secure = _parse_minio_endpoint()
    return Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY.strip(),
        secret_key=MINIO_SECRET_KEY.strip(),
        secure=secure,
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def normalize_dataset_id(dataset_id: str) -> str:
    raw = (dataset_id or "").strip().lower()
    safe = re.sub(r"[^a-z0-9_-]", "", raw)
    if not safe:
        raise ValueError("dataset_id 必填，且僅允許英數、-、_。")
    if len(safe) > 64:
        raise ValueError("dataset_id 長度不可超過 64。")
    return safe


def list_dataset_ids(*, max_scan: int = 5000) -> list[str]:
    """
    從 RAW_IMAGE_PREFIX 掃描已存在物件，回傳第一層 dataset_id 清單（去重、排序）。
    """

    client = get_minio_client()
    ensure_bucket(client, BUCKET_NAME)
    prefix = RAW_IMAGE_PREFIX.strip().strip("/") + "/"
    out: set[str] = set()
    scanned = 0
    for obj in client.list_objects(BUCKET_NAME, prefix=prefix, recursive=True):
        scanned += 1
        if scanned > max_scan:
            break
        name = getattr(obj, "object_name", "") or ""
        if not name.startswith(prefix):
            continue
        remain = name[len(prefix) :]
        if not remain:
            continue
        first = remain.split("/", 1)[0].strip().lower()
        if re.fullmatch(r"[a-z0-9_-]{1,64}", first):
            out.add(first)
    return sorted(out)


from services.media_validation import SUPPORTED_IMAGE_EXTENSIONS, validate_raw_image_upload

_IMAGE_OBJECT_SUFFIXES = tuple(SUPPORTED_IMAGE_EXTENSIONS)


def count_raw_image_objects_for_dataset(
    dataset_id: str,
    *,
    max_scan: int = 5000,
) -> int:
    """
    統計 MinIO RAW_IMAGE_PREFIX 下指定 dataset 的影像物件數（依副檔名）。
    用於新鮮度：上游原始圖 vs 銀層 distinct image_path。
    """
    ds = normalize_dataset_id(dataset_id)
    client = get_minio_client()
    ensure_bucket(client, BUCKET_NAME)
    prefix = f"{RAW_IMAGE_PREFIX.strip().strip('/')}/{ds}/"
    count = 0
    scanned = 0
    for obj in client.list_objects(BUCKET_NAME, prefix=prefix, recursive=True):
        scanned += 1
        if scanned > max_scan:
            break
        name = (getattr(obj, "object_name", "") or "").lower()
        if not name.startswith(prefix):
            continue
        if any(name.endswith(ext) for ext in _IMAGE_OBJECT_SUFFIXES):
            count += 1
    return count


def _object_exists(client: Minio, bucket: str, object_key: str) -> bool:
    try:
        client.stat_object(bucket, object_key)
        return True
    except S3Error as err:
        code = getattr(err, "code", None) or ""
        if code in ("NoSuchKey", "ResourceNotFound", "NotFound"):
            return False
        # 部分版本訊息不同，再判斷一次
        if "Not Found" in str(err) or "does not exist" in str(err).lower():
            return False
        raise


def _resolve_object_key(
    client: Minio,
    bucket: str,
    base_key: str,
    *,
    on_duplicate: str,
) -> tuple[str, bool, str | None]:
    """
    回傳 (實際寫入的 key, 是否因重複而改名, 原本的意圖 key 若改名則有值)。
    on_duplicate: \"overwrite\" | \"suffix\"
    """

    policy = (on_duplicate or "suffix").strip().lower()
    if policy not in ("overwrite", "suffix"):
        policy = "suffix"

    if policy == "overwrite":
        return base_key, False, None

    if not _object_exists(client, bucket, base_key):
        return base_key, False, None

    if "/" in base_key:
        dir_part, fname = base_key.rsplit("/", 1)
    else:
        dir_part, fname = "", base_key

    stem, ext = os.path.splitext(fname)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    n = 0
    while True:
        suffix = f"_{ts}" if n == 0 else f"_{ts}_{n}"
        candidate_name = f"{stem}{suffix}{ext}"
        candidate_key = f"{dir_part}/{candidate_name}" if dir_part else candidate_name
        if not _object_exists(client, bucket, candidate_key):
            return candidate_key, True, base_key
        n += 1


def normalize_object_key(
    filename: str,
    *,
    dataset_id: str,
    subfolder: str | None = None,
) -> str:
    """
    產生物件 key：{RAW_IMAGE_PREFIX 前綴}/{可選 subfolder}/{safe_name}
    """
    prefix = RAW_IMAGE_PREFIX.strip().strip("/")
    ds = normalize_dataset_id(dataset_id)
    parts: list[str] = [prefix, ds]
    if subfolder:
        sub = subfolder.strip().replace("\\", "/").strip("/")
        sub = re.sub(r"[^a-zA-Z0-9/_-]", "", sub)
        if sub:
            parts.append(sub)

    safe = os.path.basename(filename).strip()
    safe = re.sub(r"[^a-zA-Z0-9._-]", "_", safe)
    if not safe or safe in (".", ".."):
        raise ValueError("檔名無效。")
    parts.append(safe)
    return "/".join(parts)


def upload_file_bytes(
    *,
    filename: str,
    dataset_id: str,
    data: bytes,
    content_type: str | None = None,
    subfolder: str | None = None,
    on_duplicate: str | None = None,
) -> dict[str, str | bool | None]:
    """
    上傳至 BUCKET_NAME，回傳 object_key 與 s3a URI（供 Spark binaryFile 使用）。

    on_duplicate: None 時使用 config.UPLOAD_ON_DUPLICATE（預設 suffix：同名已存在則改檔名加時間戳）。
    """
    validate_raw_image_upload(filename, data, content_type=content_type)

    client = get_minio_client()
    ensure_bucket(client, BUCKET_NAME)
    base_key = normalize_object_key(filename, dataset_id=dataset_id, subfolder=subfolder)
    policy = (on_duplicate or UPLOAD_ON_DUPLICATE or "suffix").strip().lower()
    object_key, renamed, original_key = _resolve_object_key(
        client, BUCKET_NAME, base_key, on_duplicate=policy
    )

    from io import BytesIO

    stream: BinaryIO = BytesIO(data)
    size = len(data)
    ct = content_type or "application/octet-stream"

    try:
        client.put_object(
            BUCKET_NAME,
            object_key,
            stream,
            length=size,
            content_type=ct,
        )
    except S3Error as e:
        raise RuntimeError(f"MinIO 寫入失敗：{e}") from e

    s3a = f"s3a://{BUCKET_NAME}/{object_key}"
    out: dict[str, str | bool | None] = {
        "object_key": object_key,
        "s3a_uri": s3a,
        "bucket": BUCKET_NAME,
        "dataset_id": normalize_dataset_id(dataset_id),
        "renamed_from_duplicate": renamed,
    }
    if original_key:
        out["original_object_key"] = original_key
    return out
