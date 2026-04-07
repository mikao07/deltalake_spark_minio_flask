"""
將檔案寫入 MinIO（S3 相容 API），供 OCR 前先把圖放到 RAW_IMAGE_PREFIX。
"""

from __future__ import annotations

import os
import re
from typing import BinaryIO
from urllib.parse import urlparse

from minio import Minio
from minio.error import S3Error

from config import BUCKET_NAME, MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY, RAW_IMAGE_PREFIX


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


def normalize_object_key(filename: str, *, subfolder: str | None = None) -> str:
    """
    產生物件 key：{RAW_IMAGE_PREFIX 前綴}/{可選 subfolder}/{safe_name}
    """
    prefix = RAW_IMAGE_PREFIX.strip().strip("/")
    parts: list[str] = [prefix]
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
    data: bytes,
    content_type: str | None = None,
    subfolder: str | None = None,
) -> dict[str, str]:
    """
    上傳至 BUCKET_NAME，回傳 object_key 與 s3a URI（供 Spark binaryFile 使用）。
    """

    client = get_minio_client()
    ensure_bucket(client, BUCKET_NAME)
    object_key = normalize_object_key(filename, subfolder=subfolder)

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
    return {"object_key": object_key, "s3a_uri": s3a, "bucket": BUCKET_NAME}
