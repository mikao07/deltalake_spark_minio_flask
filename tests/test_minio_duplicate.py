"""上傳 key 解析（mock _object_exists，不需真連線）。"""

from datetime import datetime
from unittest.mock import MagicMock, patch

from services import minio_upload as mu


def test_resolve_overwrite_ignores_existing():
    client = MagicMock()
    key, renamed, orig = mu._resolve_object_key(client, "b", "raw/images/a.png", on_duplicate="overwrite")
    assert key == "raw/images/a.png"
    assert renamed is False
    assert orig is None


def test_resolve_suffix_when_missing_uses_base():
    client = MagicMock()
    with patch.object(mu, "_object_exists", return_value=False):
        key, renamed, orig = mu._resolve_object_key(client, "b", "raw/images/a.png", on_duplicate="suffix")
    assert key == "raw/images/a.png"
    assert renamed is False
    assert orig is None


def test_resolve_suffix_when_exists_adds_timestamp():
    client = MagicMock()

    def exists(client, bucket, key):
        return key == "raw/images/a.png"

    fixed = datetime(2026, 4, 5, 14, 30, 22)
    with patch.object(mu, "_object_exists", side_effect=exists):
        with patch("services.minio_upload.datetime") as mock_dt:
            mock_dt.now.return_value = fixed
            key, renamed, orig = mu._resolve_object_key(
                client, "b", "raw/images/a.png", on_duplicate="suffix"
            )
    assert key == "raw/images/a_20260405_143022.png"
    assert renamed is True
    assert orig == "raw/images/a.png"


def test_sanitize_upload_basename_strips_leading_underscores():
    """中文檔名替換後的前導底線須移除，否則 Spark binaryFile 會忽略。"""
    safe = mu._sanitize_upload_basename("店名_2026-04-08_081346.png")
    assert safe == "2026-04-08_081346.png"
    assert not safe.startswith("_")


def test_normalize_object_key_no_leading_underscore(monkeypatch):
    monkeypatch.setattr(mu, "RAW_IMAGE_PREFIX", "raw/images")
    key = mu.normalize_object_key("店名_2026-04-08.png", dataset_id="drinks")
    assert key == "raw/images/drinks/2026-04-08.png"
    basename = key.rsplit("/", 1)[-1]
    assert not basename.startswith("_")


def test_sanitize_upload_basename_all_invalid_chars_gets_timestamp():
    fixed = datetime(2026, 4, 5, 14, 30, 22)
    with patch("services.minio_upload.datetime") as mock_dt:
        mock_dt.now.return_value = fixed
        safe = mu._sanitize_upload_basename("店名.png")
    assert safe == "image_20260405_143022.png"
