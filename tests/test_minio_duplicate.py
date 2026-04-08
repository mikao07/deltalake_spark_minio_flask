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
