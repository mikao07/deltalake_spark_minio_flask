"""媒體上傳驗證：拒絕影片、假圖片。"""

import pytest

from services.media_validation import (
    looks_like_image_bytes,
    looks_like_video_bytes,
    validate_raw_image_upload,
)

_MIN_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 8
_MIN_JPEG = b"\xff\xd8\xff\xe0" + b"\x00" * 8
_FAKE_MP4 = b"\x00\x00\x00\x18ftypisom" + b"\x00" * 8


def test_looks_like_image_png():
    assert looks_like_image_bytes(_MIN_PNG)
    assert not looks_like_video_bytes(_MIN_PNG)


def test_looks_like_video_mp4():
    assert looks_like_video_bytes(_FAKE_MP4)
    assert not looks_like_image_bytes(_FAKE_MP4)


def test_validate_rejects_video_extension():
    with pytest.raises(ValueError, match="不支援影片"):
        validate_raw_image_upload("clip.mp4", _FAKE_MP4, content_type="video/mp4")


def test_validate_rejects_video_mime():
    with pytest.raises(ValueError, match="不支援影片"):
        validate_raw_image_upload("x.png", _FAKE_MP4, content_type="video/mp4")


def test_validate_rejects_renamed_video():
    with pytest.raises(ValueError, match="影片格式"):
        validate_raw_image_upload("fake.png", _FAKE_MP4)


def test_validate_rejects_non_image_bytes():
    with pytest.raises(ValueError, match="不是有效的圖片"):
        validate_raw_image_upload("x.png", b"not-an-image")


def test_validate_accepts_png():
    validate_raw_image_upload("shot.png", _MIN_PNG, content_type="image/png")


def test_validate_accepts_jpeg():
    validate_raw_image_upload("shot.jpg", _MIN_JPEG, content_type="image/jpeg")
