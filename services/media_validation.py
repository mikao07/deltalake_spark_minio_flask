"""
上傳與 OCR 共用的媒體類型驗證：僅允許靜態圖片，明確拒絕影片。
"""

from __future__ import annotations

from pathlib import Path

SUPPORTED_IMAGE_EXTENSIONS = frozenset(
    {".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp", ".tif", ".tiff"}
)

BLOCKED_VIDEO_EXTENSIONS = frozenset(
    {
        ".mp4",
        ".m4v",
        ".mov",
        ".avi",
        ".mkv",
        ".webm",
        ".wmv",
        ".flv",
        ".mpeg",
        ".mpg",
        ".3gp",
        ".ogv",
        ".ts",
        ".mts",
        ".m2ts",
    }
)


def has_supported_image_extension(path: str) -> bool:
    p = (path or "").strip().lower()
    return any(p.endswith(ext) for ext in SUPPORTED_IMAGE_EXTENSIONS)


def has_blocked_video_extension(path: str) -> bool:
    p = (path or "").strip().lower()
    return any(p.endswith(ext) for ext in BLOCKED_VIDEO_EXTENSIONS)


def looks_like_image_bytes(data: bytes) -> bool:
    """常見圖片檔頭（PNG／JPEG／GIF／BMP／WEBP／TIFF）。"""
    if not data:
        return False
    sig = bytes(data[:16])
    return (
        sig.startswith(b"\x89PNG\r\n\x1a\n")
        or sig.startswith(b"\xff\xd8\xff")
        or sig.startswith(b"GIF87a")
        or sig.startswith(b"GIF89a")
        or sig.startswith(b"BM")
        or (len(sig) >= 12 and sig[0:4] == b"RIFF" and sig[8:12] == b"WEBP")
        or sig.startswith(b"II*\x00")
        or sig.startswith(b"MM\x00*")
    )


def looks_like_video_bytes(data: bytes) -> bool:
    """常見影片容器檔頭（即使副檔名改成 .png 仍可辨識）。"""
    if len(data) < 12:
        return False
    head = bytes(data[:32])
    if len(head) >= 12 and head[0:4] == b"RIFF" and head[8:12] == b"AVI ":
        return True
    if len(head) >= 8 and head[4:8] == b"ftyp":
        return True
    if head.startswith(b"\x1a\x45\xdf\xa3"):
        return True
    if head.startswith(b"FLV"):
        return True
    if head.startswith(b"\x30\x26\xb2\x75"):
        return True
    if head.startswith(b"\x00\x00\x01\xba") or head.startswith(b"\x00\x00\x01\xb3"):
        return True
    return False


def _normalize_content_type(content_type: str | None) -> str:
    return (content_type or "").split(";", 1)[0].strip().lower()


def validate_raw_image_upload(
    filename: str,
    data: bytes,
    *,
    content_type: str | None = None,
) -> None:
    """
    上傳前驗證：拒絕影片與非圖片；通過則不拋錯。
    拋 ValueError（繁中訊息）供 API 回 400。
    """
    name = (filename or "").strip()
    if not name:
        raise ValueError("檔名無效。")

    ext = Path(name).suffix.lower()
    ct = _normalize_content_type(content_type)

    if ext in BLOCKED_VIDEO_EXTENSIONS or ct.startswith("video/"):
        raise ValueError(
            "不支援影片上傳。本管線僅接受截圖等靜態圖片（PNG／JPEG 等），"
            "請先自行截圖後再上傳。"
        )

    if ext not in SUPPORTED_IMAGE_EXTENSIONS:
        allowed = ", ".join(sorted(SUPPORTED_IMAGE_EXTENSIONS))
        blocked = ", ".join(sorted(BLOCKED_VIDEO_EXTENSIONS))
        raise ValueError(
            f"不支援的副檔名：{ext or '（無）'}。"
            f"允許圖片：{allowed}；"
            f"禁止影片：{blocked} 等。"
        )

    if looks_like_video_bytes(data):
        raise ValueError(
            "檔案內容辨識為影片格式，已拒絕上傳。"
            "請勿將影片改副檔名冒充圖片；請先截圖後再上傳。"
        )

    if not looks_like_image_bytes(data):
        raise ValueError(
            "檔案內容不是有效的圖片格式（檔頭檢查未通過）。"
            "僅接受 PNG、JPEG、GIF、BMP、WEBP、TIFF 等靜態圖。"
        )
