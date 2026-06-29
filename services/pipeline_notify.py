"""
管線告警通知：可換後端（none / discord / line_messaging）。

LINE Notify 已停服；LINE 請用 Messaging API Bot push（Channel access token + User ID）。
僅在探針彙總 FAIL／新鮮度 alert 時呼叫；成功不發送，避免告警疲勞。
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from config import (
    DISCORD_WEBHOOK_URL,
    LINE_CHANNEL_ACCESS_TOKEN,
    LINE_PUSH_USER_ID,
    PIPELINE_NOTIFY_BACKEND,
    PIPELINE_NOTIFY_WEBHOOK_URL,
)

_LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"
_MAX_MESSAGE_CHARS = 1900


@dataclass
class NotifyResult:
    backend: str
    sent: bool
    skipped_reason: Optional[str] = None
    error: Optional[str] = None


def resolve_notify_backend() -> str:
    backend = (PIPELINE_NOTIFY_BACKEND or "none").strip().lower()
    if backend in ("discord", "line_messaging", "line", "line_notify", "none"):
        if backend in ("line", "line_notify", "line_messaging"):
            return "line_messaging"
        return backend
    return "none"


def format_probe_alert_body(
    *,
    dataset_id: str,
    issues: List[Dict[str, Any]],
) -> str:
    lines = [f"[pipeline] dataset={dataset_id} 探針告警 ({len(issues)} 項)"]
    for item in issues:
        src = item.get("source") or "?"
        level = item.get("level") or "ALERT"
        msg = str(item.get("message") or "").strip()
        lines.append(f"- [{src}] {level}: {msg}")
    body = "\n".join(lines)
    if len(body) > _MAX_MESSAGE_CHARS:
        return body[: _MAX_MESSAGE_CHARS - 3] + "..."
    return body


def send_pipeline_alert(
    *,
    title: str,
    body: str,
    dataset_id: str = "",
    backend: Optional[str] = None,
) -> NotifyResult:
    """
    發送一則管線告警。backend 未指定時讀環境變數 PIPELINE_NOTIFY_BACKEND。
    """
    resolved = (backend or resolve_notify_backend()).strip().lower()
    if resolved in ("line", "line_notify"):
        resolved = "line_messaging"

    if resolved == "none":
        return NotifyResult(backend="none", sent=False, skipped_reason="PIPELINE_NOTIFY_BACKEND=none")

    text = f"{title}\n{body}".strip()
    if len(text) > _MAX_MESSAGE_CHARS:
        text = text[: _MAX_MESSAGE_CHARS - 3] + "..."

    try:
        if resolved == "discord":
            url = (DISCORD_WEBHOOK_URL or PIPELINE_NOTIFY_WEBHOOK_URL or "").strip()
            if not url:
                return NotifyResult(
                    backend="discord",
                    sent=False,
                    skipped_reason="未設定 DISCORD_WEBHOOK_URL 或 PIPELINE_NOTIFY_WEBHOOK_URL",
                )
            _post_json(url, {"content": text})
            return NotifyResult(backend="discord", sent=True)

        if resolved == "line_messaging":
            token = (LINE_CHANNEL_ACCESS_TOKEN or "").strip()
            user_id = (LINE_PUSH_USER_ID or "").strip()
            if not token:
                return NotifyResult(
                    backend="line_messaging",
                    sent=False,
                    skipped_reason="未設定 LINE_CHANNEL_ACCESS_TOKEN",
                )
            if not user_id:
                return NotifyResult(
                    backend="line_messaging",
                    sent=False,
                    skipped_reason="未設定 LINE_PUSH_USER_ID",
                )
            _post_json(
                _LINE_PUSH_URL,
                {
                    "to": user_id,
                    "messages": [{"type": "text", "text": text}],
                },
                headers={"Authorization": f"Bearer {token}"},
            )
            return NotifyResult(backend="line_messaging", sent=True)

        return NotifyResult(
            backend=resolved,
            sent=False,
            skipped_reason=f"未知後端: {resolved}",
        )
    except Exception as e:
        return NotifyResult(backend=resolved, sent=False, error=str(e))


def _post_json(
    url: str,
    payload: Dict[str, Any],
    *,
    headers: Optional[Dict[str, str]] = None,
) -> None:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    hdrs = {"Content-Type": "application/json; charset=utf-8"}
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, data=data, headers=hdrs, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        if resp.status >= 400:
            raise urllib.error.HTTPError(url, resp.status, resp.reason, resp.headers, None)


def _post_form(
    url: str,
    fields: Dict[str, str],
    *,
    headers: Optional[Dict[str, str]] = None,
) -> None:
    data = urllib.parse.urlencode(fields).encode("utf-8")
    hdrs = {"Content-Type": "application/x-www-form-urlencoded"}
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, data=data, headers=hdrs, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        if resp.status >= 400:
            raise urllib.error.HTTPError(url, resp.status, resp.reason, resp.headers, None)
