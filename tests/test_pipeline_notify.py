"""管線通知與探針。"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from services.pipeline_notify import (
    NotifyResult,
    format_probe_alert_body,
    resolve_notify_backend,
    send_pipeline_alert,
)
from services.pipeline_probe import run_pipeline_probe, write_probe_result


def test_resolve_notify_backend_aliases():
    with patch("services.pipeline_notify.PIPELINE_NOTIFY_BACKEND", "line"):
        assert resolve_notify_backend() == "line_messaging"
    with patch("services.pipeline_notify.PIPELINE_NOTIFY_BACKEND", "line_notify"):
        assert resolve_notify_backend() == "line_messaging"


def test_format_probe_alert_body():
    body = format_probe_alert_body(
        dataset_id="drinks",
        issues=[{"source": "guardian", "level": "FAIL", "message": "lexicon drift"}],
    )
    assert "drinks" in body
    assert "lexicon drift" in body


def test_send_pipeline_alert_none_skips():
    result = send_pipeline_alert(title="t", body="b", backend="none")
    assert result.sent is False
    assert result.skipped_reason


@patch("services.pipeline_notify._post_json")
def test_send_pipeline_alert_discord(mock_post):
    with patch("services.pipeline_notify.DISCORD_WEBHOOK_URL", "https://discord.test/hook"):
        result = send_pipeline_alert(title="告警", body="測試", backend="discord")
    assert result.sent is True
    mock_post.assert_called_once()
    url, payload = mock_post.call_args[0]
    assert "discord.test" in url
    assert "測試" in payload["content"]


@patch("services.pipeline_notify._post_json")
def test_send_pipeline_alert_line_messaging(mock_post):
    with patch("services.pipeline_notify.LINE_CHANNEL_ACCESS_TOKEN", "ch-token"), patch(
        "services.pipeline_notify.LINE_PUSH_USER_ID", "U123"
    ):
        result = send_pipeline_alert(title="告警", body="測試", backend="line_messaging")
    assert result.sent is True
    mock_post.assert_called_once()
    url, payload = mock_post.call_args[0]
    assert "api.line.me" in url
    assert payload["to"] == "U123"
    assert payload["messages"][0]["text"] == "告警\n測試"
    assert mock_post.call_args[1]["headers"]["Authorization"] == "Bearer ch-token"


def test_write_probe_result_roundtrip(tmp_path):
    path = tmp_path / "probe.json"
    write_probe_result({"ok": True, "dataset_id": "drinks"}, path)
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["ok"] is True


@patch("services.pipeline_probe.run_freshness_probe")
@patch("services.pipeline_probe.run_guardian_probe")
@patch("services.pipeline_probe.check_ready_local")
def test_run_pipeline_probe_ok(mock_ready, mock_guardian, mock_fresh, tmp_path):
    mock_ready.return_value = {"ok": True, "body": {"status": "ok"}}
    mock_guardian.return_value = {"ok": True, "issues": []}
    mock_fresh.return_value = {"ok": True, "issues": [], "payload": {"ok": True}}
    result = run_pipeline_probe(
        "drinks",
        strict=True,
        notify=False,
        spark=MagicMock(),
        probe_result_path=tmp_path / "last.json",
    )
    assert result["ok"] is True
    assert result["issues"] == []


@patch("services.pipeline_notify.send_pipeline_alert")
@patch("services.pipeline_probe.run_freshness_probe")
@patch("services.pipeline_probe.run_guardian_probe")
@patch("services.pipeline_probe.check_ready_local")
def test_run_pipeline_probe_notifies_on_fail(
    mock_ready, mock_guardian, mock_fresh, mock_notify, tmp_path
):
    mock_ready.return_value = {"ok": True, "body": {"status": "ok"}}
    mock_guardian.return_value = {
        "ok": False,
        "issues": [{"source": "guardian", "level": "FAIL", "message": "drift"}],
    }
    mock_fresh.return_value = {"ok": True, "issues": [], "payload": {"ok": True}}
    mock_notify.return_value = NotifyResult(backend="discord", sent=True)

    result = run_pipeline_probe(
        "drinks",
        notify=True,
        spark=MagicMock(),
        probe_result_path=tmp_path / "last.json",
    )
    assert result["ok"] is False
    mock_notify.assert_called_once()
