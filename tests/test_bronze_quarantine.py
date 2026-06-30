"""Bronze 列級隔離（單元測試：不依賴 Spark）。"""

import pytest

from services.bronze_quarantine import (
    BronzeQuarantineError,
    classify_extracted_text_status,
    resolve_melt_decision,
)


def _summary(reject_rows: int, total_rows: int, *, melt_mode: str = "soft") -> dict:
    ok_rows = total_rows - reject_rows
    reject_rate = reject_rows / total_rows if total_rows else 0.0
    return {
        "total_rows": total_rows,
        "ok_rows": ok_rows,
        "reject_rows": reject_rows,
        "reject_rate": reject_rate,
        "soft_reject_rate": 0.10,
        "hard_reject_rate": 0.30,
        "melt_mode": melt_mode,
        "by_status": {"ok": ok_rows, "empty": reject_rows} if reject_rows else {"ok": ok_rows},
    }


def test_classify_empty_and_too_short():
    assert classify_extracted_text_status("") == "empty"
    assert classify_extracted_text_status("   ") == "empty"
    assert classify_extracted_text_status("ab") == "too_short"
    assert classify_extracted_text_status("珍珠奶茶好喝") == "ok"


def test_classify_ocr_error_and_noise():
    assert classify_extracted_text_status("OCR_ERROR_REAL: boom") == "ocr_error"
    assert classify_extracted_text_status("something BARE here") == "noise"
    assert classify_extracted_text_status("LINE Pay 外送") == "ok"


def test_resolve_melt_pass_at_exactly_10_percent():
    """5/50=10% 不熔斷（門檻為 >10%）。"""
    d = resolve_melt_decision(_summary(5, 50))
    assert d["melt_action"] == "pass"
    assert d["high_reject_rate"] is False
    assert d["approve_blocked"] is False
    assert "10.0%" in d["message"] or "10%" in d["message"]


def test_resolve_melt_soft_above_10_percent():
    d = resolve_melt_decision(_summary(6, 50))
    assert d["melt_action"] == "soft"
    assert d["melted"] is False
    assert d["high_reject_rate"] is True
    assert d["approve_blocked"] is True
    assert "不可核准發行版" in d["message"]


def test_resolve_melt_hard_at_30_percent():
    d = resolve_melt_decision(_summary(15, 50))
    assert d["melt_action"] == "hard"
    assert d["melted"] is True
    assert d["approve_blocked"] is True
    assert "硬熔斷" in d["message"]
    assert "Silver" in d["message"]


def test_resolve_melt_hard_mode_above_soft_threshold():
    d = resolve_melt_decision(_summary(6, 50, melt_mode="hard"))
    assert d["melt_action"] == "hard"
    assert d["melt_reason"] == "manual_hard_mode"
    assert "hard" in d["message"]


def test_resolve_melt_messages_are_traditional_chinese():
    d = resolve_melt_decision(_summary(6, 50))
    assert "隔離占比" in d["message"]
    assert "軟門檻" in d["message"]
