"""銀層三道品質防線（單元測試：不依賴 Spark）。"""

import pytest

from services.silver_quality import (
    SilverQualityError,
    cleaned_text_has_noise,
    compute_row_metrics,
    evaluate_silver_quality_metrics,
    run_silver_quality_gate,
)


def test_cleaned_text_has_noise_detects_ocr_garbage():
    assert cleaned_text_has_noise("到了耶 2222 第二次")
    assert cleaned_text_has_noise("what ???? really")
    assert not cleaned_text_has_noise("珍珠奶茶 很好喝")


def test_compute_row_metrics_char_retention():
    m = compute_row_metrics(
        image_path="s3a://b/a.png",
        extracted_text="珍珠奶茶，很好喝！",
        cleaned_text="珍珠奶茶 很好喝",
        tokens=["珍珠", "奶茶", "好喝"],
    )
    assert m["token_count"] == 3
    assert m["char_retention"] > 0.5
    assert m["has_noise"] is False


def test_evaluate_schema_hard_fail_on_duplicate_keys():
    report = evaluate_silver_quality_metrics(
        {
            "total_rows": 10,
            "duplicate_image_path_rows": 2,
            "null_image_path_rows": 0,
            "null_extracted_text_rows": 0,
            "null_cleaned_text_rows": 0,
            "noise_cleaned_text_rows": 0,
            "pure_digit_token_in_cleaned_rows": 0,
            "total_token_instances": 100,
            "len1_token_instances": 0,
            "long_token_instances": 0,
            "top_tokens": ["珍珠", "外送"],
            "empty_cleaned_text_ratio": 0.0,
            "nonempty_tokens_ratio": 0.9,
            "avg_char_retention": 0.5,
        }
    )
    assert not report.passed
    assert any("重複" in msg for msg in report.hard_failures)


def test_evaluate_top_denylist_hard_fail():
    report = evaluate_silver_quality_metrics(
        {
            "total_rows": 5,
            "duplicate_image_path_rows": 0,
            "null_image_path_rows": 0,
            "null_extracted_text_rows": 0,
            "null_cleaned_text_rows": 0,
            "noise_cleaned_text_rows": 0,
            "pure_digit_token_in_cleaned_rows": 0,
            "total_token_instances": 50,
            "len1_token_instances": 0,
            "long_token_instances": 0,
            "top_tokens": ["2222", "珍珠", "外送"],
            "empty_cleaned_text_ratio": 0.0,
            "nonempty_tokens_ratio": 0.8,
            "avg_char_retention": 0.4,
        }
    )
    assert not report.passed


def test_run_silver_quality_gate_raises_on_hard_fail(monkeypatch):
    monkeypatch.setenv("SILVER_QUALITY_ENABLED", "true")
    monkeypatch.setenv("SILVER_QUALITY_FAIL_ON_HARD", "true")

    from importlib import reload

    import config
    import services.silver_quality as sq

    reload(config)
    reload(sq)

    class FakeDF:
        def limit(self, n):
            return self

        def count(self):
            return 1

    def fake_collect(_df):
        return {
            "total_rows": 3,
            "duplicate_image_path_rows": 0,
            "null_image_path_rows": 0,
            "null_extracted_text_rows": 0,
            "null_cleaned_text_rows": 0,
            "noise_cleaned_text_rows": 5,
            "pure_digit_token_in_cleaned_rows": 0,
            "total_token_instances": 10,
            "len1_token_instances": 0,
            "long_token_instances": 0,
            "top_tokens": ["2222"],
            "empty_cleaned_text_ratio": 0.0,
            "nonempty_tokens_ratio": 0.5,
            "avg_char_retention": 0.3,
        }

    monkeypatch.setattr(sq, "collect_silver_quality_metrics_spark", fake_collect)

    with pytest.raises(sq.SilverQualityError):
        sq.run_silver_quality_gate(FakeDF())
