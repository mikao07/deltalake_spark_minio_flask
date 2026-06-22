"""銀層分詞與內建停用詞單元測試。"""

from services.text_tokens import (
    BUILTIN_STOPWORDS,
    clean_text_for_segmentation,
    filter_segmented_tokens,
)


def test_builtin_stopwords_include_common_particles():
    assert "了" in BUILTIN_STOPWORDS
    assert "是" in BUILTIN_STOPWORDS
    assert "這" in BUILTIN_STOPWORDS
    assert "的" in BUILTIN_STOPWORDS


def test_clean_text_for_segmentation_strips_punctuation():
    assert clean_text_for_segmentation("  珍珠奶茶，很好喝！  ") == "珍珠奶茶 很好喝"


def test_clean_text_strips_ocr_digit_noise():
    raw = "可是我到了耶」2222? 2222? 第二次叫外送"
    out = clean_text_for_segmentation(raw)
    assert "2222" not in out
    assert "第二次" in out
    assert "外送" in out


def test_clean_text_strips_isolated_pure_digits():
    assert clean_text_for_segmentation("12 30 2222 拜託") == "拜託"
    assert clean_text_for_segmentation("耶 2222 2222 第二次") == "耶 第二次"


def test_strip_pure_digit_tokens_keeps_mixed_alnum():
    from services.text_tokens import strip_pure_digit_tokens

    assert strip_pure_digit_tokens("12點前 沒來") == "12點前 沒來"


def test_segment_text_to_tokens_already_cleaned_skips_restrip():
    from services.text_tokens import segment_text_to_tokens

    raw = "珍珠奶茶 很好喝"
    assert segment_text_to_tokens(raw, already_cleaned=True, apply_noise_filter=False) == segment_text_to_tokens(
        "珍珠奶茶，很好喝！", already_cleaned=False, apply_noise_filter=False
    )


def test_filter_segmented_tokens_removes_stopwords():
    words = ["珍珠", "奶茶", "很", "好喝", "了", "是", "這"]
    out = filter_segmented_tokens(words)
    assert "了" not in out
    assert "是" not in out
    assert "這" not in out
    assert "很" not in out
    assert "珍珠" in out
    assert "奶茶" in out
    assert "好喝" in out


def test_filter_segmented_tokens_respects_extra_stopwords():
    words = ["珍珠", "奶茶", "好喝"]
    out = filter_segmented_tokens(words, extra_stopwords=["好喝"])
    assert "好喝" not in out
    assert "珍珠" in out


def test_filter_segmented_tokens_can_disable_noise_filter():
    words = ["a", "珍珠"]
    out = filter_segmented_tokens(words, apply_noise_filter=False)
    assert "a" in out
    assert "珍珠" in out
