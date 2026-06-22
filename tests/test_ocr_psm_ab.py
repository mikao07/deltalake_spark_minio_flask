from services.ocr_psm_ab import (
    count_cjk_internal_spaces,
    default_keyword_hints,
    keyword_hits,
    summarize_ocr_text,
)
from services.ocr_spark import normalize_psm


def test_normalize_psm_valid():
    assert normalize_psm("11") == "11"
    assert normalize_psm(None, default="6") == "6"


def test_normalize_psm_invalid():
    try:
        normalize_psm("99")
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_count_cjk_internal_spaces():
    assert count_cjk_internal_spaces("站收 銀 反應") == 2
    assert count_cjk_internal_spaces("Line Pay 很好") == 0
    assert count_cjk_internal_spaces("綠茶很好喝") == 0


def test_keyword_hits_case_insensitive():
    text = "有電子發票與 Line Pay"
    hits = keyword_hits(text, default_keyword_hints())
    assert "發票" in hits
    assert "line pay" in hits


def test_summarize_ocr_text():
    m = summarize_ocr_text("珍珠 奶茶 好喝", ["珍珠", "好喝"])
    assert m["char_count"] > 0
    assert "珍珠" in m["keyword_hits"]
    assert m["is_error"] is False
