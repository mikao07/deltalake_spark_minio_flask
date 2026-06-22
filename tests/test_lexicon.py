"""Gold lexicon：effective_stop = stop − protected。"""

from services.lexicon import (
    build_effective_stopwords,
    expand_pain_protected_terms,
    filter_tokens_for_analytics,
    parse_stopwords_lines,
)


def test_parse_stopwords_lines_skips_comments():
    lines = ["# comment", "好喝", "珍珠  # inline"]
    assert parse_stopwords_lines(lines) == ["好喝", "珍珠"]


def test_expand_pain_protected_includes_polarity_anchors():
    protected = expand_pain_protected_terms()
    assert "珍珠" in protected
    assert "漏" in protected
    assert "態度差" in protected


def test_effective_stopwords_removes_protected():
    stop = ["好喝", "珍珠", "奶茶", "覺得"]
    protected = expand_pain_protected_terms()
    effective = build_effective_stopwords(stop, protected)
    assert "珍珠" not in effective
    assert "好喝" in effective


def test_filter_tokens_for_analytics():
    effective = frozenset(["好喝", "覺得"])
    out = filter_tokens_for_analytics(["珍珠", "好喝", "不耐煩", "好喝"], effective)
    assert out == ["珍珠", "不耐煩"]
