"""Gold lexicon：effective_stop = stop − protected。"""

from services.lexicon import (
    build_effective_stopwords,
    build_tfidf_exploration_stopwords,
    expand_pain_protected_terms,
    filter_tokens_for_analytics,
    filter_tokens_for_tfidf_exploration,
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


def test_tfidf_exploration_stopwords_drop_scene_and_function_words():
    from services.lexicon import build_tfidf_exploration_stopwords, filter_tokens_for_tfidf_exploration

    merged = ["好喝", "珍珠", "飲料", "奶茶"]
    tfidf_stop = frozenset(build_tfidf_exploration_stopwords(merged, "drinks"))
    assert "珍珠" in tfidf_stop
    assert "店員" in tfidf_stop
    assert "自己" in tfidf_stop
    out = filter_tokens_for_tfidf_exploration(
        ["自己", "不用", "店員", "珍珠", "飲料", "難喝", "不耐煩"],
        tfidf_stop,
    )
    assert out == ["難喝", "不耐煩"]


def test_effective_stopwords_keeps_protected_but_tfidf_stop_does_not():
    from services.lexicon import build_tfidf_exploration_stopwords

    stop = ["好喝", "珍珠", "飲料"]
    protected = expand_pain_protected_terms()
    effective = frozenset(build_effective_stopwords(stop, protected))
    tfidf_stop = frozenset(build_tfidf_exploration_stopwords(stop, "drinks"))
    assert "珍珠" not in effective
    assert "珍珠" in tfidf_stop
    assert "店員" in tfidf_stop
