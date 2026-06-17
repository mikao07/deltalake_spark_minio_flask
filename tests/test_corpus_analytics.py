"""Phase A/B 語料統計單元測試（純 Python）。"""

import math

from services.corpus_analytics import (
    adjacent_bigrams,
    compute_pmi,
    compute_tfidf_score,
    rank_pmi_from_bigrams,
    rank_tfidf_from_doc_terms,
)


def test_tfidf_favors_discriminative_term():
    """「異物」只在少數評論出現，應比到處都有的「珍珠」分數高。"""
    docs = []
    for i in range(10):
        docs.append((f"d{i}", "珍珠", 2))
    docs.extend(
        [
            ("d0", "異物", 1),
            ("d1", "異物", 1),
            ("d2", "異物", 1),
        ]
    )
    ranked = rank_tfidf_from_doc_terms(docs)
    by_kw = {r["keyword"]: r for r in ranked}
    assert by_kw["異物"]["tfidf_score"] > by_kw["珍珠"]["tfidf_score"]


def test_pmi_favors_collocated_bigram():
    total = 100
    # 「珍珠 奶茶」常一起出現
    pmi_high = compute_pmi(20, 25, 22, total)
    # 隨機共現
    pmi_low = compute_pmi(2, 25, 22, total)
    assert pmi_high > pmi_low


def test_rank_pmi_from_bigrams():
    rows = rank_pmi_from_bigrams(
        [
            ("珍珠", "奶茶", 10),
            ("珍珠", "奶茶", 5),
            ("很", "好喝", 2),
        ],
        min_bigram_count=2,
    )
    by_phrase = {r["phrase"]: r for r in rows}
    assert "珍珠 奶茶" in by_phrase
    assert by_phrase["珍珠 奶茶"]["bigram_count"] == 15
    assert all(r["pmi_score"] > 0 for r in rows)


def test_adjacent_bigrams_skips_empty():
    assert adjacent_bigrams(["珍珠", "", "奶茶"]) == [("珍珠", "奶茶")]


def test_compute_tfidf_score_matches_formula():
    score = compute_tfidf_score(5, 2, 20)
    idf = math.log((20 + 1) / (2 + 1))
    assert score == 5 * idf
