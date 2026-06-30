"""金層下游品質（探索 TF-IDF Top vs 探索停用詞）。"""

from services.gold_quality import evaluate_gold_downstream_quality


def test_gold_tfidf_top_warns_on_exploration_stopword_leak():
    report = evaluate_gold_downstream_quality(
        corpus_doc_count=10,
        tfidf_output_rows=5,
        topic_output_rows=3,
        tfidf_top=[{"keyword": "珍珠"}, {"keyword": "外送"}],
        tfidf_exploration_stopwords=["珍珠", "自己", "店員"],
    )
    assert report["passed"] is True
    leak = next(c for c in report["checks"] if c["name"] == "tfidf_top_exploration_stopword_leak")
    assert leak["passed"] is False
    assert "珍珠" in leak["value"]


def test_gold_tfidf_top_passes_when_filtered():
    report = evaluate_gold_downstream_quality(
        corpus_doc_count=10,
        tfidf_output_rows=5,
        topic_output_rows=3,
        tfidf_top=[{"keyword": "外送"}, {"keyword": "延遲"}],
        tfidf_exploration_stopwords=["珍珠", "自己"],
    )
    leak = next(c for c in report["checks"] if c["name"] == "tfidf_top_exploration_stopword_leak")
    assert leak["passed"] is True
