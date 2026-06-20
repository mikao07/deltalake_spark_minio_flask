"""痛點漏斗（recall → filter → sentiment）測試。"""

from services.pain_funnel import analyze_pain_review
from services.pain_topic_rules import analyze_pain_review_tokens, label_pain_topics


def test_funnel_positive_staff_no_pain_topic():
    result = analyze_pain_review(["店員", "態度", "不錯", "親切"])
    assert result.sentiment == "positive"
    assert "服務態度" not in result.pain_topics
    assert "服務態度" in result.pain_candidates
    assert "服務態度" in result.filtered_out


def test_funnel_negative_service_attitude():
    result = analyze_pain_review(["店員", "態度", "不耐煩"])
    assert result.sentiment == "negative"
    assert "服務態度" in result.pain_topics


def test_funnel_good_slow_is_negative_wait_topic():
    result = analyze_pain_review(["好慢"])
    assert result.sentiment == "negative"
    assert "等待時間" in result.pain_topics


def test_funnel_queue_only_recall_but_filtered():
    result = analyze_pain_review(["排隊", "人很多"])
    assert "等待時間" in result.pain_candidates
    assert "等待時間" not in result.pain_topics
    assert result.sentiment in ("neutral", "positive")


def test_funnel_negated_negative_not_pain():
    result = analyze_pain_review(["其實", "不難喝"])
    assert "品質口感" not in result.pain_topics
    assert result.sentiment != "negative" or not result.pain_topics


def test_label_pain_topics_wrapper_matches_funnel():
    words = ["飲料", "做錯", "重做"]
    assert label_pain_topics(words) == analyze_pain_review_tokens(words).pain_topics


def test_invoice_ui_only_not_pain_topic():
    result = analyze_pain_review(["電子發票", "好喝", "推薦"])
    assert "載具發票" not in result.pain_topics


def test_invoice_ui_with_unrelated_negative_not_pain_topic():
    result = analyze_pain_review(["電子發票", "店員", "態度", "差"])
    assert "載具發票" not in result.pain_topics
    assert "服務態度" in result.pain_topics


def test_invoice_carrier_with_drink_complaint_not_cross_tagged():
    result = analyze_pain_review(["載具", "珍珠", "奶茶", "難喝"])
    assert "品質口感" in result.pain_topics
    assert "載具發票" not in result.pain_topics


def test_invoice_real_complaint_still_detected():
    result = analyze_pain_review(["載具", "發票", "沒綁"])
    assert "載具發票" in result.pain_topics
    result2 = analyze_pain_review(["沒綁載具", "店員", "態度", "差"])
    assert "載具發票" in result2.pain_topics
