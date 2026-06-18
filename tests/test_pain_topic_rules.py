"""痛點主題規則與領域停用詞測試。"""

from services.domain_lexicons import get_builtin_domain_stopwords, merge_stopword_lists
from services.pain_topic_rules import label_pain_topics


def test_drinks_builtin_stopwords_available():
    words = get_builtin_domain_stopwords("drinks")
    assert "好喝" in words
    assert "珍珠" in words
    assert "店員" not in words


def test_merge_stopword_lists_dedupes():
    merged = merge_stopword_lists(["好喝", "珍珠"], ["好喝", "推薦"])
    assert merged == ["好喝", "珍珠", "推薦"]


def test_label_service_attitude_negative():
    topics = label_pain_topics(["店員", "態度", "不耐煩", "消費者"])
    assert "服務態度" in topics


def test_label_positive_staff_not_pain_topic():
    topics = label_pain_topics(["店員", "態度", "不錯", "親切"])
    assert "服務態度" not in topics


def test_label_wrong_order_topic():
    topics = label_pain_topics(["飲料", "做錯", "重做"])
    assert "出錯重做" in topics


def test_label_invoice_topic():
    topics = label_pain_topics(["載具", "發票", "沒綁"])
    assert "載具發票" in topics


def test_label_checkout_topic():
    topics = label_pain_topics(["結帳", "line", "pay", "問題"])
    assert "結帳支付" in topics


def test_label_service_attitude_fuzzy_ocr_typo(monkeypatch):
    monkeypatch.setenv("PAIN_FUZZY_ENABLED", "true")
    topics = label_pain_topics(["店員", "服務態渡", "不耐煩"])
    assert "服務態度" in topics


def test_label_wait_topic_fuzzy_typo(monkeypatch):
    monkeypatch.setenv("PAIN_FUZZY_ENABLED", "true")
    topics = label_pain_topics(["出餐漫"])
    assert "等待時間" in topics


def test_fuzzy_disabled_exact_only(monkeypatch):
    monkeypatch.setenv("PAIN_FUZZY_ENABLED", "false")
    topics = label_pain_topics(["出餐漫"])
    assert "等待時間" not in topics
