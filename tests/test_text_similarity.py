"""字串相似度與領域詞典測試。"""

from services.domain_lexicons import (
    get_all_builtin_ocr_user_words,
    get_builtin_jieba_terms,
    get_builtin_ocr_user_words,
    resolve_local_jieba_userdict_path,
    resolve_local_ocr_user_words_path,
)
from services.text_similarity import fuzzy_phrase_hit, fuzzy_word_hit, similarity_ratio


def test_builtin_drinks_jieba_terms():
    terms = get_builtin_jieba_terms("drinks")
    words = [t[0] for t in terms]
    assert "服務態度" in words
    assert "50嵐" in words


def test_builtin_drinks_ocr_user_words():
    words = get_builtin_ocr_user_words("drinks")
    assert "50嵐" in words
    assert "LinePay" in words
    assert len(get_all_builtin_ocr_user_words()) >= len(words)


def test_local_dict_paths_exist():
    assert resolve_local_jieba_userdict_path("drinks")
    assert resolve_local_ocr_user_words_path("drinks")


def test_fuzzy_word_hit_typo():
    assert fuzzy_word_hit("服務態渡", "服務態度", min_ratio=0.78)


def test_fuzzy_phrase_hit_partial():
    assert fuzzy_phrase_hit("服務態渡很差", "服務態度", min_ratio=0.78)


def test_similarity_exact():
    assert similarity_ratio("排隊", "排隊") == 1.0
