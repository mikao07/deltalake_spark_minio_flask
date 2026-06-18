"""
銀層文字清洗、分詞與內建停用詞（純 Python，可單元測試、供 Spark UDF 呼叫）。

分層職責：
- Bronze：保留 OCR 原文（extracted_text）
- Silver：cleaned_text（去標點、正規化空白）→ tokens（Jieba + 停用詞）
- Gold：僅消費銀層 tokens，不再清洗原文
"""

from __future__ import annotations

import re
from typing import Iterable, List, Sequence

# 內建中文贅詞／停用詞（無需人工維護辭典檔）
_BUILTIN_STOPWORDS_LIST: Sequence[str] = (
  # 使用者常提及的助詞、虛詞
    "了",
    "是",
    "這",
    "那",
    "的",
    "個",
    "月",
    "前",
    "跟",
    "到",
    "會",
    "有",
    "很",
    "和",
    "也",
    "這",
    "們",
    "並",
    "嗎",
    "呢",
    "吧",
    "啊",
    "地",
    "得",
    "著",
    "过",
    "過",
    "么",
    "什麼",
    "什么",
    "為",
    "为",
    "因",
    "而",
    "于",
    "於",
    "与",
    "與",
    "或",
    "但",
    "若",
    "虽",
    "雖",
    "则",
    "則",
    "且",
    "仍",
    "还",
    "還",
    "就",
    "都",
    "又",
    "再",
    "已",
    "被",
    "把",
    "让",
    "讓",
    "给",
    "給",
    "向",
    "从",
    "從",
    "以",
    "及",
    "其",
    "之",
    "所",
    "我",
    "你",
    "他",
    "她",
    "它",
    "我們",
    "你们",
    "你們",
    "他们",
    "他們",
    "這個",
    "那个",
    "那個",
    "不是",
    "沒有",
    "没有",
    "可以",
    "就是",
    "还是",
    "還是",
    "一个",
    "一個",
    "一些",
    "一下",
    "真的",
    "觉得",
    "覺得",
    "先生",
    # 標點與 OCR 雜訊
    "：",
    "，",
    "。",
    "、",
    "(",
    ")",
    "-",
    "+",
    "img",
    "png",
    "html",
    "the",
    "a",
    "b",
    "c",
    "x",
    "ok",
    "aer",
)

BUILTIN_STOPWORDS: frozenset[str] = frozenset(w.lower() for w in _BUILTIN_STOPWORDS_LIST if w)

_MIN_WORD_LENGTH = 2

# 與 spark_service._silver_cleaned_text_expr 的 Spark 正則對齊（\p{L}\p{N} + 底線 + 空白）
SILVER_CLEAN_TEXT_SPARK_PATTERN = r"[^\p{L}\p{N}\s_]"
_CLEAN_TEXT_RE = re.compile(r"[^\w\s]", flags=re.UNICODE)


def clean_text_for_segmentation(text: str | None) -> str:
    """與金層 regexp_replace 對齊：保留字母/數字/底線/空白，轉小寫。"""
    if text is None:
        return ""
    raw = str(text).strip()
    if not raw:
        return ""
    cleaned = _CLEAN_TEXT_RE.sub(" ", raw)
    return " ".join(cleaned.lower().split())


def filter_segmented_tokens(
    words: Iterable[str],
    *,
    extra_stopwords: Iterable[str] | None = None,
    apply_noise_filter: bool = True,
    min_word_length: int = _MIN_WORD_LENGTH,
) -> List[str]:
    """
    分詞後過濾：內建停用詞 + 可選外部停用詞；可關閉雜訊規則（長度、純數字等）。
    """
    stop = set(BUILTIN_STOPWORDS)
    if extra_stopwords:
        stop.update(str(w).strip().lower() for w in extra_stopwords if str(w).strip())

    out: List[str] = []
    seen: set[str] = set()
    for raw in words:
        w = str(raw).strip().lower()
        if not w or w in seen:
            continue
        if apply_noise_filter:
            if len(w) < min_word_length:
                continue
            if w.isdigit():
                continue
            if re.fullmatch(r"[a-z]{1,2}", w):
                continue
            if w in stop:
                continue
        out.append(w)
        seen.add(w)
    return out


def segment_text_to_tokens(
    text: str | None,
    *,
    userdict_local_path: str | None = None,
    extra_stopwords: Iterable[str] | None = None,
    apply_noise_filter: bool = True,
    already_cleaned: bool = False,
) -> List[str]:
    """
    清洗（可選）→ Jieba 分詞 → 停用詞過濾。供 Spark Python UDF 在 executor 上呼叫。
    銀層應先寫入 cleaned_text，再以 already_cleaned=True 分詞，避免重複清洗。
    """
    if already_cleaned:
        cleaned = str(text).strip().lower() if text is not None else ""
        cleaned = " ".join(cleaned.split())
    else:
        cleaned = clean_text_for_segmentation(text)
    if not cleaned:
        return []

    import jieba

    if not getattr(segment_text_to_tokens, "_jieba_initialized", False):
        jieba.initialize()
        segment_text_to_tokens._jieba_initialized = True  # type: ignore[attr-defined]

    if userdict_local_path:
        try:
            jieba.load_userdict(userdict_local_path)
        except Exception:
            pass

    words = jieba.cut(cleaned, cut_all=False)
    return filter_segmented_tokens(
        words,
        extra_stopwords=extra_stopwords,
        apply_noise_filter=apply_noise_filter,
    )
