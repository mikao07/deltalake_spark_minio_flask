"""
痛點分析漏斗：第一層撈網（recall）→ 第二層過濾（filter）→ 情緒判定。

第一層：關鍵字 + 模糊匹配，盡量撈出疑似痛點。
第二層：片語／極性規則 + 「負面優先、好僅在無負面時偏正」的情緒過濾。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Sequence, Set, Tuple

from services.pain_topic_rules import (
    PAIN_TOPIC_POLARITY_RULES,
    PAIN_TOPIC_RULES,
    _contains_hint,
    _is_near_by_char_gap,
    _is_near_by_word_gap,
)
from services.text_similarity import pain_fuzzy_anchor_ratio

# 「好」+ 負面形容：整體視為負面信號（非正向的「好」）
GOOD_PLUS_BAD_PHRASES: Tuple[str, ...] = (
    "好慢",
    "好爛",
    "好差",
    "好難喝",
    "好失望",
    "好扯",
    "好離譜",
    "好糟",
    "好兇",
    "好不耐煩",
    "好臭",
    "好髒",
    "好脏",
    "好苦",
    "好澀",
    "好硬",
)

# 正向信號（僅在無負面證據時採計）
POSITIVE_SIGNALS: Tuple[str, ...] = (
    "好喝",
    "很好喝",
    "超好喝",
    "很好",
    "超好",
    "推薦",
    "大推",
    "喜歡",
    "满意",
    "滿意",
    "親切",
    "貼心",
    "讚",
    "赞",
    "棒",
    "優秀",
    "优秀",
    "回購",
    "回购",
)

# 全域負面信號（單字或片語；會搭配否定反轉檢查）
GLOBAL_NEGATIVE_SIGNALS: Tuple[str, ...] = (
    "差",
    "爛",
    "糟",
    "慢",
    "久",
    "難喝",
    "不好喝",
    "不耐煩",
    "兇",
    "凶",
    "惡劣",
    "失望",
    "問題",
    "糟糕",
    "差勁",
    "白眼",
    "髒",
    "脏",
    "漏",
    "錯",
    "错",
    "忘",
    "少給",
    "少给",
)

NEGATION_MARKERS: Tuple[str, ...] = ("不", "没", "沒", "無", "无", "別", "别", "未")

# 僅適合第一層撈網；單獨命中不足以確認痛點（需搭配主題極性／明確抱怨片語）
WEAK_RECALL_PHRASES: frozenset[str] = frozenset(
    {
        "排隊",
        "衛生",
        "脏",
        "髒",
        "載具",
        "发具",
        "發票",
        "发票",
        "電子發票",
        "电子发票",
        "收據",
        "收据",
        "小票",
        "統編",
        "统编",
        "結帳",
        "结账",
        "付款",
        "支付",
        "等待",
        "出餐",
        "速度",
        "製作",
        "等候",
    }
)

# 載具發票：禁止僅因 anchor／UI 字樣進入第一層撈網（須有明確抱怨片語才 recall）
_INVOICE_UI_ONLY_RECALL_SEEDS: frozenset[str] = frozenset(
    {
        "載具",
        "发具",
        "發票",
        "发票",
        "電子發票",
        "电子发票",
        "收據",
        "收据",
        "小票",
        "統編",
        "统编",
    }
)

# 出現在否定句時不應視為痛點（不難喝、不算慢…）
NEGATION_AWARE_TERMS: frozenset[str] = frozenset(
    {
        "難喝",
        "不好喝",
        "沒味道",
        "走味",
        "慢",
        "久",
        "太久",
        "差",
        "爛",
        "糟",
        "甜",
        "淡",
        "苦",
        "澀",
        "硬",
        "稀",
        "髒",
        "脏",
        "不安全",
        "危險",
    }
)


@dataclass
class PainReviewAnalysis:
    """單則評論的痛點漏斗結果。"""

    sentiment: str  # positive | neutral | negative
    pain_topics: List[str] = field(default_factory=list)
    pain_candidates: List[str] = field(default_factory=list)
    filtered_out: List[str] = field(default_factory=list)


@dataclass
class _ReviewContext:
    words: List[str]
    word_set: Set[str]
    joined_text: str
    joined_no_space: str


def _normalize_words(words: Sequence[str] | None) -> _ReviewContext | None:
    if not words:
        return None
    safe = [str(w).strip().lower() for w in words if str(w).strip()]
    if not safe:
        return None
    return _ReviewContext(
        words=safe,
        word_set=set(safe),
        joined_text=" ".join(safe),
        joined_no_space="".join(safe),
    )


def _dedupe_preserve_order(items: Sequence[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for raw in items:
        t = str(raw).strip()
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def _recall_seeds_for_topic(topic: str) -> List[str]:
    seeds: set[str] = set()
    for hint in PAIN_TOPIC_RULES.get(topic, []):
        h = str(hint).strip().lower()
        if h:
            seeds.add(h)
    cfg = PAIN_TOPIC_POLARITY_RULES.get(topic)
    if cfg and topic != "載具發票":
        for key in ("anchors", "negatives"):
            for x in cfg.get(key, []):
                s = str(x).strip().lower()
                if s:
                    seeds.add(s)
    return sorted(seeds)


def recall_pain_topic_candidates(ctx: _ReviewContext) -> List[str]:
    """第一層：寬鬆撈網，回傳疑似痛點主題。"""
    anchor_ratio = pain_fuzzy_anchor_ratio()
    hit: List[str] = []
    for topic in PAIN_TOPIC_RULES:
        seeds = _recall_seeds_for_topic(topic)
        for seed in seeds:
            if topic == "載具發票" and seed in _INVOICE_UI_ONLY_RECALL_SEEDS:
                continue
            if _contains_hint(
                ctx.word_set,
                ctx.joined_text,
                ctx.joined_no_space,
                seed,
                allow_fuzzy=True,
                fuzzy_ratio=anchor_ratio,
            ):
                hit.append(topic)
                break
    return _dedupe_preserve_order(hit)


def _is_negated_at(joined_no_space: str, start: int) -> bool:
    prefix = joined_no_space[max(0, start - 2) : start]
    return any(prefix.endswith(m) for m in NEGATION_MARKERS)


def _text_has_signal(
    signal: str,
    ctx: _ReviewContext,
    *,
    allow_fuzzy: bool = False,
) -> bool:
    return _contains_hint(
        ctx.word_set,
        ctx.joined_text,
        ctx.joined_no_space,
        signal,
        allow_fuzzy=allow_fuzzy,
    )


def _negative_occurrences_excluding_negation(joined_no_space: str, term: str) -> bool:
    t = str(term).strip().lower()
    if not t or t not in joined_no_space:
        return False
    for m in re.finditer(re.escape(t), joined_no_space):
        if not _is_negated_at(joined_no_space, m.start()):
            return True
    return False


def has_negative_evidence(ctx: _ReviewContext) -> bool:
    """負面優先：含負面片語、未被否定的負面詞、或「好+壞」片語。"""
    for phrase in GOOD_PLUS_BAD_PHRASES:
        if phrase in ctx.joined_no_space:
            return True
    for term in GLOBAL_NEGATIVE_SIGNALS:
        if _negative_occurrences_excluding_negation(ctx.joined_no_space, term):
            return True
    for cfg in PAIN_TOPIC_POLARITY_RULES.values():
        for n in cfg.get("negatives", []):
            n0 = str(n).strip().lower()
            if n0 and _negative_occurrences_excluding_negation(ctx.joined_no_space, n0):
                return True
    return False


def has_positive_evidence(ctx: _ReviewContext) -> bool:
    """正向信號：僅在無負面時才用於判斷 positive。"""
    if has_negative_evidence(ctx):
        return False
    return any(_text_has_signal(p, ctx, allow_fuzzy=True) for p in POSITIVE_SIGNALS)


def _hint_confirmed(hint: str, ctx: _ReviewContext) -> bool:
    h = str(hint).strip().lower()
    if not h:
        return False
    if not _contains_hint(
        ctx.word_set,
        ctx.joined_text,
        ctx.joined_no_space,
        h,
        allow_fuzzy=True,
    ):
        return False
    if h in NEGATION_AWARE_TERMS:
        return _negative_occurrences_excluding_negation(ctx.joined_no_space, h)
    if h in WEAK_RECALL_PHRASES:
        return has_negative_evidence(ctx)
    return True


def _topic_phrase_confirmed(topic: str, ctx: _ReviewContext) -> bool:
    for hint in PAIN_TOPIC_RULES.get(topic, []):
        if _hint_confirmed(str(hint), ctx):
            return True
    return False


def _topic_polarity_confirmed(topic: str, ctx: _ReviewContext) -> bool:
    cfg = PAIN_TOPIC_POLARITY_RULES.get(topic)
    if not cfg:
        return False
    anchors = [str(x).strip().lower() for x in cfg.get("anchors", []) if str(x).strip()]
    negatives = [str(x).strip().lower() for x in cfg.get("negatives", []) if str(x).strip()]
    if not anchors or not negatives:
        return False
    anchor_ratio = pain_fuzzy_anchor_ratio()
    has_anchor = any(
        _contains_hint(
            ctx.word_set,
            ctx.joined_text,
            ctx.joined_no_space,
            a,
            allow_fuzzy=True,
            fuzzy_ratio=anchor_ratio,
        )
        for a in anchors
    )
    has_negative = any(
        _contains_hint(ctx.word_set, ctx.joined_text, ctx.joined_no_space, n, allow_fuzzy=False)
        for n in negatives
    )
    if not (has_anchor and has_negative):
        return False
    max_word_gap = int(cfg.get("max_word_gap", 3))
    max_char_gap = int(cfg.get("max_char_gap", 8))
    return _is_near_by_word_gap(
        ctx.words,
        anchors,
        negatives,
        max_word_gap,
        anchor_fuzzy_ratio=anchor_ratio,
    ) or _is_near_by_char_gap(ctx.joined_no_space, anchors, negatives, max_char_gap)


def filter_pain_candidates(
    ctx: _ReviewContext,
    candidates: Sequence[str],
) -> Tuple[List[str], List[str]]:
    """
    第二層：片語或極性規則確認後才保留。
    高精確路徑可跳過第一層（例如載具+沒綁近距離）；僅被撈網但未確認者列入 filtered_out。
    """
    confirmed: List[str] = []
    filtered_out: List[str] = []
    candidate_set = set(candidates)

    for topic in PAIN_TOPIC_RULES:
        if _topic_phrase_confirmed(topic, ctx) or _topic_polarity_confirmed(topic, ctx):
            confirmed.append(topic)
        elif topic in candidate_set:
            filtered_out.append(topic)

    return _dedupe_preserve_order(confirmed), _dedupe_preserve_order(filtered_out)


def classify_review_sentiment(ctx: _ReviewContext, pain_topics: Sequence[str]) -> str:
    """
    情緒：有痛點主題或負面證據 → negative；
    否則有正向且無負面 → positive；其餘 neutral。
    """
    if pain_topics:
        return "negative"
    if has_negative_evidence(ctx):
        return "negative"
    if has_positive_evidence(ctx):
        return "positive"
    return "neutral"


def analyze_pain_review(words: Sequence[str] | None) -> PainReviewAnalysis:
    """執行完整漏斗並回傳結構化結果。"""
    ctx = _normalize_words(words)
    if ctx is None:
        return PainReviewAnalysis(sentiment="neutral")

    candidates = recall_pain_topic_candidates(ctx)
    pain_topics, filtered_out = filter_pain_candidates(ctx, candidates)
    sentiment = classify_review_sentiment(ctx, pain_topics)

    return PainReviewAnalysis(
        sentiment=sentiment,
        pain_topics=pain_topics,
        pain_candidates=candidates,
        filtered_out=filtered_out,
    )
