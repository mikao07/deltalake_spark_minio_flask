"""
痛點主題規則（規則式分類 MVP）。

以銀層 tokens 為輸入，輸出商業主題標籤（等待時間、服務態度…）。
支援模糊匹配以容忍 OCR 錯字（PAIN_FUZZY_* 環境變數）。
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Sequence

from services.text_similarity import (
    fuzzy_phrase_hit,
    fuzzy_word_hit,
    pain_fuzzy_anchor_ratio,
    pain_fuzzy_enabled,
    pain_fuzzy_min_chars,
    pain_fuzzy_min_ratio,
)

TOPIC_RULE_VERSION = "v1.4.1-drinks-funnel-invoice"

# 明確負面／抱怨片語（不含「親切、貼心」等正面詞）
PAIN_TOPIC_RULES: Dict[str, List[str]] = {
    "等待時間": [
        "等很久",
        "等超久",
        "久等",
        "排隊",
        "等待太久",
        "出餐慢",
        "等半天",
        "時間管理",
        "等太久",
        "好慢",
        "好久",
    ],
    "服務態度": [
        "態度差",
        "不耐煩",
        "兇",
        "服務差",
        "白眼",
        "口氣差",
        "沒禮貌",
        "愛理不理",
        "翻白眼",
        "兇巴巴",
        "態度惡劣",
        "很不耐煩",
    ],
    "出錯重做": [
        "做錯",
        "弄錯",
        "漏單",
        "少做",
        "重做",
        "漏做",
        "沒做",
        "點錯",
        "做不出來",
        "忘記",
        "給錯",
        "飲料做錯",
        "做錯飲料",
        "少給",
    ],
    "品質口感": [
        "太甜",
        "太淡",
        "沒味道",
        "難喝",
        "走味",
        "稀",
        "不新鮮",
        "不好喝",
        "太苦",
        "太澀",
        "硬邦邦",
        "珍珠硬",
    ],
    "安全衛生": [
        "不安全",
        "危險",
        "衛生",
        "髒",
        "脏",
        "地板黏",
        "蟲",
        "異物",
        "不乾淨",
        "不干净",
    ],
    "載具發票": [
        # 僅保留明確抱怨片語；「載具／發票／電子發票」等 UI 字樣改由極性規則近距離判斷
        "沒綁載具",
        "没绑载具",
        "載具沒綁",
        "载具没绑",
        "沒綁发具",
        "發票打錯",
        "发票打错",
        "發票開錯",
        "没开发票",
        "沒開發票",
        "漏開發票",
        "漏开发票",
    ],
    "結帳支付": [
        "結帳",
        "结账",
        "付錢",
        "付钱",
        "付款",
        "支付",
        "line pay",
        "linepay",
        "信用卡",
        "行動支付",
        "行动支付",
        "找零",
        "收銀",
        "收银",
        "結帳慢",
        "付款問題",
    ],
}

# 錨點詞 + 負面詞 + 距離（優先於單純關鍵詞命中）
PAIN_TOPIC_POLARITY_RULES: Dict[str, Dict[str, Any]] = {
    "服務態度": {
        "anchors": ["服務態度", "態度", "店員", "服務人員", "服務員", "員工", "櫃台", "收銀"],
        "negatives": ["差", "不好", "爛", "糟", "差勁", "不耐煩", "兇", "口氣差", "白眼", "惡劣"],
        "max_word_gap": 4,
        "max_char_gap": 10,
    },
    "等待時間": {
        "anchors": ["等", "等待", "排隊", "出餐", "速度", "等候", "製作"],
        "negatives": ["久", "慢", "太久", "很久", "超久", "超慢", "過久", "半天"],
        "max_word_gap": 4,
        "max_char_gap": 10,
    },
    "出錯重做": {
        "anchors": ["飲料", "杯", "訂單", "做", "給", "點", "餐"],
        "negatives": ["錯", "漏", "忘", "少", "重", "給錯", "做錯"],
        "max_word_gap": 4,
        "max_char_gap": 10,
    },
    "品質口感": {
        "anchors": ["珍珠", "奶茶", "茶", "口感", "味道", "飲料"],
        "negatives": ["硬", "難喝", "淡", "甜", "苦", "澀", "稀", "走味"],
        "max_word_gap": 3,
        "max_char_gap": 8,
    },
    "載具發票": {
        "anchors": ["載具", "发具", "發票", "发票", "電子發票", "电子发票", "收據", "收据", "小票", "統編"],
        "negatives": [
            "沒綁",
            "没绑",
            "沒開",
            "没开",
            "漏開",
            "漏开",
            "開錯",
            "开错",
            "打錯",
            "打错",
            "失敗",
            "失败",
            "無法",
            "无法",
            "綁定",
            "绑定",
        ],
        "max_word_gap": 3,
        "max_char_gap": 8,
    },
    "結帳支付": {
        "anchors": ["結帳", "结账", "付", "付款", "支付", "收銀", "收银", "line", "pay"],
        "negatives": ["慢", "錯", "爛", "久", "問題", "麻烦", "麻煩", "搞", "卡"],
        "max_word_gap": 4,
        "max_char_gap": 12,
    },
}


def _contains_hint(
    word_set: set[str],
    joined_text: str,
    joined_no_space: str,
    hint: str,
    *,
    allow_fuzzy: bool = False,
    fuzzy_ratio: float | None = None,
) -> bool:
    h = str(hint).strip().lower()
    if not h:
        return False
    if h in word_set or h in joined_text or h in joined_no_space:
        return True
    if not allow_fuzzy or not pain_fuzzy_enabled():
        return False
    min_chars = pain_fuzzy_min_chars()
    if len(h) < min_chars:
        return False
    ratio = pain_fuzzy_min_ratio() if fuzzy_ratio is None else fuzzy_ratio
    for w in word_set:
        if len(w) >= min_chars and fuzzy_word_hit(w, h, min_ratio=ratio):
            return True
    return fuzzy_phrase_hit(joined_no_space, h, min_chars=min_chars, min_ratio=ratio)


def _word_matches_candidates(
    word: str,
    candidates: List[str],
    *,
    allow_fuzzy: bool,
    fuzzy_ratio: float | None = None,
) -> bool:
    w = str(word).strip().lower()
    if not w:
        return False
    for c in candidates:
        c0 = str(c).strip().lower()
        if not c0:
            continue
        if w == c0 or c0 in w or w in c0:
            return True
        if allow_fuzzy and pain_fuzzy_enabled():
            min_chars = pain_fuzzy_min_chars()
            if len(c0) >= min_chars and fuzzy_word_hit(w, c0, min_ratio=fuzzy_ratio):
                return True
    return False


def _is_near_by_word_gap(
    words: List[str],
    anchors: List[str],
    negatives: List[str],
    max_gap: int,
    *,
    anchor_fuzzy_ratio: float | None = None,
) -> bool:
    anchor_idx = [
        i
        for i, w in enumerate(words)
        if _word_matches_candidates(
            w,
            anchors,
            allow_fuzzy=True,
            fuzzy_ratio=anchor_fuzzy_ratio,
        )
    ]
    neg_idx = [i for i, w in enumerate(words) if w in negatives]
    if not anchor_idx or not neg_idx:
        return False
    return any(abs(ai - ni) <= max_gap for ai in anchor_idx for ni in neg_idx)


def _is_near_by_char_gap(
    joined_no_space: str,
    anchors: List[str],
    negatives: List[str],
    max_gap: int,
) -> bool:
    if not joined_no_space:
        return False
    for a in anchors:
        a0 = str(a).strip().lower()
        if not a0:
            continue
        for n in negatives:
            n0 = str(n).strip().lower()
            if not n0:
                continue
            if re.search(rf"{re.escape(a0)}.{{0,{max_gap}}}{re.escape(n0)}", joined_no_space):
                return True
            if re.search(rf"{re.escape(n0)}.{{0,{max_gap}}}{re.escape(a0)}", joined_no_space):
                return True
    return False


def label_pain_topics(words: Sequence[str] | None) -> List[str]:
    """由單則評論的 token 列表產出痛點主題標籤（漏斗第二層確認後）。"""
    from services.pain_funnel import analyze_pain_review

    return analyze_pain_review(words).pain_topics


def analyze_pain_review_tokens(words: Sequence[str] | None):
    """執行完整漏斗；回傳 PainReviewAnalysis（含 candidates、sentiment）。"""
    from services.pain_funnel import analyze_pain_review

    return analyze_pain_review(words)
