"""
痛點主題規則（規則式分類 MVP）。

以銀層 tokens 為輸入，輸出商業主題標籤（等待時間、服務態度…）。
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Sequence

TOPIC_RULE_VERSION = "v1.2-drinks"

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
        "載具",
        "发具",
        "發票",
        "发票",
        "收據",
        "收据",
        "小票",
        "統編",
        "电子发票",
        "電子發票",
        "沒綁載具",
        "没绑载具",
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
        "anchors": ["載具", "發票", "收據", "小票", "統編"],
        "negatives": ["沒", "无", "無", "錯", "漏", "問題", "綁", "绑", "爛"],
        "max_word_gap": 4,
        "max_char_gap": 12,
    },
    "結帳支付": {
        "anchors": ["結帳", "结账", "付", "付款", "支付", "收銀", "收银", "line", "pay"],
        "negatives": ["慢", "錯", "爛", "久", "問題", "麻烦", "麻煩", "搞", "卡"],
        "max_word_gap": 4,
        "max_char_gap": 12,
    },
}


def _contains_hint(word_set: set[str], joined_text: str, joined_no_space: str, hint: str) -> bool:
    h = str(hint).strip().lower()
    if not h:
        return False
    return h in word_set or h in joined_text or h in joined_no_space


def _is_near_by_word_gap(
    words: List[str],
    anchors: List[str],
    negatives: List[str],
    max_gap: int,
) -> bool:
    anchor_idx = [i for i, w in enumerate(words) if w in anchors]
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
    """由單則評論的 token 列表產出痛點主題標籤（去重、保留順序）。"""
    if not words:
        return []
    safe_words = [str(w).strip().lower() for w in words if str(w).strip()]
    if not safe_words:
        return []
    word_set = set(safe_words)
    joined_text = " ".join(safe_words)
    joined_no_space = "".join(safe_words)
    hit_topics: List[str] = []

    for topic, cfg in PAIN_TOPIC_POLARITY_RULES.items():
        anchors = [str(x).strip().lower() for x in cfg.get("anchors", []) if str(x).strip()]
        negatives = [str(x).strip().lower() for x in cfg.get("negatives", []) if str(x).strip()]
        if not anchors or not negatives:
            continue
        has_anchor = any(_contains_hint(word_set, joined_text, joined_no_space, a) for a in anchors)
        has_negative = any(_contains_hint(word_set, joined_text, joined_no_space, n) for n in negatives)
        if not (has_anchor and has_negative):
            continue
        max_word_gap = int(cfg.get("max_word_gap", 3))
        max_char_gap = int(cfg.get("max_char_gap", 8))
        if _is_near_by_word_gap(safe_words, anchors, negatives, max_word_gap) or _is_near_by_char_gap(
            joined_no_space,
            anchors,
            negatives,
            max_char_gap,
        ):
            hit_topics.append(topic)

    for topic, hints in PAIN_TOPIC_RULES.items():
        if topic in hit_topics:
            continue
        for hint in hints:
            if _contains_hint(word_set, joined_text, joined_no_space, str(hint)):
                hit_topics.append(topic)
                break

    return hit_topics
