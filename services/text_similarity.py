"""
字串相似度工具：供痛點規則在 OCR 錯字時做模糊匹配。
"""

from __future__ import annotations

import os
from difflib import SequenceMatcher


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def pain_fuzzy_enabled() -> bool:
    return _env_bool("PAIN_FUZZY_ENABLED", True)


def pain_fuzzy_min_ratio() -> float:
    return _env_float("PAIN_FUZZY_MIN_RATIO", 0.78)


def pain_fuzzy_anchor_ratio() -> float:
    return _env_float("PAIN_FUZZY_ANCHOR_RATIO", 0.88)


def pain_fuzzy_min_chars() -> int:
    return max(2, _env_int("PAIN_FUZZY_MIN_CHARS", 3))


def similarity_ratio(a: str, b: str) -> float:
    a0, b0 = (a or "").strip().lower(), (b or "").strip().lower()
    if not a0 or not b0:
        return 0.0
    if a0 == b0:
        return 1.0
    try:
        from rapidfuzz import fuzz

        return float(fuzz.ratio(a0, b0)) / 100.0
    except ImportError:
        return SequenceMatcher(None, a0, b0).ratio()


def partial_similarity_ratio(haystack: str, needle: str) -> float:
    h, n = (haystack or "").strip().lower(), (needle or "").strip().lower()
    if not h or not n:
        return 0.0
    if n in h:
        return 1.0
    try:
        from rapidfuzz import fuzz

        return float(fuzz.partial_ratio(n, h)) / 100.0
    except ImportError:
        if len(n) > len(h):
            return SequenceMatcher(None, h, n).ratio()
        best = 0.0
        size = len(n)
        for delta in (-1, 0, 1):
            win = max(1, size + delta)
            if win > len(h):
                continue
            for i in range(0, len(h) - win + 1):
                chunk = h[i : i + win]
                best = max(best, SequenceMatcher(None, chunk, n).ratio())
        return best


def fuzzy_phrase_hit(
    haystack: str,
    needle: str,
    *,
    min_chars: int | None = None,
    min_ratio: float | None = None,
) -> bool:
    n = (needle or "").strip().lower()
    if not n:
        return False
    min_len = pain_fuzzy_min_chars() if min_chars is None else min_chars
    if len(n) < min_len:
        return False
    h = (haystack or "").strip().lower()
    if not h:
        return False
    if n in h:
        return True
    threshold = pain_fuzzy_min_ratio() if min_ratio is None else min_ratio
    return partial_similarity_ratio(h, n) >= threshold


def fuzzy_word_hit(word: str, candidate: str, *, min_ratio: float | None = None) -> bool:
    w, c = (word or "").strip().lower(), (candidate or "").strip().lower()
    if not w or not c:
        return False
    if w == c:
        return True
    min_len = pain_fuzzy_min_chars()
    if len(w) < min_len and len(c) < min_len:
        return False
    threshold = pain_fuzzy_min_ratio() if min_ratio is None else min_ratio
    score = max(similarity_ratio(w, c), partial_similarity_ratio(w, c), partial_similarity_ratio(c, w))
    return score >= threshold
