"""
語料統計：TF-IDF 痛點候選詞（Phase A）與 PMI 片語發現（Phase B）。

純 Python 函式可單元測試；Spark 聚合邏輯見 spark_service.build_gold_*_dataframe。
"""

from __future__ import annotations

import math
from typing import Iterable, List, Sequence, Tuple


def compute_idf(doc_frequency: int, corpus_doc_count: int) -> float:
    """平滑 IDF：log((N+1)/(df+1))。"""
    n = max(0, int(corpus_doc_count))
    df = max(0, int(doc_frequency))
    return math.log((n + 1) / (df + 1))


def compute_tfidf_score(total_tf: int, doc_frequency: int, corpus_doc_count: int) -> float:
    """語料級 TF-IDF：total_tf × IDF（高 IDF = 只在少數評論突出）。"""
    tf = max(0, int(total_tf))
    return tf * compute_idf(doc_frequency, corpus_doc_count)


def compute_pmi(
    bigram_count: int,
    w1_count: int,
    w2_count: int,
    total_bigrams: int,
) -> float:
    """
    相鄰 bigram 的 PMI：log(P(w1,w2) / (P(w1) P(w2)))。
    w1_count / w2_count 為該词作為 bigram 第一／第二位置的总次数。
    """
    total = int(total_bigrams)
    c_xy = int(bigram_count)
    c_x = int(w1_count)
    c_y = int(w2_count)
    if total <= 0 or c_xy <= 0 or c_x <= 0 or c_y <= 0:
        return float("-inf")
    p_xy = c_xy / total
    p_x = c_x / total
    p_y = c_y / total
    denom = p_x * p_y
    if denom <= 0:
        return float("-inf")
    return math.log(p_xy / denom)


def adjacent_bigrams(tokens: Iterable[str]) -> List[Tuple[str, str]]:
    """從有序 token 列舉相鄰二元組（供 PMI 片語發現）。"""
    safe = [str(t).strip().lower() for t in tokens if str(t) and str(t).strip()]
    if len(safe) < 2:
        return []
    return [(safe[i], safe[i + 1]) for i in range(len(safe) - 1)]


def rank_tfidf_from_doc_terms(
    doc_term_tfs: Sequence[Tuple[str, str, int]],
) -> List[dict]:
    """
    由 (image_path, keyword, tf) 列表計算語料級 TF-IDF 排名（小資料測試用）。

    doc_term_tfs: 每列 (document_id, keyword, term_frequency_in_doc)
    """
    if not doc_term_tfs:
        return []

    docs: set[str] = set()
    term_total_tf: dict[str, int] = {}
    term_docs: dict[str, set[str]] = {}

    for doc_id, keyword, tf in doc_term_tfs:
        kw = str(keyword).strip().lower()
        if not kw or tf <= 0:
            continue
        docs.add(doc_id)
        term_total_tf[kw] = term_total_tf.get(kw, 0) + int(tf)
        term_docs.setdefault(kw, set()).add(doc_id)

    n = len(docs)
    rows: List[dict] = []
    for kw, total_tf in term_total_tf.items():
        df = len(term_docs.get(kw, set()))
        idf = compute_idf(df, n)
        rows.append(
            {
                "keyword": kw,
                "total_tf": total_tf,
                "doc_frequency": df,
                "corpus_doc_count": n,
                "idf": idf,
                "tfidf_score": total_tf * idf,
            }
        )
    rows.sort(key=lambda r: (-r["tfidf_score"], -r["total_tf"], r["keyword"]))
    return rows


def rank_pmi_from_bigrams(
    bigram_counts: Sequence[Tuple[str, str, int]],
    *,
    min_bigram_count: int = 2,
) -> List[dict]:
    """由 (word1, word2, count) 計算 PMI 排名（小資料測試用）。"""
    if not bigram_counts:
        return []

    pair_count: dict[Tuple[str, str], int] = {}
    w1_count: dict[str, int] = {}
    w2_count: dict[str, int] = {}
    total = 0

    for w1, w2, cnt in bigram_counts:
        a = str(w1).strip().lower()
        b = str(w2).strip().lower()
        c = int(cnt)
        if not a or not b or c <= 0:
            continue
        key = (a, b)
        pair_count[key] = pair_count.get(key, 0) + c
        w1_count[a] = w1_count.get(a, 0) + c
        w2_count[b] = w2_count.get(b, 0) + c
        total += c

    rows: List[dict] = []
    for (a, b), c_xy in pair_count.items():
        if c_xy < min_bigram_count:
            continue
        pmi = compute_pmi(c_xy, w1_count[a], w2_count[b], total)
        rows.append(
            {
                "word1": a,
                "word2": b,
                "phrase": f"{a} {b}",
                "bigram_count": c_xy,
                "pmi_score": pmi,
                "total_bigrams": total,
            }
        )
    rows.sort(key=lambda r: (-r["pmi_score"], -r["bigram_count"], r["phrase"]))
    return rows
