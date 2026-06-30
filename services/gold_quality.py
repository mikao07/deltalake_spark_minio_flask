"""金層下游品質：探索 TF-IDF Top 與探索停用詞一致性（warn，不擋 ETL）。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Sequence


@dataclass
class QualityCheck:
    name: str
    severity: str  # hard | warn
    passed: bool
    message: str
    value: Any = None


def evaluate_gold_downstream_quality(
    *,
    corpus_doc_count: int,
    tfidf_output_rows: int,
    topic_output_rows: int,
    tfidf_top: Sequence[Dict[str, Any]] | None = None,
    tfidf_exploration_stopwords: Sequence[str] | None = None,
    tfidf_top_check_limit: int = 10,
) -> Dict[str, Any]:
    """
    金層 ETL 後檢查：TF-IDF 探索 Top 不應仍含已列入探索停用詞的 keyword（代表濾網漏網）。
    """
    checks: List[QualityCheck] = []
    warnings: List[str] = []
    hard_failures: List[str] = []

    if corpus_doc_count <= 0:
        return {"passed": True, "skipped": True, "reason": "empty_corpus"}

    tfidf_ok = tfidf_output_rows > 0
    checks.append(
        QualityCheck(
            "tfidf_nonempty",
            "warn",
            tfidf_ok,
            f"TF-IDF 輸出列數 {tfidf_output_rows}",
            value=tfidf_output_rows,
        )
    )
    if not tfidf_ok:
        warnings.append(f"TF-IDF 輸出為 0（語料 {corpus_doc_count} 筆）")

    stop_set = {
        str(w).strip().lower()
        for w in (tfidf_exploration_stopwords or [])
        if str(w).strip()
    }
    lim = max(1, int(tfidf_top_check_limit))
    top_kw = [str(r.get("keyword", "")).strip().lower() for r in (tfidf_top or [])[:lim]]
    top_kw = [k for k in top_kw if k]
    leak_hits = [k for k in top_kw if k in stop_set] if stop_set else []
    checks.append(
        QualityCheck(
            "tfidf_top_exploration_stopword_leak",
            "warn",
            not leak_hits,
            (
                f"TF-IDF Top-{lim} 仍含探索停用詞（濾網可能漏網）：{leak_hits}"
                if leak_hits
                else f"TF-IDF Top-{lim} 未含探索停用詞"
            ),
            value=leak_hits,
        )
    )
    if leak_hits:
        warnings.append(f"TF-IDF Top 仍命中探索停用詞：{leak_hits}")

    if corpus_doc_count >= 5 and topic_output_rows == 0:
        warnings.append("語料 ≥5 筆但痛點主題輸出為 0，請檢查 lexicon 是否過濾過頭")

    passed = not hard_failures
    return {
        "passed": passed,
        "warnings": warnings,
        "hard_failures": hard_failures,
        "checks": [
            {
                "name": c.name,
                "severity": c.severity,
                "passed": c.passed,
                "message": c.message,
                "value": c.value,
            }
            for c in checks
        ],
    }
