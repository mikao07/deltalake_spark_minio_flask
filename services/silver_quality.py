"""
銀層資料品質：三道防線（Schema、Token 分佈、下游對齊）。

- Hard fail：阻擋 ETL 成功回報（主鍵、雜訊率、Top-N denylist）
- Soft warn：寫入報告與 log，不阻擋（空 tokens、超長詞、留存率）
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Sequence

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, expr, length, lit, size, trim

from config import (
    SILVER_QUALITY_ENABLED,
    SILVER_QUALITY_FAIL_ON_HARD,
    SILVER_QUALITY_MAX_EMPTY_CLEANED_RATIO,
    SILVER_QUALITY_MAX_LEN1_TOKEN_RATIO,
    SILVER_QUALITY_MAX_LONG_TOKEN_RATIO,
    SILVER_QUALITY_MAX_NOISE_ROW_RATIO,
    SILVER_QUALITY_MIN_CHAR_RETENTION_RATIO,
    SILVER_QUALITY_MIN_NONEMPTY_TOKENS_RATIO,
    SILVER_QUALITY_TOP_N,
    SILVER_TOP_TOKEN_DENYLIST,
    SILVER_TRANSFORM_VERSION,
)
from services.text_tokens import strip_pure_digit_tokens

_logger = logging.getLogger(__name__)

# cleaned_text 雜訊：連續 4+ 同數字、????、html 標籤殘留
_NOISE_CLEANED_TEXT_RE = re.compile(
    r"(?:\d{4,}|\?{3,}|<\s*html|&[a-z]+;)",
    flags=re.IGNORECASE,
)

_TOP_DENYLIST_DEFAULT = frozenset(
    w.lower()
    for w in (
        "2222",
        "????",
        "<html",
        "html",
        "png",
        "img",
        "ocr_error",
        # TF-IDF 探索 Top 不應出現的贅詞／場景詞
        "自己",
        "不用",
        "知道",
        "結果",
        "店員",
        "店员",
        "飲料",
        "饮料",
        "珍珠",
        "奶茶",
        "只是",
        "另外",
        "到底",
        "兩次",
    )
)


class SilverQualityError(RuntimeError):
    """銀層品質 hard fail；report 為完整品質報告 dict（供 API／UI）。"""

    def __init__(self, message: str, *, report: Dict[str, Any] | None = None):
        super().__init__(message)
        self.report = dict(report or {})


@dataclass
class QualityCheck:
    name: str
    severity: str  # hard | warn
    passed: bool
    message: str
    value: Any = None
    threshold: Any = None


@dataclass
class SilverQualityReport:
    passed: bool
    transform_version: str
    total_rows: int
    checks: List[QualityCheck] = field(default_factory=list)
    hard_failures: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "transform_version": self.transform_version,
            "total_rows": self.total_rows,
            "hard_failures": list(self.hard_failures),
            "warnings": list(self.warnings),
            "metrics": dict(self.metrics),
            "checks": [
                {
                    "name": c.name,
                    "severity": c.severity,
                    "passed": c.passed,
                    "message": c.message,
                    "value": c.value,
                    "threshold": c.threshold,
                }
                for c in self.checks
            ],
        }


def cleaned_text_has_noise(text: str | None) -> bool:
    if not text:
        return False
    return bool(_NOISE_CLEANED_TEXT_RE.search(str(text)))


def compute_row_metrics(
    *,
    image_path: str | None,
    extracted_text: str | None,
    cleaned_text: str | None,
    tokens: Sequence[str] | None,
) -> Dict[str, Any]:
    """單列指標（供單元測試與抽樣）。"""
    ext = str(extracted_text or "")
    cleaned = str(cleaned_text or "")
    tok_list = [str(t).strip().lower() for t in (tokens or []) if str(t).strip()]
    char_retention = (len(cleaned) / len(ext)) if ext else 1.0
    token_char_coverage = (
        sum(len(t) for t in tok_list) / len(cleaned) if cleaned and tok_list else 0.0
    )
    len1 = sum(1 for t in tok_list if len(t) == 1)
    long_t = sum(1 for t in tok_list if len(t) > 10)
    return {
        "image_path": image_path,
        "extracted_len": len(ext),
        "cleaned_len": len(cleaned),
        "token_count": len(tok_list),
        "char_retention": char_retention,
        "token_char_coverage": token_char_coverage,
        "len1_token_count": len1,
        "long_token_count": long_t,
        "has_noise": cleaned_text_has_noise(cleaned),
        "cleaned_has_pure_digit_token": any(
            p.isdigit() for p in cleaned.split() if p
        ),
    }


def _top_token_denylist() -> frozenset[str]:
    extra = {str(w).strip().lower() for w in SILVER_TOP_TOKEN_DENYLIST if str(w).strip()}
    return _TOP_DENYLIST_DEFAULT | extra


def evaluate_silver_quality_metrics(metrics: Dict[str, Any]) -> SilverQualityReport:
    """依聚合指標評估三道防線。"""
    total = int(metrics.get("total_rows") or 0)
    checks: List[QualityCheck] = []
    hard_failures: List[str] = []
    warnings: List[str] = []

    def add_check(name: str, severity: str, passed: bool, message: str, value=None, threshold=None):
        checks.append(
            QualityCheck(
                name=name,
                severity=severity,
                passed=passed,
                message=message,
                value=value,
                threshold=threshold,
            )
        )
        if not passed:
            (hard_failures if severity == "hard" else warnings).append(message)

    if total <= 0:
        add_check("non_empty_corpus", "warn", False, "銀層檢查資料為 0 列", value=0)
        return SilverQualityReport(
            passed=True,
            transform_version=SILVER_TRANSFORM_VERSION,
            total_rows=0,
            checks=checks,
            warnings=warnings,
            metrics=metrics,
        )

    # --- 第一道：Schema ---
    dup = int(metrics.get("duplicate_image_path_rows") or 0)
    add_check(
        "image_path_unique",
        "hard",
        dup == 0,
        f"image_path 重複列數應為 0（目前 {dup}）",
        value=dup,
        threshold=0,
    )
    for field_name, key in (
        ("image_path", "null_image_path_rows"),
        ("extracted_text", "null_extracted_text_rows"),
        ("cleaned_text", "null_cleaned_text_rows"),
    ):
        n = int(metrics.get(key) or 0)
        add_check(
            f"{field_name}_not_null",
            "hard",
            n == 0,
            f"{field_name} 為 null 的列數應為 0（目前 {n}）",
            value=n,
            threshold=0,
        )

    noise_rows = int(metrics.get("noise_cleaned_text_rows") or 0)
    noise_ratio = noise_rows / total
    add_check(
        "noise_row_ratio",
        "hard",
        noise_ratio <= SILVER_QUALITY_MAX_NOISE_ROW_RATIO,
        f"cleaned_text 雜訊列占比 {noise_ratio:.4%}（門檻 {SILVER_QUALITY_MAX_NOISE_ROW_RATIO:.4%}）",
        value=noise_ratio,
        threshold=SILVER_QUALITY_MAX_NOISE_ROW_RATIO,
    )

    digit_noise_rows = int(metrics.get("pure_digit_token_in_cleaned_rows") or 0)
    add_check(
        "no_pure_digit_tokens_in_cleaned",
        "hard",
        digit_noise_rows == 0,
        f"cleaned_text 仍含純數字 token 的列數應為 0（目前 {digit_noise_rows}）",
        value=digit_noise_rows,
        threshold=0,
    )

    # --- 第二道：Token 分佈 ---
    total_tokens = int(metrics.get("total_token_instances") or 0)
    len1_ratio = (int(metrics.get("len1_token_instances") or 0) / total_tokens) if total_tokens else 0.0
    add_check(
        "len1_token_ratio",
        "warn",
        len1_ratio <= SILVER_QUALITY_MAX_LEN1_TOKEN_RATIO,
        f"長度為 1 的 token 占比 {len1_ratio:.2%}（門檻 {SILVER_QUALITY_MAX_LEN1_TOKEN_RATIO:.2%}）",
        value=len1_ratio,
        threshold=SILVER_QUALITY_MAX_LEN1_TOKEN_RATIO,
    )

    long_ratio = (int(metrics.get("long_token_instances") or 0) / total_tokens) if total_tokens else 0.0
    add_check(
        "long_token_ratio",
        "warn",
        long_ratio <= SILVER_QUALITY_MAX_LONG_TOKEN_RATIO,
        f"長度 > 10 的 token 占比 {long_ratio:.2%}（門檻 {SILVER_QUALITY_MAX_LONG_TOKEN_RATIO:.2%}）",
        value=long_ratio,
        threshold=SILVER_QUALITY_MAX_LONG_TOKEN_RATIO,
    )

    denylist = _top_token_denylist()
    top_tokens: List[str] = list(metrics.get("top_tokens") or [])
    hits = [t for t in top_tokens[: SILVER_QUALITY_TOP_N] if t in denylist]
    add_check(
        "top_token_denylist",
        "hard",
        not hits,
        f"Top-{SILVER_QUALITY_TOP_N} 含禁止詞：{hits}" if hits else "Top-N 未命中禁止詞",
        value=hits,
    )

    # --- 第三道：留存（銀層段）---
    empty_cleaned_ratio = float(metrics.get("empty_cleaned_text_ratio") or 0.0)
    add_check(
        "empty_cleaned_ratio",
        "warn",
        empty_cleaned_ratio <= SILVER_QUALITY_MAX_EMPTY_CLEANED_RATIO,
        f"空 cleaned_text 占比 {empty_cleaned_ratio:.2%}",
        value=empty_cleaned_ratio,
        threshold=SILVER_QUALITY_MAX_EMPTY_CLEANED_RATIO,
    )

    nonempty_tokens_ratio = float(metrics.get("nonempty_tokens_ratio") or 0.0)
    add_check(
        "nonempty_tokens_ratio",
        "warn",
        nonempty_tokens_ratio >= SILVER_QUALITY_MIN_NONEMPTY_TOKENS_RATIO,
        f"tokens 非空列占比 {nonempty_tokens_ratio:.2%}（門檻 {SILVER_QUALITY_MIN_NONEMPTY_TOKENS_RATIO:.2%}）",
        value=nonempty_tokens_ratio,
        threshold=SILVER_QUALITY_MIN_NONEMPTY_TOKENS_RATIO,
    )

    avg_char_retention = float(metrics.get("avg_char_retention") or 0.0)
    add_check(
        "avg_char_retention",
        "warn",
        avg_char_retention >= SILVER_QUALITY_MIN_CHAR_RETENTION_RATIO,
        f"平均字元留存率 len(cleaned)/len(extracted)={avg_char_retention:.2%}",
        value=avg_char_retention,
        threshold=SILVER_QUALITY_MIN_CHAR_RETENTION_RATIO,
    )

    passed = not hard_failures
    return SilverQualityReport(
        passed=passed,
        transform_version=SILVER_TRANSFORM_VERSION,
        total_rows=total,
        checks=checks,
        hard_failures=hard_failures,
        warnings=warnings,
        metrics=metrics,
    )


def collect_silver_quality_metrics_spark(df: DataFrame) -> Dict[str, Any]:
    """以 Spark 聚合銀層品質指標。"""
    if df.limit(1).count() == 0:
        return {"total_rows": 0}

    total_rows = int(df.count())
    metrics: Dict[str, Any] = {"total_rows": total_rows}

    metrics["null_image_path_rows"] = int(
        df.filter(col("image_path").isNull()).count()
    )
    metrics["null_extracted_text_rows"] = int(
        df.filter(col("extracted_text").isNull()).count()
    )
    if "cleaned_text" in df.columns:
        metrics["null_cleaned_text_rows"] = int(
            df.filter(col("cleaned_text").isNull()).count()
        )
        metrics["empty_cleaned_text_rows"] = int(
            df.filter(length(trim(col("cleaned_text"))) == 0).count()
        )
        metrics["empty_cleaned_text_ratio"] = metrics["empty_cleaned_text_rows"] / total_rows
    else:
        metrics["null_cleaned_text_rows"] = total_rows
        metrics["empty_cleaned_text_ratio"] = 1.0

    dup_df = (
        df.groupBy("image_path")
        .count()
        .filter(col("count") > 1)
    )
    metrics["duplicate_image_path_rows"] = int(dup_df.agg(expr("sum(count)").alias("s")).collect()[0]["s"] or 0)

    if "cleaned_text" in df.columns:
        noise_expr = r"(?i)(\d{4,}|\?{3,}|<\s*html|&[a-z]+;)"
        metrics["noise_cleaned_text_rows"] = int(
            df.filter(col("cleaned_text").rlike(noise_expr)).count()
        )

    if "tokens" in df.columns:
        metrics["empty_tokens_rows"] = int(
            df.filter(col("tokens").isNull() | (size(col("tokens")) == 0)).count()
        )
        metrics["nonempty_tokens_ratio"] = 1.0 - (metrics["empty_tokens_rows"] / total_rows)

        tok = df.filter(col("tokens").isNotNull() & (size(col("tokens")) > 0)).select(
            explode(col("tokens")).alias("token")
        )
        if tok.limit(1).count() > 0:
            metrics["total_token_instances"] = int(tok.count())
            metrics["len1_token_instances"] = int(
                tok.filter(length(col("token")) == 1).count()
            )
            metrics["long_token_instances"] = int(
                tok.filter(length(col("token")) > 10).count()
            )
            top_rows = (
                tok.groupBy("token")
                .count()
                .orderBy(col("count").desc(), col("token"))
                .limit(max(SILVER_QUALITY_TOP_N, 50))
                .collect()
            )
            metrics["top_tokens"] = [str(r["token"]).lower() for r in top_rows]
    else:
        metrics["nonempty_tokens_ratio"] = 0.0
        metrics["top_tokens"] = []

    if "extracted_text" in df.columns and "cleaned_text" in df.columns:
        retention_row = df.select(
            expr(
                "avg(case when length(trim(extracted_text)) > 0 "
                "then length(trim(cleaned_text)) / length(trim(extracted_text)) else 1.0 end)"
            ).alias("avg_ret")
        ).collect()
        metrics["avg_char_retention"] = float(retention_row[0]["avg_ret"] or 0.0)

    if "cleaned_text" in df.columns:
        # 抽樣檢查純數字 token（Spark rlike 對空白分隔數字）
        digit_rows = 0
        sample = df.select("cleaned_text").limit(5000).collect()
        for row in sample:
            cleaned = str(row["cleaned_text"] or "")
            if any(p.isdigit() for p in cleaned.split() if p):
                digit_rows += 1
        # 外推比例僅供告警；hard 用抽樣全中則 fail
        metrics["pure_digit_token_in_cleaned_rows"] = digit_rows if sample else 0
        if sample and digit_rows > 0:
            # 若抽樣內有任何列含純數字 token → hard（與 strip_pure_digit_tokens 合約不符）
            metrics["pure_digit_token_in_cleaned_rows"] = int(
                sum(
                    1
                    for row in sample
                    if any(p.isdigit() for p in str(row["cleaned_text"] or "").split() if p)
                )
            )

    return metrics


def run_silver_quality_gate(
    df: DataFrame,
    *,
    fail_on_hard: bool | None = None,
) -> Dict[str, Any]:
    """執行銀層品質閘門；回傳報告 dict，hard fail 時拋 SilverQualityError。"""
    if not SILVER_QUALITY_ENABLED:
        return {"passed": True, "skipped": True, "reason": "SILVER_QUALITY_ENABLED=false"}

    metrics = collect_silver_quality_metrics_spark(df)
    report = evaluate_silver_quality_metrics(metrics)
    payload = report.to_dict()

    if report.warnings:
        _logger.warning("silver_quality_warnings: %s", report.warnings)
    if report.hard_failures:
        _logger.error("silver_quality_hard_failures: %s", report.hard_failures)

    should_fail = SILVER_QUALITY_FAIL_ON_HARD if fail_on_hard is None else bool(fail_on_hard)
    if should_fail and not report.passed:
        raise SilverQualityError("; ".join(report.hard_failures), report=payload)

    return payload


def evaluate_gold_downstream_quality(
    *,
    corpus_doc_count: int,
    tfidf_output_rows: int,
    topic_output_rows: int,
    tfidf_top: Sequence[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    """第三道防線（金層段）：下游 TF-IDF / 痛點是否可分析。"""
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

    denylist = _top_token_denylist()
    top_kw = [str(r.get("keyword", "")).lower() for r in (tfidf_top or [])[:10]]
    junk_hits = [k for k in top_kw if k in denylist]
    checks.append(
        QualityCheck(
            "tfidf_top_not_junk",
            "warn",
            not junk_hits,
            f"TF-IDF Top 含雜訊詞：{junk_hits}" if junk_hits else "TF-IDF Top 未含禁止詞",
            value=junk_hits,
        )
    )
    if junk_hits:
        warnings.append(f"TF-IDF Top 命中禁止詞：{junk_hits}")

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
