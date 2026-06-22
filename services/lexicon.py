"""
Gold 分析辭典：版本化停用詞、痛點保護詞（allow）、effective_stop = stop − protected。

Silver 不套用動態停用詞；本模組僅在 Gold 讀取銀層 tokens 後使用。
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Set

from pyspark.sql import SparkSession

from config import STOPWORDS_DATASET_PATTERN, STOPWORDS_LEXICON_VERSION, STOPWORDS_PATH
from services.domain_lexicons import get_builtin_domain_stopwords, merge_stopword_lists
from services.pain_topic_rules import PAIN_TOPIC_POLARITY_RULES, PAIN_TOPIC_RULES

_logger = logging.getLogger(__name__)
_REPO_ROOT = Path(__file__).resolve().parent.parent


def parse_stopwords_lines(lines: Iterable[str]) -> List[str]:
    """每行一詞；空白行與 # 開頭行略過；行內 # 之後視為註解。"""
    out: List[str] = []
    seen: set[str] = set()
    for raw in lines:
        if raw is None:
            continue
        line = str(raw).strip()
        if not line or line.startswith("#"):
            continue
        if "#" in line:
            line = line.split("#", 1)[0].strip()
        if not line:
            continue
        w = line.lower()
        if w not in seen:
            seen.add(w)
            out.append(w)
    return out


def expand_pain_protected_terms() -> frozenset[str]:
    """由痛點規則展開保護詞（不可被 effective_stop 移除）。"""
    terms: set[str] = set()
    for hints in PAIN_TOPIC_RULES.values():
        for hint in hints:
            h = str(hint).strip().lower()
            if h:
                terms.add(h)
    for cfg in PAIN_TOPIC_POLARITY_RULES.values():
        for key in ("anchors", "negatives"):
            for word in cfg.get(key, []):
                w = str(word).strip().lower()
                if w:
                    terms.add(w)
    return frozenset(terms)


def resolve_local_stopwords_lexicon_path(
    dataset_id: str | None,
    *,
    lexicon_version: str | None = None,
) -> str | None:
    """本機辭典：dic/stop_words/{version}/{dataset}.txt，退回 dic/stop_words/{dataset}.txt。"""
    if not dataset_id:
        return None
    key = str(dataset_id).strip().lower()
    version = str(lexicon_version or STOPWORDS_LEXICON_VERSION or "").strip()
    if version:
        versioned = _REPO_ROOT / "dic" / "stop_words" / version / f"{key}.txt"
        if versioned.is_file():
            return str(versioned)
    fallback = _REPO_ROOT / "dic" / "stop_words" / f"{key}.txt"
    return str(fallback) if fallback.is_file() else None


def _hadoop_path_exists(spark: SparkSession, path: str) -> bool:
    path = str(path or "").strip()
    if not path:
        return False
    try:
        jvm = spark._jvm
        hconf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(path), hconf)
        return bool(fs.exists(jvm.org.apache.hadoop.fs.Path(path)))
    except Exception:
        return False


def resolve_stopwords_lexicon_path(
    spark: SparkSession,
    dataset_id: str | None,
    *,
    lexicon_version: str | None = None,
) -> str | None:
    """遠端或本機停用詞路徑（含版本模板）。"""
    ds = _normalize_dataset_id(dataset_id)
    version = str(lexicon_version or STOPWORDS_LEXICON_VERSION or "").strip()
    pattern = str(STOPWORDS_DATASET_PATTERN or "").strip()
    fallback = str(STOPWORDS_PATH or "").strip()

    dataset_candidate = ""
    if ds and pattern:
        try:
            dataset_candidate = pattern.format(dataset_id=ds, version=version).strip()
        except Exception as e:
            _logger.warning("invalid_stopwords_dataset_pattern: %s", e)
            dataset_candidate = ""

    if dataset_candidate and _hadoop_path_exists(spark, dataset_candidate):
        return dataset_candidate

    local = resolve_local_stopwords_lexicon_path(ds, lexicon_version=version)
    if local:
        return local

    if fallback and _hadoop_path_exists(spark, fallback):
        return fallback

    return None


def _normalize_dataset_id(dataset_id: str | None) -> str | None:
    if dataset_id is None:
        return None
    raw = str(dataset_id).strip().lower()
    if not raw:
        return None
    safe = re.sub(r"[^a-z0-9_-]", "", raw)
    return safe or None


def load_stopwords_from_path(spark: SparkSession, path: str) -> List[str]:
    try:
        rows = spark.read.text(str(path).strip()).select("value").collect()
    except Exception as e:
        _logger.warning("stopwords_read_failed: path=%s error=%s", path, e)
        return []
    lines = [r[0] for r in rows if r[0] is not None]
    return parse_stopwords_lines(lines)


def build_effective_stopwords(
    stopwords: Iterable[str],
    protected: Iterable[str] | None = None,
) -> List[str]:
    """effective_stop = stop − protected（保護痛點詞不被過濾）。"""
    prot = {str(w).strip().lower() for w in (protected or []) if str(w).strip()}
    out: List[str] = []
    seen: set[str] = set()
    for raw in stopwords:
        w = str(raw).strip().lower()
        if not w or w in prot or w in seen:
            continue
        seen.add(w)
        out.append(w)
    return out


def filter_tokens_for_analytics(
    tokens: Sequence[str] | None,
    effective_stopwords: Set[str] | frozenset[str],
) -> List[str]:
    """Gold 分析用：移除 effective_stop 中的詞，保留順序與去重。"""
    if not tokens:
        return []
    stop = set(effective_stopwords)
    out: List[str] = []
    seen: set[str] = set()
    for raw in tokens:
        w = str(raw).strip().lower()
        if not w or w in seen or w in stop:
            continue
        out.append(w)
        seen.add(w)
    return out


def collect_gold_lexicon(
    spark: SparkSession,
    dataset_id: str | None,
    *,
    lexicon_version: str | None = None,
) -> Dict[str, Any]:
    """載入 Gold 辭典 bundle（停用詞、保護詞、effective_stop）。"""
    ds = _normalize_dataset_id(dataset_id)
    version = str(lexicon_version or STOPWORDS_LEXICON_VERSION or "v1.0.0").strip()
    path = resolve_stopwords_lexicon_path(spark, ds, lexicon_version=version)
    from_file = load_stopwords_from_path(spark, path) if path else []
    builtin = get_builtin_domain_stopwords(ds)
    merged_stop = merge_stopword_lists(from_file, builtin)
    protected = expand_pain_protected_terms()
    effective = build_effective_stopwords(merged_stop, protected)
    return {
        "dataset_id": ds,
        "lexicon_version": version,
        "stopwords_path": path or "",
        "stopwords_from_file_count": len(from_file),
        "domain_stopwords_count": len(builtin),
        "stopwords_merged_count": len(merged_stop),
        "protected_terms_count": len(protected),
        "effective_stopwords_count": len(effective),
        "effective_stopwords": effective,
        "protected_terms": sorted(protected),
    }
