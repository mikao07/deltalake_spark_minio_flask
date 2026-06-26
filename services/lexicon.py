"""
Gold 分析辭典：版本化停用詞、痛點保護詞（allow）、effective_stop = stop − protected。

Silver 不套用動態停用詞；本模組僅在 Gold 讀取銀層 tokens 後使用。
"""

from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Set

from pyspark.sql import SparkSession

from config import (
    STOPWORDS_DATASET_PATTERN,
    STOPWORDS_EXPLORATION_LEXICON_VERSION,
    STOPWORDS_LEXICON_VERSION,
    STOPWORDS_PATH,
)
from services.domain_lexicons import (
    get_builtin_domain_stopwords,
    get_builtin_tfidf_exploration_stopwords,
    merge_stopword_lists,
)
from services.pain_topic_rules import PAIN_TOPIC_POLARITY_RULES, PAIN_TOPIC_RULES

_logger = logging.getLogger(__name__)
_REPO_ROOT = Path(__file__).resolve().parent.parent


def compute_lexicon_content_hash(merged_stop: Iterable[str]) -> str:
    """合併後停用詞列表的穩定 SHA256（小寫、排序、去重）。"""
    words = sorted({str(w).strip().lower() for w in merged_stop if str(w).strip()})
    payload = "\n".join(words).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


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


def build_tfidf_exploration_stopwords(
    merged_stop: Iterable[str],
    dataset_id: str | None = None,
) -> List[str]:
    """
    TF-IDF Phase A 探索用停用詞：完整 domain stop + 虛詞／場景詞。
    不扣痛點保護詞（與漏斗 analytics_tokens 的 effective_stop 分離）。
    """
    extra = get_builtin_tfidf_exploration_stopwords(dataset_id)
    return merge_stopword_lists(merged_stop, extra)


def filter_tokens_for_tfidf_exploration(
    tokens: Sequence[str] | None,
    tfidf_stopwords: Set[str] | frozenset[str],
) -> List[str]:
    """TF-IDF 探索：移除完整停用詞表中的詞（語意同 filter_tokens_for_analytics）。"""
    return filter_tokens_for_analytics(tokens, tfidf_stopwords)


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
    lexicon_role: str = "release",
) -> Dict[str, Any]:
    """載入單一版本 Gold 辭典 bundle（停用詞、保護詞、effective_stop）。"""
    ds = _normalize_dataset_id(dataset_id)
    version = str(lexicon_version or STOPWORDS_LEXICON_VERSION or "v1.0.0").strip()
    path = resolve_stopwords_lexicon_path(spark, ds, lexicon_version=version)
    from_file = load_stopwords_from_path(spark, path) if path else []
    builtin = get_builtin_domain_stopwords(ds)
    merged_stop = merge_stopword_lists(from_file, builtin)
    protected = expand_pain_protected_terms()
    effective = build_effective_stopwords(merged_stop, protected)
    tfidf_stop = build_tfidf_exploration_stopwords(merged_stop, ds)
    return {
        "dataset_id": ds,
        "lexicon_role": lexicon_role,
        "lexicon_version": version,
        "stopwords_path": path or "",
        "stopwords_from_file_count": len(from_file),
        "domain_stopwords_count": len(builtin),
        "stopwords_merged_count": len(merged_stop),
        "lexicon_content_hash": compute_lexicon_content_hash(merged_stop),
        "protected_terms_count": len(protected),
        "effective_stopwords_count": len(effective),
        "effective_stopwords": effective,
        "tfidf_exploration_stopwords_count": len(tfidf_stop),
        "tfidf_exploration_stopwords": tfidf_stop,
        "protected_terms": sorted(protected),
    }


def collect_gold_dual_lexicon(
    spark: SparkSession,
    dataset_id: str | None,
) -> Dict[str, Any]:
    """
    黃金發行 + 探索測試雙 lexicon：
    - release（STOPWORDS_LEXICON_VERSION）→ analytics_tokens／痛點快照
    - exploration（STOPWORDS_EXPLORATION_LEXICON_VERSION）→ tfidf_exploration_tokens
    """
    release = collect_gold_lexicon(
        spark,
        dataset_id,
        lexicon_version=STOPWORDS_LEXICON_VERSION,
        lexicon_role="release",
    )
    exp_ver = str(STOPWORDS_EXPLORATION_LEXICON_VERSION or "dev").strip()
    exploration = collect_gold_lexicon(
        spark,
        dataset_id,
        lexicon_version=exp_ver,
        lexicon_role="exploration",
    )
    return {
        "dataset_id": release.get("dataset_id"),
        "release": release,
        "exploration": exploration,
        "release_lexicon_version": release.get("lexicon_version"),
        "release_lexicon_content_hash": release.get("lexicon_content_hash"),
        "release_stopwords_path": release.get("stopwords_path"),
        "exploration_lexicon_version": exploration.get("lexicon_version"),
        "exploration_lexicon_content_hash": exploration.get("lexicon_content_hash"),
        "exploration_stopwords_path": exploration.get("stopwords_path"),
        # 相容舊欄位（指向 release）
        "lexicon_version": release.get("lexicon_version"),
        "stopwords_path": release.get("stopwords_path"),
        "stopwords_merged_count": release.get("stopwords_merged_count"),
        "effective_stopwords_count": release.get("effective_stopwords_count"),
        "effective_stopwords": release.get("effective_stopwords"),
        "tfidf_exploration_stopwords": exploration.get("tfidf_exploration_stopwords"),
        "tfidf_exploration_stopwords_count": exploration.get("tfidf_exploration_stopwords_count"),
        "protected_terms_count": release.get("protected_terms_count"),
    }


def load_merged_stop_offline(
    dataset_id: str,
    *,
    lexicon_version: str | None = None,
) -> List[str]:
    """無 Spark：本機 dic + 內建詞。"""
    path = resolve_local_stopwords_lexicon_path(
        dataset_id,
        lexicon_version=lexicon_version or STOPWORDS_LEXICON_VERSION,
    )
    from_file: List[str] = []
    if path:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
        from_file = parse_stopwords_lines(lines)
    return merge_stopword_lists(from_file, get_builtin_domain_stopwords(dataset_id))
