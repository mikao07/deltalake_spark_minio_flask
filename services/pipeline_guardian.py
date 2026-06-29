"""
管線守護神：比對 manifest 與 Delta／執行中設定，偵測版本漂移與「改詞忘 bump」風險。

設計原則
- 銅／銀：單一欄位，不符即 FAIL（可硬擋下游 ETL）。
- 金層：規則版本 + 辭典 content hash；hash 變了但版本號未在 manifest 登記 → FAIL（防人為疏忽）。
- 計算成本：辭典 hash 為 O(n log n) 排序 + 一次 SHA256；Delta 僅 DISTINCT／LIMIT，drinks 規模可忽略。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

from config import (
    BRONZE_TABLE_PATH,
    GOLD_TOPIC_SNAPSHOT_PATH,
    SILVER_OCR_TABLE_PATH,
    SILVER_TRANSFORM_VERSION,
    STOPWORDS_EXPLORATION_LEXICON_VERSION,
    STOPWORDS_LEXICON_VERSION,
)
from services.domain_lexicons import get_builtin_domain_stopwords, merge_stopword_lists
from services.lexicon import (
    collect_gold_dual_lexicon,
    compute_lexicon_content_hash,
    load_merged_stop_offline,
    parse_stopwords_lines,
    resolve_local_stopwords_lexicon_path,
)
from services.pain_topic_rules import TOPIC_RULE_VERSION

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_MANIFEST_DIR = _REPO_ROOT / "manifests"


class AuditLevel(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


@dataclass
class AuditFinding:
    check_id: str
    level: AuditLevel
    message: str
    detail: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AuditReport:
    dataset_id: str
    findings: List[AuditFinding] = field(default_factory=list)

    @property
    def has_fail(self) -> bool:
        return any(f.level == AuditLevel.FAIL for f in self.findings)

    @property
    def has_warn(self) -> bool:
        return any(f.level == AuditLevel.WARN for f in self.findings)

    def exit_code(self, *, strict: bool = False) -> int:
        if self.has_fail:
            return 1
        if strict and self.has_warn:
            return 1
        if self.has_warn:
            return 2
        return 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "summary": {
                "pass": sum(1 for f in self.findings if f.level == AuditLevel.PASS),
                "warn": sum(1 for f in self.findings if f.level == AuditLevel.WARN),
                "fail": sum(1 for f in self.findings if f.level == AuditLevel.FAIL),
            },
            "findings": [
                {
                    "check_id": f.check_id,
                    "level": f.level.value,
                    "message": f.message,
                    "detail": f.detail,
                }
                for f in self.findings
            ],
        }


def compute_lexicon_content_hash(merged_stop: Iterable[str]) -> str:
    """向後相容；實作於 services.lexicon。"""
    from services.lexicon import compute_lexicon_content_hash as _hash

    return _hash(merged_stop)


def compute_merged_stop_offline(
    dataset_id: str,
    *,
    lexicon_version: str | None = None,
) -> List[str]:
    return load_merged_stop_offline(dataset_id, lexicon_version=lexicon_version)


def load_manifest(path: Path) -> Dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    if path.suffix.lower() in (".yaml", ".yml"):
        try:
            import yaml  # type: ignore
        except ImportError as e:
            raise ImportError(
                "讀取 .yaml manifest 需要 PyYAML；請改用 .json 或 pip install pyyaml"
            ) from e
        data = yaml.safe_load(raw)
    else:
        data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError(f"manifest 格式錯誤: {path}")
    return data


def save_manifest(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def resolve_manifest_path(dataset_id: str, manifest_path: Optional[Path] = None) -> Path:
    if manifest_path is not None:
        return manifest_path
    safe = str(dataset_id).strip().lower()
    return _DEFAULT_MANIFEST_DIR / f"{safe}.json"


def _normalize_dataset_id(dataset_id: str) -> str:
    return str(dataset_id).strip().lower()


def audit_lexicon_bump_risk(
    *,
    dataset_id: str,
    manifest: Dict[str, Any],
    current_lexicon_version: str,
    current_content_hash: str,
    stopwords_path: str = "",
) -> List[AuditFinding]:
    """偵測「改了辭典卻忘記更新 manifest／bump」。"""
    findings: List[AuditFinding] = []
    gold = manifest.get("gold") or {}
    expected_version = str(
        gold.get("release_lexicon_version") or gold.get("lexicon_version") or ""
    ).strip()
    expected_hash = str(gold.get("lexicon_content_hash") or "").strip()

    if not expected_hash:
        findings.append(
            AuditFinding(
                check_id="gold.lexicon_hash_registered",
                level=AuditLevel.WARN,
                message="manifest 未登記 lexicon_content_hash；無法偵測改詞忘 bump",
                detail={"hint": "執行 pipeline_guardian.py --print-hashes 後寫入 manifest"},
            )
        )
        return findings

    hash_match = current_content_hash == expected_hash
    version_match = current_lexicon_version == expected_version

    if hash_match and version_match:
        findings.append(
            AuditFinding(
                check_id="gold.lexicon_aligned",
                level=AuditLevel.PASS,
                message="黃金發行停用詞版本與 content hash 與 manifest 一致",
                detail={
                    "lexicon_version": current_lexicon_version,
                    "lexicon_content_hash": current_content_hash[:16] + "...",
                    "stopwords_path": stopwords_path,
                },
            )
        )
        return findings

    if not hash_match and version_match:
        findings.append(
            AuditFinding(
                check_id="gold.lexicon_silent_drift",
                level=AuditLevel.FAIL,
                message=(
                    "黃金發行停用詞內容已變，但 release_lexicon_version 仍與 manifest 相同——"
                    "可能改了 v1.0.0 詞表卻未發版；請只改 dev/ 或 bump 發行版"
                ),
                detail={
                    "expected_hash": expected_hash[:16] + "...",
                    "current_hash": current_content_hash[:16] + "...",
                    "lexicon_version": current_lexicon_version,
                    "stopwords_path": stopwords_path,
                    "remediation": "更新 manifest lexicon_content_hash，或 bump STOPWORDS_LEXICON_VERSION 後重跑 Gold",
                },
            )
        )
        return findings

    if not hash_match and not version_match:
        findings.append(
            AuditFinding(
                check_id="gold.lexicon_manifest_stale",
                level=AuditLevel.WARN,
                message="辭典版本與 content hash 皆與 manifest 不同；請確認是否已 bump 但未更新 manifest",
                detail={
                    "expected_version": expected_version,
                    "current_version": current_lexicon_version,
                    "expected_hash": expected_hash[:16] + "...",
                    "current_hash": current_content_hash[:16] + "...",
                },
            )
        )
        return findings

  # hash_match and not version_match: version label changed, content identical
    findings.append(
        AuditFinding(
            check_id="gold.lexicon_version_only_change",
            level=AuditLevel.PASS,
            message="lexicon_version 已變更但 content hash 相同（路徑／標籤調整，詞表內容未變）",
            detail={
                "expected_version": expected_version,
                "current_version": current_lexicon_version,
            },
        )
    )
    return findings


def audit_exploration_lexicon_note(
    *,
    release_hash: str,
    exploration_hash: str,
    exploration_version: str,
) -> List[AuditFinding]:
    """探索版停用詞允許變動；僅提示與黃金版差異。"""
    if release_hash == exploration_hash:
        return [
            AuditFinding(
                check_id="gold.exploration_lexicon",
                level=AuditLevel.PASS,
                message=f"探索停用詞（{exploration_version}）與黃金發行內容相同",
                detail={"exploration_lexicon_version": exploration_version},
            )
        ]
    return [
        AuditFinding(
            check_id="gold.exploration_lexicon",
            level=AuditLevel.PASS,
            message=(
                f"探索停用詞（{exploration_version}）與黃金發行不同——"
                "屬預期（測試軌可改）；不影響已凍結 snapshot"
            ),
            detail={
                "exploration_lexicon_version": exploration_version,
                "release_hash_prefix": release_hash[:16] + "...",
                "exploration_hash_prefix": exploration_hash[:16] + "...",
            },
        )
    ]


def audit_runtime_config(manifest: Dict[str, Any]) -> List[AuditFinding]:
    findings: List[AuditFinding] = []
    runtime = manifest.get("runtime") or {}

    expected_silver = str(runtime.get("silver_transform_version") or "").strip()
    if expected_silver and expected_silver != SILVER_TRANSFORM_VERSION:
        findings.append(
            AuditFinding(
                check_id="runtime.silver_transform_version",
                level=AuditLevel.FAIL,
                message="執行中 SILVER_TRANSFORM_VERSION 與 manifest 不符",
                detail={
                    "expected": expected_silver,
                    "current": SILVER_TRANSFORM_VERSION,
                },
            )
        )
    elif expected_silver:
        findings.append(
            AuditFinding(
                check_id="runtime.silver_transform_version",
                level=AuditLevel.PASS,
                message="SILVER_TRANSFORM_VERSION 與 manifest 一致",
                detail={"version": SILVER_TRANSFORM_VERSION},
            )
        )

    expected_rule = str((manifest.get("gold") or {}).get("topic_rule_version") or "").strip()
    if expected_rule and expected_rule != TOPIC_RULE_VERSION:
        findings.append(
            AuditFinding(
                check_id="runtime.topic_rule_version",
                level=AuditLevel.WARN,
                message="執行中 TOPIC_RULE_VERSION 與 manifest 不符（需重跑 Gold）",
                detail={"expected": expected_rule, "current": TOPIC_RULE_VERSION},
            )
        )
    elif expected_rule:
        findings.append(
            AuditFinding(
                check_id="runtime.topic_rule_version",
                level=AuditLevel.PASS,
                message="TOPIC_RULE_VERSION 與 manifest 一致",
                detail={"version": TOPIC_RULE_VERSION},
            )
        )

    return findings


def _ocr_signature_matches_allowed(signature: str, allowed_entry: str) -> bool:
    """
    manifest 可用精簡鍵（例 psm=6|pre=v1.1|profile=dark_ui）；
    Bronze 實際為 build_ocr_signature 完整字串（含 tesseract|lang=...|scale=...）。
  允許：完全一致，或 allowed 每一段 key=value 皆出現在 signature 中。
    """
    sig = str(signature or "").strip()
    allowed = str(allowed_entry or "").strip()
    if not sig or not allowed:
        return False
    if sig == allowed:
        return True
    parts = [p.strip() for p in allowed.split("|") if p.strip()]
    return bool(parts) and all(part in sig for part in parts)


def _bronze_signatures_not_allowed(signatures: Set[str], allowed: List[str]) -> Set[str]:
    unexpected: Set[str] = set()
    for sig in signatures:
        if not any(_ocr_signature_matches_allowed(sig, entry) for entry in allowed):
            unexpected.add(sig)
    return unexpected


def audit_bronze_signatures(
    *,
    signatures: Set[str],
    manifest: Dict[str, Any],
    row_count: int,
) -> List[AuditFinding]:
    findings: List[AuditFinding] = []
    bronze = manifest.get("bronze") or {}
    allowed: List[str] = list(bronze.get("allowed_ocr_signatures") or [])

    if row_count <= 0:
        findings.append(
            AuditFinding(
                check_id="bronze.rows",
                level=AuditLevel.WARN,
                message="Bronze 表無資料或無法讀取",
                detail={"row_count": row_count},
            )
        )
        return findings

    if not allowed:
        findings.append(
            AuditFinding(
                check_id="bronze.ocr_signature",
                level=AuditLevel.WARN,
                message="manifest 未設定 allowed_ocr_signatures，略過銅層簽名檢查",
                detail={"distinct_signatures": sorted(signatures)},
            )
        )
        return findings

    unexpected = _bronze_signatures_not_allowed(signatures, allowed)
    if unexpected:
        findings.append(
            AuditFinding(
                check_id="bronze.ocr_signature",
                level=AuditLevel.FAIL,
                message="Bronze 出現未允許的 ocr_signature",
                detail={
                    "allowed": allowed,
                    "unexpected": sorted(unexpected),
                    "distinct": sorted(signatures),
                },
            )
        )
        return findings

    if len(signatures) > 1:
        findings.append(
            AuditFinding(
                check_id="bronze.ocr_signature",
                level=AuditLevel.WARN,
                message="Bronze 有多種 ocr_signature（封板後通常應只有一種）",
                detail={"distinct": sorted(signatures)},
            )
        )
        return findings

    findings.append(
        AuditFinding(
            check_id="bronze.ocr_signature",
            level=AuditLevel.PASS,
            message="Bronze ocr_signature 符合 manifest",
            detail={"signature": sorted(signatures)[0] if signatures else None, "row_count": row_count},
        )
    )
    return findings


def audit_silver_transform_versions(
    *,
    versions: Set[str],
    manifest: Dict[str, Any],
    row_count: int,
) -> List[AuditFinding]:
    findings: List[AuditFinding] = []
    runtime = manifest.get("runtime") or {}
    expected = str(runtime.get("silver_transform_version") or SILVER_TRANSFORM_VERSION).strip()

    if row_count <= 0:
        findings.append(
            AuditFinding(
                check_id="silver.rows",
                level=AuditLevel.WARN,
                message="Silver OCR 表無資料或無法讀取",
                detail={"row_count": row_count},
            )
        )
        return findings

    stale = versions - {expected}
    if stale or expected not in versions:
        findings.append(
            AuditFinding(
                check_id="silver.transform_version",
                level=AuditLevel.FAIL,
                message="Silver 表內 silver_transform_version 與預期不符（應重跑 Silver → Gold）",
                detail={
                    "expected": expected,
                    "distinct_in_table": sorted(versions),
                    "stale": sorted(stale),
                },
            )
        )
        return findings

    findings.append(
        AuditFinding(
            check_id="silver.transform_version",
            level=AuditLevel.PASS,
            message="Silver silver_transform_version 全表一致且符合 manifest",
            detail={"version": expected, "row_count": row_count},
        )
    )
    return findings


def collect_approved_snapshot_facts(
    spark,
    dataset_id: str,
    manifest: Dict[str, Any],
) -> Dict[str, Any]:
    """比對 manifest.approved_snapshot_at 與 Delta topic_snapshot。"""
    from services.spark_service import (
        GOLD_TOPIC_SNAPSHOT_PATH,
        _topic_snapshot_iso_from_cell,
        delta_table_exists,
        find_latest_topic_snapshot_at_for_release,
        read_delta_table,
        verify_topic_snapshot_at_for_release,
    )

    gold = manifest.get("gold") or {}
    approved_iso = str(gold.get("approved_snapshot_at") or "").strip()
    rel_ver = str(
        gold.get("release_lexicon_version") or gold.get("lexicon_version") or STOPWORDS_LEXICON_VERSION
    ).strip()
    lex_hash = str(gold.get("lexicon_content_hash") or "").strip()
    facts: Dict[str, Any] = {
        "approved_snapshot_at": approved_iso,
        "approved_found": False,
        "approved_matches_manifest": False,
        "latest_matching_snapshot_at": "",
        "latest_snapshot_at": "",
    }
    if spark is None or not delta_table_exists(spark, GOLD_TOPIC_SNAPSHOT_PATH):
        return facts

    facts["latest_matching_snapshot_at"] = (
        find_latest_topic_snapshot_at_for_release(
            spark,
            dataset_id=dataset_id,
            release_lexicon_version=rel_ver,
            lexicon_content_hash=lex_hash,
        )
        or ""
    )

    try:
        from pyspark.sql.functions import col, max as spark_max

        gdf = _filter_dataset_df(read_delta_table(spark, GOLD_TOPIC_SNAPSHOT_PATH), dataset_id)
        if int(gdf.count()) > 0 and "snapshot_at" in gdf.columns:
            max_row = gdf.agg(spark_max(col("snapshot_at")).alias("mx")).collect()
            mx = max_row[0]["mx"] if max_row else None
            facts["latest_snapshot_at"] = _topic_snapshot_iso_from_cell(mx) if mx is not None else ""
    except Exception:
        pass

    if not approved_iso:
        return facts

    facts["approved_found"] = verify_topic_snapshot_at_for_release(
        spark,
        dataset_id=dataset_id,
        snapshot_at_iso=approved_iso,
        release_lexicon_version=rel_ver,
        lexicon_content_hash=lex_hash,
    )
    facts["approved_matches_manifest"] = facts["approved_found"]
    return facts


def audit_approved_topic_snapshot(
    *,
    manifest: Dict[str, Any],
    approved_facts: Dict[str, Any],
    snapshot_row_count: int,
) -> List[AuditFinding]:
    findings: List[AuditFinding] = []
    gold = manifest.get("gold") or {}
    approved_iso = str(gold.get("approved_snapshot_at") or "").strip()
    latest_matching = str(approved_facts.get("latest_matching_snapshot_at") or "").strip()
    latest_any = str(approved_facts.get("latest_snapshot_at") or "").strip()

    if snapshot_row_count <= 0:
        if approved_iso:
            findings.append(
                AuditFinding(
                    check_id="gold.approved_snapshot",
                    level=AuditLevel.WARN,
                    message="manifest 已登記核准快照，但 topic_snapshot 表無資料",
                    detail={"approved_snapshot_at": approved_iso},
                )
            )
        return findings

    if not approved_iso:
        findings.append(
            AuditFinding(
                check_id="gold.approved_snapshot",
                level=AuditLevel.WARN,
                message="尚未設定 approved_snapshot_at（發版後請執行 --approve-snapshot）",
                detail={
                    "latest_matching_snapshot_at": latest_matching or None,
                    "hint": "python scripts/pipeline_guardian.py --dataset drinks --approve-snapshot",
                },
            )
        )
        return findings

    if not approved_facts.get("approved_found"):
        findings.append(
            AuditFinding(
                check_id="gold.approved_snapshot",
                level=AuditLevel.FAIL,
                message="manifest 的 approved_snapshot_at 在 topic_snapshot 找不到或 lexicon 不符",
                detail={
                    "approved_snapshot_at": approved_iso,
                    "latest_matching_snapshot_at": latest_matching or None,
                },
            )
        )
        return findings

    if latest_matching and approved_iso != latest_matching:
        findings.append(
            AuditFinding(
                check_id="gold.approved_snapshot_stale",
                level=AuditLevel.WARN,
                message="已有更新的符合黃金 lexicon 快照，但 manifest 仍指向舊核准時間",
                detail={
                    "approved_snapshot_at": approved_iso,
                    "latest_matching_snapshot_at": latest_matching,
                },
            )
        )

    findings.append(
        AuditFinding(
            check_id="gold.approved_snapshot",
            level=AuditLevel.PASS,
            message="核准 topic_snapshot 存在且與 manifest lexicon 一致",
            detail={
                "approved_snapshot_at": approved_iso,
                "latest_snapshot_at": latest_any or None,
            },
        )
    )
    return findings


def stamp_approved_snapshot(
    dataset_id: str,
    *,
    manifest_path: Optional[Path] = None,
    snapshot_at_iso: Optional[str] = None,
    spark=None,
) -> Dict[str, Any]:
    """
    將符合 manifest 黃金 lexicon 的 topic_snapshot 寫入 gold.approved_snapshot_at。
    未指定 snapshot_at_iso 時，自動選最新符合者。
    """
    if spark is None:
        raise ValueError("stamp_approved_snapshot 需要 SparkSession（請勿使用 --offline）")

    from services.spark_service import (
        find_latest_topic_snapshot_at_for_release,
        verify_topic_snapshot_at_for_release,
    )

    ds = _normalize_dataset_id(dataset_id)
    path = resolve_manifest_path(ds, manifest_path)
    manifest = load_manifest(path)
    gold = manifest.setdefault("gold", {})
    rel_ver = str(
        gold.get("release_lexicon_version") or gold.get("lexicon_version") or STOPWORDS_LEXICON_VERSION
    ).strip()
    lex_hash = str(gold.get("lexicon_content_hash") or "").strip()
    if not lex_hash:
        raise ValueError("manifest 缺少 gold.lexicon_content_hash，請先 --print-hashes 並更新 manifest")

    user_iso = str(snapshot_at_iso or "").strip()
    if user_iso:
        if not verify_topic_snapshot_at_for_release(
            spark,
            dataset_id=ds,
            snapshot_at_iso=user_iso,
            release_lexicon_version=rel_ver,
            lexicon_content_hash=lex_hash,
        ):
            raise ValueError(
                f"找不到符合 manifest 的 topic_snapshot: snapshot_at={user_iso}"
            )
        approved_iso = user_iso
    else:
        approved_iso = find_latest_topic_snapshot_at_for_release(
            spark,
            dataset_id=ds,
            release_lexicon_version=rel_ver,
            lexicon_content_hash=lex_hash,
        )
        if not approved_iso:
            raise ValueError(
                "找不到符合 manifest lexicon 的 topic_snapshot；請先重跑 Gold 並寫入快照"
            )

    gold["approved_snapshot_at"] = approved_iso
    from services.spark_service import count_silver_distinct_image_paths

    gold["processed_image_count"] = count_silver_distinct_image_paths(spark, ds)
    save_manifest(path, manifest)
    return {
        "dataset_id": ds,
        "manifest_path": str(path),
        "approved_snapshot_at": approved_iso,
        "processed_image_count": gold["processed_image_count"],
        "release_lexicon_version": rel_ver,
        "lexicon_content_hash": lex_hash,
    }


def revoke_approved_snapshot(
    dataset_id: str,
    *,
    manifest_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    撤回發行版指標（開發期用）：清除 manifest 的 approved_snapshot_at 與 processed_image_count。
    不刪除 Delta topic_snapshot 列；僅取消「對外發行」契約。
    """
    ds = _normalize_dataset_id(dataset_id)
    path = resolve_manifest_path(ds, manifest_path)
    manifest = load_manifest(path)
    gold = manifest.setdefault("gold", {})
    prev_approved = str(gold.get("approved_snapshot_at") or "").strip() or None
    prev_count = gold.get("processed_image_count")
    gold["approved_snapshot_at"] = None
    gold["processed_image_count"] = None
    save_manifest(path, manifest)
    return {
        "dataset_id": ds,
        "manifest_path": str(path),
        "revoked": True,
        "previous_approved_snapshot_at": prev_approved,
        "previous_processed_image_count": prev_count,
    }


def audit_gold_topic_snapshot(
    *,
    rule_versions: Set[str],
    manifest: Dict[str, Any],
    row_count: int,
) -> List[AuditFinding]:
    findings: List[AuditFinding] = []
    gold = manifest.get("gold") or {}
    expected = str(gold.get("topic_rule_version") or "").strip()

    if row_count <= 0:
        findings.append(
            AuditFinding(
                check_id="gold.topic_snapshot",
                level=AuditLevel.WARN,
                message="Gold topic_snapshot 無資料或無法讀取",
                detail={"row_count": row_count},
            )
        )
        return findings

    if not expected:
        return findings

    if expected not in rule_versions:
        findings.append(
            AuditFinding(
                check_id="gold.topic_snapshot_rule",
                level=AuditLevel.WARN,
                message="最新痛點快照的 rule_version 未含 manifest 預期版本（可能需重跑 Gold）",
                detail={
                    "expected": expected,
                    "latest_snapshot_versions": sorted(rule_versions),
                },
            )
        )
        return findings

    findings.append(
        AuditFinding(
            check_id="gold.topic_snapshot_rule",
            level=AuditLevel.PASS,
            message="最新痛點快照 rule_version 含 manifest 預期版本",
            detail={"expected": expected},
        )
    )
    return findings


def _filter_dataset_df(df, dataset_id: str):
    from pyspark.sql.functions import col, lower, trim

    ds = _normalize_dataset_id(dataset_id)
    if "dataset_id" not in df.columns:
        return df
    return df.filter(trim(lower(col("dataset_id"))) == ds)


def _distinct_column_values(spark_df, column: str) -> Set[str]:
    if column not in spark_df.columns:
        return set()
    rows = spark_df.select(column).distinct().collect()
    out: Set[str] = set()
    for row in rows:
        v = row[0]
        if v is None:
            continue
        s = str(v).strip()
        if s:
            out.add(s)
    return out


def collect_delta_audit_facts(spark, dataset_id: str) -> Dict[str, Any]:
    """讀 Delta 抽樣事實（僅 DISTINCT／count，成本低）。"""
    from services.spark_service import delta_table_exists, read_delta_table

    facts: Dict[str, Any] = {
        "bronze": {"row_count": 0, "ocr_signatures": set()},
        "silver": {"row_count": 0, "transform_versions": set()},
        "gold": {"snapshot_row_count": 0, "latest_rule_versions": set()},
    }

    if delta_table_exists(spark, BRONZE_TABLE_PATH):
        bdf = _filter_dataset_df(read_delta_table(spark, BRONZE_TABLE_PATH), dataset_id)
        facts["bronze"]["row_count"] = int(bdf.count())
        facts["bronze"]["ocr_signatures"] = _distinct_column_values(bdf, "ocr_signature")

    if delta_table_exists(spark, SILVER_OCR_TABLE_PATH):
        sdf = _filter_dataset_df(read_delta_table(spark, SILVER_OCR_TABLE_PATH), dataset_id)
        facts["silver"]["row_count"] = int(sdf.count())
        facts["silver"]["transform_versions"] = _distinct_column_values(sdf, "silver_transform_version")

    if delta_table_exists(spark, GOLD_TOPIC_SNAPSHOT_PATH):
        from pyspark.sql.functions import col, lit
        from pyspark.sql.functions import max as spark_max

        gdf = _filter_dataset_df(read_delta_table(spark, GOLD_TOPIC_SNAPSHOT_PATH), dataset_id)
        facts["gold"]["snapshot_row_count"] = int(gdf.count())
        if facts["gold"]["snapshot_row_count"] > 0 and "snapshot_at" in gdf.columns:
            max_row = gdf.agg(spark_max(col("snapshot_at")).alias("max_snapshot_at")).collect()
            max_snap = max_row[0]["max_snapshot_at"] if max_row else None
            if max_snap is not None:
                latest = gdf.filter(col("snapshot_at") == lit(max_snap))
                facts["gold"]["latest_rule_versions"] = _distinct_column_values(latest, "rule_version")

    return facts


def run_audit(
    dataset_id: str,
    *,
    manifest_path: Optional[Path] = None,
    spark=None,
    offline: bool = False,
) -> AuditReport:
    """執行守護神稽核。offline=True 時僅檢查 runtime + 辭典 hash（不連 Delta）。"""
    ds = _normalize_dataset_id(dataset_id)
    path = resolve_manifest_path(ds, manifest_path)
    manifest = load_manifest(path)
    report = AuditReport(dataset_id=ds)

    report.findings.extend(audit_runtime_config(manifest))

    if spark is not None and not offline:
        facts = collect_delta_audit_facts(spark, ds)
        report.findings.extend(
            audit_bronze_signatures(
                signatures=set(facts["bronze"]["ocr_signatures"]),
                manifest=manifest,
                row_count=int(facts["bronze"]["row_count"]),
            )
        )
        report.findings.extend(
            audit_silver_transform_versions(
                versions=set(facts["silver"]["transform_versions"]),
                manifest=manifest,
                row_count=int(facts["silver"]["row_count"]),
            )
        )
        report.findings.extend(
            audit_gold_topic_snapshot(
                rule_versions=set(facts["gold"]["latest_rule_versions"]),
                manifest=manifest,
                row_count=int(facts["gold"]["snapshot_row_count"]),
            )
        )
        approved_facts = collect_approved_snapshot_facts(spark, ds, manifest)
        report.findings.extend(
            audit_approved_topic_snapshot(
                manifest=manifest,
                approved_facts=approved_facts,
                snapshot_row_count=int(facts["gold"]["snapshot_row_count"]),
            )
        )
        bundle = collect_gold_dual_lexicon(spark, ds)
        release_hash = str(bundle.get("release_lexicon_content_hash") or "")
        exploration_hash = str(bundle.get("exploration_lexicon_content_hash") or "")
        lexicon_version = str(bundle.get("release_lexicon_version") or STOPWORDS_LEXICON_VERSION)
        stopwords_path = str(bundle.get("release_stopwords_path") or "")
        content_hash = release_hash
        report.findings.extend(
            audit_exploration_lexicon_note(
                release_hash=release_hash,
                exploration_hash=exploration_hash,
                exploration_version=str(
                    bundle.get("exploration_lexicon_version") or STOPWORDS_EXPLORATION_LEXICON_VERSION
                ),
            )
        )
    else:
        release_merged = compute_merged_stop_offline(ds, lexicon_version=STOPWORDS_LEXICON_VERSION)
        exploration_merged = compute_merged_stop_offline(
            ds, lexicon_version=STOPWORDS_EXPLORATION_LEXICON_VERSION
        )
        content_hash = compute_lexicon_content_hash(release_merged)
        release_hash = content_hash
        exploration_hash = compute_lexicon_content_hash(exploration_merged)
        lexicon_version = STOPWORDS_LEXICON_VERSION
        stopwords_path = resolve_local_stopwords_lexicon_path(
            ds, lexicon_version=STOPWORDS_LEXICON_VERSION
        ) or ""
        report.findings.extend(
            audit_exploration_lexicon_note(
                release_hash=release_hash,
                exploration_hash=exploration_hash,
                exploration_version=STOPWORDS_EXPLORATION_LEXICON_VERSION,
            )
        )

    report.findings.extend(
        audit_lexicon_bump_risk(
            dataset_id=ds,
            manifest=manifest,
            current_lexicon_version=lexicon_version,
            current_content_hash=content_hash,
            stopwords_path=stopwords_path,
        )
    )

    return report


def build_hash_bootstrap_manifest(dataset_id: str, *, spark=None) -> Dict[str, Any]:
    """產生可寫入 manifest 的 hash／版本片段（--print-hashes）。"""
    ds = _normalize_dataset_id(dataset_id)
    if spark is not None:
        dual = collect_gold_dual_lexicon(spark, ds)
        release = dual.get("release") or {}
        exploration = dual.get("exploration") or {}
        release_hash = str(release.get("lexicon_content_hash") or "")
        exploration_hash = str(exploration.get("lexicon_content_hash") or "")
        release_path = str(release.get("stopwords_path") or "")
        exploration_path = str(exploration.get("stopwords_path") or "")
        release_merged_count = int(release.get("stopwords_merged_count") or 0)
        exploration_merged_count = int(exploration.get("stopwords_merged_count") or 0)
    else:
        release_merged = compute_merged_stop_offline(ds, lexicon_version=STOPWORDS_LEXICON_VERSION)
        exploration_merged = compute_merged_stop_offline(
            ds, lexicon_version=STOPWORDS_EXPLORATION_LEXICON_VERSION
        )
        release_hash = compute_lexicon_content_hash(release_merged)
        exploration_hash = compute_lexicon_content_hash(exploration_merged)
        release_path = resolve_local_stopwords_lexicon_path(
            ds, lexicon_version=STOPWORDS_LEXICON_VERSION
        ) or ""
        exploration_path = resolve_local_stopwords_lexicon_path(
            ds, lexicon_version=STOPWORDS_EXPLORATION_LEXICON_VERSION
        ) or ""
        release_merged_count = len(release_merged)
        exploration_merged_count = len(exploration_merged)

    return {
        "dataset_id": ds,
        "release_id": f"{ds}-gold-v1",
        "runtime": {
            "silver_transform_version": SILVER_TRANSFORM_VERSION,
        },
        "gold": {
            "release_lexicon_version": STOPWORDS_LEXICON_VERSION,
            "lexicon_content_hash": release_hash,
            "exploration_lexicon_version": STOPWORDS_EXPLORATION_LEXICON_VERSION,
            "exploration_lexicon_content_hash": exploration_hash,
            "topic_rule_version": TOPIC_RULE_VERSION,
            "approved_snapshot_at": None,
            "release_merged_stop_count": release_merged_count,
            "exploration_merged_stop_count": exploration_merged_count,
            "release_stopwords_path": release_path,
            "exploration_stopwords_path": exploration_path,
        },
        "notes": (
            "黃金發行：dic/stop_words/v1.0.0/ + manifest lexicon_content_hash；"
            "探索：dic/stop_words/dev/ 可日常修改"
        ),
    }


def format_report_text(report: AuditReport) -> str:
    lines = [f"Pipeline Guardian — dataset={report.dataset_id}", ""]
    for f in report.findings:
        lines.append(f"[{f.level.value}] {f.check_id}: {f.message}")
        if f.detail:
            lines.append(f"    {json.dumps(f.detail, ensure_ascii=False)}")
    lines.append("")
    code = report.exit_code()
    lines.append(f"exit_code={code} (0=pass, 1=fail, 2=warn)")
    return "\n".join(lines)
