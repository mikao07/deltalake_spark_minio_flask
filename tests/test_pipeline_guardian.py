"""管線守護神：辭典 hash 與 bump 風險偵測。"""

from services.pipeline_guardian import (
    AuditLevel,
    audit_approved_topic_snapshot,
    audit_lexicon_bump_risk,
    audit_silver_transform_versions,
    compute_lexicon_content_hash,
    compute_merged_stop_offline,
    load_manifest,
    run_audit,
    save_manifest,
    stamp_approved_snapshot,
)


def _manifest_with_hash(content_hash: str, version: str = "v1.0.0") -> dict:
    return {
        "runtime": {"silver_transform_version": "v2.1.0"},
        "gold": {
            "release_lexicon_version": version,
            "lexicon_version": version,
            "lexicon_content_hash": content_hash,
            "topic_rule_version": "v1.4.1-drinks-funnel-invoice",
        },
    }


def test_compute_lexicon_content_hash_stable():
    h1 = compute_lexicon_content_hash(["好喝", "珍珠", "好喝"])
    h2 = compute_lexicon_content_hash(["珍珠", "好喝"])
    assert h1 == h2
    assert len(h1) == 64


def test_merged_stop_offline_drinks_non_empty():
    from config import STOPWORDS_LEXICON_VERSION

    merged = compute_merged_stop_offline("drinks", lexicon_version=STOPWORDS_LEXICON_VERSION)
    assert len(merged) >= 50
    assert "好喝" in merged


def test_release_and_dev_lexicon_paths_exist():
    from config import STOPWORDS_EXPLORATION_LEXICON_VERSION, STOPWORDS_LEXICON_VERSION
    from services.lexicon import resolve_local_stopwords_lexicon_path

    assert resolve_local_stopwords_lexicon_path("drinks", lexicon_version=STOPWORDS_LEXICON_VERSION)
    assert resolve_local_stopwords_lexicon_path("drinks", lexicon_version=STOPWORDS_EXPLORATION_LEXICON_VERSION)


def test_bump_risk_fail_when_hash_changes_version_unchanged():
    from config import STOPWORDS_LEXICON_VERSION

    merged = compute_merged_stop_offline("drinks", lexicon_version=STOPWORDS_LEXICON_VERSION)
    current_hash = compute_lexicon_content_hash(merged)
    tampered_hash = compute_lexicon_content_hash(merged + ["新雜詞測試"])
    manifest = _manifest_with_hash(current_hash)

    findings = audit_lexicon_bump_risk(
        dataset_id="drinks",
        manifest=manifest,
        current_lexicon_version="v1.0.0",
        current_content_hash=tampered_hash,
    )
    assert any(f.check_id == "gold.lexicon_silent_drift" and f.level == AuditLevel.FAIL for f in findings)


def test_bump_risk_pass_when_aligned():
    from config import STOPWORDS_LEXICON_VERSION

    merged = compute_merged_stop_offline("drinks", lexicon_version=STOPWORDS_LEXICON_VERSION)
    current_hash = compute_lexicon_content_hash(merged)
    manifest = _manifest_with_hash(current_hash)

    findings = audit_lexicon_bump_risk(
        dataset_id="drinks",
        manifest=manifest,
        current_lexicon_version="v1.0.0",
        current_content_hash=current_hash,
    )
    assert any(f.check_id == "gold.lexicon_aligned" and f.level == AuditLevel.PASS for f in findings)


def test_silver_transform_fail_on_stale_rows():
    findings = audit_silver_transform_versions(
        versions={"v2.0.0", "v2.1.0"},
        manifest={"runtime": {"silver_transform_version": "v2.1.0"}},
        row_count=50,
    )
    assert any(f.level == AuditLevel.FAIL for f in findings)


def test_run_audit_offline_passes_for_drinks_manifest():
    report = run_audit("drinks", offline=True)
    assert not report.has_fail
    assert any(f.check_id == "gold.lexicon_aligned" for f in report.findings)


def test_audit_approved_snapshot_warn_when_unset():
    findings = audit_approved_topic_snapshot(
        manifest={"gold": {"lexicon_content_hash": "abc"}},
        approved_facts={"latest_matching_snapshot_at": "2026-06-23T10:00:00"},
        snapshot_row_count=10,
    )
    assert any(f.check_id == "gold.approved_snapshot" and f.level == AuditLevel.WARN for f in findings)


def test_audit_approved_snapshot_pass_when_found():
    findings = audit_approved_topic_snapshot(
        manifest={"gold": {"approved_snapshot_at": "2026-06-23T10:00:00"}},
        approved_facts={
            "approved_found": True,
            "latest_matching_snapshot_at": "2026-06-23T10:00:00",
            "latest_snapshot_at": "2026-06-23T10:00:00",
        },
        snapshot_row_count=10,
    )
    assert any(f.check_id == "gold.approved_snapshot" and f.level == AuditLevel.PASS for f in findings)


def test_save_and_load_manifest_roundtrip(tmp_path):
    path = tmp_path / "drinks.json"
    payload = {"dataset_id": "drinks", "gold": {"approved_snapshot_at": "2026-06-23T12:00:00"}}
    save_manifest(path, payload)
    loaded = load_manifest(path)
    assert loaded["gold"]["approved_snapshot_at"] == "2026-06-23T12:00:00"


def test_stamp_approved_snapshot_requires_spark():
    import pytest

    with pytest.raises(ValueError, match="SparkSession"):
        stamp_approved_snapshot("drinks", spark=None)
