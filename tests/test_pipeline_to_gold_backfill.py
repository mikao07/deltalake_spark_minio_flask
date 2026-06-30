"""一鍵金層 raw 缺口自動補齊。"""

from unittest.mock import MagicMock, patch

import pytest


def _set_env(monkeypatch):
    monkeypatch.delenv("ADMIN_TOKEN", raising=False)
    monkeypatch.setenv("BUCKET_NAME", "data-lake")
    monkeypatch.setenv("ALLOWED_DELTA_PATH_PREFIXES", "s3a://data-lake/")


def test_execute_pipeline_to_gold_backfills_missing_paths(monkeypatch):
    _set_env(monkeypatch)
    import app as flask_app

    missing = [
        "s3a://data-lake/raw/images/drinks/new1.png",
        "s3a://data-lake/raw/images/drinks/new2.png",
    ]
    monkeypatch.setattr(
        flask_app,
        "_resolve_raw_backfill_gap",
        lambda dataset_id: {
            "missing_count": len(missing),
            "missing_paths": missing,
            "truncated": False,
        },
    )

    bronze_calls = []

    def fake_bronze(spark, **kwargs):
        bronze_calls.append(kwargs)
        return {"processed_rows": 2, "write_mode": kwargs.get("write_mode")}

    monkeypatch.setattr(flask_app, "run_bronze_ocr_ingest", fake_bronze)
    monkeypatch.setattr(
        flask_app,
        "run_silver_ocr_etl",
        lambda **kwargs: {"updated_rows": 2, "inserted_rows": 2, "silver_batch_ts": "t"},
    )
    monkeypatch.setattr(
        flask_app,
        "run_gold_etl",
        lambda **kwargs: {"tfidf_output_rows": 1, "is_gold_written": True},
    )
    monkeypatch.setattr(flask_app, "pipeline_etl_slot", lambda **kwargs: MagicMock(__enter__=lambda s: None, __exit__=lambda s, *a: None))
    monkeypatch.setattr(flask_app, "_get_spark_manager", lambda: MagicMock(spark=object()))
    monkeypatch.setattr(flask_app, "_record_etl_metric", lambda payload: None)
    monkeypatch.setattr(flask_app, "check_bronze_ocr_batch", lambda n: None)

    out = flask_app._execute_pipeline_to_gold_inner(
        dataset_id="drinks",
        raw_images_path="s3a://data-lake/raw/images/drinks/",
        write_mode="merge",
        coalesce_partitions=1,
        skip_gold_if_no_new_ocr=True,
        progress=lambda *a: None,
        image_paths=["s3a://data-lake/raw/images/drinks/upload-only.png"],
    )

    assert out["raw_backfill_count"] == 2
    assert out["steps"] == ["bronze_ocr", "silver_ocr", "gold_etl"]
    assert len(bronze_calls) == 1
    assert bronze_calls[0]["write_mode"] == "append"
    assert bronze_calls[0]["image_paths"] == missing


def test_execute_pipeline_to_gold_skips_bronze_when_no_gap(monkeypatch):
    _set_env(monkeypatch)
    import app as flask_app

    monkeypatch.setattr(
        flask_app,
        "_resolve_raw_backfill_gap",
        lambda dataset_id: {"missing_count": 0, "missing_paths": [], "truncated": False},
    )
    monkeypatch.setattr(
        flask_app,
        "run_bronze_ocr_ingest",
        lambda *a, **k: pytest.fail("should not call bronze OCR"),
    )
    monkeypatch.setattr(flask_app, "pipeline_etl_slot", lambda **kwargs: MagicMock(__enter__=lambda s: None, __exit__=lambda s, *a: None))
    monkeypatch.setattr(flask_app, "_get_spark_manager", lambda: MagicMock(spark=object()))
    monkeypatch.setattr(flask_app, "_record_etl_metric", lambda payload: None)

    out = flask_app._execute_pipeline_to_gold_inner(
        dataset_id="drinks",
        raw_images_path="s3a://data-lake/raw/images/drinks/",
        write_mode="append",
        coalesce_partitions=1,
        skip_gold_if_no_new_ocr=True,
        progress=lambda *a: None,
    )

    assert out["raw_backfill_count"] == 0
    assert out["is_incremental_short_circuit"] is True
    assert out["bronze_result"]["raw_backfill_skipped"] is True
