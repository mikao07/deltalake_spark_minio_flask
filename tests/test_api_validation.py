import os
from io import BytesIO


def _set_env(monkeypatch):
    # 測試只針對輸入驗證，避免真的啟動 Spark：
    # - 不需要設 MINIO_ACCESS_KEY / MINIO_SECRET_KEY（只要不觸發 Spark 初始化）
    monkeypatch.delenv("ADMIN_TOKEN", raising=False)
    monkeypatch.setenv("BUCKET_NAME", "data-lake")
    monkeypatch.setenv("ALLOWED_DELTA_PATH_PREFIXES", "s3a://data-lake/")


def _client(monkeypatch):
    _set_env(monkeypatch)
    import app as flask_app  # noqa: WPS433 (test import)

    flask_app.app.testing = True
    return flask_app.app.test_client()


def test_health_ok(monkeypatch):
    c = _client(monkeypatch)
    r = c.get("/health")
    assert r.status_code == 200
    assert r.get_json() == {"status": "ok"}


def test_upload_page_ok(monkeypatch):
    c = _client(monkeypatch)
    r = c.get("/upload")
    assert r.status_code == 200
    assert b"upload-form" in r.data
    assert b"result-table" in r.data


def test_delta_read_requires_table_path(monkeypatch):
    c = _client(monkeypatch)
    r = c.post("/delta/read", json={"limit": 10})
    assert r.status_code == 400
    assert "table_path" in r.get_json()["error"]


def test_delta_read_rejects_path_outside_whitelist(monkeypatch):
    c = _client(monkeypatch)
    r = c.post("/delta/read", json={"table_path": "s3a://other-bucket/x/", "limit": 1})
    assert r.status_code == 403


def test_delta_upsert_requires_target_path(monkeypatch):
    c = _client(monkeypatch)
    r = c.post("/delta/upsert", json={"key_col": "id", "records": [{"id": 1}]})
    assert r.status_code == 400
    assert "target_path" in r.get_json()["error"]


def test_delta_cleanup_dry_run_does_not_require_spark(monkeypatch):
    c = _client(monkeypatch)
    r = c.post(
        "/delta/cleanup-latest-only",
        json={"target_path": "s3a://data-lake/silver/ocr_features/", "dry_run": True},
    )
    assert r.status_code == 200
    assert r.get_json()["status"] == "dry_run"


def test_delta_gold_word_frequency_run_dry_run(monkeypatch):
    c = _client(monkeypatch)
    r = c.post(
        "/delta/gold/word-frequency/run",
        json={"dry_run": True},
    )
    assert r.status_code == 200
    j = r.get_json()
    assert j["status"] == "dry_run"
    assert "silver_ocr_path" in j
    assert "gold_path" in j


def test_upload_images_requires_file(monkeypatch):
    c = _client(monkeypatch)
    r = c.post("/api/upload/images", data={"dataset_id": "demo"})
    assert r.status_code == 400
    assert "file" in r.get_json()["error"] or "檔案" in r.get_json()["error"]


def test_upload_images_requires_dataset_id(monkeypatch):
    c = _client(monkeypatch)
    data = {
        "file": (BytesIO(b"fake"), "x.png"),
    }
    r = c.post("/api/upload/images", data=data, content_type="multipart/form-data")
    assert r.status_code == 400
    assert "dataset_id" in r.get_json()["error"]


def test_delta_ocr_bronze_run_dry_run_no_spark(monkeypatch):
    c = _client(monkeypatch)
    r = c.post(
        "/delta/ocr/bronze/run",
        json={"dry_run": True},
    )
    assert r.status_code == 200
    j = r.get_json()
    assert j["status"] == "dry_run"
    assert "raw_images_path" in j
    assert "bronze_path" in j


def test_delta_ocr_bronze_run_dry_run_with_dataset(monkeypatch):
    c = _client(monkeypatch)
    r = c.post(
        "/delta/ocr/bronze/run",
        json={"dry_run": True, "dataset_id": "invoice_ocr"},
    )
    assert r.status_code == 200
    j = r.get_json()
    assert j["status"] == "dry_run"
    assert j["dataset_id"] == "invoice_ocr"
    assert j["raw_images_path"].endswith("/invoice_ocr/")


def test_delta_silver_ocr_run_dry_run_with_dataset(monkeypatch):
    c = _client(monkeypatch)
    r = c.post(
        "/delta/silver/ocr/run",
        json={"dry_run": True, "dataset_id": "invoice_ocr"},
    )
    assert r.status_code == 200
    j = r.get_json()
    assert j["status"] == "dry_run"
    assert j["dataset_id"] == "invoice_ocr"
    assert "bronze_path" in j
    assert "silver_ocr_path" in j


def test_delta_gold_word_frequency_run_dry_run_with_dataset(monkeypatch):
    c = _client(monkeypatch)
    r = c.post(
        "/delta/gold/word-frequency/run",
        json={"dry_run": True, "dataset_id": "invoice_ocr"},
    )
    assert r.status_code == 200
    j = r.get_json()
    assert j["status"] == "dry_run"
    assert j["dataset_id"] == "invoice_ocr"


def test_delta_pipeline_to_gold_run_dry_run(monkeypatch):
    c = _client(monkeypatch)
    r = c.post(
        "/delta/pipeline/to-gold/run",
        json={"dry_run": True, "dataset_id": "invoice_ocr"},
    )
    assert r.status_code == 200
    j = r.get_json()
    assert j["status"] == "dry_run"
    assert j["dataset_id"] == "invoice_ocr"
    assert "steps" in j


def test_delta_ocr_bronze_run_empty_source_returns_400(monkeypatch):
    c = _client(monkeypatch)
    import app as flask_app

    monkeypatch.setattr(
        flask_app,
        "preview_raw_images_sample",
        lambda spark, raw_images_path, limit=1: [],
    )
    class _Dummy:
        spark = object()

    monkeypatch.setattr(flask_app, "_get_spark_manager", lambda: _Dummy())
    r = c.post(
        "/delta/ocr/bronze/run",
        json={"dataset_id": "invoice_ocr", "write_mode": "append", "dry_run": False},
    )
    assert r.status_code == 400
    j = r.get_json()
    assert "沒有可處理圖片" in j["error"]
    assert j["dataset_id"] == "invoice_ocr"


def test_delta_gold_word_frequency_run_rejects_bad_coalesce(monkeypatch):
    c = _client(monkeypatch)
    r = c.post(
        "/delta/gold/word-frequency/run",
        json={"dry_run": True, "coalesce_partitions": "x"},
    )
    assert r.status_code == 400
    assert "coalesce_partitions" in r.get_json()["error"]


def test_health_storage_ok_with_mock(monkeypatch):
    c = _client(monkeypatch)
    import app as flask_app

    class _DummySpark:
        spark = object()

    class _DummyClient:
        def bucket_exists(self, bucket):
            return True

        def list_objects(self, bucket, prefix=None, recursive=False):
            yield object()

    monkeypatch.setattr(flask_app, "_get_spark_manager", lambda: _DummySpark())
    monkeypatch.setattr(flask_app, "get_minio_client", lambda: _DummyClient())
    monkeypatch.setattr(flask_app, "ensure_bucket", lambda client, bucket: None)
    monkeypatch.setattr(flask_app, "preview_raw_images_sample", lambda spark, path, limit=10: [{"x": 1}])

    r = c.get("/api/health/storage?dataset_id=drinks&limit=5")
    assert r.status_code == 200
    j = r.get_json()
    assert j["status"] in ("ok", "degraded")
    assert "minio_sdk" in j
    assert "spark_binaryfile" in j

