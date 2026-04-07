import os


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
    r = c.post("/api/upload/images", data={})
    assert r.status_code == 400
    assert "file" in r.get_json()["error"] or "檔案" in r.get_json()["error"]


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


def test_delta_gold_word_frequency_run_rejects_bad_coalesce(monkeypatch):
    c = _client(monkeypatch)
    r = c.post(
        "/delta/gold/word-frequency/run",
        json={"dry_run": True, "coalesce_partitions": "x"},
    )
    assert r.status_code == 400
    assert "coalesce_partitions" in r.get_json()["error"]

