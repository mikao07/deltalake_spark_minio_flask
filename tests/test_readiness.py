from unittest.mock import MagicMock, patch


def test_resolve_include_spark_query_overrides_env(monkeypatch):
    monkeypatch.setenv("READY_CHECK_INCLUDE_SPARK", "true")
    from services.readiness import resolve_include_spark

    assert resolve_include_spark("false") is False
    assert resolve_include_spark("true") is True


def test_resolve_include_spark_env(monkeypatch):
    monkeypatch.delenv("READY_CHECK_INCLUDE_SPARK", raising=False)
    from services.readiness import resolve_include_spark

    assert resolve_include_spark(None) is False
    monkeypatch.setenv("READY_CHECK_INCLUDE_SPARK", "1")
    assert resolve_include_spark(None) is True


def test_build_ready_minio_ok_no_spark(monkeypatch):
    monkeypatch.setenv("MINIO_ACCESS_KEY", "test")
    monkeypatch.setenv("MINIO_SECRET_KEY", "test")
    from services.readiness import build_ready_payload

    with patch("services.readiness.get_minio_client") as m:
        client = MagicMock()
        client.bucket_exists.return_value = True
        m.return_value = client
        overall, body = build_ready_payload(
            include_spark=False,
            get_spark=lambda: None,
        )
    assert overall == "ok"
    assert body["status"] == "ok"
    assert body["checks"]["minio"]["status"] == "ok"
    assert body["checks"]["spark"]["status"] == "skipped"


def test_build_ready_spark_fails_sets_down(monkeypatch):
    monkeypatch.setenv("MINIO_ACCESS_KEY", "test")
    monkeypatch.setenv("MINIO_SECRET_KEY", "test")
    from services.readiness import build_ready_payload

    class Boom:
        @property
        def spark(self):
            raise RuntimeError("no spark")

    boom = Boom()

    with patch("services.readiness.get_minio_client") as m:
        client = MagicMock()
        client.bucket_exists.return_value = True
        m.return_value = client
        overall, body = build_ready_payload(
            include_spark=True,
            get_spark=lambda: boom.spark,
        )
    assert overall == "down"
    assert body["checks"]["spark"]["status"] == "error"
