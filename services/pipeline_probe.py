"""
外部管線探針：彙總 /ready、守護神、新鮮度；失敗時可發通知。

供 cron：`python scripts/pipeline_probe.py drinks --strict`
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import request as urllib_request

from config import PIPELINE_PROBE_LAST_FILE, PIPELINE_PROBE_READY_URL

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_path(path: str | Path) -> Path:
    p = Path(path)
    if not p.is_absolute():
        p = _REPO_ROOT / p
    return p


def write_probe_result(payload: Dict[str, Any], path: str | Path | None = None) -> Path:
    p = _resolve_path(path or PIPELINE_PROBE_LAST_FILE)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return p


def check_ready_via_http(url: str) -> Dict[str, Any]:
    try:
        with urllib_request.urlopen(url, timeout=60) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            body = json.loads(raw) if raw.strip() else {}
            status = str(body.get("status") or "").lower()
            ok = resp.status < 400 and status == "ok"
            return {
                "ok": ok,
                "status_code": resp.status,
                "body": body,
            }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def check_ready_local(*, include_spark: bool = True, spark=None) -> Dict[str, Any]:
    from services.readiness import build_ready_payload

    def _get_spark():
        if spark is not None:
            return spark
        from services.spark_service import SparkManager

        return SparkManager(app_name="PipelineProbe").spark

    overall, body = build_ready_payload(include_spark=include_spark, get_spark=_get_spark)
    return {"ok": overall == "ok", "body": body}


def run_guardian_probe(
    dataset_id: str,
    *,
    strict: bool,
    spark=None,
    offline: bool = False,
) -> Dict[str, Any]:
    from services.pipeline_guardian import AuditLevel, resolve_manifest_path, run_audit

    manifest_path = resolve_manifest_path(dataset_id)
    if not manifest_path.is_file():
        return {
            "ok": False,
            "issues": [
                {
                    "source": "guardian",
                    "level": "FAIL",
                    "check_id": "manifest.missing",
                    "message": f"找不到 manifest: {manifest_path}",
                }
            ],
            "report": None,
        }

    report = run_audit(dataset_id, manifest_path=manifest_path, spark=spark, offline=offline)
    issues: List[Dict[str, Any]] = []
    for f in report.findings:
        if f.level == AuditLevel.FAIL:
            issues.append(
                {
                    "source": "guardian",
                    "level": "FAIL",
                    "check_id": f.check_id,
                    "message": f.message,
                }
            )
        elif strict and f.level == AuditLevel.WARN:
            issues.append(
                {
                    "source": "guardian",
                    "level": "WARN",
                    "check_id": f.check_id,
                    "message": f.message,
                }
            )

    ok = len(issues) == 0
    return {"ok": ok, "issues": issues, "report": report.to_dict()}


def run_freshness_probe(
    dataset_id: str,
    *,
    offline: bool = False,
    spark=None,
    stale_hours: Optional[float] = None,
) -> Dict[str, Any]:
    from config import FRESHNESS_STALE_HOURS
    from services.pipeline_freshness import run_freshness_check

    stale = float(stale_hours) if stale_hours is not None else FRESHNESS_STALE_HOURS
    payload = run_freshness_check(dataset_id, offline=offline, spark=spark, stale_hours=stale)
    issues: List[Dict[str, Any]] = []
    for alert in payload.get("alerts") or []:
        issues.append(
            {
                "source": "freshness",
                "level": "ALERT",
                "check_id": alert.get("code"),
                "message": alert.get("message"),
            }
        )
    return {"ok": bool(payload.get("ok")), "issues": issues, "payload": payload}


def run_pipeline_probe(
    dataset_id: str,
    *,
    strict: bool = True,
    offline: bool = False,
    notify: bool = True,
    include_spark_ready: bool = True,
    ready_url: Optional[str] = None,
    spark=None,
    stale_hours: Optional[float] = None,
    probe_result_path: str | Path | None = None,
) -> Dict[str, Any]:
    ds = str(dataset_id or "drinks").strip().lower()
    issues: List[Dict[str, Any]] = []

    url = (ready_url if ready_url is not None else PIPELINE_PROBE_READY_URL or "").strip()
    if url:
        ready = check_ready_via_http(url)
        ready_detail = ready
    else:
        ready = check_ready_local(include_spark=include_spark_ready, spark=spark)
        ready_detail = ready

    if not ready.get("ok"):
        err = ready.get("error")
        if err:
            msg = f"/ready 失敗: {err}"
        else:
            body = ready.get("body") or {}
            msg = f"/ready status={body.get('status', 'down')}"
        issues.append({"source": "ready", "level": "FAIL", "check_id": "ready.down", "message": msg})

    if spark is None and not offline and not url:
        try:
            from services.spark_service import SparkManager

            spark = SparkManager(app_name="PipelineProbe").spark
        except Exception as e:
            issues.append(
                {
                    "source": "probe",
                    "level": "FAIL",
                    "check_id": "spark.unavailable",
                    "message": f"無法建立 SparkSession: {e}",
                }
            )

    guardian = {"ok": True, "issues": []}
    freshness = {"ok": True, "issues": []}
    if spark is not None or offline:
        guardian = run_guardian_probe(ds, strict=strict, spark=spark, offline=offline)
        issues.extend(guardian["issues"])
        freshness = run_freshness_probe(
            ds, offline=offline, spark=spark, stale_hours=stale_hours
        )
        issues.extend(freshness["issues"])

    ok = len(issues) == 0
    notify_result = None
    if notify and not ok:
        from services.pipeline_notify import format_probe_alert_body, send_pipeline_alert

        body = format_probe_alert_body(dataset_id=ds, issues=issues)
        nr = send_pipeline_alert(
            title="管線探針告警",
            body=body,
            dataset_id=ds,
        )
        notify_result = {
            "backend": nr.backend,
            "sent": nr.sent,
            "skipped_reason": nr.skipped_reason,
            "error": nr.error,
        }

    result = {
        "checked_at": _iso_now(),
        "dataset_id": ds,
        "ok": ok,
        "strict": strict,
        "offline": offline,
        "ready": ready_detail,
        "guardian_ok": guardian.get("ok"),
        "freshness_ok": freshness.get("ok"),
        "issues": issues,
        "notify": notify_result,
    }
    write_probe_result(result, probe_result_path)
    return result
