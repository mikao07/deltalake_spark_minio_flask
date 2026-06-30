"""
Bronze 列級隔離：OCR 明顯無效列不進 Silver，並寫入 quarantine Delta 表。

熔斷策略（預設 soft）：
- ≤10% 隔離：壞列 quarantine，好列進 Silver
- >10% 且 <30%：軟熔斷（好列仍進 Silver、擋核准、WARN 通知）
- ≥30% 或 MELT_MODE=hard 且 >10%：硬熔斷（不進 Silver、ALERT 通知）
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, length, lit, trim, when
from pyspark.sql.types import StringType

from config import (
    BRONZE_QUARANTINE_ENABLED,
    BRONZE_QUARANTINE_HARD_REJECT_RATE,
    BRONZE_QUARANTINE_MAX_REJECT_RATE,
    BRONZE_QUARANTINE_MELT_MODE,
    BRONZE_QUARANTINE_MIN_TEXT_LEN,
)

_BRONZE_NOISE_RE = re.compile(r"(?i)\bBARE\b")


class BronzeQuarantineError(RuntimeError):
    """Bronze 硬熔斷：拒絕進入 Silver。"""

    def __init__(self, message: str, *, report: Dict[str, Any] | None = None):
        super().__init__(message)
        self.report = dict(report or {})


def classify_extracted_text_status(
    text: str | None,
    *,
    min_len: int | None = None,
) -> str:
    """單列 OCR 狀態（單元測試與 Spark 規則對齊）。"""
    threshold = int(min_len if min_len is not None else BRONZE_QUARANTINE_MIN_TEXT_LEN)
    raw = str(text or "")
    t = raw.strip()
    if not t:
        return "empty"
    if t.startswith("OCR_ERROR_"):
        return "ocr_error"
    if len(t) < threshold:
        return "too_short"
    if _BRONZE_NOISE_RE.search(t):
        return "noise"
    return "ok"


def bronze_ocr_status_column(source_col: str = "extracted_text"):
    """Spark 欄位：ocr_status。"""
    text = trim(col(source_col))
    min_len = int(BRONZE_QUARANTINE_MIN_TEXT_LEN)
    return (
        when(text.isNull() | (length(text) == 0), lit("empty"))
        .when(text.startswith("OCR_ERROR_"), lit("ocr_error"))
        .when(length(text) < lit(min_len), lit("too_short"))
        .when(text.rlike(r"(?i)\bBARE\b"), lit("noise"))
        .otherwise(lit("ok"))
    )


def summarize_bronze_quarantine(df_checked: DataFrame) -> Dict[str, Any]:
    """依 ocr_status 聚合（單次 Spark action）。"""
    rows = df_checked.groupBy("ocr_status").count().collect()
    by_status = {str(r["ocr_status"]): int(r["count"]) for r in rows}
    total = sum(by_status.values())
    ok_count = int(by_status.get("ok", 0))
    reject_count = total - ok_count
    reject_rate = (reject_count / total) if total else 0.0
    return {
        "total_rows": total,
        "ok_rows": ok_count,
        "reject_rows": reject_count,
        "reject_rate": reject_rate,
        "by_status": by_status,
        "soft_reject_rate": float(BRONZE_QUARANTINE_MAX_REJECT_RATE),
        "hard_reject_rate": float(BRONZE_QUARANTINE_HARD_REJECT_RATE),
        "melt_mode": BRONZE_QUARANTINE_MELT_MODE,
    }


def resolve_melt_decision(summary: Dict[str, Any]) -> Dict[str, Any]:
    """
    依隔離占比與 MELT_MODE 決定 pass / soft / hard。
    回傳欄位含 melt_action、melt_reason（繁中說明用 key）、approve_blocked 等。
    """
    reject_rate = float(summary.get("reject_rate") or 0.0)
    soft_rate = float(summary.get("soft_reject_rate") or BRONZE_QUARANTINE_MAX_REJECT_RATE)
    hard_rate = float(summary.get("hard_reject_rate") or BRONZE_QUARANTINE_HARD_REJECT_RATE)
    mode = str(summary.get("melt_mode") or BRONZE_QUARANTINE_MELT_MODE).strip().lower()
    total = int(summary.get("total_rows") or 0)
    ok_rows = int(summary.get("ok_rows") or 0)
    reject_rows = int(summary.get("reject_rows") or 0)

    base = {
        "total_rows": total,
        "ok_rows": ok_rows,
        "reject_rows": reject_rows,
        "reject_rate": reject_rate,
        "analyzed_rows": ok_rows,
    }

    if total <= 0:
        return {
            **base,
            "melt_action": "pass",
            "melt_reason": "empty_batch",
            "melted": False,
            "high_reject_rate": False,
            "approve_blocked": False,
            "message": "無可評估列。",
        }

    if reject_rate >= hard_rate:
        return {
            **base,
            "melt_action": "hard",
            "melt_reason": "hard_rate",
            "melted": True,
            "high_reject_rate": True,
            "approve_blocked": True,
            "message": (
                f"隔離占比 {reject_rate:.1%} 達硬熔斷門檻 {hard_rate:.0%}（{reject_rows}/{total} 列），"
                f"疑似批次或資料問題，已停止進入 Silver。"
            ),
        }

    if mode == "hard" and reject_rate > soft_rate:
        return {
            **base,
            "melt_action": "hard",
            "melt_reason": "manual_hard_mode",
            "melted": True,
            "high_reject_rate": True,
            "approve_blocked": True,
            "message": (
                f"熔斷模式為 hard，隔離占比 {reject_rate:.1%} 超過軟門檻 {soft_rate:.0%}（{reject_rows}/{total} 列），"
                f"已停止進入 Silver。"
            ),
        }

    if reject_rate > soft_rate:
        return {
            **base,
            "melt_action": "soft",
            "melt_reason": "soft_rate",
            "melted": False,
            "high_reject_rate": True,
            "approve_blocked": True,
            "message": (
                f"隔離占比 {reject_rate:.1%} 超過軟門檻 {soft_rate:.0%}（{reject_rows}/{total} 列）；"
                f"有效 OCR {ok_rows} 列仍進 Silver，但不可核准發行版。"
            ),
        }

    return {
        **base,
        "melt_action": "pass",
        "melt_reason": "within_soft_limit",
        "melted": False,
        "high_reject_rate": False,
        "approve_blocked": False,
        "message": (
            f"隔離 {reject_rows}/{total} 列（{reject_rate:.1%}），在可接受範圍內。"
            if reject_rows
            else "全部列通過 Bronze 隔離檢查。"
        ),
    }


def format_quarantine_notify_body(
    summary: Dict[str, Any],
    *,
    dataset_id: str | None,
) -> str:
    """組裝繁中告警內文。"""
    ds = dataset_id or summary.get("dataset_id") or "—"
    action = summary.get("melt_action") or "?"
    lines = [
        f"資料集：{ds}",
        f"熔斷：{'硬熔斷' if action == 'hard' else '軟熔斷（警告）' if action == 'soft' else action}",
        (
            f"有效 OCR：{summary.get('ok_rows')}/{summary.get('total_rows')} 列"
            f"（隔離 {summary.get('reject_rows')} 列，占比 {float(summary.get('reject_rate') or 0):.1%}）"
        ),
    ]
    by_status = summary.get("by_status") or {}
    if by_status:
        parts = [f"{k}={v}" for k, v in sorted(by_status.items()) if k != "ok"]
        if parts:
            lines.append("隔離原因：" + "、".join(parts))
    msg = str(summary.get("message") or "").strip()
    if msg:
        lines.append(msg)
    if summary.get("approve_blocked"):
        lines.append("發行核准：暫時不可執行 --approve-snapshot。")
    if action == "hard":
        lines.append("請檢查 MinIO 原圖、上傳批次與 Bronze OCR 後再重跑。")
    return "\n".join(lines)


def notify_bronze_quarantine_event(
    summary: Dict[str, Any],
    *,
    dataset_id: str | None,
) -> Optional[Dict[str, Any]]:
    """軟熔斷 WARN、硬熔斷 ALERT；≤10% 不通知。"""
    action = str(summary.get("melt_action") or "pass")
    if action not in ("soft", "hard"):
        return None

    from services.pipeline_notify import NotifyResult, send_pipeline_alert

    if action == "hard":
        title = "[管線] Bronze 硬熔斷"
    else:
        title = "[管線] Bronze 隔離警告（軟熔斷）"

    body = format_quarantine_notify_body(summary, dataset_id=dataset_id)
    result: NotifyResult = send_pipeline_alert(
        title=title,
        body=body,
        dataset_id=str(dataset_id or ""),
    )
    return {
        "backend": result.backend,
        "sent": result.sent,
        "skipped_reason": result.skipped_reason,
        "error": result.error,
    }


def assert_approve_snapshot_allowed(dataset_id: str) -> None:
    """最近一次 Silver ETL 若為軟／硬高隔離，阻擋核准發行。"""
    from services.etl_metrics import read_etl_metrics

    ds = str(dataset_id or "").strip().lower()
    if not ds:
        return

    rows = read_etl_metrics(limit=30, dataset_id=ds, etl_name="silver_ocr_etl")
    if not rows:
        return

    latest = rows[0]
    status = str(latest.get("status") or "")
    bq = latest.get("bronze_quarantine") or {}
    if not isinstance(bq, dict):
        bq = {}

    if status == "bronze_quarantine_failed" or bq.get("melted"):
        rate = bq.get("reject_rate")
        rate_txt = f"{float(rate):.1%}" if rate is not None else "—"
        raise ValueError(
            f"最近一次 Silver ETL 觸發 Bronze 硬熔斷（隔離占比 {rate_txt}），"
            f"請先處理 quarantine 與資料問題後重跑，不可核准發行版。"
        )

    if bq.get("approve_blocked") or bq.get("high_reject_rate"):
        rate = bq.get("reject_rate")
        rate_txt = f"{float(rate):.1%}" if rate is not None else "—"
        raise ValueError(
            f"最近一次 Silver ETL 隔離占比過高（{rate_txt}，軟熔斷），"
            f"有效樣本尚未完整；請修復後重跑 Silver，再執行 --approve-snapshot。"
        )


def write_bronze_quarantine_rows(
    spark: SparkSession,
    quarantine_path: str,
    df_rejects: DataFrame,
    *,
    dataset_id: str | None,
    bronze_path: str,
) -> int:
    """將隔離列 append 至 quarantine Delta 表。"""
    if int(df_rejects.limit(1).count()) == 0:
        return 0

    cols = {c for c in df_rejects.columns}
    select_exprs = [
        col("image_path"),
        col("extracted_text"),
        col("ocr_status"),
        col("latest_ingestion_timestamp").alias("ingestion_timestamp")
        if "latest_ingestion_timestamp" in cols
        else col("ingestion_timestamp"),
    ]
    if "source_bucket" in cols:
        select_exprs.append(col("source_bucket"))
    else:
        select_exprs.append(lit(None).cast(StringType()).alias("source_bucket"))
    if "ocr_signature" in cols:
        select_exprs.append(col("ocr_signature"))
    if "dataset_id" in cols:
        select_exprs.append(col("dataset_id"))
    elif dataset_id:
        select_exprs.append(lit(dataset_id).alias("dataset_id"))

    df_out = (
        df_rejects.select(*select_exprs)
        .withColumn("quarantined_at", current_timestamp())
        .withColumn("bronze_source_path", lit(bronze_path))
    )

    writer = df_out.write.format("delta").mode("append").option("mergeSchema", "true")
    writer.save(quarantine_path)
    return int(df_rejects.count())


def apply_bronze_quarantine_gate(
    spark: SparkSession,
    df_deduped: DataFrame,
    *,
    quarantine_path: str,
    bronze_path: str,
    dataset_id: str | None = None,
) -> tuple[DataFrame, Dict[str, Any]]:
    """
    標記 ocr_status → 寫入 quarantine → 軟／硬熔斷判斷 → 回傳 ok 列與摘要。
    """
    if not BRONZE_QUARANTINE_ENABLED:
        return df_deduped, {
            "skipped": True,
            "reason": "BRONZE_QUARANTINE_ENABLED=false",
            "passed": True,
        }

    df_checked = df_deduped.withColumn("ocr_status", bronze_ocr_status_column())
    summary = summarize_bronze_quarantine(df_checked)
    summary.update(
        {
            "skipped": False,
            "quarantine_path": quarantine_path,
            "dataset_id": dataset_id,
            "evaluated_at": datetime.utcnow().isoformat(),
        }
    )

    df_ok = df_checked.filter(col("ocr_status") == lit("ok")).drop("ocr_status")
    df_rejects = df_checked.filter(col("ocr_status") != lit("ok"))

    written = write_bronze_quarantine_rows(
        spark,
        quarantine_path,
        df_rejects,
        dataset_id=dataset_id,
        bronze_path=bronze_path,
    )
    summary["quarantined_rows_written"] = written

    decision = resolve_melt_decision(summary)
    summary.update(decision)
    summary["passed"] = decision.get("melt_action") != "hard"

    notify_result = notify_bronze_quarantine_event(summary, dataset_id=dataset_id)
    if notify_result is not None:
        summary["notify"] = notify_result

    if decision.get("melt_action") == "hard":
        raise BronzeQuarantineError(
            str(decision.get("message") or "Bronze 硬熔斷，拒絕進入 Silver"),
            report={**summary, "passed": False},
        )

    return df_ok, summary
