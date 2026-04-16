from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

_LOCK = threading.Lock()


def _metrics_file_path() -> Path:
    raw = (os.getenv("ETL_METRICS_FILE", "var/etl_metrics.jsonl") or "").strip()
    p = Path(raw)
    if not p.is_absolute():
        p = Path.cwd() / p
    return p


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_etl_metric(payload: Dict[str, Any]) -> None:
    """
    將一筆 ETL 指標落地為 JSONL。
    任何寫入失敗都拋出例外，由呼叫端決定是否吞掉。
    """
    row = dict(payload)
    row.setdefault("recorded_at", _now_iso())
    fp = _metrics_file_path()
    fp.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(row, ensure_ascii=False)
    with _LOCK:
        with fp.open("a", encoding="utf-8") as f:
            f.write(line + "\n")


def read_etl_metrics(
    *,
    limit: int = 100,
    dataset_id: Optional[str] = None,
    etl_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    讀取最新 ETL 指標（依 recorded_at 逆序）。
    """
    lim = max(1, min(int(limit), 1000))
    fp = _metrics_file_path()
    if not fp.exists():
        return []

    rows: List[Dict[str, Any]] = []
    with _LOCK:
        with fp.open("r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                try:
                    item = json.loads(s)
                except Exception:
                    continue
                if dataset_id and str(item.get("dataset_id") or "") != dataset_id:
                    continue
                if etl_name and str(item.get("etl_name") or "") != etl_name:
                    continue
                rows.append(item)

    rows.sort(key=lambda x: str(x.get("recorded_at") or ""), reverse=True)
    return rows[:lim]
