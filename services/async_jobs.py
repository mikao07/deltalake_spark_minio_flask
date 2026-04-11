"""
記憶體內背景任務（單機 / 單一 Gunicorn worker 適用）。
長時間 Spark OCR／ETL 改由執行緒執行，HTTP 立即回傳 job_id，再以 GET /api/jobs/<id> 輪詢。

多 worker 時各程序記憶體不共享，輪詢可能打到不同 worker 而 404——請使用 1 worker 或之後改 Redis。
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

ProgressCallback = Callable[[int, int, str], None]
JobTarget = Callable[[ProgressCallback], dict[str, Any]]


@dataclass
class JobRecord:
    job_id: str
    job_type: str
    status: str  # queued | running | succeeded | failed
    step: int = 0
    step_total: int = 0
    message: str = ""
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


class JobRegistry:
    def __init__(self, *, max_jobs: int = 2000) -> None:
        self._jobs: dict[str, JobRecord] = {}
        self._lock = threading.Lock()
        self._max_jobs = max(100, int(max_jobs))

    def create(self, job_type: str, *, step_total: int = 1) -> str:
        jid = str(uuid.uuid4())
        with self._lock:
            self._prune_locked()
            self._jobs[jid] = JobRecord(
                job_id=jid,
                job_type=job_type,
                status="queued",
                step_total=max(1, step_total),
                step=0,
                message="排隊中",
            )
        return jid

    def _prune_locked(self) -> None:
        if len(self._jobs) <= self._max_jobs:
            return
        # 依 updated_at 刪除最舊的一批
        items = sorted(self._jobs.items(), key=lambda x: x[1].updated_at)
        for k, _ in items[: max(1, len(items) - self._max_jobs + 100)]:
            self._jobs.pop(k, None)

    def update(self, job_id: str, **kwargs: Any) -> None:
        with self._lock:
            r = self._jobs.get(job_id)
            if not r:
                return
            for key, val in kwargs.items():
                if hasattr(r, key):
                    setattr(r, key, val)
            r.updated_at = time.time()

    def get(self, job_id: str) -> Optional[JobRecord]:
        with self._lock:
            r = self._jobs.get(job_id)
            if r is None:
                return None
            return JobRecord(
                job_id=r.job_id,
                job_type=r.job_type,
                status=r.status,
                step=r.step,
                step_total=r.step_total,
                message=r.message,
                result=dict(r.result) if r.result else None,
                error=r.error,
                created_at=r.created_at,
                updated_at=r.updated_at,
            )

    def run_async(self, job_id: str, target: JobTarget) -> None:
        def progress(step: int, total: int, message: str) -> None:
            self.update(
                job_id,
                step=step,
                step_total=max(1, total),
                message=message,
                status="running",
            )

        def runner() -> None:
            try:
                self.update(job_id, status="running", message="執行中")
                out = dict(target(progress))
                if "status" not in out:
                    out["status"] = "ok"
                self.update(job_id, status="succeeded", result=out, message="完成", error=None)
            except Exception as e:
                self.update(job_id, status="failed", error=str(e), message="失敗", result=None)

        threading.Thread(target=runner, daemon=True).start()


def job_to_public_dict(r: JobRecord) -> dict[str, Any]:
    return {
        "job_id": r.job_id,
        "job_type": r.job_type,
        "status": r.status,
        "step": r.step,
        "step_total": r.step_total,
        "message": r.message,
        "result": r.result,
        "error": r.error,
        "created_at": r.created_at,
        "updated_at": r.updated_at,
    }


job_registry = JobRegistry()
