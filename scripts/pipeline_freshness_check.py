#!/usr/bin/env python3
"""
外部探針：管線新鮮度檢查，寫入 var/pipeline_heartbeat.json。

建議 cron（與 /ready、pipeline_guardian 併跑）：
  python scripts/pipeline_freshness_check.py drinks
  python scripts/pipeline_freshness_check.py drinks --strict
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from config import FRESHNESS_STALE_HOURS  # noqa: E402
from services.pipeline_freshness import run_freshness_check  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="管線新鮮度探針（條件式滯後 + heartbeat）")
    parser.add_argument("dataset_id", nargs="?", default="drinks", help="dataset_id（預設 drinks）")
    parser.add_argument(
        "--offline",
        action="store_true",
        help="跳過 Silver Delta 計數（僅比對 MinIO raw 與 manifest 水位）",
    )
    parser.add_argument(
        "--stale-hours",
        type=float,
        default=None,
        help=f"滯後門檻小時數（預設 {FRESHNESS_STALE_HOURS}）",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="有 alert 時 exit 1",
    )
    args = parser.parse_args(argv)

    stale = float(args.stale_hours) if args.stale_hours is not None else FRESHNESS_STALE_HOURS
    payload = run_freshness_check(
        args.dataset_id,
        offline=args.offline,
        stale_hours=stale,
    )

    for alert in payload.get("alerts") or []:
        print(f"[ALERT] {alert.get('code')}: {alert.get('message')}", file=sys.stderr)

    counts = payload.get("counts") or {}
    print(
        f"freshness ok={payload.get('ok')} dataset={payload.get('dataset_id')} "
        f"raw={counts.get('raw_image_count')} silver={counts.get('silver_image_count')} "
        f"release={counts.get('release_image_count')}"
    )

    if args.strict and not payload.get("ok"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
