#!/usr/bin/env python3
"""
外部管線探針（P2）：/ready + 守護神 + 新鮮度；失敗時可發 Discord／LINE Notify。

建議 cron（每 6～12h）：
  python scripts/pipeline_probe.py drinks --strict

Docker：
  docker compose exec web python scripts/pipeline_probe.py drinks --strict

Windows 排程（本機）：
  .\scripts\run_pipeline_probe.ps1
  詳見 docs/架構與維運手冊.md §5.4

環境變數：
  PIPELINE_NOTIFY_BACKEND=none|discord|line_notify
  DISCORD_WEBHOOK_URL 或 PIPELINE_NOTIFY_WEBHOOK_URL
  LINE_NOTIFY_TOKEN
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from services.pipeline_probe import run_pipeline_probe  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="管線外部探針（ready + guardian + freshness + notify）")
    parser.add_argument("dataset_id", nargs="?", default="drinks")
    parser.add_argument("--strict", action="store_true", help="守護神 WARN 也視為失敗")
    parser.add_argument("--offline", action="store_true", help="守護神／新鮮度 offline（略過部分 Delta）")
    parser.add_argument("--no-notify", action="store_true", help="不發送 webhook／LINE")
    parser.add_argument("--json", dest="as_json", action="store_true", help="JSON 輸出")
    parser.add_argument(
        "--ready-url",
        default=None,
        help="改以 HTTP 檢查 /ready（預設本機 build_ready_payload）",
    )
    args = parser.parse_args(argv)

    result = run_pipeline_probe(
        args.dataset_id,
        strict=args.strict,
        offline=args.offline,
        notify=not args.no_notify,
        ready_url=args.ready_url,
    )

    if args.as_json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print(f"probe ok={result.get('ok')} dataset={result.get('dataset_id')} issues={len(result.get('issues') or [])}")
        for item in result.get("issues") or []:
            print(
                f"  [{item.get('source')}] {item.get('level')}: {item.get('message')}",
                file=sys.stderr,
            )
        notify = result.get("notify") or {}
        if notify.get("sent"):
            print(f"notify sent via {notify.get('backend')}")
        elif notify.get("skipped_reason"):
            print(f"notify skipped: {notify.get('skipped_reason')}")

    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
