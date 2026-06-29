#!/usr/bin/env python3
"""
管線守護神 CLI。

用法：
  python scripts/pipeline_guardian.py --dataset drinks
  python scripts/pipeline_guardian.py --dataset drinks --offline
  python scripts/pipeline_guardian.py --dataset drinks --print-hashes
  python scripts/pipeline_guardian.py --dataset drinks --approve-snapshot
  python scripts/pipeline_guardian.py --dataset drinks --revoke-snapshot
  python scripts/pipeline_guardian.py --dataset drinks --strict --json

exit code：0=PASS，1=FAIL，2=僅 WARN
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from services.pipeline_guardian import (  # noqa: E402
    build_hash_bootstrap_manifest,
    format_report_text,
    resolve_manifest_path,
    run_audit,
    stamp_approved_snapshot,
    revoke_approved_snapshot,
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pipeline Guardian — 管線版本與辭典漂移稽核")
    p.add_argument("--dataset", "-d", default="drinks", help="dataset_id（預設 drinks）")
    p.add_argument("--manifest", type=Path, default=None, help="manifest 路徑（預設 manifests/{dataset}.json）")
    p.add_argument("--offline", action="store_true", help="不連 Spark／Delta，僅檢查 runtime + 辭典 hash")
    p.add_argument("--strict", action="store_true", help="WARN 也視為失敗（exit 1）")
    p.add_argument("--json", dest="as_json", action="store_true", help="JSON 輸出")
    p.add_argument(
        "--print-hashes",
        action="store_true",
        help="印出目前 lexicon hash／版本（更新 manifest 用）",
    )
    p.add_argument(
        "--approve-snapshot",
        action="store_true",
        help="將符合 manifest 的 topic_snapshot 寫入 gold.approved_snapshot_at",
    )
    p.add_argument(
        "--snapshot-at",
        default=None,
        help="搭配 --approve-snapshot：指定 snapshot_at ISO（預設自動選最新符合者）",
    )
    p.add_argument(
        "--revoke-snapshot",
        action="store_true",
        help="撤回發行版（清除 manifest approved_snapshot_at／processed_image_count；不刪 Delta）",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    dataset = str(args.dataset).strip().lower()

    spark = None
    if not args.offline and not args.print_hashes:
        try:
            from services.spark_service import SparkManager

            spark = SparkManager(app_name="PipelineGuardian").spark
        except Exception as e:
            print(f"無法建立 SparkSession，改為 offline 模式: {e}", file=sys.stderr)
            args.offline = True

    if args.approve_snapshot and args.revoke_snapshot:
        print("--approve-snapshot 與 --revoke-snapshot 不可同時使用", file=sys.stderr)
        return 1

    if args.revoke_snapshot:
        manifest_path = args.manifest or resolve_manifest_path(dataset)
        try:
            result = revoke_approved_snapshot(dataset, manifest_path=manifest_path)
        except (FileNotFoundError, ValueError, OSError) as e:
            print(str(e), file=sys.stderr)
            return 1
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.approve_snapshot:
        if args.offline or spark is None:
            try:
                from services.spark_service import SparkManager

                spark = SparkManager(app_name="PipelineGuardian").spark
            except Exception as e:
                print(f"--approve-snapshot 需要 Spark：{e}", file=sys.stderr)
                return 1
        manifest_path = args.manifest or resolve_manifest_path(dataset)
        try:
            result = stamp_approved_snapshot(
                dataset,
                manifest_path=manifest_path,
                snapshot_at_iso=args.snapshot_at,
                spark=spark,
            )
        except ValueError as e:
            print(str(e), file=sys.stderr)
            return 1
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0

    if args.print_hashes:
        payload = build_hash_bootstrap_manifest(dataset, spark=spark)
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0

    manifest_path = args.manifest or resolve_manifest_path(dataset)
    if not manifest_path.is_file():
        print(
            f"找不到 manifest: {manifest_path}\n"
            f"請先執行: python scripts/pipeline_guardian.py --dataset {dataset} --print-hashes",
            file=sys.stderr,
        )
        return 1

    report = run_audit(dataset, manifest_path=manifest_path, spark=spark, offline=args.offline)

    if args.as_json:
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))
    else:
        print(format_report_text(report))

    return report.exit_code(strict=args.strict)


if __name__ == "__main__":
    raise SystemExit(main())
