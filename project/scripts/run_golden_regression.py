from __future__ import annotations
from project.core.config import get_data_root

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict
from project import PROJECT_ROOT

REPO_ROOT = PROJECT_ROOT.parent
DATA_ROOT = get_data_root()

from project.core.golden_regression import (
    collect_core_artifact_snapshot,
    compare_golden_snapshots,
    load_tolerance_config,
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing baseline snapshot: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Baseline snapshot must be a JSON object: {path}")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build and compare golden-run snapshots for core pipeline artifacts."
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument(
        "--data_root",
        default=str(DATA_ROOT),
        help="Data root path (default: BACKTEST_DATA_ROOT or repo/data).",
    )
    parser.add_argument(
        "--baseline_snapshot",
        default="",
        help="Optional baseline snapshot JSON path. If omitted, snapshot is generated only.",
    )
    parser.add_argument(
        "--tolerance_spec",
        default=str(REPO_ROOT / "spec" / "benchmarks" / "golden_regression_tolerances.yaml"),
        help="YAML file with numeric tolerances.",
    )
    parser.add_argument(
        "--out_dir",
        default="",
        help="Output directory (default: data/reports/golden_regression/<run_id>).",
    )
    parser.add_argument(
        "--snapshot_out",
        default="",
        help="Optional output path for generated snapshot JSON.",
    )
    parser.add_argument(
        "--report_out",
        default="",
        help="Optional output path for regression report JSON.",
    )
    args = parser.parse_args()

    data_root = Path(args.data_root).resolve()
    out_dir = (
        Path(args.out_dir).resolve()
        if str(args.out_dir).strip()
        else data_root / "reports" / "golden_regression" / args.run_id
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    snapshot_out = (
        Path(args.snapshot_out).resolve()
        if str(args.snapshot_out).strip()
        else out_dir / "snapshot.json"
    )
    report_out = (
        Path(args.report_out).resolve()
        if str(args.report_out).strip()
        else out_dir / "regression_report.json"
    )

    snapshot = collect_core_artifact_snapshot(data_root=data_root, run_id=args.run_id)
    snapshot_out.write_text(json.dumps(snapshot, indent=2, sort_keys=True), encoding="utf-8")

    baseline_path = str(args.baseline_snapshot).strip()
    if not baseline_path:
        report = {
            "run_id": args.run_id,
            "created_at_utc": _utc_now_iso(),
            "mode": "snapshot_only",
            "passed": True,
            "snapshot_path": str(snapshot_out),
            "baseline_snapshot": "",
            "diff_count": 0,
            "diffs": [],
        }
        report_out.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        print(
            json.dumps(
                {
                    "run_id": args.run_id,
                    "mode": "snapshot_only",
                    "snapshot": str(snapshot_out),
                    "report": str(report_out),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    baseline_snapshot = _read_json(Path(baseline_path).resolve())
    tolerance_cfg = load_tolerance_config(Path(args.tolerance_spec).resolve())
    comparison = compare_golden_snapshots(
        baseline=baseline_snapshot,
        candidate=snapshot,
        tolerance=tolerance_cfg,
    )
    report = {
        "run_id": args.run_id,
        "created_at_utc": _utc_now_iso(),
        "mode": "compare",
        "passed": bool(comparison.get("passed", False)),
        "baseline_snapshot": str(Path(baseline_path).resolve()),
        "snapshot_path": str(snapshot_out),
        "tolerance_spec": str(Path(args.tolerance_spec).resolve()),
        "checked_metric_count": int(comparison.get("checked_metric_count", 0)),
        "diff_count": int(comparison.get("diff_count", 0)),
        "diffs": list(comparison.get("diffs", [])),
    }
    report_out.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(
        json.dumps(
            {
                "run_id": args.run_id,
                "mode": "compare",
                "passed": bool(report["passed"]),
                "diff_count": int(report["diff_count"]),
                "snapshot": str(snapshot_out),
                "report": str(report_out),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if bool(report["passed"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
