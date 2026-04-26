from __future__ import annotations

import argparse
import json
import sys

import pandas as pd

from project import PROJECT_ROOT
from project.core.config import get_data_root
from project.io.utils import read_parquet, write_parquet
from project.runtime.normalized_event import normalized_events_from_frame
from project.runtime.oms_replay import audit_oms_replay
from project.specs.invariants import load_runtime_invariants_specs
from project.specs.manifest import finalize_manifest, start_manifest


def main() -> int:
    data_root = get_data_root()
    parser = argparse.ArgumentParser(
        description="Run OMS replay state-machine validation on normalized events."
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--fail_on_violations", type=int, default=0)
    args = parser.parse_args()

    runtime_dir = data_root / "runs" / args.run_id / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    normalized_path = runtime_dir / "normalized_events.parquet"
    replay_rows_path = runtime_dir / "oms_replay_rows.parquet"
    report_path = runtime_dir / "oms_replay_validation.json"

    params = {
        "run_id": str(args.run_id),
        "fail_on_violations": bool(int(args.fail_on_violations)),
    }
    inputs: list[dict[str, object]] = [
        {"path": str(normalized_path), "rows": None, "start_ts": None, "end_ts": None}
    ]
    outputs: list[dict[str, object]] = [
        {"path": str(replay_rows_path), "rows": None, "start_ts": None, "end_ts": None},
        {"path": str(report_path), "rows": 1, "start_ts": None, "end_ts": None},
    ]
    manifest = start_manifest(
        "run_oms_replay_validation", str(args.run_id), params, inputs, outputs
    )

    try:
        if normalized_path.exists():
            normalized_events = normalized_events_from_frame(read_parquet(normalized_path))
        else:
            normalized_events = []

        specs = load_runtime_invariants_specs(PROJECT_ROOT.parent)
        out = audit_oms_replay(normalized_events, hashing_spec=specs["hashing"])
        replay_rows = list(out.get("replay_rows", []))
        write_parquet(pd.DataFrame(replay_rows), replay_rows_path)

        report = {
            "run_id": str(args.run_id),
            "status": str(out.get("status", "failed")),
            "execution_event_count": int(out.get("execution_event_count", 0)),
            "order_count": int(out.get("order_count", 0)),
            "violation_count": int(out.get("violation_count", 0)),
            "violations_by_type": dict(out.get("violations_by_type", {})),
            "violation_examples": list(out.get("violation_examples", [])),
            "replay_digest": str(out.get("replay_digest", "")),
            "replay_rows_path": str(replay_rows_path),
        }
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

        fail_on_violations = bool(int(args.fail_on_violations))
        if fail_on_violations and int(report["violation_count"]) > 0:
            finalize_manifest(
                manifest,
                "failed",
                error="OMS replay validation failed",
                stats={
                    "status": report["status"],
                    "violation_count": int(report["violation_count"]),
                },
            )
            return 1

        finalize_manifest(
            manifest,
            "success",
            stats={
                "status": report["status"],
                "execution_event_count": int(report["execution_event_count"]),
                "order_count": int(report["order_count"]),
                "violation_count": int(report["violation_count"]),
            },
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
