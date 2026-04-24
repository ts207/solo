from __future__ import annotations

import argparse
import json
import sys
from typing import Dict, List

import pandas as pd

from project.core.config import get_data_root
from project.io.runtime_adapter import read_raw_event_rows
from project.io.utils import write_parquet
from project.runtime.normalized_event import events_to_records, normalize_event_rows
from project.specs.manifest import finalize_manifest, start_manifest


def main() -> int:
    data_root = get_data_root()
    parser = argparse.ArgumentParser(
        description="Normalize runtime events into deterministic replay records."
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--max_events", type=int, default=250_000)
    args = parser.parse_args()

    runtime_dir = data_root / "runs" / args.run_id / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    normalized_path = runtime_dir / "normalized_events.parquet"
    summary_path = runtime_dir / "normalized_stream.json"

    params = {
        "run_id": str(args.run_id),
        "max_events": int(args.max_events),
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = [
        {"path": str(normalized_path), "rows": None, "start_ts": None, "end_ts": None},
        {"path": str(summary_path), "rows": 1, "start_ts": None, "end_ts": None},
    ]
    manifest = start_manifest(
        "build_normalized_replay_stream",
        str(args.run_id),
        params,
        inputs,
        outputs,
    )

    try:
        rows, source_path = read_raw_event_rows(data_root=data_root, run_id=str(args.run_id))
        if source_path:
            inputs.append(
                {"path": str(source_path), "rows": int(len(rows)), "start_ts": None, "end_ts": None}
            )

        normalized, issues = normalize_event_rows(rows, max_events=int(args.max_events))
        normalized_records = events_to_records(normalized)
        write_parquet(pd.DataFrame(normalized_records), normalized_path)

        summary = {
            "run_id": str(args.run_id),
            "status": "no_runtime_events" if not normalized_records else "pass",
            "event_source_path": str(source_path),
            "event_count": int(len(rows)),
            "normalized_event_count": int(len(normalized_records)),
            "normalization_issue_count": int(len(issues)),
            "normalization_issue_examples": list(issues[:20]),
            "normalized_events_path": str(normalized_path),
        }
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        finalize_manifest(
            manifest,
            "success",
            stats={
                "status": summary["status"],
                "event_count": int(summary["event_count"]),
                "normalized_event_count": int(summary["normalized_event_count"]),
                "normalization_issue_count": int(summary["normalization_issue_count"]),
            },
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
