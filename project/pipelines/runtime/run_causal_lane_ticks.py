from __future__ import annotations

import argparse
import json
import sys
from typing import Dict, List

import pandas as pd

from project import PROJECT_ROOT
from project.core.config import get_data_root
from project.io.utils import read_parquet, write_parquet
from project.runtime.lane_runner import run_causal_lane_ticks
from project.runtime.normalized_event import normalized_events_from_frame
from project.specs.invariants import load_runtime_invariants_specs
from project.specs.manifest import finalize_manifest, start_manifest


def main() -> int:
    data_root = get_data_root()
    parser = argparse.ArgumentParser(
        description="Run causal lane ticks and watermark/firewall audits."
    )
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()

    runtime_dir = data_root / "runs" / args.run_id / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    normalized_path = runtime_dir / "normalized_events.parquet"
    ticks_path = runtime_dir / "causal_ticks.parquet"
    audit_path = runtime_dir / "causal_ticks_audit.json"

    params = {"run_id": str(args.run_id)}
    inputs: List[Dict[str, object]] = [
        {"path": str(normalized_path), "rows": None, "start_ts": None, "end_ts": None}
    ]
    outputs: List[Dict[str, object]] = [
        {"path": str(ticks_path), "rows": None, "start_ts": None, "end_ts": None},
        {"path": str(audit_path), "rows": 1, "start_ts": None, "end_ts": None},
    ]
    manifest = start_manifest("run_causal_lane_ticks", str(args.run_id), params, inputs, outputs)

    try:
        if normalized_path.exists():
            normalized_df = read_parquet(normalized_path)
            normalized_events = normalized_events_from_frame(normalized_df)
        else:
            normalized_events = []
        specs = load_runtime_invariants_specs(PROJECT_ROOT.parent)
        out = run_causal_lane_ticks(
            normalized_events,
            lanes_spec=specs["lanes"],
            firewall_spec=specs["firewall"],
            hashing_spec=specs["hashing"],
        )
        ticks = list(out.get("ticks", []))
        write_parquet(pd.DataFrame(ticks), ticks_path)

        audit = {
            "run_id": str(args.run_id),
            "status": "no_runtime_events"
            if not normalized_events
            else str(out.get("status", "failed")),
            "tick_count": int(out.get("tick_count", 0)),
            "watermark_violation_count": int(out.get("watermark_violation_count", 0)),
            "watermark_violations_by_type": dict(out.get("watermark_violations_by_type", {})),
            "watermark_violation_examples": list(out.get("watermark_violation_examples", [])),
            "firewall_violation_count": int(out.get("firewall_violation_count", 0)),
            "firewall_violations_by_type": dict(out.get("firewall_violations_by_type", {})),
            "firewall_violation_examples": list(out.get("firewall_violation_examples", [])),
            "max_observed_lag_us": int(out.get("max_observed_lag_us", 0)),
            "replay_digest": str(out.get("replay_digest", "")),
            "normalized_event_count": int(len(normalized_events)),
            "causal_ticks_path": str(ticks_path),
        }
        audit_path.write_text(json.dumps(audit, indent=2, sort_keys=True), encoding="utf-8")
        finalize_manifest(
            manifest,
            "success",
            stats={
                "status": audit["status"],
                "tick_count": int(audit["tick_count"]),
                "watermark_violation_count": int(audit["watermark_violation_count"]),
                "firewall_violation_count": int(audit["firewall_violation_count"]),
            },
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
