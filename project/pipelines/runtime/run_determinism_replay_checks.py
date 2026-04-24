from __future__ import annotations

import argparse
import json
import sys
from typing import Dict, List

from project import PROJECT_ROOT
from project.core.config import get_data_root
from project.io.utils import read_parquet
from project.runtime.replay import determinism_replay_check
from project.specs.invariants import load_runtime_invariants_specs
from project.specs.manifest import finalize_manifest, start_manifest


def main() -> int:
    data_root = get_data_root()
    parser = argparse.ArgumentParser(
        description="Run deterministic replay digest checks on causal ticks."
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--fail_on_mismatch", type=int, default=0)
    args = parser.parse_args()

    runtime_dir = data_root / "runs" / args.run_id / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    ticks_path = runtime_dir / "causal_ticks.parquet"
    report_path = runtime_dir / "determinism_replay.json"

    params = {
        "run_id": str(args.run_id),
        "fail_on_mismatch": bool(int(args.fail_on_mismatch)),
    }
    inputs: List[Dict[str, object]] = [
        {"path": str(ticks_path), "rows": None, "start_ts": None, "end_ts": None}
    ]
    outputs: List[Dict[str, object]] = [
        {"path": str(report_path), "rows": 1, "start_ts": None, "end_ts": None}
    ]
    manifest = start_manifest(
        "run_determinism_replay_checks", str(args.run_id), params, inputs, outputs
    )

    try:
        ticks: List[Dict[str, object]]
        if ticks_path.exists():
            ticks = list(read_parquet(ticks_path).to_dict(orient="records"))
        else:
            ticks = []
        specs = load_runtime_invariants_specs(PROJECT_ROOT.parent)
        out = determinism_replay_check(ticks, hashing_spec=specs["hashing"])
        report = {
            "run_id": str(args.run_id),
            "status": str(out.get("status", "failed")),
            "tick_count": int(out.get("tick_count", 0)),
            "replay_digest": str(out.get("replay_digest", "")),
            "variant_digests": dict(out.get("variant_digests", {})),
            "causal_ticks_path": str(ticks_path),
        }
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

        fail_on_mismatch = bool(int(args.fail_on_mismatch))
        status = str(report["status"]).strip().lower()
        if fail_on_mismatch and status == "failed":
            finalize_manifest(
                manifest,
                "failed",
                error="determinism replay digest mismatch",
                stats={
                    "status": status,
                    "tick_count": int(report["tick_count"]),
                },
            )
            return 1

        finalize_manifest(
            manifest,
            "success",
            stats={
                "status": status,
                "tick_count": int(report["tick_count"]),
            },
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
