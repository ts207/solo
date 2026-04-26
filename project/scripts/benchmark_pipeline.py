from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from statistics import median

from project.core.config import get_data_root
from project.core.exceptions import DataIntegrityError

DATA_ROOT = get_data_root()


def _load_manifest(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise DataIntegrityError(f"Failed to read benchmark pipeline manifest {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise DataIntegrityError(f"Benchmark pipeline manifest {path} did not contain an object payload")
    return payload if isinstance(payload, dict) else {}


def _collect_run_ids(explicit: list[str]) -> list[str]:
    if explicit:
        return [str(r).strip() for r in explicit if str(r).strip()]
    runs_root = DATA_ROOT / "runs"
    if not runs_root.exists():
        return []
    out: list[str] = []
    for run_dir in sorted(p for p in runs_root.iterdir() if p.is_dir()):
        if (run_dir / "run_manifest.json").exists():
            out.append(run_dir.name)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark pipeline stage timings from run manifests."
    )
    parser.add_argument(
        "--run_id", action="append", default=[], help="Run ID to include (repeatable)."
    )
    parser.add_argument(
        "--out_dir",
        default=None,
        help="Output directory for benchmark artifacts. Defaults to data/reports/perf_benchmarks/<timestamp>.",
    )
    args = parser.parse_args()

    run_ids = _collect_run_ids(args.run_id)
    if not run_ids:
        print("No run manifests found.")
        return 1

    benchmark_id = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    out_dir = (
        Path(args.out_dir)
        if args.out_dir
        else DATA_ROOT / "reports" / "perf_benchmarks" / benchmark_id
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    stage_rows: list[dict[str, object]] = []
    per_stage: dict[str, list[float]] = defaultdict(list)
    run_summaries: list[dict[str, object]] = []

    for run_id in run_ids:
        manifest_path = DATA_ROOT / "runs" / run_id / "run_manifest.json"
        manifest = _load_manifest(manifest_path)
        if not manifest:
            continue
        stage_timings = manifest.get("stage_timings_sec", {})
        if not isinstance(stage_timings, dict):
            stage_timings = {}
        total = 0.0
        for stage_name, raw in stage_timings.items():
            try:
                duration = float(raw)
            except (TypeError, ValueError):
                continue
            total += duration
            per_stage[str(stage_name)].append(duration)
            stage_rows.append(
                {
                    "run_id": run_id,
                    "stage": str(stage_name),
                    "duration_sec": round(duration, 3),
                    "status": str(manifest.get("status", "")),
                }
            )
        run_summaries.append(
            {
                "run_id": run_id,
                "status": str(manifest.get("status", "")),
                "failed_stage": str(manifest.get("failed_stage", "")),
                "total_stage_time_sec": round(total, 3),
                "stage_count": len(stage_timings),
                "symbols": list(manifest.get("symbols", []))
                if isinstance(manifest.get("symbols"), list)
                else [],
                "start": str(manifest.get("start", "")),
                "end": str(manifest.get("end", "")),
            }
        )

    stage_summary = []
    for stage_name, values in sorted(per_stage.items(), key=lambda kv: sum(kv[1]), reverse=True):
        stage_summary.append(
            {
                "stage": stage_name,
                "runs": len(values),
                "total_sec": round(sum(values), 3),
                "median_sec": round(float(median(values)), 3),
                "max_sec": round(max(values), 3),
            }
        )

    summary = {
        "benchmark_id": benchmark_id,
        "created_at_utc": datetime.now(UTC).isoformat(),
        "data_root": str(DATA_ROOT),
        "run_count": len(run_summaries),
        "runs": run_summaries,
        "stage_summary": stage_summary,
    }

    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8"
    )
    with (out_dir / "stages.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["run_id", "stage", "duration_sec", "status"])
        writer.writeheader()
        writer.writerows(stage_rows)
    with (out_dir / "runs.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "run_id",
                "status",
                "failed_stage",
                "total_stage_time_sec",
                "stage_count",
                "symbols",
                "start",
                "end",
            ],
        )
        writer.writeheader()
        for row in run_summaries:
            row_copy = dict(row)
            row_copy["symbols"] = ",".join(str(x) for x in row.get("symbols", []))
            writer.writerow(row_copy)

    print(f"[bench] wrote summary to {out_dir / 'summary.json'}")
    print(f"[bench] wrote stage rows to {out_dir / 'stages.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
