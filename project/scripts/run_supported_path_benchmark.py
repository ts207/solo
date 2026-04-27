from __future__ import annotations

import argparse
import contextlib
import io
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from project import PROJECT_ROOT


@dataclass(frozen=True)
class BenchmarkSlice:
    slice_id: str
    event_id: str
    spec_dir: str
    symbol: str = "BTCUSDT"


SUITE: tuple[BenchmarkSlice, ...] = (
    BenchmarkSlice(
        slice_id="price_down_oi_down",
        event_id="PRICE_DOWN_OI_DOWN",
        spec_dir="spec/discovery/benchmark_eligible_v1",
    ),
)


def _json_load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _table_count(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        import pandas as pd

        return len(pd.read_parquet(path))
    except Exception:
        return None


def _run_command(cmd: list[str]) -> dict[str, Any]:
    completed = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT.parent,
        text=True,
        capture_output=True,
    )
    return {
        "command": cmd,
        "returncode": int(completed.returncode),
        "stdout_tail": completed.stdout[-4000:],
        "stderr_tail": completed.stderr[-4000:],
    }


def _candidate_count(data_root: Path, run_id: str) -> dict[str, Any]:
    phase2_dir = data_root / "reports" / "phase2" / run_id
    diagnostics = _json_load(phase2_dir / "phase2_diagnostics.json")
    count = _table_count(phase2_dir / "phase2_candidates.parquet")
    if count is None:
        count = int(
            diagnostics.get("bridge_candidates_rows")
            or diagnostics.get("phase2_candidates_written")
            or 0
        )
    return {"count": count, "diagnostics_present": bool(diagnostics)}


def _validation_metrics(data_root: Path, run_id: str) -> dict[str, Any]:
    validation_dir = data_root / "reports" / "validation" / run_id
    validated = _table_count(validation_dir / "validated_candidates.parquet") or 0
    ready = _table_count(validation_dir / "promotion_ready_candidates.parquet") or 0
    bundle = _json_load(validation_dir / "validation_bundle.json")
    report = _json_load(validation_dir / "validation_report.json")
    denominator = max(validated, 1)
    return {
        "validated_count": validated,
        "promotion_ready_count": ready,
        "pass_rate": ready / denominator,
        "bundle_present": bool(bundle),
        "report_present": bool(report),
    }


def _promotion_metrics(data_root: Path, run_id: str) -> dict[str, Any]:
    promotion_dir = data_root / "reports" / "promotions" / run_id
    diagnostics = _json_load(promotion_dir / "promotion_diagnostics.json")
    promoted_table = _table_count(promotion_dir / "promoted_candidates.parquet")
    promoted = promoted_table
    if promoted is None:
        summary = diagnostics.get("decision_summary", {})
        promoted = int(summary.get("promoted_count") or summary.get("accepted_count") or 0)
    return {
        "promotion_count": int(promoted or 0),
        "diagnostics_present": bool(diagnostics),
    }


def _resolve_generated_proposal(
    generated_proposal_dir: Path,
    event_id: str,
) -> Path | None:
    proposals = sorted(generated_proposal_dir.glob("*.yaml"))
    if not proposals:
        return None

    event_key = event_id.lower()
    matching = [path for path in proposals if event_key in path.name.lower()]
    if matching:
        return matching[0]

    return proposals[0]


def _thesis_metrics(data_root: Path, run_id: str) -> dict[str, Any]:
    thesis_path = data_root / "live" / "theses" / run_id / "promoted_theses.json"
    payload = _json_load(thesis_path)
    theses = payload.get("theses", [])
    if not isinstance(theses, list):
        theses = []
    return {
        "thesis_export_count": len(theses),
        "active_thesis_count": int(payload.get("active_thesis_count") or 0),
        "pending_thesis_count": int(payload.get("pending_thesis_count") or 0),
        "path": str(thesis_path),
        "present": thesis_path.exists(),
        "theses": theses,
    }


def _contract_failures(data_root: Path, run_id: str) -> list[str]:
    failures: list[str] = []
    manifest = _json_load(data_root / "runs" / run_id / "run_manifest.json")
    if manifest:
        status = str(manifest.get("status", "") or "").lower()
        if status and status not in {"success", "complete", "completed"}:
            failures.append(f"run_manifest_status:{status}")
        conformance_status = str(manifest.get("contract_conformance_status", "") or "").lower()
        if conformance_status and conformance_status != "pass":
            failures.append(f"contract_conformance_status:{conformance_status}")
        failed_stage = str(manifest.get("failed_stage", "") or "").strip()
        if failed_stage:
            failures.append(f"failed_stage:{failed_stage}")
    conformance = _json_load(data_root / "runs" / run_id / "execution" / "contract_conformance.json")
    artifact_results = conformance.get("artifact_results")
    if not isinstance(artifact_results, list):
        artifact_results = conformance.get("artifacts")
    for item in artifact_results if isinstance(artifact_results, list) else []:
        if str(item.get("status", "")).lower() in {
            "missing",
            "invalid",
            "failed",
            "schema_violation",
        }:
            failures.append(f"{item.get('contract_id', 'artifact')}:{item.get('status')}")
    return failures


def _runtime_event_count(slice_spec: BenchmarkSlice, *, execute: bool, max_rows: int) -> dict[str, Any]:
    if not execute:
        return {"count": 0, "status": "planned", "reason": "execute_0_no_runtime_scan"}
    try:
        import logging

        logging.disable(logging.WARNING)
        from project.live.event_detector import build_live_event_detection_adapter
    except Exception as exc:
        return {"count": 0, "status": "failed", "reason": f"import_error:{exc}"}

    roots = [
        PROJECT_ROOT.parent / "offline-data" / "cleaned_bars",
        PROJECT_ROOT.parent / "data" / "lake" / "cleaned",
    ]
    files: list[Path] = []
    for root in roots:
        files.extend(sorted(root.rglob(f"bars_{slice_spec.symbol}_5m_*.parquet")))
    if not files:
        return {"count": 0, "status": "missing_data", "reason": "no_cleaned_5m_bars"}

    try:
        import pandas as pd

        frame = pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)
    except Exception as exc:
        return {"count": 0, "status": "failed", "reason": f"load_error:{exc}"}

    if frame.empty:
        return {"count": 0, "status": "empty_data", "reason": "bar_frame_empty"}
    timestamp_col = "timestamp" if "timestamp" in frame.columns else frame.columns[0]
    frame = frame.sort_values(timestamp_col).reset_index(drop=True)
    scanned_rows = len(frame)
    if max_rows > 0 and scanned_rows > max_rows:
        frame = frame.tail(max_rows).reset_index(drop=True)
    close_col = "close" if "close" in frame.columns else "c"
    volume_col = "volume" if "volume" in frame.columns else "v"
    if close_col not in frame.columns:
        return {"count": 0, "status": "failed", "reason": "missing_close"}

    adapter = build_live_event_detection_adapter({"adapter": "governed_runtime_core"})
    count = 0
    previous_close: float | None = None
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        for _, row in frame.iterrows():
            close = float(row.get(close_col) or 0.0)
            events = adapter.detect_events(
                symbol=slice_spec.symbol,
                timeframe="5m",
                current_close=close,
                previous_close=previous_close,
                volume=float(row.get(volume_col) or 0.0) if volume_col in frame.columns else None,
                market_features=row.to_dict(),
                supported_event_ids=[slice_spec.event_id],
            )
            count += sum(1 for event in events if event.event_id == slice_spec.event_id)
            previous_close = close
    return {
        "count": count,
        "status": "ok",
        "rows_scanned": len(frame),
        "rows_available": scanned_rows,
        "source_files": [str(path) for path in files[:8]],
    }


def _portfolio_allocations(thesis_metrics: dict[str, Any]) -> dict[str, Any]:
    theses = thesis_metrics.get("theses", [])
    if not theses:
        return {"allocated_count": 0, "total_allocated_notional": 0.0, "allocations": []}
    try:
        from project.portfolio.engine import PortfolioDecisionEngine, ThesisIntent
    except Exception as exc:
        return {"allocated_count": 0, "total_allocated_notional": 0.0, "error": str(exc)}

    intents: list[ThesisIntent] = []
    for index, thesis in enumerate(theses):
        if not isinstance(thesis, dict):
            continue
        thesis_id = str(thesis.get("thesis_id") or thesis.get("id") or f"thesis_{index}")
        symbol = str(thesis.get("symbol") or "BTCUSDT").upper()
        family = str(
            thesis.get("primary_event_id")
            or thesis.get("event_family")
            or thesis.get("family")
            or "unknown"
        ).upper()
        intents.append(
            ThesisIntent(
                thesis_id=thesis_id,
                symbol=symbol,
                family=family,
                overlap_group_id=f"{symbol}:{family}",
                requested_notional=10_000.0,
                support_score=float(thesis.get("support_score") or thesis.get("score") or 1.0),
                raw=thesis,
            )
        )
    decisions = PortfolioDecisionEngine(max_portfolio_notional=100_000.0).decide(intents)
    allocations = [
        {
            "thesis_id": decision.thesis_id,
            "allocated_notional": decision.allocated_notional,
            "decision_status": decision.decision_status,
            "reasons": list(decision.reasons),
        }
        for decision in decisions
    ]
    return {
        "allocated_count": sum(1 for item in allocations if item["allocated_notional"] > 0.0),
        "total_allocated_notional": sum(float(item["allocated_notional"]) for item in allocations),
        "allocations": allocations,
    }


def _collect_metrics(
    data_root: Path,
    run_id: str,
    slice_spec: BenchmarkSlice,
    *,
    execute: bool,
    runtime_max_rows: int,
) -> dict[str, Any]:
    thesis = _thesis_metrics(data_root, run_id)
    return {
        "candidate_counts": _candidate_count(data_root, run_id),
        "validation": _validation_metrics(data_root, run_id),
        "promotion": _promotion_metrics(data_root, run_id),
        "thesis_export": {
            key: value for key, value in thesis.items() if key != "theses"
        },
        "runtime_events": _runtime_event_count(
            slice_spec,
            execute=execute,
            max_rows=runtime_max_rows,
        ),
        "portfolio_allocations": _portfolio_allocations(thesis),
        "artifact_contract_failures": _contract_failures(data_root, run_id),
    }


def _benchmark_slice(
    *,
    slice_spec: BenchmarkSlice,
    execute: bool,
    run_prefix: str,
    data_root: Path,
    runtime_max_rows: int,
) -> dict[str, Any]:
    run_id = f"{run_prefix}_{slice_spec.slice_id}"
    start_date = "2022-01-01"
    end_date = "2024-12-31"

    generated_proposal_dir = data_root / "reports" / "phase2" / run_id / "generated_proposals"

    commands = [
        [
            sys.executable,
            "-m",
            "project.cli",
            "discover",
            "cells",
            "run",
            "--run_id",
            run_id,
            "--symbols",
            slice_spec.symbol,
            "--start",
            start_date,
            "--end",
            end_date,
            "--data_root",
            str(data_root),
            "--spec_dir",
            slice_spec.spec_dir,
        ],
        [
            sys.executable,
            "-m",
            "project.cli",
            "discover",
            "cells",
            "summarize",
            "--run_id",
            run_id,
            "--data_root",
            str(data_root),
        ],
        [
            sys.executable,
            "-m",
            "project.cli",
            "discover",
            "cells",
            "assemble-theses",
            "--run_id",
            run_id,
            "--data_root",
            str(data_root),
        ],
        [
            sys.executable,
            "-m",
            "project.cli",
            "discover",
            "run",
            "--proposal",
            "RESOLVED_AT_RUNTIME",
            "--run_id",
            run_id,
            "--data_root",
            str(data_root),
            "--promotion_profile",
            "research",
        ],
        [
            sys.executable,
            "-m",
            "project.cli",
            "validate",
            "run",
            "--run_id",
            run_id,
            "--data_root",
            str(data_root),
        ],
        [
            sys.executable,
            "-m",
            "project.cli",
            "promote",
            "run",
            "--run_id",
            run_id,
            "--symbols",
            slice_spec.symbol,
            "--out_dir",
            str(data_root / "reports" / "promotions" / run_id),
        ],
        [
            sys.executable,
            "-m",
            "project.cli",
            "promote",
            "export",
            "--run_id",
            run_id,
            "--data_root",
            str(data_root),
        ],
    ]
    if not execute:
        # Dry-run display indicating dynamic resolution
        placeholder = f"<{generated_proposal_dir}/*.yaml matched on {slice_spec.event_id.lower()}>"
        commands[3][commands[3].index("RESOLVED_AT_RUNTIME")] = placeholder
        return {
            "slice": asdict(slice_spec),
            "run_id": run_id,
            "status": "planned",
            "commands": commands,
            "metrics": _collect_metrics(
                data_root,
                run_id,
                slice_spec,
                execute=False,
                runtime_max_rows=runtime_max_rows,
            ),
        }

    command_results: list[dict[str, Any]] = []
    for index, command in enumerate(commands):
        if index == 3:
            resolved = _resolve_generated_proposal(generated_proposal_dir, slice_spec.event_id)
            if resolved is None:
                command_results.append(
                    {
                        "command": command,
                        "returncode": 1,
                        "stdout_tail": "",
                        "stderr_tail": f"no generated proposals found in: {generated_proposal_dir}",
                        "status": "failed_generated_proposal_missing",
                    }
                )
                break
            command[command.index("RESOLVED_AT_RUNTIME")] = str(resolved)

        if index == 6:
            promotion_count = _promotion_metrics(data_root, run_id)["promotion_count"]
            if promotion_count <= 0:
                command_results.append(
                    {
                        "command": command,
                        "returncode": 0,
                        "stdout_tail": "",
                        "stderr_tail": "",
                        "status": "skipped_no_promotions",
                    }
                )
                continue
        command_results.append(_run_command(command))
        if command_results[-1]["returncode"] != 0:
            break
    metrics = _collect_metrics(
        data_root,
        run_id,
        slice_spec,
        execute=True,
        runtime_max_rows=runtime_max_rows,
    )
    failures = list(metrics["artifact_contract_failures"])
    failures.extend(
        f"command_{index}:{result['returncode']}"
        for index, result in enumerate(command_results)
        if int(result["returncode"]) != 0
    )
    return {
        "slice": asdict(slice_spec),
        "run_id": run_id,
        "status": "failed" if failures else "completed",
        "commands": command_results,
        "metrics": metrics,
        "failures": failures,
    }


def _write_markdown(report: dict[str, Any], path: Path) -> None:
    lines = [
        "# Supported Path Benchmark",
        "",
        f"- mode: `{report['mode']}`",
        f"- run_prefix: `{report['run_prefix']}`",
        f"- status: `{report['status']}`",
        "",
        "| slice | event | candidates | pass_rate | promotions | theses | runtime_events | allocations | failures |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for item in report["slices"]:
        metrics = item["metrics"]
        failures = item.get("failures") or metrics.get("artifact_contract_failures") or []
        lines.append(
            "| {slice_id} | {event_id} | {candidates} | {pass_rate:.3f} | {promotions} | {theses} | {runtime_events} | {allocations} | {failures} |".format(
                slice_id=item["slice"]["slice_id"],
                event_id=item["slice"]["event_id"],
                candidates=metrics["candidate_counts"]["count"],
                pass_rate=float(metrics["validation"]["pass_rate"]),
                promotions=metrics["promotion"]["promotion_count"],
                theses=metrics["thesis_export"]["thesis_export_count"],
                runtime_events=metrics["runtime_events"]["count"],
                allocations=metrics["portfolio_allocations"]["allocated_count"],
                failures=", ".join(failures) if failures else "",
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the current supported-path benchmark suite.")
    parser.add_argument("--execute", type=int, choices=[0, 1], default=0)
    parser.add_argument("--data_root", default=str(PROJECT_ROOT.parent / "data"))
    parser.add_argument("--out_dir", default=str(PROJECT_ROOT.parent / "data" / "reports" / "benchmarks" / "supported_path"))
    parser.add_argument("--runtime_max_rows", type=int, default=500)
    args = parser.parse_args(argv)

    execute = bool(args.execute)
    data_root = Path(args.data_root)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_prefix = f"supported_path_{stamp}"
    out_dir = Path(args.out_dir) / run_prefix
    out_dir.mkdir(parents=True, exist_ok=True)

    slices = [
        _benchmark_slice(
            slice_spec=slice_spec,
            execute=execute,
            run_prefix=run_prefix,
            data_root=data_root,
            runtime_max_rows=args.runtime_max_rows,
        )
        for slice_spec in SUITE
    ]
    failures = [
        failure
        for item in slices
        for failure in (
            item.get("failures")
            or item.get("metrics", {}).get("artifact_contract_failures")
            or []
        )
    ]
    report = {
        "schema_version": "supported_path_benchmark_v1",
        "mode": "execute" if execute else "dry_run",
        "run_prefix": run_prefix,
        "status": "failed" if failures and execute else "completed",
        "suite": [asdict(item) for item in SUITE],
        "slices": slices,
        "failures": failures,
    }
    json_path = out_dir / "supported_path_benchmark.json"
    md_path = out_dir / "supported_path_benchmark.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
    _write_markdown(report, md_path)
    print(json.dumps({"report": str(json_path), "summary": str(md_path), "status": report["status"]}, indent=2))
    return 1 if execute and failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
