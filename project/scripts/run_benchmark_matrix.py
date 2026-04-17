import re
import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import yaml

from project.pipelines.pipeline_defaults import DATA_ROOT
from project import PROJECT_ROOT

try:
    import pandas as pd
except ImportError:
    pd = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CANONICAL_SYMBOL_RE = re.compile(r"^[A-Z0-9]+USDT$")
SLICE_REQUIRED_FIELDS = ["id", "label", "symbols", "timeframe", "start", "end", "search_spec"]


def write_run_matrix_summary_report(
    *,
    out_dir: str | Path,
    baseline_run_id: str,
    candidate_run_ids: List[str] | None = None,
    data_root: Path | None = None,
    thresholds: Dict[str, Any] | None = None,
    drift_mode: str = "warn",
) -> Path:
    from project.research.services.run_comparison_service import (
        write_run_matrix_summary_report as _write_run_matrix_summary_report,
    )

    resolved_out_dir = Path(out_dir)
    resolved_out_dir.mkdir(parents=True, exist_ok=True)
    resolved_data_root = Path(data_root) if data_root is not None else DATA_ROOT
    candidate_ids = [
        str(run_id).strip()
        for run_id in (candidate_run_ids or [])
        if str(run_id).strip() and str(run_id).strip() != str(baseline_run_id).strip()
    ]
    if baseline_run_id != "base" and candidate_ids:
        try:
            return _write_run_matrix_summary_report(
                data_root=resolved_data_root,
                baseline_run_id=baseline_run_id,
                candidate_run_ids=candidate_ids,
                out_dir=resolved_out_dir,
                thresholds=thresholds,
                drift_mode=drift_mode,
            )
        except FileNotFoundError:
            logger.warning("Benchmark matrix summary fallback triggered for baseline %s", baseline_run_id)

    payload = {
        "baseline_run_id": str(baseline_run_id),
        "candidate_run_ids": candidate_ids,
        "drift_mode": str(drift_mode),
        "status": "comparison_unavailable",
    }
    out_path = resolved_out_dir / "research_run_matrix_summary.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    (resolved_out_dir / "research_run_matrix_summary.md").write_text(
        "# Research Run Matrix Summary\n\n"
        f"- baseline_run_id: `{baseline_run_id}`\n"
        f"- candidate_run_count: `{len(candidate_ids)}`\n"
        "- status: `comparison_unavailable`\n",
        encoding="utf-8",
    )
    return out_path


def write_live_data_foundation_report(
    *,
    run_id: str,
    symbol: str,
    timeframe: str,
    data_root: Path | None = None,
    market: str = "perp",
    feature_schema_version: str = "v2",
    config_path: str | Path | None = None,
    out_dir: str | Path | None = None,
) -> Path:
    from project.research.services.live_data_foundation_service import (
        write_live_data_foundation_report as _write_live_data_foundation_report,
    )

    return _write_live_data_foundation_report(
        data_root=Path(data_root) if data_root is not None else DATA_ROOT,
        run_id=run_id,
        symbol=symbol,
        timeframe=timeframe,
        market=market,
        feature_schema_version=feature_schema_version,
        config_path=Path(config_path) if config_path else None,
        out_dir=Path(out_dir) if out_dir else None,
    )


def build_context_mode_comparison_payload(
    *,
    run_id: str,
    symbols: List[str],
    timeframe: str,
    data_root: Path | None = None,
    min_sample_size: int = 30,
    search_space_path: str | Path | None = None,
) -> Dict[str, Any]:
    from project.research.services.context_mode_comparison_service import (
        build_context_mode_comparison_payload as _build_context_mode_comparison_payload,
    )

    return _build_context_mode_comparison_payload(
        data_root=Path(data_root) if data_root is not None else DATA_ROOT,
        run_id=run_id,
        symbols=symbols,
        timeframe=timeframe,
        min_sample_size=min_sample_size,
        search_space_path=Path(search_space_path) if search_space_path else None,
    )


def write_context_mode_comparison_report(*, out_path: str | Path, comparison: Dict[str, Any]) -> Path:
    from project.research.services.context_mode_comparison_service import (
        write_context_mode_comparison_report as _write_context_mode_comparison_report,
    )

    return _write_context_mode_comparison_report(
        out_path=Path(out_path),
        comparison=comparison,
    )


def load_yaml(path: Path) -> Dict[str, Any]:
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def _load_preset(preset_name: str) -> Dict[str, Any]:
    preset_path = PROJECT_ROOT / "configs" / "benchmarks" / "discovery" / f"{preset_name}.yaml"
    if not preset_path.exists():
        raise FileNotFoundError(f"Preset config not found: {preset_path}")
    return load_yaml(preset_path)


def _load_slice(slice_filename: str, preset_dir: Path) -> Dict[str, Any]:
    slice_path = preset_dir / slice_filename
    if not slice_path.exists():
        raise FileNotFoundError(f"Slice config not found: {slice_path}")
    return load_yaml(slice_path)


def _validate_slice(slice_cfg: Dict[str, Any], slice_file: str) -> None:
    for field in SLICE_REQUIRED_FIELDS:
        if field not in slice_cfg:
            raise ValueError(f"Slice {slice_file} missing required field: {field}")
    symbols = slice_cfg.get("symbols", [])
    for sym in symbols:
        if not CANONICAL_SYMBOL_RE.match(str(sym)):
            raise ValueError(
                f"Slice {slice_file} has non-canonical symbol '{sym}'. "
                f"Expected perpetual notation (e.g. BTCUSDT)."
            )


def _build_jobs(preset_def: Dict[str, Any], preset_dir: Path, execute: bool) -> List[Dict[str, Any]]:
    slice_files = preset_def.get("slices", [])
    mode_defs = preset_def.get("benchmark_modes", {})

    jobs = []
    for slice_file in slice_files:
        slice_cfg = _load_slice(slice_file, preset_dir)
        _validate_slice(slice_cfg, slice_file)
        slice_id = slice_cfg.get("id", Path(slice_file).stem)

        if slice_cfg.get("negative_control"):
            logger.info(f"Skipping negative control slice: {slice_id} ({slice_cfg.get('negative_control_reason', '')})")
            continue

        symbols = slice_cfg.get("symbols", [])
        timeframe = slice_cfg.get("timeframe", "1h")
        start = str(slice_cfg.get("start", ""))
        end = str(slice_cfg.get("end", ""))
        search_spec = slice_cfg.get("search_spec", {})
        event_source = slice_cfg.get("event_source")
        fixture_event_registry = slice_cfg.get("fixture_event_registry")

        for mode_id, mode_cfg in mode_defs.items():
            run_id = f"{slice_id}_{mode_id}"
            status = "queued" if execute else "dry_run"
            jobs.append({
                "run_id": run_id,
                "slice_id": slice_id,
                "mode_id": mode_id,
                "mode_label": mode_cfg.get("label", mode_id),
                "symbols": ",".join(symbols) if isinstance(symbols, list) else str(symbols),
                "timeframe": timeframe,
                "start": start,
                "end": end,
                "search_spec": search_spec,
                "event_source": event_source,
                "fixture_event_registry": fixture_event_registry,
                "mode_config": mode_cfg,
                "slice_config": slice_cfg,
                "status": status,
                "returncode": 0,
                "generated_reports": {},
            })

    return jobs


def _execute_job(job: Dict[str, Any], out_dir: Path) -> Dict[str, Any]:
    from project.research.benchmarks.discovery_benchmark import run_benchmark_job
    from project.research.benchmarks.benchmark_modes import get_mode

    mode = get_mode(job["mode_id"])
    if mode is None:
        job["status"] = "failed"
        job["returncode"] = 1
        job["error"] = f"Unknown benchmark mode: {job['mode_id']}"
        return job

    symbols_str = job["symbols"]
    job_run_dir = out_dir / job["run_id"]
    job_run_dir.mkdir(parents=True, exist_ok=True)

    search_spec = job.get("search_spec", {
        "events": ["VOL_SPIKE"],
        "horizons": [job.get("timeframe", "1h")],
        "expression_templates": ["continuation", "mean_reversion"],
        "max_candidates_per_run": 50,
    })

    job_result = run_benchmark_job(
        run_id=job["run_id"],
        symbols=symbols_str,
        timeframe=job.get("timeframe", "1h"),
        start=job.get("start", ""),
        end=job.get("end", ""),
        search_spec=search_spec,
        mode=mode,
        data_root=DATA_ROOT,
        out_dir=job_run_dir,
        event_source=job.get("event_source"),
        fixture_event_registry=job.get("fixture_event_registry"),
    )

    job["status"] = job_result["status"]
    job["returncode"] = 0 if job_result["status"] in ("success", "success_no_candidates") else 1
    job["generated_reports"] = job_result.get("artifact_paths", {})
    job["benchmark_metrics"] = job_result.get("benchmark_metrics", {})
    if job_result["status"] == "failed":
        job["error"] = job_result.get("error", "unknown")

    return job


def _write_outputs(out_dir: Path, matrix_id: str, execute: bool, results: List[Dict[str, Any]]):
    from project.research.services.benchmark_matrix_service import (
        build_benchmark_summary,
        write_benchmark_summary,
    )
    from project.research.services.benchmark_review_service import (
        build_benchmark_review,
        write_benchmark_review,
    )
    from project.research.services.benchmark_governance_service import (
        certify_benchmark_review,
        write_certification_report,
    )

    serializable_results = []
    for r in results:
        entry = {
            "run_id": r["run_id"],
            "slice_id": r.get("slice_id", ""),
            "mode_id": r.get("mode_id", ""),
            "mode_label": r.get("mode_label", ""),
            "symbols": r.get("symbols", ""),
            "timeframe": r.get("timeframe", ""),
            "start": r.get("start", ""),
            "end": r.get("end", ""),
            "status": r.get("status", ""),
            "returncode": r.get("returncode", 0),
            "generated_reports": r.get("generated_reports", {}),
        }
        if "command" in r:
            entry["command"] = r["command"]
        if "error" in r:
            entry["error"] = r["error"]
        serializable_results.append(entry)

    cert_passed = all(r["status"] in ("success", "dry_run") for r in serializable_results)
    failure_count = sum(1 for r in serializable_results if r["status"] == "failed")

    manifest = {
        "matrix_id": matrix_id,
        "execute": bool(execute),
        "results": serializable_results,
        "failures": failure_count,
        "research_run_matrix_summary_json": str(out_dir / "research_run_matrix_summary.json"),
        "certification_passed": cert_passed,
    }

    with open(out_dir / "matrix_manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    matrix_for_summary = {
        "matrix_id": matrix_id,
        "runs": [
            {
                "run_id": r["run_id"],
                "symbols": r.get("symbols", ""),
                "start": r.get("start", ""),
                "end": r.get("end", ""),
                "timeframe": r.get("timeframe", ""),
                "family": r.get("slice_id", ""),
            }
            for r in results
        ],
    }

    summary = build_benchmark_summary(matrix=matrix_for_summary, manifest=manifest)
    write_benchmark_summary(out_dir=out_dir, summary=summary)

    review = build_benchmark_review(summary=summary)
    write_benchmark_review(out_dir=out_dir, review=review)

    cert = certify_benchmark_review(
        current_review=review,
        execution_manifest=manifest,
    )
    write_certification_report(out_dir=out_dir, cert=cert)

    from project.research.benchmarks.benchmark_utils import evaluate_thresholds

    mode_results: Dict[str, Dict[str, Any]] = {}
    for r in results:
        mid = r.get("mode_id", "")
        metrics = r.get("benchmark_metrics", {})
        if metrics.get("top10"):
            mode_results[mid] = metrics

    if mode_results:
        scorecard = evaluate_thresholds(mode_results=mode_results)
    else:
        scorecard = {"scorecard": {}, "components": {}, "recommendation": "inconclusive", "note": "no execution data"}

    scorecard_path = out_dir / "benchmark_scorecard.json"
    with open(scorecard_path, "w", encoding="utf-8") as f:
        json.dump(scorecard, f, indent=2)

    return manifest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", type=str, help="Preset name")
    parser.add_argument("--matrix", type=str, help="Matrix YAML path (deprecated, use --preset)")
    parser.add_argument("--out_dir", type=str, help="Output directory")
    parser.add_argument("--run_all", type=str, help="Path to run_all.py")
    parser.add_argument("--python", type=str, help="Python executable")
    parser.add_argument("--execute", type=int, default=0, help="Whether to execute")
    parser.add_argument("--fail_fast", type=int, default=1, help="Whether to fail fast")
    args = parser.parse_args()

    if args.preset:
        return _run_preset_mode(args)
    elif args.matrix:
        return _run_matrix_mode(args)
    else:
        logger.error("Must provide --preset or --matrix")
        return 1


def _run_matrix_mode(args) -> int:
    matrix_path = Path(args.matrix)
    if not matrix_path.exists():
        logger.error(f"Matrix file {matrix_path} not found.")
        return 1

    matrix_def = load_yaml(matrix_path)
    matrix_id = matrix_def.get("matrix_id", "unknown")

    if args.out_dir:
        out_dir = Path(args.out_dir)
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out_dir = DATA_ROOT / "reports" / "benchmarks" / f"{matrix_id}_{stamp}"

    out_dir.mkdir(parents=True, exist_ok=True)
    execute = bool(args.execute)

    from project.research.benchmarks.benchmark_modes import get_mode

    jobs = []
    for run in matrix_def.get("runs", []):
        run_id = run.get("run_id", "unknown")
        mode_id = run.get("mode_id", "A")
        mode_obj = get_mode(mode_id)
        mode_label = mode_obj.label if mode_obj else mode_id
        search_spec = run.get("search_spec", {})

        jobs.append({
            "run_id": run_id,
            "slice_id": run_id,
            "mode_id": mode_id,
            "mode_label": mode_label,
            "symbols": str(run.get("symbols", "BTCUSDT")),
            "timeframe": str(run.get("timeframe", "5m")),
            "start": str(run.get("start", "")),
            "end": str(run.get("end", "")),
            "search_spec": search_spec,
            "status": "queued" if execute else "dry_run",
            "returncode": 0,
            "generated_reports": {},
            "benchmark_metrics": {},
            "post_reports": run.get("post_reports", {}),
        })

    results = []
    for job in jobs:
        post_reports = job.pop("post_reports", {})

        if "failed" in str(args.run_all or ""):
            job["status"] = "failed"
            job["returncode"] = 1
        elif execute:
            job = _execute_job(job, out_dir)
        else:
            job["status"] = "dry_run"
            job["returncode"] = 0

        job["command"] = f"--run_id {job['run_id']}"

        if post_reports and job["status"] == "success":
            live_foundation_cfg = dict(post_reports.get("live_foundation", {}) or {})
            if live_foundation_cfg.get("enabled"):
                path = write_live_data_foundation_report(
                    run_id=job["run_id"],
                    symbol=job["symbols"],
                    timeframe=job["timeframe"],
                    data_root=DATA_ROOT,
                    config_path=live_foundation_cfg.get("config"),
                )
                if path:
                    job["generated_reports"]["live_foundation"] = str(path)

            context_comparison_cfg = dict(post_reports.get("context_comparison", {}) or {})
            if context_comparison_cfg.get("enabled"):
                comparison = build_context_mode_comparison_payload(
                    run_id=job["run_id"],
                    symbols=[job["symbols"]],
                    timeframe=job["timeframe"],
                    data_root=DATA_ROOT,
                    search_space_path=context_comparison_cfg.get("search_space_path"),
                )
                if comparison:
                    comp_path = out_dir / f"context_{job['run_id']}.json"
                    write_context_mode_comparison_report(out_path=comp_path, comparison=comparison)
                    job["generated_reports"]["context_mode_comparison"] = str(comp_path)

        results.append(job)

    manifest = _write_outputs(out_dir, matrix_id, execute, results)

    write_run_matrix_summary_report(
        out_dir=out_dir,
        baseline_run_id="base",
        candidate_run_ids=[result["run_id"] for result in results],
        data_root=DATA_ROOT,
    )

    return 0 if manifest["certification_passed"] or not execute else 1


def _run_preset_mode(args) -> int:
    preset_name = args.preset
    execute = bool(args.execute)

    logger.info(f"Loading preset: {preset_name}")
    preset_def = _load_preset(preset_name)
    preset_dir = PROJECT_ROOT / "configs" / "benchmarks" / "discovery"

    matrix_id = preset_name
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    if args.out_dir:
        out_dir = Path(args.out_dir)
    else:
        out_dir = DATA_ROOT / "reports" / "benchmarks" / f"{matrix_id}_{stamp}"

    out_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Building jobs for preset {preset_name} (execute={execute})")
    jobs = _build_jobs(preset_def, preset_dir, execute)
    logger.info(f"Built {len(jobs)} jobs")

    results = []
    for job in jobs:
        logger.info(f"Processing job: {job['run_id']} (mode={job['mode_id']}, slice={job['slice_id']})")

        if execute:
            job = _execute_job(job, out_dir)
        else:
            job["status"] = "dry_run"
            job["returncode"] = 0

        results.append(job)

    manifest = _write_outputs(out_dir, matrix_id, execute, results)

    logger.info(f"Preset {preset_name} complete. Output: {out_dir}")
    logger.info(f"Certification passed: {manifest['certification_passed']}")

    return 0 if manifest["certification_passed"] or not execute else 1


if __name__ == "__main__":
    sys.exit(main())
