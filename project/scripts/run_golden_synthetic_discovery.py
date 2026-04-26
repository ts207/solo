from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import pandas as pd

from project import PROJECT_ROOT
from project.io.utils import read_parquet
from project.research.services.pathing import (
    resolve_phase2_candidates_path,
    resolve_phase2_diagnostics_path,
)
from project.scripts.generate_synthetic_crypto_regimes import generate_synthetic_crypto_run
from project.scripts.run_golden_workflow import load_workflow_config
from project.scripts.validate_synthetic_detector_truth import validate_detector_truth


def _default_config_path() -> Path:
    return PROJECT_ROOT / "configs" / "golden_synthetic_discovery.yaml"


def _run_pipeline(*, data_root: Path, argv: list[str]) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["BACKTEST_DATA_ROOT"] = str(data_root)
    env["BACKTEST_STRICT_RUN_SCOPED_READS"] = "1"
    return subprocess.run(
        [sys.executable, "-m", "project.pipelines.run_all", *argv],
        cwd=str(PROJECT_ROOT.parent),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _normalized_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        items = value.replace(",", " ").split()
    elif isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray, dict)):
        items = [str(item).strip() for item in value]
    else:
        items = [str(value).strip()]
    return [item for item in items if item]


def _render_required_outputs(raw_outputs: object, *, run_id: str) -> list[str]:
    rendered: list[str] = []
    for item in _normalized_list(raw_outputs):
        rendered.append(item.format(run_id=run_id))
    return rendered


def _candidate_summary(search_candidates_path: Path) -> dict[str, Any]:
    if (
        not search_candidates_path.exists()
        and not search_candidates_path.with_suffix(".csv").exists()
    ):
        return {"candidate_rows": 0, "candidate_event_types": []}
    frame = read_parquet(
        search_candidates_path
        if search_candidates_path.exists()
        else search_candidates_path.with_suffix(".csv")
    )
    return {
        "candidate_rows": len(frame),
        "candidate_event_types": sorted(
            frame.get("event_type", pd.Series(dtype=str)).astype(str).unique().tolist()
        )
        if not frame.empty and "event_type" in frame.columns
        else [],
    }


def run_golden_synthetic_discovery(
    *,
    root: Path,
    config_path: Path,
    pipeline_runner=_run_pipeline,
    overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    config = load_workflow_config(config_path)
    if overrides:
        config.update({key: value for key, value in overrides.items() if value is not None})
    run_id = str(config.get("run_id", "golden_synthetic_discovery"))
    symbols = str(config.get("symbols", "BTCUSDT,ETHUSDT"))
    start_date = str(config.get("start_date", "2026-01-01"))
    end_date = str(config.get("end_date", "2026-02-28"))
    discovery_profile = str(config.get("discovery_profile", "synthetic"))
    phase2_gate_profile = str(config.get("phase2_gate_profile", "synthetic"))
    search_spec = str(config.get("search_spec", "synthetic_truth"))
    search_min_n = int(config.get("search_min_n", 8))
    volatility_profile = str(config.get("volatility_profile", "default"))
    noise_scale = float(config.get("noise_scale", 1.0))
    timeframes = _normalized_list(config.get("timeframes")) or ["5m"]
    events = _normalized_list(config.get("events"))
    templates = _normalized_list(config.get("templates"))
    horizons = _normalized_list(config.get("horizons"))
    directions = _normalized_list(config.get("directions"))
    contexts = _normalized_list(config.get("contexts"))
    entry_lags = _normalized_list(config.get("entry_lags"))
    search_budget = config.get("search_budget")
    interpretation_scope = (
        str(config.get("interpretation_scope", "full_discovery_validation")).strip()
        or "full_discovery_validation"
    )

    synthetic_manifest = generate_synthetic_crypto_run(
        run_id=run_id,
        start_date=start_date,
        end_date=end_date,
        data_root=root,
        symbols=[token.strip().upper() for token in symbols.split(",") if token.strip()],
        volatility_profile=volatility_profile,
        noise_scale=noise_scale,
    )
    preseeded_clean_root = root / "lake" / "runs" / run_id / "cleaned"
    if preseeded_clean_root.exists():
        shutil.rmtree(preseeded_clean_root)

    pipeline_args = [
        "--run_id",
        run_id,
        "--symbols",
        symbols,
        "--start",
        start_date,
        "--end",
        end_date,
        "--timeframes",
        ",".join(timeframes),
        "--skip_ingest_ohlcv",
        "1",
        "--skip_ingest_funding",
        "1",
        "--skip_ingest_spot_ohlcv",
        "1",
        "--run_phase2_conditional",
        "1",
        "--phase2_event_type",
        "all" if not events else events[0],
        "--run_bridge_eval_phase2",
        "0",
        "--run_candidate_promotion",
        "0",
        "--run_recommendations_checklist",
        "0",
        "--run_strategy_builder",
        "0",
        "--run_strategy_blueprint_compiler",
        "0",
        "--run_profitable_selector",
        "0",
        "--run_interaction_lift",
        "0",
        "--run_promotion_audit",
        "0",
        "--run_edge_registry_update",
        "0",
        "--run_edge_candidate_universe",
        "0",
        "--run_discovery_quality_summary",
        "0",
        "--run_naive_entry_eval",
        "0",
        "--runtime_invariants_mode",
        "off",
        "--funding_scale",
        "decimal",
        "--discovery_profile",
        discovery_profile,
        "--phase2_gate_profile",
        phase2_gate_profile,
        "--search_spec",
        search_spec,
        "--search_min_n",
        str(search_min_n),
        "--feature_schema_version",
        "v2",
        "--config",
        "project/configs/pipeline.yaml",
    ]
    if events:
        pipeline_args.extend(["--events", *events])
    if templates:
        pipeline_args.extend(["--templates", *templates])
    if horizons:
        pipeline_args.extend(["--horizons", *horizons])
    if directions:
        pipeline_args.extend(["--directions", *directions])
    if contexts:
        pipeline_args.extend(["--contexts", *contexts])
    if entry_lags:
        pipeline_args.extend(["--entry_lags", *entry_lags])
    if search_budget is not None:
        pipeline_args.extend(["--search_budget", str(int(search_budget))])
    completed = pipeline_runner(data_root=root, argv=pipeline_args)
    if completed.returncode != 0:
        raise RuntimeError(
            "golden synthetic discovery pipeline failed\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )

    truth_map_path = Path(synthetic_manifest["truth_map_path"])
    truth_validation = validate_detector_truth(
        data_root=root,
        run_id=run_id,
        truth_map_path=truth_map_path,
        event_types=events or None,
    )
    search_diag_path = resolve_phase2_diagnostics_path(data_root=root, run_id=run_id)
    search_diag = (
        json.loads(search_diag_path.read_text(encoding="utf-8"))
        if search_diag_path.exists()
        else {}
    )
    candidate_summary = _candidate_summary(
        resolve_phase2_candidates_path(data_root=root, run_id=run_id)
    )

    payload = {
        "workflow_id": str(config.get("workflow_id", "golden_synthetic_discovery_v1")),
        "config_path": str(config_path),
        "root": str(root),
        "run_id": run_id,
        "synthetic_manifest": synthetic_manifest,
        "pipeline": {
            "argv": pipeline_args,
            "returncode": int(completed.returncode),
        },
        "selection": {
            "timeframes": timeframes,
            "events": events,
            "templates": templates,
            "horizons": horizons,
            "directions": directions,
            "contexts": contexts,
            "entry_lags": [int(item) for item in entry_lags],
            "search_budget": int(search_budget) if search_budget is not None else None,
        },
        "interpretation_scope": interpretation_scope,
        "truth_validation": truth_validation,
        "search_engine_diagnostics": search_diag,
        "candidate_summary": candidate_summary,
        "required_outputs": _render_required_outputs(
            config.get("required_outputs", []), run_id=run_id
        ),
    }
    out_path = root / "reliability" / "golden_synthetic_discovery_summary.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the canonical synthetic discovery workflow.")
    parser.add_argument("--root", default=None)
    parser.add_argument("--config", default=str(_default_config_path()))
    parser.add_argument("--run_id", default=None)
    parser.add_argument("--symbols", default=None)
    parser.add_argument("--start_date", default=None)
    parser.add_argument("--end_date", default=None)
    parser.add_argument("--search_spec", default=None)
    parser.add_argument("--search_min_n", type=int, default=None)
    parser.add_argument("--search_budget", type=int, default=None)
    parser.add_argument("--events", nargs="+", default=None)
    parser.add_argument("--templates", nargs="+", default=None)
    parser.add_argument("--horizons", nargs="+", default=None)
    parser.add_argument("--directions", nargs="+", default=None)
    parser.add_argument("--contexts", nargs="+", default=None)
    parser.add_argument("--entry_lags", nargs="+", type=int, default=None)
    args = parser.parse_args(argv)

    root = (
        Path(args.root)
        if args.root
        else (PROJECT_ROOT.parent / "artifacts" / "golden_synthetic_discovery")
    )
    overrides = {
        "run_id": args.run_id,
        "symbols": args.symbols,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "search_spec": args.search_spec,
        "search_min_n": args.search_min_n,
        "search_budget": args.search_budget,
        "events": args.events,
        "templates": args.templates,
        "horizons": args.horizons,
        "directions": args.directions,
        "contexts": args.contexts,
        "entry_lags": args.entry_lags,
    }
    run_golden_synthetic_discovery(root=root, config_path=Path(args.config), overrides=overrides)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
