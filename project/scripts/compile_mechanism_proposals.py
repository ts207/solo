#!/usr/bin/env python3
"""Compile a bounded set of mechanism-backed proposal YAML files."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import yaml

from project.research.mechanisms import (
    CandidateHypothesis,
    MechanismSpec,
    load_mechanism,
    validate_candidate_against_mechanism,
    validate_mechanism_spec,
)

DEFAULT_TIMEFRAME = "5m"
DEFAULT_OBJECTIVE = "retail_profitability"
DEFAULT_PROMOTION_PROFILE = "research"


def _symbol_slug(symbol: str) -> str:
    token = str(symbol or "").strip().lower()
    if token.endswith("usdt"):
        return token.removesuffix("usdt")
    return token


def _forced_flow_seeds(symbol: str) -> list[dict[str, Any]]:
    slug = _symbol_slug(symbol)
    return [
        {
            "program_id": f"forced_flow_oi_flush_highvol_h24_{slug}",
            "filename": f"forced_flow_oi_flush_highvol_long_h24_{slug}.yaml",
            "description": (
                "Forced-flow reversal test for OI_FLUSH in high-volatility context."
            ),
            "event_id": "OI_FLUSH",
            "contexts": {"vol_regime": ["high"]},
            "template_id": "exhaustion_reversal",
            "direction": "long",
            "horizon_bars": 24,
        },
        {
            "program_id": f"forced_flow_climax_volume_funding_neg_h24_{slug}",
            "filename": f"forced_flow_climax_volume_funding_neg_long_h24_{slug}.yaml",
            "description": (
                "Forced-flow reversal test for CLIMAX_VOLUME_BAR under negative carry."
            ),
            "event_id": "CLIMAX_VOLUME_BAR",
            "contexts": {"carry_state": ["funding_neg"]},
            "template_id": "exhaustion_reversal",
            "direction": "long",
            "horizon_bars": 24,
        },
        {
            "program_id": f"forced_flow_liquidation_exhaustion_highvol_h24_{slug}",
            "filename": f"forced_flow_liquidation_exhaustion_highvol_long_h24_{slug}.yaml",
            "description": (
                "Forced-flow reversal test for LIQUIDATION_EXHAUSTION_REVERSAL in high-volatility context."
            ),
            "event_id": "LIQUIDATION_EXHAUSTION_REVERSAL",
            "contexts": {"vol_regime": ["high"]},
            "template_id": "exhaustion_reversal",
            "direction": "long",
            "horizon_bars": 24,
        },
    ]


def proposal_payload_from_seed(
    seed: dict[str, Any],
    *,
    mechanism: MechanismSpec,
    symbol: str,
    start: str,
    end: str,
    search_spec_path: Path | None = None,
    timeframe: str = DEFAULT_TIMEFRAME,
) -> dict[str, Any]:
    contexts = dict(seed["contexts"])
    search_spec = {"path": str(search_spec_path)} if search_spec_path is not None else {}
    return {
        "program_id": seed["program_id"],
        "description": seed["description"],
        "run_mode": "research",
        "objective_name": DEFAULT_OBJECTIVE,
        "promotion_profile": DEFAULT_PROMOTION_PROFILE,
        "mechanism": {
            "id": mechanism.mechanism_id,
            "version": mechanism.version,
            "claim": mechanism.claim,
        },
        "symbols": [symbol],
        "timeframe": timeframe,
        "start": str(start),
        "end": str(end),
        "instrument_classes": ["crypto"],
        "search_spec": search_spec,
        "avoid_region_keys": [],
        "contexts": contexts,
        "hypothesis": {
            "anchor": {
                "type": "event",
                "event_id": seed["event_id"],
            },
            "filters": {
                "contexts": contexts,
            },
            "sampling_policy": {
                "mode": "episodic",
                "entry_lag_bars": 1,
                "overlap_policy": "suppress",
            },
            "template": {
                "id": seed["template_id"],
            },
            "direction": seed["direction"],
            "horizon_bars": int(seed["horizon_bars"]),
        },
        "required_falsification": list(mechanism.required_falsification),
        "forbidden_rescue_actions": list(mechanism.forbidden_rescue_actions),
        "artifacts": {
            "mechanism_id": mechanism.mechanism_id,
            "mechanism_version": mechanism.version,
            "compiler": "compile_mechanism_proposals.py",
        },
        "version": 1,
    }


def search_spec_payload_from_seed(
    seed: dict[str, Any],
    *,
    symbol: str,
    mechanism: MechanismSpec,
) -> dict[str, Any]:
    return {
        "version": 1,
        "kind": "search_space",
        "metadata": {
            "phase": f"mechanism_{mechanism.mechanism_id}",
            "description": seed["description"],
            "search_tier": "single_event_context",
            "default_symbols": [symbol],
        },
        "triggers": {
            "events": [seed["event_id"]],
        },
        "horizons": [f"{int(seed['horizon_bars'])}b"],
        "directions": [seed["direction"]],
        "entry_lag": 1,
        "cost_profiles": ["standard"],
        "expression_templates": [seed["template_id"]],
        "filter_templates": [],
        "execution_templates": [],
        "include_sequences": False,
        "include_interactions": False,
        "contexts": dict(seed["contexts"]),
        "discovery_search": {
            "mode": "flat",
            "trigger_viability": {
                "enabled": True,
                "max_templates": 1,
                "max_horizons": 1,
                "max_entry_lags": 1,
                "allow_both_directions": False,
                "top_k_triggers": None,
                "min_stage_score": 0.0,
            },
            "template_refinement": {
                "enabled": False,
                "top_k_templates_per_trigger": 1,
                "min_stage_score": 0.0,
            },
            "execution_refinement": {
                "enabled": False,
                "top_k_shapes_per_template": 1,
                "min_stage_score": 0.0,
            },
            "context_refinement": {
                "enabled": False,
                "max_context_dims": 1,
                "top_k_contexts_per_candidate": 1,
                "require_unconditional_baseline": True,
                "min_context_gain": 0.0,
            },
        },
        "discovery_selection": {
            "mode": "off",
        },
        "template_policy": {
            "generic_templates_allowed": True,
            "reason": f"mechanism_backed_single_event_context_{mechanism.mechanism_id}",
        },
    }


def compile_mechanism_proposals(
    *,
    mechanism_id: str,
    symbol: str,
    start: str,
    end: str,
    data_root: Path,
    limit: int = 3,
    output_dir: Path | None = None,
) -> list[Path]:
    if limit < 1:
        raise ValueError("--limit must be >= 1")

    mechanism = load_mechanism(mechanism_id)
    spec_issues = validate_mechanism_spec(mechanism)
    if any(issue.status == "fail" for issue in spec_issues):
        details = "; ".join(issue.detail for issue in spec_issues if issue.status == "fail")
        raise ValueError(f"Mechanism spec is invalid: {details}")

    if mechanism.mechanism_id != "forced_flow_reversal":
        raise ValueError(
            "Only forced_flow_reversal has a seeded compiler in Wave 1; draft mechanisms are registry-only"
        )

    out_dir = output_dir or (
        data_root
        / "reports"
        / "mechanisms"
        / mechanism.mechanism_id
        / "generated_proposals"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    search_spec_dir = out_dir / "search_specs"
    search_spec_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    for seed in _forced_flow_seeds(symbol)[:limit]:
        search_spec_path = search_spec_dir / seed["filename"].replace(".yaml", "_search.yaml")
        search_spec_path.write_text(
            yaml.safe_dump(
                search_spec_payload_from_seed(seed, symbol=symbol, mechanism=mechanism),
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        payload = proposal_payload_from_seed(
            seed,
            mechanism=mechanism,
            symbol=symbol,
            start=start,
            end=end,
            search_spec_path=search_spec_path,
        )
        candidate = CandidateHypothesis.from_proposal_payload(payload)
        preflight = validate_candidate_against_mechanism(candidate, mechanism)
        if preflight.status != "pass":
            failed = [check.detail for check in preflight.checks if check.status == "fail"]
            raise ValueError(f"Generated proposal violates mechanism: {'; '.join(failed)}")

        path = out_dir / seed["filename"]
        path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
        written.append(path)
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mechanism", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--limit", type=int, default=3)
    parser.add_argument("--output-dir")
    args = parser.parse_args(argv)

    paths = compile_mechanism_proposals(
        mechanism_id=args.mechanism,
        symbol=args.symbol,
        start=args.start,
        end=args.end,
        data_root=Path(args.data_root),
        limit=args.limit,
        output_dir=Path(args.output_dir) if args.output_dir else None,
    )
    for path in paths:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
