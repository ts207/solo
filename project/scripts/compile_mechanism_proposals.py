#!/usr/bin/env python3
"""Compile a bounded set of mechanism-backed proposal YAML files."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import yaml

from project.research.event_lift import parse_regime_id
from project.research.mechanisms import (
    CandidateHypothesis,
    MechanismSpec,
    load_mechanism,
    validate_candidate_against_mechanism,
    validate_mechanism_spec,
)
from project.research.proposal_evidence import (
    EventLiftEvidence,
    event_lift_bool,
    event_lift_is_passing,
    event_lift_not_promotable_message,
    find_event_lift_evidence,
)

DEFAULT_TIMEFRAME = "5m"
DEFAULT_OBJECTIVE = "retail_profitability"
DEFAULT_PROMOTION_PROFILE = "research"
EVENT_LIFT_TUPLE_REQUIRED_MESSAGE = (
    "--event-id, --regime-id, --direction, and --horizon-bars are required "
    "with --require-event-lift-pass"
)


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


def _mechanism_seeds(mechanism_id: str, symbol: str) -> list[dict[str, Any]]:
    if mechanism_id == "forced_flow_reversal":
        return _forced_flow_seeds(symbol)
    raise ValueError(f"No seeded compiler is defined for {mechanism_id}")


def _no_passing_event_lift_message(
    *,
    mechanism_id: str,
    event_id: str,
    regime_id: str,
    symbol: str,
    direction: str,
    horizon_bars: int,
) -> str:
    return (
        "no passing event_lift report found for "
        f"mechanism={mechanism_id} event={event_id} regime={regime_id} "
        f"symbol={symbol} direction={direction} horizon_bars={int(horizon_bars)}"
    )


def _display_artifact_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(Path.cwd().resolve()))
    except ValueError:
        return str(path)


def _select_template_id(mechanism: MechanismSpec, template_id: str | None) -> str:
    if template_id:
        if template_id not in mechanism.allowed_templates:
            raise ValueError(f"--template-id {template_id} is not allowed by {mechanism.mechanism_id}")
        return template_id
    if len(mechanism.allowed_templates) != 1:
        raise ValueError("--template-id is required when mechanism has multiple allowed templates")
    return mechanism.allowed_templates[0]


def event_lift_seed(
    *,
    mechanism: MechanismSpec,
    symbol: str,
    evidence: EventLiftEvidence,
    template_id: str,
) -> dict[str, Any]:
    row = evidence.row
    event_id = str(row["event_id"])
    direction = str(row["direction"]).lower()
    horizon_bars = int(row["horizon_bars"])
    symbol_slug = _symbol_slug(symbol)
    program_id = f"{mechanism.mechanism_id}_{event_id.lower()}_{direction}_h{horizon_bars}_{symbol_slug}"
    return {
        "program_id": program_id,
        "filename": f"{program_id}.yaml",
        "description": f"Mechanism-gated proposal from passing event_lift run {row['run_id']}.",
        "event_id": event_id,
        "contexts": parse_regime_id(str(row["regime_id"])),
        "template_id": template_id,
        "direction": direction,
        "horizon_bars": horizon_bars,
        "event_lift": {
            "run_id": row["run_id"],
            "decision": row["decision"],
            "classification": row["classification"],
            "promotion_eligible": event_lift_bool(row["promotion_eligible"]),
            "audit_only": event_lift_bool(row["audit_only"]),
            "regime_id": row["regime_id"],
            "event_id": event_id,
        },
        "event_lift_path": _display_artifact_path(evidence.path),
    }


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
    artifacts = {
        "mechanism_id": mechanism.mechanism_id,
        "mechanism_version": mechanism.version,
        "compiler": "compile_mechanism_proposals.py",
    }
    if seed.get("event_lift_path"):
        artifacts["event_lift_path"] = seed["event_lift_path"]
    payload = {
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
        "artifacts": artifacts,
        "version": 1,
    }
    if seed.get("event_lift"):
        payload["evidence"] = {"event_lift": dict(seed["event_lift"])}
    return payload


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
    require_event_lift_pass: bool = False,
    event_lift_run_id: str | None = None,
    regime_id: str | None = None,
    event_id: str | None = None,
    direction: str | None = None,
    horizon_bars: int | None = None,
    template_id: str | None = None,
) -> list[Path]:
    if limit < 1:
        raise ValueError("--limit must be >= 1")

    mechanism = load_mechanism(mechanism_id)
    spec_issues = validate_mechanism_spec(mechanism)
    if any(issue.status == "fail" for issue in spec_issues):
        details = "; ".join(issue.detail for issue in spec_issues if issue.status == "fail")
        raise ValueError(f"Mechanism spec is invalid: {details}")

    readiness_path = data_root / "reports" / "mechanism_readiness" / "mechanism_readiness.json"
    if readiness_path.exists():
        import json
        with open(readiness_path) as f:
            readiness_rows = json.load(f).get("rows", [])
        for row in readiness_rows:
            if row["mechanism_id"] == mechanism_id and row.get("readiness") == "remain_parked":
                raise ValueError(f"Mechanism {mechanism_id} is explicitly parked. Proposal compilation is blocked.")

    gate_required = require_event_lift_pass or mechanism.mechanism_id == "funding_squeeze"
    seeds: list[dict[str, Any]]
    if gate_required:
        if not (event_id and regime_id and direction and horizon_bars):
            raise ValueError(EVENT_LIFT_TUPLE_REQUIRED_MESSAGE)
        evidence = find_event_lift_evidence(
            data_root=data_root,
            mechanism_id=mechanism.mechanism_id,
            event_id=event_id,
            regime_id=regime_id,
            symbol=symbol,
            direction=direction,
            horizon_bars=horizon_bars,
            event_lift_run_id=event_lift_run_id,
        )
        if evidence is None:
            raise ValueError(
                _no_passing_event_lift_message(
                    mechanism_id=mechanism.mechanism_id,
                    event_id=event_id,
                    regime_id=regime_id,
                    symbol=symbol,
                    direction=direction,
                    horizon_bars=horizon_bars,
                )
            )
        if not event_lift_is_passing(evidence.row):
            raise ValueError(event_lift_not_promotable_message(evidence.row))
        selected_template_id = _select_template_id(mechanism, template_id)
        seeds = [
            event_lift_seed(
                mechanism=mechanism,
                symbol=symbol,
                evidence=evidence,
                template_id=selected_template_id,
            )
        ]
    else:
        seeds = _mechanism_seeds(mechanism.mechanism_id, symbol)

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
    for seed in seeds[:limit]:
        search_spec_path = search_spec_dir / seed["filename"].replace(".yaml", "_search.yaml")
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

        search_spec_path.write_text(
            yaml.safe_dump(
                search_spec_payload_from_seed(seed, symbol=symbol, mechanism=mechanism),
                sort_keys=False,
            ),
            encoding="utf-8",
        )
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
    parser.add_argument("--require-event-lift-pass", action="store_true")
    parser.add_argument("--event-lift-run-id")
    parser.add_argument("--regime-id")
    parser.add_argument("--event-id")
    parser.add_argument("--direction", choices=("long", "short"))
    parser.add_argument("--horizon-bars", type=int)
    parser.add_argument("--template-id")
    args = parser.parse_args(argv)

    try:
        paths = compile_mechanism_proposals(
            mechanism_id=args.mechanism,
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            data_root=Path(args.data_root),
            limit=args.limit,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            require_event_lift_pass=args.require_event_lift_pass,
            event_lift_run_id=args.event_lift_run_id,
            regime_id=args.regime_id,
            event_id=args.event_id,
            direction=args.direction,
            horizon_bars=args.horizon_bars,
            template_id=args.template_id,
        )
    except ValueError as exc:
        print(f"fail: {exc}")
        return 1
    for path in paths:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
