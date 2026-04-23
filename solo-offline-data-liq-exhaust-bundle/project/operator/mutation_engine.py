from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from project.research.agent_io.proposal_schema import load_operator_proposal


@dataclass(frozen=True)
class MutationResult:
    mutation_type: str
    changed_field: str
    parent_run_id: str
    branch_depth: int
    proposal_payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "mutation_type": self.mutation_type,
            "changed_field": self.changed_field,
            "parent_run_id": self.parent_run_id,
            "branch_depth": int(self.branch_depth),
            "proposal_payload": self.proposal_payload,
        }


def _single_change_copy(payload: dict[str, Any]) -> dict[str, Any]:
    return copy.deepcopy(payload)


def _next_horizon(values: list[int], *, strategy: str) -> list[int]:
    ordered = sorted({int(v) for v in values})
    if not ordered:
        return [12]
    if strategy == "horizon_shorten":
        return [ordered[0]]
    if strategy == "horizon_extend":
        current = ordered[-1]
        return [max(current + current // 2, current + 1)]
    return [ordered[0]]


def _next_lag(values: list[int], *, strategy: str) -> list[int]:
    ordered = sorted({max(1, int(v)) for v in values})
    if not ordered:
        return [1]
    current = ordered[0]
    if strategy == "lag_plus_one":
        return [current + 1]
    if strategy == "lag_minus_one":
        return [max(1, current - 1)]
    return [current + 1]


def derive_mutation_strategy(*, baseline_summary: dict[str, Any], diagnostics: dict[str, Any] | None = None, decision: dict[str, Any] | None = None) -> str:
    diagnostics = diagnostics or {}
    decision = decision or {}
    diagnosis = str(diagnostics.get("diagnosis", "") or "").strip().lower()
    classification = str(decision.get("classification", "") or "").strip().lower()
    recommended = str(diagnostics.get("recommended_next_action", "") or "").strip().lower()
    if diagnosis == "low_sample_power":
        return "horizon_extend"
    if diagnosis == "regime_instability" or "regime" in recommended:
        return "lag_plus_one"
    if classification == "near_miss":
        return "horizon_shorten"
    return "lag_plus_one"


def generate_next_proposal(*, baseline_proposal_path: str | Path, parent_run_id: str, diagnostics: dict[str, Any] | None = None, decision: dict[str, Any] | None = None, strategy: str | None = None, campaign_id: str | None = None, cycle_number: int = 1, branch_id: str = "main", branch_depth: int = 1) -> MutationResult:
    proposal = load_operator_proposal(baseline_proposal_path)
    payload = proposal.to_dict()
    mutation = strategy or derive_mutation_strategy(
        baseline_summary={}, diagnostics=diagnostics, decision=decision
    )
    mutated = _single_change_copy(payload)
    changed_field = ""

    if mutation in {"horizon_shorten", "horizon_extend"}:
        mutated["horizons_bars"] = _next_horizon(list(proposal.horizons_bars), strategy=mutation)
        changed_field = "horizons_bars"
    elif mutation in {"lag_plus_one", "lag_minus_one"}:
        mutated["entry_lags"] = _next_lag(list(proposal.entry_lags), strategy=mutation)
        changed_field = "entry_lags"
    elif mutation == "direction_flip":
        current = [str(v) for v in proposal.directions]
        flipped = []
        for value in current:
            if value == "long":
                flipped.append("short")
            elif value == "short":
                flipped.append("long")
        mutated["directions"] = flipped or ["short"]
        changed_field = "directions"
    else:
        mutated["entry_lags"] = _next_lag(list(proposal.entry_lags), strategy="lag_plus_one")
        changed_field = "entry_lags"
        mutation = "lag_plus_one"

    mutated["description"] = (str(mutated.get("description", "") or "").strip() + f" | campaign mutation {mutation} from {parent_run_id}").strip(" |")
    mutated["bounded"] = {
        "baseline_run_id": parent_run_id,
        "experiment_type": "confirmation",
        "allowed_change_field": changed_field,
        "change_reason": f"campaign mutation: {mutation}",
        "compare_to_baseline": True,
    }
    mutated["campaign"] = {
        "campaign_id": campaign_id or "",
        "cycle_number": int(cycle_number),
        "branch_id": branch_id,
        "parent_run_id": parent_run_id,
        "mutation_type": mutation,
        "branch_depth": int(branch_depth),
    }
    return MutationResult(
        mutation_type=mutation,
        changed_field=changed_field,
        parent_run_id=parent_run_id,
        branch_depth=int(branch_depth),
        proposal_payload=mutated,
    )


def write_mutated_proposal(*, mutation: MutationResult, destination: str | Path) -> Path:
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(mutation.proposal_payload, sort_keys=False), encoding="utf-8")
    return path
