import hashlib
import json
from typing import Any

from project.strategy.dsl.schema import Blueprint


def compute_ontology_invariant_hash(blueprint: Blueprint) -> str:
    """Computes a sha256 hash of a blueprint that is invariant to run_id, timestamps, and IDs.

    This ensures that the same strategy logic discovered in different runs produces the same hash,
    allowing for consistent enforcement of the 'Holdout Burn' rule.
    """
    # 1. Define the core fields that define the strategy 'logic'
    core_data = {
        "event_type": blueprint.event_type,
        "symbol_scope": {
            "mode": blueprint.symbol_scope.mode,
            "symbols": sorted(blueprint.symbol_scope.symbols),
            "candidate_symbol": blueprint.symbol_scope.candidate_symbol
        },
        "direction": blueprint.direction,
        "entry": blueprint.entry.model_dump(),
        "exit": blueprint.exit.model_dump(),
        "sizing": blueprint.sizing.model_dump(),
        "overlays": sorted([o.model_dump() for o in blueprint.overlays], key=lambda x: x["name"]),
        "execution": {
            "mode": blueprint.execution.mode,
            "urgency": blueprint.execution.urgency,
            "max_slippage_bps": blueprint.execution.max_slippage_bps,
            "fill_profile": blueprint.execution.fill_profile
        }
    }

    # 2. Serialize to a stable JSON string (sorted keys)
    stable_json = json.dumps(core_data, sort_keys=True, separators=(",", ":"))

    # 3. Hash the string
    return f"sha256:{hashlib.sha256(stable_json.encode('utf-8')).hexdigest()}"

def is_blueprint_burned(blueprint: Blueprint, burn_ledger: dict[str, Any]) -> bool:
    """Checks if a blueprint's invariant hash is in the burned_strategies list."""
    bp_hash = compute_ontology_invariant_hash(blueprint)
    return bp_hash in burn_ledger.get("burned_strategies", [])
