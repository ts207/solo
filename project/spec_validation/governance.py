from __future__ import annotations

from project.events.contract_registry import load_active_event_contracts
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Tuple
import yaml

def _nested_mapping(mapping: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = mapping.get(key)
    return value if isinstance(value, Mapping) else {}


def _load_json(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with open(path, "r") as f:
        return json.load(f)


def _load_active_event_contracts(root: Path = Path(".")) -> Mapping[str, Any]:
    # Support root-safe loading of event contracts
    from project.spec_validation.loaders import load_yaml
    event_spec_dir = root / "spec" / "events"
    active_contracts = {}
    for p in sorted(event_spec_dir.glob("*.yaml")):
        if p.name.startswith("_"):
             continue
        spec = load_yaml(p)
        if not spec: continue
        event_type = spec.get("event_type") or p.stem.upper()
        active_contracts[event_type] = {"raw": spec}
    return active_contracts


def validate_governance_consistency(root: Path = Path(".")) -> List[Tuple[str, str]]:
    """Validate authored governance hints against generated detector eligibility.

    Detector planning, promotion, runtime, anchor, and band eligibility is owned
    by generated detector governance, not by local event YAML runtime hints.
    Authored event specs may repeat generated values for readability, but a
    mismatch is an operator-risk error.
    """

    eligibility_path = root / "docs/generated/detector_eligibility_matrix.json"
    matrix_rows = _load_json(eligibility_path)
    if not matrix_rows and not eligibility_path.exists():
        return [("docs/generated/detector_eligibility_matrix.json", "Missing eligibility matrix")]

    matrix = {
        str(row.get("event_name", "")).strip().upper(): row
        for row in matrix_rows
        if str(row.get("event_name", "")).strip()
    }
    errors: List[Tuple[str, str]] = []

    active_contracts = _load_active_event_contracts(root=root)

    for event_type, contract in sorted(active_contracts.items()):
        raw = _nested_mapping(contract, "raw")
        if not raw: continue
        governance = _nested_mapping(raw, "governance")
        trade_runtime = _nested_mapping(raw, "trade_runtime")
        generated = matrix.get(event_type)
        if generated is None:
            if trade_runtime or governance:
                errors.append(
                    (
                        f"spec/events/{event_type}.yaml",
                        "local governance eligibility is declared but generated detector eligibility is missing",
                    )
                )
            continue

        # Check band
        local_band = governance.get("detector_band")
        generated_band = generated.get("detector_band")
        if local_band and local_band != generated_band:
             errors.append(
                (
                    f"spec/events/{event_type}.yaml",
                    "governance.detector_band must match generated detector eligibility "
                    f"(local={local_band or 'missing'}, generated={generated_band or 'missing'})",
                )
            )

        # Check promotion_eligible vs promotion
        local_promotion = governance.get("promotion_eligible")
        generated_promotion = generated.get("promotion")
        if local_promotion is not None and local_promotion != generated_promotion:
            errors.append(
                (
                    f"spec/events/{event_type}.yaml",
                    "governance.promotion_eligible must match generated detector eligibility "
                    f"(local={local_promotion}, generated={generated_promotion})",
                )
            )

        # Check primary_anchor_eligible vs anchor
        local_anchor = governance.get("primary_anchor_eligible")
        generated_anchor = generated.get("anchor")
        if local_anchor is not None and local_anchor != generated_anchor:
            errors.append(
                (
                    f"spec/events/{event_type}.yaml",
                    "governance.primary_anchor_eligible must match generated detector eligibility "
                    f"(local={local_anchor}, generated={generated_anchor})",
                )
            )

        # Check runtime_eligible vs runtime
        local_runtime = trade_runtime.get("eligible")
        generated_runtime = generated.get("runtime")
        if local_runtime is not None and local_runtime != generated_runtime:
            errors.append(
                (
                    f"spec/events/{event_type}.yaml",
                    "trade_runtime.eligible must match generated detector eligibility "
                    f"(local={local_runtime}, generated={generated_runtime})",
                )
            )

    return errors
