from __future__ import annotations

from typing import Any, List, Mapping, Tuple

from project.events.contract_registry import load_active_event_contracts
from project.events.registry import build_detector_eligibility_matrix_rows


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _nested_mapping(row: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = row.get(key)
    return value if isinstance(value, Mapping) else {}


def validate_governance_consistency() -> List[Tuple[str, str]]:
    """Validate authored governance hints against generated detector eligibility.

    Runtime trade eligibility is owned by detector governance, not by local event YAML
    runtime hints. Authored event specs may repeat the generated value in
    ``trade_runtime.eligible`` for readability, but a mismatch is an operator-risk
    error.
    """

    matrix = {
        str(row.get("event_name", "")).strip().upper(): row
        for row in build_detector_eligibility_matrix_rows()
        if str(row.get("event_name", "")).strip()
    }
    errors: List[Tuple[str, str]] = []

    for event_type, contract in sorted(load_active_event_contracts().items()):
        raw = _nested_mapping(contract, "raw")
        trade_runtime = _nested_mapping(raw, "trade_runtime")
        if not trade_runtime or "eligible" not in trade_runtime:
            continue

        generated = matrix.get(event_type)
        if generated is None:
            errors.append(
                (
                    f"spec/events/{event_type}.yaml",
                    "trade_runtime.eligible is declared but generated detector eligibility is missing",
                )
            )
            continue

        local_runtime = _as_bool(trade_runtime.get("eligible"))
        generated_runtime = _as_bool(generated.get("runtime"))
        if local_runtime != generated_runtime:
            errors.append(
                (
                    f"spec/events/{event_type}.yaml",
                    "trade_runtime.eligible must match generated detector eligibility "
                    f"(local={local_runtime}, generated={generated_runtime})",
                )
            )

    return errors
