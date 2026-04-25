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


def _append_bool_mismatch(
    errors: List[Tuple[str, str]],
    *,
    event_type: str,
    field: str,
    local_value: Any,
    generated_value: Any,
) -> None:
    local_bool = _as_bool(local_value)
    generated_bool = _as_bool(generated_value)
    if local_bool == generated_bool:
        return
    errors.append(
        (
            f"spec/events/{event_type}.yaml",
            f"{field} must match generated detector eligibility "
            f"(local={local_bool}, generated={generated_bool})",
        )
    )


def validate_governance_consistency() -> List[Tuple[str, str]]:
    """Validate authored governance hints against generated detector eligibility.

    Detector planning, promotion, runtime, anchor, and band eligibility is owned
    by generated detector governance, not by local event YAML runtime hints.
    Authored event specs may repeat generated values for readability, but a
    mismatch is an operator-risk error.
    """

    matrix = {
        str(row.get("event_name", "")).strip().upper(): row
        for row in build_detector_eligibility_matrix_rows()
        if str(row.get("event_name", "")).strip()
    }
    errors: List[Tuple[str, str]] = []

    for event_type, contract in sorted(load_active_event_contracts().items()):
        raw = _nested_mapping(contract, "raw")
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

        if "eligible" in trade_runtime:
            _append_bool_mismatch(
                errors,
                event_type=event_type,
                field="trade_runtime.eligible",
                local_value=trade_runtime.get("eligible"),
                generated_value=generated.get("runtime"),
            )
        field_pairs = (
            ("runtime_trade_eligible", "runtime"),
            ("promotion_eligible", "promotion"),
            ("primary_anchor_eligible", "anchor"),
            ("planning_eligible", "planning"),
        )
        for local_field, generated_field in field_pairs:
            if local_field not in governance:
                continue
            _append_bool_mismatch(
                errors,
                event_type=event_type,
                field=f"governance.{local_field}",
                local_value=governance.get(local_field),
                generated_value=generated.get(generated_field),
            )

        if "detector_band" in governance:
            local_band = str(governance.get("detector_band", "")).strip().lower()
            generated_band = str(generated.get("detector_band", "")).strip().lower()
            if local_band != generated_band:
                errors.append(
                    (
                        f"spec/events/{event_type}.yaml",
                        "governance.detector_band must match generated detector eligibility "
                        f"(local={local_band or 'missing'}, generated={generated_band or 'missing'})",
                    )
                )

    return errors
