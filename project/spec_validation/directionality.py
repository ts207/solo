from __future__ import annotations

from typing import Any, List, Mapping, Tuple

from project.spec_registry.loaders import load_yaml_relative


_EXPECTED_FUNDING_VARIANTS = {
    "FUNDING_POS_EXTREME_ONSET",
    "FUNDING_NEG_EXTREME_ONSET",
    "FUNDING_POS_PERSISTENCE",
    "FUNDING_NEG_PERSISTENCE",
    "FUNDING_POS_NORMALIZATION",
    "FUNDING_NEG_NORMALIZATION",
    "FUNDING_FLIP_TO_POSITIVE",
    "FUNDING_FLIP_TO_NEGATIVE",
}

_EXPECTED_OI_QUADRANTS = {
    "PRICE_UP_OI_UP": "price_up_oi_up",
    "PRICE_UP_OI_DOWN": "price_up_oi_down",
    "PRICE_DOWN_OI_UP": "price_down_oi_up",
    "PRICE_DOWN_OI_DOWN": "price_down_oi_down",
}


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def validate_event_directionality_contracts() -> List[Tuple[str, str]]:
    contract = load_yaml_relative("spec/events/event_directionality_contract.yaml")
    funding_variants = _mapping(contract.get("funding_phase_events"))
    quadrant_events = _mapping(contract.get("quadrant_events"))
    legacy_mappings = _mapping(contract.get("legacy_event_mappings"))

    errors: List[Tuple[str, str]] = []
    missing_funding = _EXPECTED_FUNDING_VARIANTS - set(map(str, funding_variants))
    if missing_funding:
        errors.append(
            (
                "spec/events/event_directionality_contract.yaml",
                "missing funding phase variants: " + ", ".join(sorted(missing_funding)),
            )
        )

    missing_quadrants = set(_EXPECTED_OI_QUADRANTS) - set(map(str, quadrant_events))
    if missing_quadrants:
        errors.append(
            (
                "spec/events/event_directionality_contract.yaml",
                "missing price/OI quadrant variants: " + ", ".join(sorted(missing_quadrants)),
            )
        )

    for event_type, row in sorted(funding_variants.items()):
        event = str(event_type).strip().upper()
        spec = load_yaml_relative(f"spec/events/{event}.yaml")
        expected_phase = str(_mapping(row).get("funding_phase", "")).strip()
        context_phase = str(_mapping(spec.get("context_requirements")).get("funding_phase", "")).strip()
        source = str(_mapping(row).get("source_event_type", "")).strip().upper()
        spec_source = str(spec.get("source_event_type", "")).strip().upper()
        runtime_enabled = bool(_mapping(spec.get("runtime")).get("enabled", True))
        layer = str(_mapping(spec.get("identity")).get("layer", "")).strip()
        if spec.get("event_type") != event:
            errors.append((f"spec/events/{event}.yaml", "event_type must match filename"))
        if not source or spec_source != source:
            errors.append((f"spec/events/{event}.yaml", "source_event_type must match funding variant contract"))
        if not expected_phase or context_phase != expected_phase:
            errors.append((f"spec/events/{event}.yaml", "funding_phase context must match variant contract"))
        if runtime_enabled:
            errors.append((f"spec/events/{event}.yaml", "semantic funding variants must not enable detector runtime"))
        if layer != "research_placeholder":
            errors.append((f"spec/events/{event}.yaml", "semantic funding variants must use research_placeholder layer"))

    for event_type, expected_context in sorted(_EXPECTED_OI_QUADRANTS.items()):
        spec = load_yaml_relative(f"spec/events/{event_type}.yaml")
        context_value = str(
            _mapping(spec.get("context_requirements")).get("price_oi_quadrant", "")
        ).strip()
        runtime_enabled = bool(_mapping(spec.get("runtime")).get("enabled", True))
        layer = str(_mapping(spec.get("identity")).get("layer", "")).strip()
        if spec.get("event_type") != event_type:
            errors.append((f"spec/events/{event_type}.yaml", "event_type must match filename"))
        if context_value != expected_context:
            errors.append((f"spec/events/{event_type}.yaml", "price_oi_quadrant context must match variant contract"))
        if runtime_enabled:
            errors.append((f"spec/events/{event_type}.yaml", "semantic OI variants must not enable detector runtime"))
        if layer != "research_placeholder":
            errors.append((f"spec/events/{event_type}.yaml", "semantic OI variants must use research_placeholder layer"))

    for legacy_event in (
        "FUNDING_EXTREME_ONSET",
        "FUNDING_PERSISTENCE_TRIGGER",
        "FUNDING_NORMALIZATION_TRIGGER",
        "FUNDING_FLIP",
        "OI_SPIKE_POSITIVE",
        "OI_SPIKE_NEGATIVE",
    ):
        row = _mapping(legacy_mappings.get(legacy_event))
        if not row.get("requires_context_dimension"):
            errors.append(
                (
                    "spec/events/event_directionality_contract.yaml",
                    f"{legacy_event}: legacy mapping must declare requires_context_dimension",
                )
            )

    return errors
