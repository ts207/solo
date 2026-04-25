from __future__ import annotations

from functools import lru_cache
from typing import Any

from project.domain.hypotheses import HypothesisSpec
from project.spec_registry.loaders import load_yaml_relative


@lru_cache(maxsize=1)
def _context_gate() -> dict[str, Any]:
    payload = load_yaml_relative("spec/contexts/context_dimension_registry.yaml")
    gate = payload.get("context_gate", {}) if isinstance(payload, dict) else {}
    return dict(gate) if isinstance(gate, dict) else {}


def validate_context_overfit_gate(spec: HypothesisSpec) -> list[str]:
    context = spec.context or {}
    if not context:
        return []

    gate = _context_gate()
    max_dims = int(gate.get("max_context_dims_discovery", 2) or 2)
    dim_count = len(context)
    if dim_count > max_dims:
        return [
            "context-overfit gate rejects "
            f"{dim_count} context dimensions; max_context_dims_discovery={max_dims}"
        ]
    return []
