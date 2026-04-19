from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ContradictionAssessment:
    penalty_bps: float
    probability_penalty: float
    reasons: tuple[str, ...] = ()


def assess_contradictions(*, match: Any, context: Any) -> ContradictionAssessment:
    base = max(0.0, min(1.0, float(getattr(match, "contradiction_penalty", 0.0) or 0.0)))
    reasons = [
        str(item)
        for item in getattr(match, "reasons_against", []) or []
        if "contradiction" in str(item) or "invalidation" in str(item)
    ]
    context_contradictions = list(getattr(context, "contradiction_event_ids", []) or [])
    if context_contradictions:
        reasons.append("context_contradiction_events")
        base = min(1.0, base + 0.15)
    return ContradictionAssessment(
        penalty_bps=base * 25.0,
        probability_penalty=base * 0.35,
        reasons=tuple(reasons),
    )
