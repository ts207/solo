from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


def _finite(value: Any, default: float) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


@dataclass(frozen=True)
class RegimeReliability:
    reliability: float
    reason: str


def evaluate_regime_reliability(*, thesis: Any, context: Any) -> RegimeReliability:
    thesis_regime = str(
        getattr(thesis, "canonical_regime", "")
        or (getattr(thesis, "supportive_context", {}) or {}).get("canonical_regime", "")
    ).strip().upper()
    current_regime = str(
        getattr(context, "canonical_regime", "")
        or (getattr(context, "regime_snapshot", {}) or {}).get("canonical_regime", "")
    ).strip().upper()
    stability = max(0.0, min(1.0, _finite(getattr(thesis.evidence, "stability_score", None), 0.65)))
    if not thesis_regime or not current_regime:
        return RegimeReliability(max(0.35, 0.75 * stability), "regime_unknown")
    if thesis_regime == current_regime:
        return RegimeReliability(max(0.50, stability), "regime_match")
    return RegimeReliability(max(0.05, 0.35 * stability), "regime_mismatch")
