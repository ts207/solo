from __future__ import annotations

import json
import math
from typing import Any, Dict

import numpy as np

from project.research.utils.decision_safety import coerce_numeric_nan


def _count_context_dimensions(context_json: Any) -> int:
    """Count non-trivial context conditions for the complexity penalty.

    Phase 4.3: Extracts the number of conditioning dimensions from a
    context_json value (str or dict).  A zero-condition unconditional
    hypothesis pays no penalty; each additional dimension adds to the
    penalty.  Parsing failures return 0 (no penalty applied).
    """
    if context_json is None:
        return 0
    if isinstance(context_json, dict):
        ctx = context_json
    else:
        try:
            ctx = json.loads(str(context_json))
        except Exception:
            return 0
    if not isinstance(ctx, dict):
        return 0
    return len(ctx)


def _context_complexity_penalty(dimension_count: int) -> float:
    """Compute the stability-score penalty for over-conditioned hypotheses.

    Phase 4.3: penalty = log(1 + max(0, dimension_count - 1)) × 0.05

    A zero-condition unconditional hypothesis pays 0.
    A one-condition (single regime) hypothesis pays 0.
    A four-condition conjunction pays ≈ 0.072 — meaningful relative to
    typical min_stability_score values.
    """
    extra = max(0, dimension_count - 1)
    return math.log1p(extra) * 0.05


def stability_score(
    row: Dict[str, Any],
    sign_consistency_val: float,
    *,
    apply_context_penalty: bool = True,
) -> float:
    """Compute the stability score for a promotion candidate.

    Phase 4.3: When *apply_context_penalty* is True (the default), a
    context complexity penalty is subtracted from the raw score before
    returning.  Hypotheses that only pass under high-dimensional context
    conjunctions receive a lower stability score than equivalent
    unconditional strategies.

    The penalty is ``log(1 + max(0, dim_count - 1)) × 0.05``:
    - 0 or 1 dimensions → no penalty (unconditional / single regime)
    - 2 dimensions → ≈ 0.035
    - 4 dimensions → ≈ 0.072
    """
    # Respect a pre-computed stability_score column (e.g. from phase2 bridge evaluation)
    # when std_return is unavailable for recomputation.
    pre_computed = row.get("stability_score")
    effect = abs(coerce_numeric_nan(row.get("effect_shrunk_state", row.get("expectancy"))))
    volatility = abs(coerce_numeric_nan(row.get("std_return")))
    if np.isnan(effect) or np.isnan(volatility):
        if pre_computed is not None and np.isfinite(float(pre_computed)):
            return float(pre_computed)
        return np.nan
    denominator = max(volatility, 1e-8)
    raw_score = float(sign_consistency_val * (effect / denominator))

    if apply_context_penalty:
        dim_count = _count_context_dimensions(row.get("context_json"))
        penalty = _context_complexity_penalty(dim_count)
        return raw_score - penalty

    return raw_score


def calculate_promotion_score(
    statistical_pass: bool,
    stability_pass: bool,
    cost_pass: bool,
    tob_pass: bool,
    oos_pass: bool | None,
    multiplicity_pass: bool,
    placebo_pass: bool,
    timeframe_consensus_pass: bool,
) -> float:
    # Phase 1.4: oos_pass may be None (not evaluated). Treat None as 0.0 so that
    # candidates without OOS evidence do not receive credit toward the score.
    oos_score = 0.0 if oos_pass is None else float(oos_pass)
    score = (
        float(statistical_pass)
        + float(stability_pass)
        + float(cost_pass)
        + float(tob_pass)
        + oos_score
        + float(multiplicity_pass)
        + float(placebo_pass)
        + float(timeframe_consensus_pass)
    ) / 8.0
    return float(score)
