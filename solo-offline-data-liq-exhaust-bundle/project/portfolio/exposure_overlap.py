from __future__ import annotations

import numpy as np


def overlap_exposure_multiplier(
    *,
    overlap_score: float | None = None,
    active_overlap_notional: float = 0.0,
    overlap_budget: float | None = None,
    min_multiplier: float = 0.20,
) -> float:
    """Soft-size overlapping theses before hard overlap or family caps fire."""

    score = float(np.clip(float(overlap_score or 0.0), 0.0, 1.0))
    score_multiplier = 1.0 - (0.50 * score)
    budget_multiplier = 1.0
    if overlap_budget is not None and float(overlap_budget) > 0.0:
        utilization = abs(float(active_overlap_notional)) / float(overlap_budget)
        budget_multiplier = 1.0 - (0.60 * float(np.clip(utilization, 0.0, 1.0)))
    return float(np.clip(score_multiplier * budget_multiplier, min_multiplier, 1.0))
