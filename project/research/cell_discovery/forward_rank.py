from __future__ import annotations

import math
from typing import Any

from project.research.promotion.promotion_scoring import _context_complexity_penalty


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(out):
        return default
    return out


def rank_score(row: dict[str, Any], policy: Any) -> float:
    forward = 1.0 if bool(row.get("forward_pass")) else 0.0
    expectancy = max(0.0, _safe_float(row.get("net_mean_bps"))) / 100.0
    stability = max(
        0.0,
        _safe_float(row.get("stability_score"), _safe_float(row.get("robustness_score"))),
    )
    contrast = max(0.0, _safe_float(row.get("contrast_lift_bps"))) / 100.0
    context_dims = int(_safe_float(row.get("context_dimension_count"), 0.0))
    simplicity = max(0.0, 1.0 - _context_complexity_penalty(context_dims))
    return float(
        policy.forward_weight * forward
        + policy.expectancy_weight * expectancy
        + policy.stability_weight * stability
        + policy.contrast_weight * contrast
        + policy.simplicity_weight * simplicity
    )
