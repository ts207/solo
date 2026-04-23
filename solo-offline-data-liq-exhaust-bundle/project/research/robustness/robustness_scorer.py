# project/research/robustness/robustness_scorer.py
"""
Regime-aware robustness score for hypothesis evaluation.

Replaces the naive sign-retention score in evaluator.py with a composite
score across three axes: regime sign consistency, worst-case regime t-stat,
and event coverage in supporting regimes.

Score is in [0, 1]. Score >= 0.7 suggests cross-regime stability.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_robustness_score(
    regime_results: pd.DataFrame,
    *,
    overall_direction: float = 1.0,  # +1 for long, -1 for short
    weight_sign: float = 0.5,
    weight_min_t: float = 0.3,
    weight_coverage: float = 0.2,
    min_t_floor: float = -3.0,
    min_t_target: float = 2.0,
) -> float:
    """
    Compute composite robustness score from per-regime evaluation results.

    Audit 1.5: Improved calibration and regime-count-aware weighting.
    """
    if regime_results.empty:
        return 0.0

    valid = regime_results[regime_results["valid"].fillna(False)]
    if valid.empty:
        return 0.0

    n_valid = len(valid)
    t_stats = valid["t_stat"].dropna()
    ns = valid["n"].fillna(0)

    # ── Component 1: Regime sign consistency ──
    # Fraction of valid regimes where t_stat has the correct sign
    # Audit 1.5: Penalize heavily if only 1 valid regime exists (cannot measure robustness)
    correct_sign = (t_stats * overall_direction > 0).sum()
    sign_consistency = float(correct_sign / n_valid)
    if n_valid < 2:
        sign_consistency *= 0.5

    # ── Component 2: Minimum regime t-stat (normalized) ──
    if len(t_stats) > 0:
        signed_t = t_stats * overall_direction
        min_signed_t = float(signed_t.min())

        # Audit 1.5: Use non-linear mapping or multi-segment to distinguish
        # "slightly negative" from "structural reversal"
        if min_signed_t < 0:
            # Reversal gets 0-0.3 score
            min_t_score = (
                0.3 * (1.0 - min_signed_t / min_t_floor) if min_signed_t > min_t_floor else 0.0
            )
        else:
            # Positive min_t gets 0.3-1.0 score
            min_t_score = (
                0.3 + 0.7 * (min_signed_t / min_t_target) if min_signed_t < min_t_target else 1.0
            )

        min_t_score = float(np.clip(min_t_score, 0.0, 1.0))
    else:
        min_t_score = 0.0

    # ── Component 3: Coverage in supporting regimes ──
    # Fraction of all event fires (n) that fall in regimes with correct direction
    total_n = float(ns.sum())
    if total_n > 0:
        signed_ts_correct = valid[t_stats * overall_direction > 0]
        supporting_n = float(signed_ts_correct["n"].sum())
        coverage_score = supporting_n / total_n
    else:
        coverage_score = 0.0

    score = (
        weight_sign * sign_consistency
        + weight_min_t * min_t_score
        + weight_coverage * coverage_score
    )
    return float(np.clip(score, 0.0, 1.0))
