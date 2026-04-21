# project/research/robustness/kill_switch.py
"""
Kill-switch condition detector.

Searches for simple feature conditions (e.g. rv_pct > 0.8) that predict
hypothesis failure. These are "kill-switches" because they identify regimes
where the hypothesis should be avoided entirely.

Uses a brute-force sweep over candidate features and threshold levels.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from project.domain.compiled_registry import get_domain_registry
from project.domain.hypotheses import HypothesisSpec
from project.research.search.evaluator_utils import (
    trigger_mask,
    forward_log_returns,
    signed_returns_for_spec,
)

log = logging.getLogger(__name__)


# Features to check for kill-switch conditions.
# We check both > and < operators for each.
def _load_kill_switch_candidates() -> list[str]:
    try:
        return get_domain_registry().kill_switch_candidates()
    except Exception as e:
        log.warning("Failed to load kill-switch config from compiled registry: %s", e)
        return []


KILL_SWITCH_CANDIDATE_FEATURES: list[str] = _load_kill_switch_candidates()


def detect_kill_switches(
    spec: HypothesisSpec,
    features: pd.DataFrame,
    *,
    horizon_bars: int = 12,
    min_n: int = 20,
    min_accuracy: float = 0.6,
    candidates: list[str] | None = None,
    event_mask: pd.Series | None = None,
    forward_returns: pd.Series | None = None,
) -> pd.DataFrame:
    """
    Search for simple feature conditions that predict hypothesis failure.

    Failure is defined as a forward return having the opposite sign
    of the hypothesis direction.

    Parameters
    ----------
    spec : HypothesisSpec to analyze
    features : wide feature DataFrame
    horizon_bars : forward return horizon
    min_n : minimum number of event fires in the "killed" subset
    min_accuracy : only return conditions that predict failure with this accuracy
    candidates : override default KILL_SWITCH_CANDIDATE_FEATURES

    Returns
    -------
    DataFrame sorted by lift:
        feature, operator, threshold, n, accuracy, lift, coverage
    """
    if candidates is None:
        candidates = KILL_SWITCH_CANDIDATE_FEATURES

    if features.empty or "close" not in features.columns:
        return pd.DataFrame()

    if event_mask is None:
        mask_raw = trigger_mask(spec, features)
        if spec.feature_condition is not None:
            fc_spec = HypothesisSpec(
                trigger=spec.feature_condition,
                direction=spec.direction,
                horizon=spec.horizon,
                template_id=spec.template_id,
            )
            mask_raw = mask_raw & trigger_mask(fc_spec, features)

        # 1. Trigger mask (with entry lag)
        if spec.entry_lag > 0:
            mask = mask_raw.astype("boolean").shift(spec.entry_lag, fill_value=False).astype(bool)
        else:
            mask = mask_raw
    else:
        mask = event_mask.astype(bool)

    base_n = int(mask.sum())
    if base_n < min_n:
        return pd.DataFrame()

    fwd = (
        forward_returns
        if forward_returns is not None
        else forward_log_returns(features["close"], horizon_bars)
    )
    signed_fwd, reason = signed_returns_for_spec(spec, features, fwd)
    if signed_fwd is None:
        return pd.DataFrame()
    # Binary target: 1 if hypothesis fails (return sign < 0)
    failed = (signed_fwd < 0).astype(float)
    base_failure_rate = float(failed[mask].mean())

    results: list[dict[str, Any]] = []

    for feat in candidates:
        if feat not in features.columns:
            continue

        feat_vals = pd.to_numeric(features[feat], errors="coerce")[mask]
        if feat_vals.dropna().empty:
            continue

        # Sweep percentiles for thresholds
        thresholds = np.nanpercentile(feat_vals, [10, 20, 30, 40, 50, 60, 70, 80, 90])

        for thr in thresholds:
            for op_name, op_func in [(">", lambda x, t: x > t), ("<", lambda x, t: x < t)]:
                subset_mask = op_func(feat_vals, thr)
                n = int(subset_mask.sum())

                if n < min_n:
                    continue

                # How often did it fail in this subset?
                accuracy = float(failed[mask][subset_mask].mean())

                if accuracy >= min_accuracy:
                    lift = accuracy / base_failure_rate if base_failure_rate > 0 else 0.0
                    results.append(
                        {
                            "feature": feat,
                            "operator": op_name,
                            "threshold": round(float(thr), 6),
                            "n": n,
                            "accuracy": round(accuracy, 4),
                            "lift": round(lift, 4),
                            "coverage": round(n / base_n, 4),
                        }
                    )

    if not results:
        return pd.DataFrame(
            columns=["feature", "operator", "threshold", "n", "accuracy", "lift", "coverage"]
        )

    res_df = pd.DataFrame(results).sort_values("lift", ascending=False)
    # Deduplicate: if multiple thresholds for same feature/op are similar, keep best
    res_df = res_df.drop_duplicates(subset=["feature", "operator"], keep="first")

    return res_df
