"""
Phase 2 Gating Logic: Expectancy calculation, FDR adjustment, and Drawdown gating.
Refactored to improve testability and separate concerns.
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

try:
    from scipy import stats
except ModuleNotFoundError:
    from project.core.stats import stats

from project.core.constants import parse_horizon_bars
from project.core.stats import bh_adjust
from project.core.validation import ts_ns_utc
from project.research.holdout_integrity import assert_no_lookahead_join

log = logging.getLogger(__name__)


def distribution_stats(returns: np.ndarray) -> dict[str, float]:
    """Compute mean, std, HAC t-stat, p-value for a return distribution."""
    clean = np.asarray(returns, dtype=float)
    clean = clean[np.isfinite(clean)]
    if len(clean) < 2:
        return {"mean": 0.0, "std": 0.0, "t_stat": 0.0, "p_value": 1.0}
    mean = float(np.mean(clean))
    std = float(np.std(clean, ddof=1))
    if std == 0:
        return {"mean": mean, "std": 0.0, "t_stat": 0.0, "p_value": 1.0}
    from project.core.stats import newey_west_t_stat_for_mean

    nw = newey_west_t_stat_for_mean(clean)
    t_stat = float(nw.t_stat) if np.isfinite(nw.t_stat) else mean / (std / np.sqrt(len(clean)))
    p_value = one_sided_p_from_t(t_stat, df=max(len(clean) - 1, 1))
    return {"mean": mean, "std": std, "t_stat": t_stat, "p_value": p_value}


def one_sided_p_from_t(t_stat: float, df: int) -> float:
    """Compute right-tail (one-sided) p-value. Large negative t-stat -> p close to 1.0."""
    if df < 1:
        return 1.0
    return float(stats.t.sf(t_stat, df=df))


def two_sided_p_from_t(t_stat: float, df: int) -> float:
    """
    DEPRECATED: Now aliased to one_sided_p_from_t to ensure all directional hypotheses
    are gated correctly in the research pipeline. Large negative t-stats will now
    receive high p-values (approaching 1.0) rather than low p-values.

    This function will be removed in a future release. Callers must migrate to
    one_sided_p_from_t. In production environments where DeprecationWarnings are
    suppressed, an ERROR-level log is also emitted to ensure visibility.
    """
    import logging as _logging
    import warnings
    _msg = (
        "two_sided_p_from_t is deprecated; use one_sided_p_from_t for directional hypotheses. "
        "Results produced before this function was aliased may have incorrectly passed gating "
        "on strongly negative t-stats."
    )
    warnings.warn(_msg, DeprecationWarning, stacklevel=2)
    _logging.getLogger(__name__).error("DEPRECATED CALL: %s", _msg)
    return one_sided_p_from_t(t_stat, df=df)


def horizon_to_bars(horizon: str) -> int:
    return parse_horizon_bars(horizon, default=12)


def join_events_to_features(
    events_df: pd.DataFrame,
    features_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge event timestamps to the features table using a backward merge.
    """
    ts_col = "enter_ts" if "enter_ts" in events_df.columns else "timestamp"
    if ts_col not in events_df.columns:
        return pd.DataFrame()

    evt = events_df.copy()
    evt["event_ts"] = ts_ns_utc(evt[ts_col], allow_nat=True)
    evt = evt.dropna(subset=["event_ts"]).sort_values("event_ts").reset_index(drop=True)
    if evt.empty:
        return pd.DataFrame()

    if "timestamp" not in features_df.columns:
        return pd.DataFrame()
    feat = features_df.copy()
    feat["feature_ts"] = ts_ns_utc(feat["timestamp"], allow_nat=True)
    feat = feat.dropna(subset=["feature_ts"]).sort_values("feature_ts").reset_index(drop=True)
    if feat.empty:
        return pd.DataFrame()
    feat["_feature_pos"] = feat.index.astype(int)

    # Use merge_asof: for each event, find the latest feature bar <= event_ts
    extra_evt_cols = [
        col
        for col in (
            "vol_regime",
            "liquidity_state",
            "market_liquidity_state",
            "depth_state",
            "event_direction",
            "direction",
            "signal_direction",
            "flow_direction",
            "breakout_direction",
            "shock_direction",
            "move_direction",
            "leader_direction",
            "return_1",
            "return_sign",
            "sign",
            "polarity",
            "funding_z",
            "basis_z",
            "side",
            "trade_side",
            "direction_label",
            "split_label",
        )
        if col in evt.columns
    ]

    evt_cols = ["event_ts"] + extra_evt_cols
    evt_for_join = evt[evt_cols].rename(columns={c: f"evt_{c}" for c in extra_evt_cols})

    merged = pd.merge_asof(
        evt_for_join,
        feat,
        left_on="event_ts",
        right_on="feature_ts",
        direction="backward",
    )
    assert_no_lookahead_join(
        merged,
        event_ts_col="event_ts",
        feature_ts_col="feature_ts",
        context="project.research.gating.join_events_to_features",
    )
    return merged


def empty_expectancy_stats() -> dict[str, Any]:
    return {
        "mean_return": 0.0,
        "p_value": 1.0,
        "n_events": 0.0,
        "n_effective": 0.0,
        "stability_pass": False,
        "std_return": 0.0,
        "t_stat": 0.0,
        "time_weight_sum": 0.0,
        "mean_weight_age_days": 0.0,
        "mean_tau_days": 0.0,
        "learning_rate_mean": 0.0,
        "mean_tau_up_days": 0.0,
        "mean_tau_down_days": 0.0,
        "tau_directional_ratio": 0.0,
        "directional_up_share": 0.0,
        "mean_train_return": 0.0,
        "mean_validation_return": 0.0,
        "mean_test_return": 0.0,
        "train_samples": 0,
        "validation_samples": 0,
        "test_samples": 0,
        "t_train": 0.0,
        "t_validation": 0.0,
        "t_test": 0.0,
    }


__all__ = [
    "bh_adjust",
    "distribution_stats",
    "empty_expectancy_stats",
    "horizon_to_bars",
    "join_events_to_features",
    "two_sided_p_from_t",
]
