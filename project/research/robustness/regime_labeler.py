# project/research/robustness/regime_labeler.py
"""
Regime labeler: assigns a discrete regime label to each bar in the features table.

Uses the 8 state columns already available in the features table (via
ColumnRegistry state column conventions) to produce a composite string label
across four dimensions: volatility, funding, trend, spread.

Label format: "high_vol.funding_pos.trend.tight" (dot-separated dimension values).
Unknown dimension (state column missing) produces "unknown_<dim>".
"""

from __future__ import annotations

import pandas as pd

from project.core.column_registry import ColumnRegistry

# Four regime dimensions, each with named states and their state_id lookup key.
# For each dimension, the first matching state determines the label.
# If no state column found in features, the dimension is labeled "unknown_<dim>".
REGIME_DIMENSIONS: dict = {
    "vol": {
        "states": {
            "high_vol_regime": "high_vol",
            "low_vol_regime": "low_vol",
        },
        "default_label": "unknown_vol",
    },
    "funding": {
        "states": {
            "funding_positive": "funding_pos",
            "funding_negative": "funding_neg",
        },
        "default_label": "unknown_funding",
    },
    "trend": {
        "states": {
            "trend_active": "trend",
            "chop_active": "chop",
        },
        "default_label": "unknown_trend",
    },
    "spread": {
        "states": {
            "spread_tight": "tight",
            "spread_wide": "wide",
        },
        "default_label": "unknown_spread",
    },
}


def _numeric_series(features: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(features[column], errors="coerce").fillna(0.0)


def _resolve_state_col(state_id: str, features: pd.DataFrame) -> str | None:
    """Return the first column name matching state_id that exists in features."""
    for col in ColumnRegistry.state_cols(state_id):
        if col in features.columns:
            return col
    return None


def _alias_state_mask(state_id: str, features: pd.DataFrame) -> pd.Series | None:
    """Resolve common search-prepared aliases that are not registered as state columns."""
    sid = str(state_id).strip().lower()

    if sid == "funding_positive" and "carry_state_code" in features.columns:
        return _numeric_series(features, "carry_state_code") > 0
    if sid == "funding_negative" and "carry_state_code" in features.columns:
        return _numeric_series(features, "carry_state_code") < 0

    if sid == "trend_active":
        if "trending_state" in features.columns:
            return _numeric_series(features, "trending_state") > 0
        bull = (
            _numeric_series(features, "bull_trend_regime")
            if "bull_trend_regime" in features.columns
            else pd.Series(0.0, index=features.index)
        )
        bear = (
            _numeric_series(features, "bear_trend_regime")
            if "bear_trend_regime" in features.columns
            else pd.Series(0.0, index=features.index)
        )
        if "bull_trend_regime" in features.columns or "bear_trend_regime" in features.columns:
            return (bull > 0) | (bear > 0)

    if sid == "chop_active":
        if "chop_state" in features.columns:
            return _numeric_series(features, "chop_state") > 0
        if "chop_regime" in features.columns:
            return _numeric_series(features, "chop_regime") > 0

    if sid == "spread_tight":
        if "prob_spread_tight" in features.columns:
            return _numeric_series(features, "prob_spread_tight") >= 0.5
    if sid == "spread_wide":
        if "prob_spread_wide" in features.columns:
            return _numeric_series(features, "prob_spread_wide") >= 0.5
        if "spread_elevated_state" in features.columns:
            return _numeric_series(features, "spread_elevated_state") > 0

    return None


def _state_active_mask(state_id: str, features: pd.DataFrame) -> pd.Series | None:
    col = _resolve_state_col(state_id, features)
    if col is not None:
        return _numeric_series(features, col) == 1
    return _alias_state_mask(state_id, features)


def label_regimes(features: pd.DataFrame) -> pd.Series:
    """
    Assign a composite regime label to each bar in features.

    Parameters
    ----------
    features : wide feature DataFrame; must have state_* or ms_* columns for
               any dimensions to be resolved

    Returns
    -------
    pd.Series of string labels, same index as features.
    Format: "high_vol.funding_pos.trend.tight" (dot-separated).
    """
    import logging

    _log = logging.getLogger(__name__)

    dimension_labels: list[pd.Series] = []
    unknown_dims: list[str] = []

    for dim_name, cfg in REGIME_DIMENSIONS.items():
        default_label = cfg["default_label"]
        dim_series = pd.Series(default_label, index=features.index)

        resolved_any = False
        for state_id, label in cfg["states"].items():
            active = _state_active_mask(state_id, features)
            if active is None:
                continue
            resolved_any = True
            # Set label where this state is active (earlier states take priority)
            currently_unknown = dim_series == default_label
            dim_series = dim_series.where(~(active & currently_unknown), other=label)

        if not resolved_any:
            unknown_dims.append(dim_name)

        dimension_labels.append(dim_series)

    if unknown_dims:
        _log.warning(
            "label_regimes: no state columns found for dimensions %s — "
            "those dimensions will be labelled 'unknown_<dim>' for all %d bars. "
            "Regime-gated strategies will receive uniform labels for these dimensions. "
            "Expected state columns: %s",
            unknown_dims,
            len(features),
            {d: list(REGIME_DIMENSIONS[d]["states"].keys()) for d in unknown_dims},
        )

    if not dimension_labels:
        return pd.Series("unknown.unknown.unknown.unknown", index=features.index)

    # Join dimension labels with "."
    result = dimension_labels[0].copy()
    for s in dimension_labels[1:]:
        result = result + "." + s

    return result
