from __future__ import annotations

from typing import Any, cast

import pandas as pd

_CONTEXT_DIMENSIONS: dict[str, dict[str, str]] = {
    "vol": {
        "state": "ms_vol_state",
        "confidence": "ms_vol_confidence",
        "entropy": "ms_vol_entropy",
    },
    "liq": {
        "state": "ms_liq_state",
        "confidence": "ms_liq_confidence",
        "entropy": "ms_liq_entropy",
    },
    "oi": {
        "state": "ms_oi_state",
        "confidence": "ms_oi_confidence",
        "entropy": "ms_oi_entropy",
    },
    "funding": {
        "state": "ms_funding_state",
        "confidence": "ms_funding_confidence",
        "entropy": "ms_funding_entropy",
    },
    "trend": {
        "state": "ms_trend_state",
        "confidence": "ms_trend_confidence",
        "entropy": "ms_trend_entropy",
    },
    "spread": {
        "state": "ms_spread_state",
        "confidence": "ms_spread_confidence",
        "entropy": "ms_spread_entropy",
    },
}


def _distribution_stats(series: pd.Series) -> dict[str, float | None]:
    numeric = cast(pd.Series, pd.to_numeric(series, errors="coerce")).dropna().astype(float)
    if numeric.empty:
        return {
            "count": 0,
            "mean": None,
            "median": None,
            "p10": None,
            "p90": None,
            "min": None,
            "max": None,
        }
    return {
        "count": len(numeric),
        "mean": float(numeric.mean()),
        "median": float(numeric.median()),
        "p10": float(numeric.quantile(0.10)),
        "p90": float(numeric.quantile(0.90)),
        "min": float(numeric.min()),
        "max": float(numeric.max()),
    }


def _state_key(value: float) -> str:
    numeric = float(value)
    return str(int(numeric)) if numeric.is_integer() else str(numeric)


def summarize_context_quality(frame: pd.DataFrame) -> dict[str, Any]:
    dimensions: dict[str, dict[str, Any]] = {}

    for name, columns in _CONTEXT_DIMENSIONS.items():
        state_col = columns["state"]
        conf_col = columns["confidence"]
        entropy_col = columns["entropy"]

        state = cast(pd.Series, pd.to_numeric(
            frame.get(state_col, pd.Series(index=frame.index, dtype=float)), errors="coerce"
        )).astype(float)
        confidence = cast(pd.Series, pd.to_numeric(
            frame.get(conf_col, pd.Series(index=frame.index, dtype=float)),
            errors="coerce",
        )).astype(float)
        entropy = cast(pd.Series, pd.to_numeric(
            frame.get(entropy_col, pd.Series(index=frame.index, dtype=float)),
            errors="coerce",
        )).astype(float)

        valid_state = state.dropna()
        occupancy = {
            _state_key(cast(float, key)): float(value)
            for key, value in valid_state.value_counts(normalize=True).sort_index().items()
        }
        transitions = (
            int((valid_state != valid_state.shift(1)).iloc[1:].sum())
            if len(valid_state) >= 2
            else 0
        )
        transition_rate = (
            float(transitions / max(len(valid_state) - 1, 1)) if len(valid_state) >= 2 else 0.0
        )

        dimensions[name] = {
            "state_column": state_col,
            "valid_rows": len(valid_state),
            "occupancy": occupancy,
            "transition_count": transitions,
            "transition_rate": transition_rate,
            "confidence": _distribution_stats(confidence),
            "entropy": _distribution_stats(entropy),
        }

    return {
        "dimension_count": len(dimensions),
        "dimensions": dimensions,
    }
