from __future__ import annotations

import logging
from typing import Any, Dict, List

import pandas as pd

_LOG = logging.getLogger(__name__)


def evaluate_event_occurrence(
    events: pd.DataFrame,
    min_hits_per_year: int = 24,
) -> Dict[str, Any]:
    """
    Check if the event occurs frequently enough to be tradable.
    """
    if events.empty:
        return {"pass": False, "hits_per_year": 0}

    timestamps = pd.to_datetime(events["timestamp"], utc=True)
    time_span_years = (timestamps.max() - timestamps.min()).days / 365.25
    hits_per_year = len(events) / max(0.1, time_span_years)

    return {
        "total_hits": len(events),
        "hits_per_year": float(hits_per_year),
        "pass": bool(hits_per_year >= min_hits_per_year),
    }


def evaluate_regime_variance(
    events: pd.DataFrame,
    regimes: pd.Series,
    min_regimes: int = 2,
) -> Dict[str, Any]:
    """
    Check if the event occurs across multiple regimes (vol, trend, etc.).
    """
    if events.empty or regimes.empty:
        return {"pass": False, "n_regimes": 0}

    event_regimes = regimes.reindex(pd.to_datetime(events["timestamp"], utc=True)).dropna().unique()
    n_regimes = len(event_regimes)

    return {
        "n_regimes": int(n_regimes),
        "unique_regimes": list(event_regimes),
        "pass": bool(n_regimes >= min_regimes),
    }


def evaluate_parameter_sensitivity(
    events: pd.DataFrame,
    perturbed_events: List[pd.DataFrame],
    max_drop_ratio: float = 0.5,
) -> Dict[str, Any]:
    """
    Check if small parameter changes cause the event count to drop wildly.
    """
    base_count = len(events)
    if base_count == 0:
        return {"pass": False}

    counts = [len(p) for p in perturbed_events]
    min_perturbed_count = min(counts) if counts else 0
    drop_ratio = 1.0 - (min_perturbed_count / base_count)

    return {
        "drop_ratio": float(drop_ratio),
        "pass": bool(drop_ratio <= max_drop_ratio),
    }
