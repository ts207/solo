# project/research/event_quality/firing_rate.py
"""
Firing rate audit for event columns in the wide features DataFrame.

Scans all columns matching the `event_*` pattern, computes per-event
fire counts and rates, and flags events below a minimum n threshold.
"""

from __future__ import annotations

import re

import pandas as pd

_EVENT_COL_RE = re.compile(r"^event_(.+)$")


def compute_firing_rates(
    features: pd.DataFrame,
    *,
    bars_per_day: int = 288,  # 5m bars: 288/day
    min_n: int = 100,
) -> pd.DataFrame:
    """
    Compute firing statistics for every event_* column in features.

    Parameters
    ----------
    features : wide feature DataFrame with boolean event_* columns
    bars_per_day : number of bars per calendar day (for rate computation)
    min_n : events with n_fires < min_n are flagged as below_min_n

    Returns
    -------
    DataFrame with columns:
        event_id, column_name, n_fires, fire_rate_per_1000_bars,
        events_per_day, pct_of_bars, below_min_n
    Sorted by n_fires descending.
    """
    rows = []
    n_bars = len(features)
    if n_bars == 0:
        return pd.DataFrame()

    for col in features.columns:
        m = _EVENT_COL_RE.match(col)
        if m is None:
            continue
        event_id = m.group(1)
        # Ensure column is boolean and fill NaNs
        s = features[col].fillna(False).astype(bool)
        n_fires = int(s.sum())
        fire_rate_per_1000 = round(n_fires / n_bars * 1000, 4)
        days = n_bars / bars_per_day
        events_per_day = round(n_fires / days, 4) if days > 0 else 0.0
        pct = round(n_fires / n_bars * 100, 4)
        rows.append(
            {
                "event_id": event_id,
                "column_name": col,
                "n_fires": n_fires,
                "fire_rate_per_1000_bars": fire_rate_per_1000,
                "events_per_day": events_per_day,
                "pct_of_bars": pct,
                "below_min_n": n_fires < min_n,
            }
        )

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows).sort_values("n_fires", ascending=False).reset_index(drop=True)
