# project/research/event_quality/cooccurrence.py
"""
Event co-occurrence analysis.

For each ordered pair (A, B), computes:
  P(B fires within ±window_bars | A fires)

High co-occurrence (> redundancy_threshold) suggests A and B are measuring
the same underlying microstructure state and one is redundant.
"""

from __future__ import annotations

import re
import numpy as np
import pandas as pd

_EVENT_COL_RE = re.compile(r"^event_(.+)$")


def _event_columns(features: pd.DataFrame) -> list[tuple[str, str]]:
    """Return [(event_id, col_name)] for all event_* columns."""
    result = []
    for col in features.columns:
        m = _EVENT_COL_RE.match(col)
        if m:
            result.append((m.group(1), col))
    return result


def compute_cooccurrence(
    features: pd.DataFrame,
    *,
    window_bars: int = 5,
    redundancy_threshold: float = 0.5,
) -> pd.DataFrame:
    """
    Compute pairwise co-occurrence rates for all event_* columns.

    Parameters
    ----------
    features : wide feature DataFrame with boolean event_* columns
    window_bars : half-width of co-occurrence window (bars before or after)
    redundancy_threshold : pairs with p_b_given_a >= this are flagged

    Returns
    -------
    Long-form DataFrame with one row per ordered (A, B) pair:
        event_a, event_b, n_a_fires, n_co_fires, p_b_given_a,
        redundancy_candidate
    Sorted by p_b_given_a descending.
    """
    event_cols = _event_columns(features)
    if len(event_cols) < 2:
        return pd.DataFrame()

    # Convert to boolean numpy arrays once
    arrays: dict[str, np.ndarray] = {}
    for eid, col in event_cols:
        arrays[eid] = features[col].fillna(False).astype(bool).to_numpy()

    n_bars = len(features)
    rows = []

    for eid_a, _ in event_cols:
        arr_a = arrays[eid_a]
        a_indices = np.where(arr_a)[0]
        n_a = len(a_indices)
        if n_a == 0:
            continue

        for eid_b, _ in event_cols:
            if eid_a == eid_b:
                continue
            arr_b = arrays[eid_b]

            # Build a windowed OR mask: any bar within ±window_bars of a B fire
            # Efficient: convolve B with a ones window or use cumsum trick
            # Using cumsum trick for better performance on long series
            b_cumsum = np.cumsum(arr_b.astype(int))
            # For each bar i, sum of B in [i-window, i+window]
            left = np.maximum(0, np.arange(n_bars) - window_bars)
            right = np.minimum(n_bars - 1, np.arange(n_bars) + window_bars)

            # Use np.where to handle the boundary at index 0
            windowed_b_count = b_cumsum[right] - np.where(left > 0, b_cumsum[left - 1], 0)
            b_in_window = windowed_b_count > 0

            n_co = int(b_in_window[a_indices].sum())
            p_b_given_a = n_co / n_a if n_a > 0 else 0.0

            rows.append(
                {
                    "event_a": eid_a,
                    "event_b": eid_b,
                    "n_a_fires": n_a,
                    "n_co_fires": n_co,
                    "p_b_given_a": round(p_b_given_a, 4),
                    "redundancy_candidate": p_b_given_a >= redundancy_threshold,
                }
            )

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows).sort_values("p_b_given_a", ascending=False).reset_index(drop=True)
