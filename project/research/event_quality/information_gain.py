# project/research/event_quality/information_gain.py
"""
Information gain analysis for event columns.

For each event_* column, computes:
  IG = H(Y) - H(Y|event)

where Y is forward log-return discretized into n_bins quantile bins.
High IG indicates the event carries genuine predictive information.
Low IG (near 0) indicates the event fires independently of future returns.
"""

from __future__ import annotations

import re

import numpy as np
import pandas as pd
from scipy.stats import entropy as scipy_entropy

_EVENT_COL_RE = re.compile(r"^event_(.+)$")


def _entropy_bits(counts: np.ndarray) -> float:
    """Shannon entropy in bits from an array of counts."""
    counts = counts[counts > 0]
    if len(counts) == 0:
        return 0.0
    p = counts / counts.sum()
    # base=2 for bits
    return float(scipy_entropy(p, base=2))


def compute_information_gain(
    features: pd.DataFrame,
    *,
    horizon_bars: int = 12,
    n_bins: int = 4,
    min_fires: int = 10,
) -> pd.DataFrame:
    """
    Compute information gain (bits) for each event_* column vs forward returns.

    Parameters
    ----------
    features : wide feature DataFrame with 'close' and boolean event_* columns
    horizon_bars : forward return horizon in bars
    n_bins : number of quantile bins for discretizing forward returns
    min_fires : events with fewer fires are assigned ig_bits=NaN

    Returns
    -------
    DataFrame with columns:
        event_id, column_name, n_fires, n_nonfires, baseline_entropy_bits,
        ig_bits, conditional_entropy_bits
    Sorted by ig_bits descending.
    """
    if "close" not in features.columns:
        raise ValueError("features must contain 'close' column")

    close = features["close"].values.astype(float)
    n_bars = len(close)

    # Forward log-returns
    fwd = np.full(n_bars, np.nan)
    if n_bars > horizon_bars:
        fwd[: n_bars - horizon_bars] = np.log(np.maximum(close[horizon_bars:], 1e-12)) - np.log(
            np.maximum(close[: n_bars - horizon_bars], 1e-12)
        )

    # Discretize into quantile bins (ignore NaN)
    valid = np.isfinite(fwd)
    if not valid.any():
        return pd.DataFrame()

    bin_edges = np.nanpercentile(fwd[valid], np.linspace(0, 100, n_bins + 1))
    bin_edges[0] = -np.inf
    bin_edges[-1] = np.inf
    y_bins = np.digitize(fwd, bin_edges) - 1  # 0..n_bins-1
    y_bins = np.clip(y_bins, 0, n_bins - 1)

    # Baseline entropy H(Y)
    baseline_counts = np.bincount(y_bins[valid], minlength=n_bins).astype(float)
    h_y = _entropy_bits(baseline_counts)

    rows = []
    for col in features.columns:
        m = _EVENT_COL_RE.match(col)
        if m is None:
            continue
        event_id = m.group(1)

        mask = features[col].fillna(False).astype(bool).to_numpy()
        n_fires = int(mask.sum())

        if n_fires < min_fires:
            rows.append(
                {
                    "event_id": event_id,
                    "column_name": col,
                    "n_fires": n_fires,
                    "n_nonfires": n_bars - n_fires,
                    "baseline_entropy_bits": round(h_y, 6),
                    "conditional_entropy_bits": float("nan"),
                    "ig_bits": float("nan"),
                }
            )
            continue

        # H(Y | event) = P(event=1)*H(Y|event=1) + P(event=0)*H(Y|event=0)
        # Using bars where fwd is valid
        fire_valid = mask & valid
        nfire_valid = (~mask) & valid

        n_v = valid.sum()
        p_fire = fire_valid.sum() / n_v
        p_nofire = nfire_valid.sum() / n_v

        counts_fire = np.bincount(y_bins[fire_valid], minlength=n_bins).astype(float)
        counts_nofire = np.bincount(y_bins[nfire_valid], minlength=n_bins).astype(float)

        h_y_given_fire = _entropy_bits(counts_fire)
        h_y_given_nofire = _entropy_bits(counts_nofire)

        h_y_given_event = p_fire * h_y_given_fire + p_nofire * h_y_given_nofire
        ig = max(0.0, h_y - h_y_given_event)

        rows.append(
            {
                "event_id": event_id,
                "column_name": col,
                "n_fires": n_fires,
                "n_nonfires": n_bars - n_fires,
                "baseline_entropy_bits": round(h_y, 6),
                "conditional_entropy_bits": round(h_y_given_event, 6),
                "ig_bits": round(ig, 6),
            }
        )

    if not rows:
        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .sort_values("ig_bits", ascending=False, na_position="last")
        .reset_index(drop=True)
    )
