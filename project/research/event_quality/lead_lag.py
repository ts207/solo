# project/research/event_quality/lead_lag.py
"""
Lead-lag analysis for events.

Two analyses:
1. event_return_lead_lag: mean return + t-stat at multiple forward horizons per event
2. event_event_lead_lag: causal ordering — how often does event B follow event A within k bars?
"""

from __future__ import annotations

import re
import numpy as np
import pandas as pd
from scipy import stats

_EVENT_COL_RE = re.compile(r"^event_(.+)$")


def _event_columns(features: pd.DataFrame) -> list[tuple[str, str]]:
    return [(m.group(1), col) for col in features.columns if (m := _EVENT_COL_RE.match(col))]


def compute_event_return_lead_lag(
    features: pd.DataFrame,
    *,
    horizons: list[int] | None = None,
    min_n: int = 5,
) -> pd.DataFrame:
    """
    For each event and each forward horizon h, compute the mean return and t-stat.
    Reveals the "peak predictive lag" per event.

    - n: number of valid event-fires with forward return available
    - mean_return_bps: mean forward log-return in basis points
    - t_stat: t-statistic (mean / se)

    Parameters
    ----------
    features : DataFrame with 'close' and event_* columns
    horizons : list of horizons in bars (default [3, 6, 12, 24, 48])
    min_n : events with fewer valid observations get t_stat=NaN

    Returns
    -------
    Long-form DataFrame: event_id × horizon_bars → metrics
    """
    if "close" not in features.columns:
        raise ValueError("features must contain 'close' column")
    if horizons is None:
        horizons = [3, 6, 12, 24, 48]

    close = features["close"].values.astype(float)
    n_bars = len(close)
    event_cols = _event_columns(features)

    rows = []
    for eid, col in event_cols:
        mask = features[col].fillna(False).astype(bool).to_numpy()
        fire_indices = np.where(mask)[0]

        for h in horizons:
            valid_idx = fire_indices[fire_indices + h < n_bars]
            n = len(valid_idx)
            if n < min_n:
                rows.append(
                    {
                        "event_id": eid,
                        "horizon_bars": h,
                        "n": n,
                        "mean_return_bps": float("nan"),
                        "t_stat": float("nan"),
                    }
                )
                continue

            fwd_returns = np.log(np.maximum(close[valid_idx + h], 1e-12)) - np.log(
                np.maximum(close[valid_idx], 1e-12)
            )
            mean_r = float(fwd_returns.mean())
            std_r = float(fwd_returns.std(ddof=1))
            t = mean_r / (std_r / np.sqrt(n)) if std_r > 1e-10 else 0.0
            rows.append(
                {
                    "event_id": eid,
                    "horizon_bars": h,
                    "n": n,
                    "mean_return_bps": round(mean_r * 10_000, 4),
                    "t_stat": round(t, 4),
                }
            )

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def compute_event_event_lead_lag(
    features: pd.DataFrame,
    *,
    max_lag: int = 24,
) -> pd.DataFrame:
    """
    For each ordered pair (A, B) and each lag in 1..max_lag, compute:
    - frequency: P(B fires at bar t+lag | A fires at bar t)

    This reveals causal ordering: if A tends to precede B, A→B frequency
    will peak at a specific lag while B→A frequency will be near 0 at the
    same lag.

    Returns
    -------
    Long-form DataFrame: source_event × target_event × lag_bars → frequency, n_source
    """
    event_cols = _event_columns(features)
    if len(event_cols) < 2:
        return pd.DataFrame()

    arrays: dict[str, np.ndarray] = {
        eid: features[col].fillna(False).astype(bool).to_numpy() for eid, col in event_cols
    }
    n_bars = len(features)

    rows = []
    for eid_a, arr_a in arrays.items():
        a_indices = np.where(arr_a)[0]
        n_a = len(a_indices)
        if n_a == 0:
            continue

        for eid_b, arr_b in arrays.items():
            if eid_a == eid_b:
                continue

            for lag in range(1, max_lag + 1):
                target_indices = a_indices + lag
                valid = target_indices < n_bars
                n_valid = int(valid.sum())
                if n_valid == 0:
                    freq = 0.0
                else:
                    freq = float(arr_b[target_indices[valid]].sum() / n_valid)

                rows.append(
                    {
                        "source_event": eid_a,
                        "target_event": eid_b,
                        "lag_bars": lag,
                        "frequency": round(freq, 6),
                        "n_source": n_valid,
                        "n_source_total": n_a,
                    }
                )

    return pd.DataFrame(rows) if rows else pd.DataFrame()
