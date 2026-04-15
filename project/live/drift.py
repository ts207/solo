from __future__ import annotations

import logging
from typing import Dict, Any, List

import numpy as np
import pandas as pd

_LOG = logging.getLogger(__name__)

# Minimum observations per PSI bin.  With fewer obs, PSI estimates are unstable
# (swing ±0.1-0.2 from sampling noise alone at n=10 per bin).
_MIN_OBS_PER_BIN = 30
# Absolute minimum number of bins even when sample size is small.
_MIN_BINS = 2


def _adaptive_n_bins(n_research: int) -> int:
    """
    Compute the number of PSI bins so that each bin holds at least
    _MIN_OBS_PER_BIN research observations.  Clamps to [_MIN_BINS, 20].
    """
    if n_research <= 0:
        return _MIN_BINS
    return int(np.clip(n_research // _MIN_OBS_PER_BIN, _MIN_BINS, 20))


def _ks_statistic(research: np.ndarray, live: np.ndarray) -> float:
    """
    Two-sample Kolmogorov-Smirnov statistic.  More sensitive to tail divergence
    than PSI because extreme live values are not absorbed into outer bins.
    Returns a value in [0, 1]; higher = more divergent.
    """
    all_vals = np.concatenate([research, live])
    all_vals = np.sort(np.unique(all_vals))
    if all_vals.size == 0:
        return 0.0
    n_r, n_l = len(research), len(live)
    if n_r == 0 or n_l == 0:
        return 0.0
    cdf_r = np.searchsorted(np.sort(research), all_vals, side="right") / n_r
    cdf_l = np.searchsorted(np.sort(live), all_vals, side="right") / n_l
    return float(np.max(np.abs(cdf_r - cdf_l)))


def _population_stability_index(
    research_samples: pd.Series,
    live_samples: pd.Series,
    *,
    n_bins: int | None = None,
) -> float:
    """
    PSI with adaptive binning.

    ``n_bins`` is determined automatically when None (default), targeting at
    least _MIN_OBS_PER_BIN research observations per bin.  This prevents the
    severe PSI estimate instability that occurs when bins contain < 30 obs.

    Extreme live values that fall outside the research range are counted in the
    outermost bins rather than being discarded; edge bins are extended to ±inf
    so all live observations are captured.
    """
    research = pd.to_numeric(research_samples, errors="coerce").dropna().to_numpy(dtype=float)
    live = pd.to_numeric(live_samples, errors="coerce").dropna().to_numpy(dtype=float)
    if research.size == 0 or live.size == 0:
        return 0.0

    if np.allclose(research, research[0]) and np.allclose(live, live[0]):
        return 0.0 if np.isclose(research[0], live[0]) else float("inf")

    effective_n_bins = n_bins if n_bins is not None else _adaptive_n_bins(len(research))
    quantiles = np.linspace(0.0, 1.0, num=effective_n_bins + 1)
    edges = np.unique(np.quantile(research, quantiles))
    if edges.size < 2:
        min_edge = float(min(np.min(research), np.min(live)))
        max_edge = float(max(np.max(research), np.max(live)))
        if np.isclose(min_edge, max_edge):
            return 0.0
        edges = np.array([min_edge, max_edge], dtype=float)

    # Extend outer edges to ±inf so extreme live values are captured in the
    # outermost bins rather than being clumped or lost.
    edges = edges.astype(float, copy=True)
    edges[0] = -np.inf
    edges[-1] = np.inf

    expected, _ = np.histogram(research, bins=edges)
    actual, _ = np.histogram(live, bins=edges)
    expected = expected.astype(float)
    actual = actual.astype(float)

    expected_pct = expected / max(1.0, expected.sum())
    actual_pct = actual / max(1.0, actual.sum())

    smoothing = 1e-12
    expected_pct = np.clip(expected_pct, smoothing, None)
    actual_pct = np.clip(actual_pct, smoothing, None)
    expected_pct /= expected_pct.sum()
    actual_pct /= actual_pct.sum()

    return float(np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct)))


def calculate_feature_drift(
    research_feature_samples: pd.Series,
    live_feature_samples: pd.Series,
    threshold: float = 0.2,
) -> Dict[str, Any]:
    """
    Calculate feature drift using Population Stability Index (PSI) and the
    Kolmogorov-Smirnov statistic.

    PSI uses adaptive binning (see ``_population_stability_index``).
    KS is reported as a supplementary signal — it is more sensitive to tail
    divergence and does not underdetect when extreme live values are absorbed
    into PSI's outer bins.

    Standard PSI interpretation:
      < 0.10  stable
      0.10-0.25  minor shift (warn)
      > 0.25  major shift (error)
    """
    if research_feature_samples.empty or live_feature_samples.empty:
        return {}

    research = pd.to_numeric(research_feature_samples, errors="coerce").dropna()
    live = pd.to_numeric(live_feature_samples, errors="coerce").dropna()

    psi = _population_stability_index(research, live)
    ks = _ks_statistic(research.to_numpy(dtype=float), live.to_numpy(dtype=float))
    research_mean = float(research.mean())
    live_mean = float(live.mean())

    return {
        "drift_score": float(psi),
        "psi": float(psi),
        "ks_statistic": float(ks),
        "is_drifting": bool(psi > threshold),
        "research_mean": research_mean,
        "live_mean": live_mean,
        "n_bins_used": _adaptive_n_bins(len(research)),
    }


def monitor_execution_drift(
    research_slippage_bps: float,
    live_slippage_bps: float,
    research_fill_rate: float,
    live_fill_rate: float,
) -> Dict[str, Any]:
    """
    Monitor if execution conditions are worse than research assumptions.
    """
    slippage_drift = live_slippage_bps / max(1e-6, abs(research_slippage_bps))
    fill_rate_drift = live_fill_rate / max(1e-6, research_fill_rate)

    return {
        "slippage_drift_ratio": float(slippage_drift),
        "fill_rate_drift_ratio": float(fill_rate_drift),
        "alert": bool(slippage_drift > 2.0 or fill_rate_drift < 0.5),
    }
