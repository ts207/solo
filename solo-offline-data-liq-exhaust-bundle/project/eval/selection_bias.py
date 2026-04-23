from __future__ import annotations

import numpy as np
import pandas as pd
from project.core.constants import BARS_PER_YEAR_BY_TIMEFRAME

try:
    from scipy import stats
except ModuleNotFoundError:  # pragma: no cover - environment-specific fallback
    from project.core.stats import stats


def probabilistic_sharpe_ratio(
    pnl: pd.Series,
    benchmark_sr: float = 0.0,
    periods_per_year: int = BARS_PER_YEAR_BY_TIMEFRAME["5m"],
) -> float:
    """
    PSR: probability that the true SR exceeds benchmark_sr, corrected for
    skewness, kurtosis, and finite sample length.
    Bailey & Lopez de Prado (2012).

    The SR is computed in raw (per-observation) units so that the z-score
    remains in a numerically precise range regardless of annualization factor.
    The benchmark_sr is interpreted in the same raw units.
    """
    pnl_arr = pd.to_numeric(pnl, errors="coerce").dropna().values
    n = len(pnl_arr)
    if n < 10:
        return 0.0
    sr = float(np.mean(pnl_arr) / np.std(pnl_arr, ddof=1))
    skew = float(stats.skew(pnl_arr))
    kurt = float(stats.kurtosis(pnl_arr, fisher=True))  # excess kurtosis
    # Standard error of SR (Lo 2002 adjusted for non-normality)
    # Corrected: Bailey & Lopez de Prado (2012) uses (excess_kurtosis / 4.0)
    radicand = (1.0 + 0.5 * sr**2 - skew * sr + (kurt / 4.0) * sr**2) / (n - 1)
    se = np.sqrt(max(0.0, radicand))
    if se <= 0.0:
        return 1.0 if sr > benchmark_sr else 0.0
    z = (sr - benchmark_sr) / se
    return float(stats.norm.cdf(z))


def deflated_sharpe_ratio(
    pnl: pd.Series,
    n_trials: int = 1,
    benchmark_sr: float = 0.0,
    periods_per_year: int = BARS_PER_YEAR_BY_TIMEFRAME["5m"],
) -> float:
    """
    DSR: PSR with benchmark SR replaced by the expected maximum SR from n_trials
    independent tests (assumes IID SR estimates across trials).
    Bailey & Lopez de Prado (2014).

    expected_max is a dimensionless z-score scaled by 1/sqrt(n), which is the
    standard deviation of the raw SR estimator (mean/std of pnl observations).
    """
    pnl_arr = pd.to_numeric(pnl, errors="coerce").dropna().values
    n = len(pnl_arr)
    if n < 10 or n_trials < 1:
        return 0.0
    
    # Expected maximum of n_trials standard normal draws
    if n_trials == 1:
        expected_max = 0.0
    elif n_trials < 20:
        # Hardcoded expected maximums for small N (IID standard normals)
        # Values from: https://en.wikipedia.org/wiki/Expected_value_of_the_maximum_of_independent_normal_random_variables
        expected_max_lookup = {
            2: 0.5642, 3: 0.8463, 4: 1.0294, 5: 1.1630, 6: 1.2672,
            7: 1.3522, 8: 1.4236, 9: 1.4850, 10: 1.5388, 11: 1.5857,
            12: 1.6272, 13: 1.6652, 14: 1.6992, 15: 1.7307, 16: 1.7594,
            17: 1.7863, 18: 1.8114, 19: 1.8348
        }
        expected_max = expected_max_lookup.get(n_trials, 1.8)
    else:
        # Euler–Mascheroni approximation for large N
        euler_mascheroni = 0.5772156649
        expected_max = (
            (1.0 - euler_mascheroni) * stats.norm.ppf(1.0 - 1.0 / n_trials)
            + euler_mascheroni * stats.norm.ppf(1.0 - 1.0 / (n_trials * np.e))
        )
    
    # Std of the raw SR estimator across trials: 1/sqrt(n)
    sr_std = 1.0 / np.sqrt(n)
    # Deflated benchmark SR in raw units
    deflated_benchmark = max(benchmark_sr, expected_max * sr_std)
    return probabilistic_sharpe_ratio(
        pnl, benchmark_sr=deflated_benchmark, periods_per_year=periods_per_year
    )
