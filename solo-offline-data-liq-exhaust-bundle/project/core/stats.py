from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

try:
    from scipy import stats as scipy_stats
except ModuleNotFoundError:  # pragma: no cover - environment-specific fallback
    scipy_stats = None

# Re-export so downstream `from project.core.stats import stats` resolves to scipy.stats
# rather than to this module itself.
stats = scipy_stats

import logging
_LOG = logging.getLogger(__name__)


def calculate_kendalls_tau(x: np.ndarray | pd.Series, y: np.ndarray | pd.Series) -> float:
    """
    Calculate Kendall's Tau rank correlation.
    """
    try:
        from scipy import stats as scipy_stats

        tau, _ = scipy_stats.kendalltau(x, y)
    except ImportError:
        tau, _ = _StatsCompat.kendalltau(x, y)
    if tau is None or not np.isfinite(tau):
        return 0.0
    return float(np.clip(tau, -0.999, 0.999))


def test_cointegration(x: pd.Series, y: pd.Series) -> float:
    """
    Engle-Granger-style cointegration test.
    Returns the p-value for the residual unit-root test.
    Uses statsmodels when available and falls back to a residual ADF-style t test.
    """
    aligned = pd.concat(
        [
            pd.to_numeric(pd.Series(x), errors="coerce"),
            pd.to_numeric(pd.Series(y), errors="coerce"),
        ],
        axis=1,
    ).dropna()
    if len(aligned) < 20:
        return 1.0

    xa = aligned.iloc[:, 0].to_numpy(dtype=float)
    ya = aligned.iloc[:, 1].to_numpy(dtype=float)
    try:
        from statsmodels.tsa.stattools import coint

        _stat, _pvalue, _crit = coint(xa, ya)
        if np.isfinite(_pvalue):
            return float(np.clip(_pvalue, 0.0, 1.0))
    except Exception as e:
        _LOG.warning("statsmodels.tsa.stattools.coint failed or missing; using finite-sample ADF fallback. Error: %s", e)
        pass

    X = np.column_stack([np.ones(len(xa)), xa])
    beta, *_ = np.linalg.lstsq(X, ya, rcond=None)
    resid = ya - X @ beta
    if len(resid) < 10 or np.allclose(resid, resid[0]):
        return 1.0

    # Lag-augmented ADF-style test on residuals.
    # Regression: d(resid_t) = alpha + gamma * resid_{t-1} + phi * d(resid_{t-1}) + eps_t
    diff_resid = np.diff(resid)
    lag_resid = resid[:-1]
    if len(diff_resid) < 3:
        return 1.0
    lag_diff = diff_resid[:-1]
    y_dep = diff_resid[1:]
    x_level = lag_resid[1:]
    design = np.column_stack([np.ones(len(y_dep)), x_level, lag_diff])
    coef, *_ = np.linalg.lstsq(design, y_dep, rcond=None)
    gamma_val = float(coef[1])
    eps = y_dep - design @ coef
    dof = max(1, len(y_dep) - design.shape[1])
    sample_var = float(np.sum(eps**2) / dof)
    xtx = design.T @ design
    try:
        cov = sample_var * np.linalg.inv(xtx)
        gamma_se = float(np.sqrt(max(cov[1, 1], 0.0)))
    except Exception as e:
        _LOG.error("Cointegration fallback calculation failed: %s. Returning conservative p=1.0", e)
        return 1.0

    t_stat = gamma_val / gamma_se if (gamma_se and np.isfinite(gamma_se) and gamma_se > 0) else 0.0

    # MacKinnon (1994) approximate p-values for cointegration (N=2, constant, no trend)
    return _approx_coint_pvalue(t_stat, n=len(resid))


def _approx_coint_pvalue(t_stat: float, n: int) -> float:
    """
    Approximate p-value for Engle-Granger cointegration test (N=2, constant).
    Uses MacKinnon-style response surfaces when available and falls back to a
    finite-sample logistic fit anchored to the sample-size-dependent critical
    values.
    """
    if n < 50:
        return 1.0

    try:
        from statsmodels.tsa.adfvalues import mackinnoncrit

        crit_1, crit_5, crit_10 = map(float, mackinnoncrit(N=2, regression="c", nobs=max(3, n - 1)))
        probs = np.array([0.01, 0.05, 0.10], dtype=float)
        criticals = np.array([crit_1, crit_5, crit_10], dtype=float)
        logits = np.log(probs / (1.0 - probs))
        a, b = np.polyfit(criticals, logits, 1)
        return float(np.clip(1.0 / (1.0 + np.exp(-(a * t_stat + b))), 0.0, 1.0))
    except Exception:
        # Finite-sample fallback calibrated against sample-size-dependent critical values.
        crit_1 = -3.90 + 12.0 / max(50.0, float(n))
        crit_5 = -3.34 + 10.0 / max(50.0, float(n))
        crit_10 = -3.04 + 8.0 / max(50.0, float(n))
        probs = np.array([0.01, 0.05, 0.10], dtype=float)
        criticals = np.array([crit_1, crit_5, crit_10], dtype=float)
        logits = np.log(probs / (1.0 - probs))
        a, b = np.polyfit(criticals, logits, 1)
        return float(np.clip(1.0 / (1.0 + np.exp(-(a * t_stat + b))), 0.0, 1.0))


def _to_array(values: object) -> np.ndarray:
    return np.asarray(values, dtype=float)


def _to_output(values: np.ndarray, original: object) -> float | np.ndarray:
    if np.isscalar(original):
        return float(values.reshape(-1)[0])
    return values


def _norm_cdf(x: object) -> float | np.ndarray:
    arr = _to_array(x)
    out = 0.5 * (1.0 + np.vectorize(math.erf)(arr / math.sqrt(2.0)))
    return _to_output(out, x)


def _norm_ppf(p: object) -> float | np.ndarray:
    arr = np.clip(_to_array(p), 1e-12, 1.0 - 1e-12)
    a = np.array(
        [
            -3.969683028665376e01,
            2.209460984245205e02,
            -2.759285104469687e02,
            1.383577518672690e02,
            -3.066479806614716e01,
            2.506628277459239e00,
        ],
        dtype=float,
    )
    b = np.array(
        [
            -5.447609879822406e01,
            1.615858368580409e02,
            -1.556989798598866e02,
            6.680131188771972e01,
            -1.328068155288572e01,
        ],
        dtype=float,
    )
    c = np.array(
        [
            -7.784894002430293e-03,
            -3.223964580411365e-01,
            -2.400758277161838e00,
            -2.549732539343734e00,
            4.374664141464968e00,
            2.938163982698783e00,
        ],
        dtype=float,
    )
    d = np.array(
        [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00],
        dtype=float,
    )
    plow = 0.02425
    phigh = 1.0 - plow
    x = np.zeros_like(arr, dtype=float)
    low_mask = arr < plow
    high_mask = arr > phigh
    mid_mask = ~(low_mask | high_mask)
    if np.any(low_mask):
        q = np.sqrt(-2.0 * np.log(arr[low_mask]))
        x[low_mask] = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            (((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0
        )
    if np.any(mid_mask):
        q = arr[mid_mask] - 0.5
        r = q * q
        x[mid_mask] = (
            (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5])
            * q
            / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
        )
    if np.any(high_mask):
        q = np.sqrt(-2.0 * np.log(1.0 - arr[high_mask]))
        # Note: Rational part is negative for these coefficients, so multiply by -1.0 to get positive x (Finding 94)
        x[high_mask] = (
            -1.0
            * (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
            / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        )
    return _to_output(x, p)


def _student_t_pdf(x: np.ndarray, df: float) -> np.ndarray:
    if df > 100:
        # Normal approximation for large df
        return (1.0 / math.sqrt(2 * math.pi)) * np.exp(-0.5 * x**2)

    try:
        coeff = math.gamma((df + 1.0) / 2.0) / (math.sqrt(df * math.pi) * math.gamma(df / 2.0))
    except OverflowError:
        # Fallback if gamma still overflows
        return (1.0 / math.sqrt(2 * math.pi)) * np.exp(-0.5 * x**2)

    return coeff * np.power(1.0 + (x * x) / df, -(df + 1.0) / 2.0)


def _student_t_cdf_scalar(x: float, df: float) -> float:
    if not np.isfinite(x):
        return 0.0 if x < 0 else 1.0
    if not np.isfinite(df) or df <= 0:
        return float(_norm_cdf(x))
    if x == 0.0:
        return 0.5
        
    # Optimization: Use regularized incomplete beta function if available
    try:
        from scipy.special import betainc
        # CDF = 1 - 0.5 * I_z(df/2, 0.5) where z = df / (df + x^2) for x > 0
        # CDF = 0.5 * I_z(df/2, 0.5) for x < 0
        # Symmetry: F(-x) = 1 - F(x)
        # Using the x > 0 formula:
        # F(x) = 1 - 0.5 * betainc(df/2, 0.5, df / (df + x**2))
        
        x_sq = x * x
        z = df / (df + x_sq)
        val = 0.5 * betainc(df / 2.0, 0.5, z)
        return float(1.0 - val) if x > 0 else float(val)
    except ImportError:
        pass

    upper = abs(float(x))
    n_steps = int(min(4000, max(400, math.ceil(upper * 200))))
    grid = np.linspace(0.0, upper, n_steps + 1)
    y = _student_t_pdf(grid, float(df))
    # NumPy 2.0 compatibility: trapz -> trapezoid
    if hasattr(np, "trapezoid"):
        area = np.trapezoid(y, grid)
    elif hasattr(np, "trapz"):
        area = np.trapz(y, grid)
    else:
        # manual trapezoidal rule
        area = float(np.sum((y[:-1] + y[1:]) / 2.0 * np.diff(grid)))
    cdf = 0.5 + math.copysign(area, x)
    return float(np.clip(cdf, 0.0, 1.0))


def _student_t_cdf(x: object, df: object) -> float | np.ndarray:
    arr = _to_array(x)
    if np.isscalar(df):
        out = np.vectorize(lambda v: _student_t_cdf_scalar(float(v), float(df)))(arr)
    else:
        df_arr = _to_array(df)
        out = np.vectorize(lambda v, d: _student_t_cdf_scalar(float(v), float(d)))(arr, df_arr)
    return _to_output(out, x)


def _student_t_sf(x: object, df: object) -> float | np.ndarray:
    cdf = _to_array(_student_t_cdf(x, df))
    out = np.clip(1.0 - cdf, 0.0, 1.0)
    return _to_output(out, x)


def _skew(values: Iterable[float]) -> float:
    arr = _to_array(list(values))
    arr = arr[np.isfinite(arr)]
    n = arr.size
    if n < 3:
        return 0.0
    centered = arr - float(np.mean(arr))
    m2 = float(np.mean(centered**2))
    if m2 <= 0.0:
        return 0.0
    m3 = float(np.mean(centered**3))
    return float(m3 / (m2**1.5))


def _kurtosis(values: Iterable[float], fisher: bool = True) -> float:
    arr = _to_array(list(values))
    arr = arr[np.isfinite(arr)]
    n = arr.size
    if n < 4:
        return 0.0
    centered = arr - float(np.mean(arr))
    m2 = float(np.mean(centered**2))
    if m2 <= 0.0:
        return 0.0

    if fisher:
        # Implementation of unbiased excess kurtosis
        # Reference: https://en.wikipedia.org/wiki/Kurtosis#Standard_unbiased_estimator
        # Manual formula using m2 and m4 (biased moments)
        # g2 = m4/m2^2 - 3 (biased excess)
        # G2 = ((n+1) * g2 + 6) * (n-1) / ((n-2)*(n-3))
        g2 = (float(np.mean(centered**4)) / (m2**2)) - 3.0
        return ((n + 1) * g2 + 6) * (n - 1) / ((n - 2) * (n - 3))
    else:
        # Biased population kurtosis (Pearson)
        m4 = float(np.mean(centered**4))
        return float(m4 / (m2 * m2))


def _kendalltau(x: object, y: object) -> Tuple[float, float]:
    xa = _to_array(x)
    ya = _to_array(y)
    mask = np.isfinite(xa) & np.isfinite(ya)
    xa = xa[mask]
    ya = ya[mask]
    n = xa.size
    if n < 2:
        return 0.0, 1.0
    
    # Scalability guard: pure-Python implementation is O(n^2)
    if n > 1000:
        raise ImportError(
            f"Sample size n={n} > 1000 for Kendall's Tau but scipy is not available. "
            "The pure-Python fallback is O(n^2) and will be too slow. Please install scipy."
        )

    concordant = 0
    discordant = 0
    ties_x = 0
    ties_y = 0
    for i in range(n - 1):
        dx = xa[i + 1 :] - xa[i]
        dy = ya[i + 1 :] - ya[i]
        sx = np.sign(dx)
        sy = np.sign(dy)
        prod = sx * sy
        concordant += int(np.sum(prod > 0))
        discordant += int(np.sum(prod < 0))
        ties_x += int(np.sum((sx == 0) & (sy != 0)))
        ties_y += int(np.sum((sy == 0) & (sx != 0)))
    denom = math.sqrt(
        max((concordant + discordant + ties_x) * (concordant + discordant + ties_y), 0)
    )
    tau = 0.0 if denom == 0.0 else float((concordant - discordant) / denom)
    if n < 3 or not np.isfinite(tau):
        return tau, 1.0
    variance = 2.0 * (2.0 * n + 5.0) / (9.0 * n * (n - 1.0))
    z = tau / math.sqrt(max(variance, 1e-18))
    p_value = float(2.0 * _norm_cdf(-abs(z)))
    return tau, float(np.clip(p_value, 0.0, 1.0))


@dataclass(frozen=True)
class NeweyWestMeanResult:
    t_stat: float
    se: float
    mean: float
    n: int
    max_lag: int

    @property
    def lag(self) -> int:
        return self.max_lag


@dataclass(frozen=True)
class NonOverlappingSubsampleResult:
    selected_positions: np.ndarray
    sample_size: int
    min_separation: int


@dataclass(frozen=True)
class _NormCompat:
    def cdf(self, x: object) -> float | np.ndarray:
        return _norm_cdf(x)

    def ppf(self, p: object) -> float | np.ndarray:
        return _norm_ppf(p)


@dataclass(frozen=True)
class _TCompat:
    def cdf(self, x: object, df: object) -> float | np.ndarray:
        return _student_t_cdf(x, df)

    def sf(self, x: object, df: object) -> float | np.ndarray:
        return _student_t_sf(x, df)


@dataclass(frozen=True)
class _StatsCompat:
    norm: _NormCompat = _NormCompat()
    t: _TCompat = _TCompat()

    @staticmethod
    def kendalltau(x: object, y: object) -> Tuple[float, float]:
        return _kendalltau(x, y)

    @staticmethod
    def skew(values: Iterable[float]) -> float:
        return _skew(values)

    @staticmethod
    def kurtosis(values: Iterable[float], fisher: bool = True) -> float:
        return _kurtosis(values, fisher=fisher)


try:
    from scipy import stats as scipy_stats
except ImportError:
    stats = _StatsCompat()

# --- Appended from bh_fdr_grouping.py ---


def canonical_bh_group_key(
    *,
    canonical_family: str,
    canonical_event_type: str,
    template_verb: str,
    horizon: str,
    state_id: Optional[str] = None,
    symbol: Optional[str] = None,
    include_symbol: bool = False,
    direction_bucket: Optional[str] = None,
) -> str:
    """Canonical BH-FDR grouping key.

    Primary ontology dimensions:
      (canonical_family, canonical_event_type, template_verb, horizon)
    Optional dimensions:
      state_id (when statistically stable), direction_bucket, symbol.
    """
    family = str(canonical_family or "").strip().upper()
    event_type = str(canonical_event_type or "").strip().upper()
    verb = str(template_verb or "").strip()
    h = str(horizon or "").strip()
    state = str(state_id or "").strip().upper()
    sym = str(symbol or "").strip().upper()
    direction = str(direction_bucket or "").strip().lower()

    parts = [
        family or "UNKNOWN_FAMILY",
        event_type or "UNKNOWN_EVENT_TYPE",
        verb or "UNKNOWN_VERB",
        h or "UNKNOWN_HORIZON",
    ]
    if state:
        parts.append(state)
    if direction:
        parts.append(direction)
    if include_symbol and sym:
        parts.append(sym)
    return "::".join(parts)


def newey_west_t_stat_for_mean(
    values: object,
    max_lag: Optional[int] = None,
    *,
    weights: object | None = None,
) -> NeweyWestMeanResult:
    """Compute a HAC/Newey-West t-statistic for the sample mean."""
    value_series = pd.to_numeric(pd.Series(values), errors="coerce")
    if weights is None:
        weight_series = pd.Series(1.0, index=value_series.index, dtype=float)
    else:
        weight_series = pd.to_numeric(pd.Series(weights), errors="coerce").reindex(
            value_series.index
        )
    mask = value_series.notna() & np.isfinite(value_series) & weight_series.notna() & np.isfinite(weight_series)
    arr = value_series.loc[mask].to_numpy(dtype=float)
    weight_arr = np.clip(weight_series.loc[mask].to_numpy(dtype=float), 0.0, None)
    positive_mask = weight_arr > 0.0
    arr = arr[positive_mask]
    weight_arr = weight_arr[positive_mask]
    n = int(arr.size)
    if n < 2:
        return NeweyWestMeanResult(
            t_stat=float("nan"), se=float("nan"), mean=float("nan"), n=n, max_lag=0
        )
    total_weight = float(weight_arr.sum())
    if not np.isfinite(total_weight) or total_weight <= 0.0:
        return NeweyWestMeanResult(
            t_stat=float("nan"), se=float("nan"), mean=float("nan"), n=n, max_lag=0
        )
    norm_weights = weight_arr / total_weight
    mean = float(np.dot(norm_weights, arr))
    centered = arr - mean
    if max_lag is None:
        # Andrews (1991) rule of thumb
        max_lag = int(max(1, min(n - 1, math.floor(4.0 * ((n / 100.0) ** (2.0 / 9.0))))))
        # Cap at 50 to ensure numerical stability for very long series (Audit L-5)
        max_lag = min(max_lag, 50)
    max_lag = int(max(0, min(max_lag, n - 1)))
    weighted_centered = norm_weights * centered
    lr_var = float(np.dot(weighted_centered, weighted_centered))
    for lag in range(1, max_lag + 1):
        weight = 1.0 - lag / (max_lag + 1.0)
        cov = float(np.dot(weighted_centered[lag:], weighted_centered[:-lag]))
        lr_var += 2.0 * weight * cov
    if not np.isfinite(lr_var) or lr_var <= 0.0:
        return NeweyWestMeanResult(
            t_stat=float("nan"), se=float("nan"), mean=mean, n=n, max_lag=max_lag
        )
    se = math.sqrt(lr_var)
    t_stat = float(mean / se) if se > 0.0 else float("nan")
    return NeweyWestMeanResult(t_stat=t_stat, se=float(se), mean=mean, n=n, max_lag=max_lag)


def bh_adjust(p_values: np.ndarray, n_tests: int | None = None) -> np.ndarray:
    """
    Benjamini-Hochberg FDR adjustment. Returns adjusted p-values clipped to [0, 1].
    Canonical implementation.
    """
    arr = np.asarray(p_values, dtype=float)
    m = len(arr)
    n = n_tests if n_tests is not None else m
    if m == 0:
        return arr
    idx = np.argsort(arr)
    sorted_p = arr[idx]
    adj = np.zeros(m)
    min_p = 1.0
    for i in range(m - 1, -1, -1):
        q = sorted_p[i] * n / (i + 1)
        min_p = min(min_p, q)
        adj[idx[i]] = min_p
    return np.clip(adj, 0.0, 1.0)


def subsample_non_overlapping_positions(
    positions: object, min_separation: int
) -> NonOverlappingSubsampleResult:
    arr = pd.to_numeric(pd.Series(positions), errors="coerce").to_numpy(dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        out = np.asarray([], dtype=int)
        return NonOverlappingSubsampleResult(
            selected_positions=out, sample_size=0, min_separation=int(max(1, min_separation))
        )
    pos = np.sort(arr.astype(int, copy=False))
    selected = [int(pos[0])]
    min_sep = int(max(1, min_separation))
    for value in pos[1:]:
        if int(value) - int(selected[-1]) >= min_sep:
            selected.append(int(value))
    out = np.asarray(selected, dtype=int)
    return NonOverlappingSubsampleResult(
        selected_positions=out, sample_size=int(out.size), min_separation=min_sep
    )
