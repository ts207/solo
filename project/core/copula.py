from __future__ import annotations

import math

import numpy as np
import pandas as pd

try:
    from scipy import stats
except ModuleNotFoundError:  # pragma: no cover - environment-specific fallback
    from project.core.stats import stats


def fit_gaussian_copula(u1: np.ndarray, u2: np.ndarray) -> float:
    """
    Estimate the correlation parameter rho for a Gaussian copula.
    Transforms uniforms to standard normal and calculates Pearson correlation.
    """
    # Inverse probability transform
    z1 = stats.norm.ppf(np.clip(u1, 1e-6, 1 - 1e-6))
    z2 = stats.norm.ppf(np.clip(u2, 1e-6, 1 - 1e-6))

    rho = np.corrcoef(z1, z2)[0, 1]
    return float(rho)


def calculate_gaussian_conditional_prob(u1: float, u2: float, rho: float) -> float:
    """
    Calculate P(U1 <= u1 | U2 = u2) for a Gaussian copula with correlation rho.
    """
    z1 = stats.norm.ppf(np.clip(u1, 1e-6, 1 - 1e-6))
    z2 = stats.norm.ppf(np.clip(u2, 1e-6, 1 - 1e-6))

    num = z1 - rho * z2
    denom = np.sqrt(max(1e-9, 1 - rho**2))

    prob = stats.norm.cdf(num / denom)
    return float(prob)


def fit_t_copula(u1: np.ndarray, u2: np.ndarray) -> tuple[float, float]:
    """
    Estimate parameters for a Student-t copula: (rho, df).
    Degrees of freedom (df) controls tail dependence.
    
    Note: The relationship rho = sin(pi/2 * tau) assumes the copula correctly 
    captures the underlying dependence structure (elliptical margins).
    """
    # Quick estimate of rho using Spearman's rho or Kendall's tau
    # For Student-t, rho = sin(pi/2 * tau)
    try:
        from scipy import stats as scipy_stats
        tau, _ = scipy_stats.kendalltau(u1, u2)
    except (ImportError, AttributeError):
        # Fallback to simple correlation if kendalltau fails
        tau = np.corrcoef(u1, u2)[0, 1] * 0.6 # rough proxy

    rho = float(np.sin(np.pi / 2 * tau))

    # Estimate degrees of freedom (df) via MLE
    df = _estimate_t_df_mle(u1, u2, rho)
    return rho, df


def calculate_t_conditional_prob(u1: float, u2: float, rho: float, df: float = 4.0) -> float:
    """
    Calculate P(U1 <= u1 | U2 = u2) for a Student-t copula.
    Captures symmetric tail dependence.
    """
    try:
        from scipy import stats as scipy_stats
        # t-distribution inverse CDF
        x1 = scipy_stats.t.ppf(np.clip(u1, 1e-6, 1 - 1e-6), df=df)
        x2 = scipy_stats.t.ppf(np.clip(u2, 1e-6, 1 - 1e-6), df=df)

        # Conditional distribution of Student-t is also Student-t
        # with adjusted parameters:
        # mu_cond = rho * x2
        # scale_cond = sqrt((df + x2^2) / (df + 1) * (1 - rho^2))
        # df_cond = df + 1

        mu_cond = rho * x2
        scale_cond = np.sqrt(max(1e-9, (df + x2**2) / (df + 1.0) * (1.0 - rho**2)))

        # P(X1 <= x1 | X2 = x2) = F_t_df+1((x1 - mu_cond) / scale_cond)
        prob = scipy_stats.t.cdf((x1 - mu_cond) / scale_cond, df=df + 1.0)
        return float(prob)
    except (ImportError, AttributeError):
        # Fallback to Gaussian if scipy.stats.t is unavailable
        return calculate_gaussian_conditional_prob(u1, u2, rho)


def get_empirical_uniforms(x: pd.Series) -> pd.Series:
    """
    Transform a series to empirical uniforms (ranks / (N+1)).
    """
    n = len(x)
    return x.rank(method="average") / (n + 1)


def _gaussian_copula_log_likelihood(u1: np.ndarray, u2: np.ndarray, rho: float) -> float:
    """
    Log-likelihood of a bivariate Gaussian copula density.

    log c(u1, u2; rho) = -0.5*log(1-rho²) + 0.5*(2*rho*z1*z2 - rho²*(z1²+z2²)) / (1-rho²)
    where z1 = Phi^{-1}(u1), z2 = Phi^{-1}(u2).
    """
    try:
        from scipy import stats as scipy_stats
        _norm_ppf = scipy_stats.norm.ppf
    except ImportError:
        from project.core.stats import stats as _stats
        _norm_ppf = _stats.norm.ppf

    z1 = _norm_ppf(np.clip(u1, 1e-7, 1 - 1e-7))
    z2 = _norm_ppf(np.clip(u2, 1e-7, 1 - 1e-7))
    rho2 = rho ** 2
    safe_denom = max(1.0 - rho2, 1e-10)
    log_dens = (
        -0.5 * np.log(safe_denom)
        + (rho * z1 * z2 - 0.5 * rho2 * (z1 ** 2 + z2 ** 2)) / safe_denom
    )
    finite = log_dens[np.isfinite(log_dens)]
    return float(finite.sum()) if finite.size > 0 else -np.inf


def _t_copula_log_likelihood(
    u1: np.ndarray, u2: np.ndarray, rho: float, df: float
) -> float:
    """
    Log-likelihood of a bivariate Student-t copula density.

    Uses the closed-form bivariate t-copula density:
    log c = log f_t2(t1,t2;rho,df) - log f_t(t1;df) - log f_t(t2;df)
    where ti = t_df^{-1}(ui).
    """
    try:
        from scipy import stats as scipy_stats
    except ImportError:
        return -np.inf

    t1 = scipy_stats.t.ppf(np.clip(u1, 1e-7, 1 - 1e-7), df=df)
    t2 = scipy_stats.t.ppf(np.clip(u2, 1e-7, 1 - 1e-7), df=df)

    # Log marginal t densities
    log_ft1 = scipy_stats.t.logpdf(t1, df=df)
    log_ft2 = scipy_stats.t.logpdf(t2, df=df)

    # Log bivariate t density (Demarta & McNeil 2005)
    rho2 = rho ** 2
    safe_denom = max(1.0 - rho2, 1e-10)
    quad_form = (t1 ** 2 - 2 * rho * t1 * t2 + t2 ** 2) / safe_denom

    # Constant part: log(gamma((df+2)/2) / (gamma(df/2) * pi * df * sqrt(1-rho^2)))
    log_bivariate_t = (
        math.lgamma((df + 2) / 2.0)
        - math.lgamma(df / 2.0)
        - np.log(df * np.pi)
        - 0.5 * np.log(safe_denom)
        - ((df + 2) / 2) * np.log(1.0 + quad_form / df)
    )
    log_copula_dens = log_bivariate_t - log_ft1 - log_ft2

    finite = log_copula_dens[np.isfinite(log_copula_dens)]
    return float(finite.sum()) if finite.size > 0 else -np.inf


def _estimate_t_df_mle(
    u1: np.ndarray,
    u2: np.ndarray,
    rho: float,
    df_candidates: tuple = (2.5, 3, 3.5, 4, 4.5, 5, 6, 7, 8, 10, 12, 15, 20, 30, 50)
) -> float:
    """
    Grid-search MLE for Student-t copula degrees of freedom.
    Higher resolution grid for heavy-tail regimes.
    """
    if len(u1) < 50:
        import logging
        logging.getLogger(__name__).warning(
            "Sample size N=%d is small for t-copula MLE; df estimate may be unstable. Defaulting to df=4.0 if likelihood is flat.",
            len(u1)
        )

    best_ll = -np.inf
    best_df = 4.0
    for df in df_candidates:
        ll = _t_copula_log_likelihood(u1, u2, rho, float(df))
        if ll > best_ll:
            best_ll = ll
            best_df = float(df)
    return best_df


def select_best_copula(
    u1: np.ndarray,
    u2: np.ndarray,
    *,
    df_candidates: tuple = (3, 4, 5, 6, 8, 10, 15, 20),
) -> dict:
    """
    Select between Gaussian and Student-t copulas using AIC (lower = better).

    The Gaussian copula has 1 parameter (rho); the t-copula has 2 (rho, df).
    AIC = -2 * log_likelihood + 2 * k.

    Returns a dict with keys:
      ``copula_type``   : "gaussian" or "t"
      ``rho``           : estimated correlation parameter
      ``df``            : degrees of freedom (None for Gaussian)
      ``aic_gaussian``  : AIC for the Gaussian model
      ``aic_t``         : AIC for the t-copula model
      ``tail_dependence``: lower/upper tail dependence coefficient (0 for Gaussian)

    Notes
    -----
    The t-copula captures *symmetric* tail dependence λ = 2 * t_{df+1}(-√((df+1)(1-ρ)/(1+ρ))).
    For crypto crash regimes, df ≤ 6 and λ > 0 is the common finding.
    """
    u1 = np.asarray(u1, dtype=float)
    u2 = np.asarray(u2, dtype=float)
    mask = np.isfinite(u1) & np.isfinite(u2)
    u1, u2 = u1[mask], u2[mask]

    if len(u1) < 20:
        # Too few observations for reliable model selection — default to t for safety
        rho = fit_gaussian_copula(u1, u2)
        return {
            "copula_type": "t",
            "rho": rho,
            "df": 4.0,
            "aic_gaussian": np.nan,
            "aic_t": np.nan,
            "tail_dependence": float(_tail_dependence_t(rho, 4.0)),
            "note": "insufficient_data_default_t",
        }

    rho_gauss = fit_gaussian_copula(u1, u2)
    rho_t, _ = fit_t_copula(u1, u2)
    df_mle = _estimate_t_df_mle(u1, u2, rho_t, df_candidates=df_candidates)

    ll_gauss = _gaussian_copula_log_likelihood(u1, u2, rho_gauss)
    ll_t = _t_copula_log_likelihood(u1, u2, rho_t, df_mle)

    aic_gauss = -2 * ll_gauss + 2 * 1  # k=1 (rho only)
    aic_t = -2 * ll_t + 2 * 2          # k=2 (rho + df)

    if aic_t < aic_gauss:
        return {
            "copula_type": "t",
            "rho": rho_t,
            "df": df_mle,
            "aic_gaussian": aic_gauss,
            "aic_t": aic_t,
            "tail_dependence": float(_tail_dependence_t(rho_t, df_mle)),
        }
    return {
        "copula_type": "gaussian",
        "rho": rho_gauss,
        "df": None,
        "aic_gaussian": aic_gauss,
        "aic_t": aic_t,
        "tail_dependence": 0.0,  # Gaussian has zero tail dependence by construction
    }


def _tail_dependence_t(rho: float, df: float) -> float:
    """
    Lower (= upper) tail dependence coefficient for a bivariate t-copula.

    λ = 2 * t_{df+1}( -sqrt((df+1) * (1-ρ) / (1+ρ)) )

    For df→∞ this converges to 0 (Gaussian limit).
    For df=4, ρ=0.7: λ ≈ 0.39 — significant joint tail risk.
    """
    try:
        from scipy import stats as scipy_stats
        threshold = -np.sqrt(max(0.0, (df + 1.0) * (1.0 - rho) / max(1e-9, 1.0 + rho)))
        return float(2.0 * scipy_stats.t.cdf(threshold, df=df + 1.0))
    except Exception:
        return 0.0
