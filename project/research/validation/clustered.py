from __future__ import annotations

from typing import Optional, Tuple

import numpy as np
import pandas as pd
from scipy import stats


def _clean_inputs(
    values: pd.Series, clusters: Optional[pd.Series] = None
) -> Tuple[pd.Series, Optional[pd.Series]]:
    vals = pd.to_numeric(values, errors="coerce")
    mask = vals.notna()
    vals = vals.loc[mask]
    if clusters is None:
        return vals, None
    raw_clusters = clusters.loc[mask]
    mask2 = raw_clusters.notna()
    cl = raw_clusters.loc[mask2].astype(str)
    mask2 = cl.notna() & (cl != "") & (cl.str.lower() != "nan")
    valid_index = cl.index[mask2]
    return vals.loc[valid_index], cl.loc[valid_index]


def clustered_standard_error(
    values: pd.Series, clusters: Optional[pd.Series] = None
) -> tuple[float, int, str]:
    vals, cl = _clean_inputs(values, clusters)
    n = len(vals)
    if n == 0:
        return 0.0, 0, "empty"
    if n == 1:
        return 0.0, 1, "singleton"

    if cl is None or cl.nunique() <= 1:
        stderr = float(vals.std(ddof=1) / np.sqrt(n)) if n > 1 else 0.0
        return max(0.0, stderr), int(max(1, cl.nunique() if cl is not None else n)), "naive"

    # Intercept-only OLS: beta = mean(vals)
    mean_val = vals.mean()
    residuals = vals - mean_val
    
    # Square of sum of residuals for each cluster
    cluster_sums = residuals.groupby(cl, observed=True).sum()
    sum_sq_cluster_sums = (cluster_sums**2).sum()
    
    # Variance of the intercept estimator: (X'X)^-1 * (Sum_g X_g' u_g u_g' X_g) * (X'X)^-1
    # X is a vector of ones, so X'X = n.
    var_beta = sum_sq_cluster_sums / (n**2)
    
    # Scale for small sample adjustment (G / (G-1)) * ((n-1) / (n-k))
    # where G is number of clusters, k=1 here.
    g = cl.nunique()
    if g > 1:
        adjustment = (g / (g - 1)) * ((n - 1) / (max(1, n - 1)))
        var_beta *= adjustment
        
    stderr = float(np.sqrt(var_beta))
    return max(0.0, stderr), int(g), "clustered"


def clustered_t_stat(estimate: float, stderr: float) -> float:
    if not np.isfinite(stderr) or stderr <= 0.0:
        return 0.0
    return float(estimate / stderr)


def p_value_from_t(t_stat: float, dof: int) -> float:
    """One-sided right-tail p-value for a directional t-statistic.

    E-MISC-001: the previous formula was 2*(1-CDF(|t|)) — two-sided — which inflated
    p-values for directional hypotheses by exactly 2×.  All callers in the candidate
    evaluation path test directional (long/short) hypotheses and require the one-sided form.
    """
    if dof <= 0:
        return 1.0
    return float(stats.t.sf(float(t_stat), df=int(dof)))


def two_sided_p_value_from_t(t_stat: float, dof: int) -> float:
    """Two-sided p-value for symmetric / non-directional tests.

    Use this only when you are testing H0: μ = 0 against H1: μ ≠ 0 (no directional prior).
    For directional hypotheses use p_value_from_t (one-sided).
    """
    if dof <= 0:
        return 1.0
    return float(2.0 * stats.t.sf(abs(float(t_stat)), df=int(dof)))

