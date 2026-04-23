from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from project.research.validation.bootstrap import bootstrap_mean_ci
from project.research.validation.clustered import (
    clustered_standard_error,
    clustered_t_stat,
    p_value_from_t,
)
from project.research.validation.schemas import EffectEstimate


def estimate_effect(
    values: pd.Series,
    *,
    clusters: Optional[pd.Series] = None,
    alpha: float = 0.05,
    use_bootstrap_ci: bool = True,
    n_boot: int = 1000,
) -> EffectEstimate:
    vals = pd.to_numeric(values, errors="coerce").dropna()
    if vals.empty:
        return EffectEstimate(
            estimate=0.0,
            stderr=0.0,
            ci_low=0.0,
            ci_high=0.0,
            p_value_raw=1.0,
            n_obs=0,
            n_clusters=0,
            method="empty",
            cluster_col=None,
        )
    aligned_clusters = None
    if clusters is not None:
        raw_clusters = clusters.reindex(vals.index)
        valid_cluster_mask = raw_clusters.notna()
        vals = vals.loc[valid_cluster_mask]
        raw_clusters = raw_clusters.loc[valid_cluster_mask]
        aligned_clusters = raw_clusters.astype(str)
        valid_text_mask = (
            aligned_clusters.notna()
            & (aligned_clusters != "")
            & (aligned_clusters.str.lower() != "nan")
        )
        valid_index = aligned_clusters.index[valid_text_mask]
        vals = vals.loc[valid_index]
        aligned_clusters = aligned_clusters.loc[valid_index]
    if vals.empty:
        return EffectEstimate(
            estimate=0.0,
            stderr=0.0,
            ci_low=0.0,
            ci_high=0.0,
            p_value_raw=1.0,
            n_obs=0,
            n_clusters=0,
            method="empty",
            cluster_col=None,
        )
    estimate = float(vals.mean())
    stderr, n_clusters, method = clustered_standard_error(vals, aligned_clusters)
    t_stat = clustered_t_stat(estimate, stderr)
    dof = max(1, (n_clusters - 1) if n_clusters > 1 else (len(vals) - 1))
    p_val = p_value_from_t(t_stat, dof)
    if use_bootstrap_ci:
        ci_low, ci_high = bootstrap_mean_ci(
            vals, clusters=aligned_clusters, n_boot=n_boot, ci=1.0 - alpha
        )
    else:
        width = 1.96 * stderr
        ci_low, ci_high = estimate - width, estimate + width
    return EffectEstimate(
        estimate=estimate,
        stderr=float(stderr),
        ci_low=float(ci_low),
        ci_high=float(ci_high),
        p_value_raw=float(np.clip(p_val, 0.0, 1.0)),
        n_obs=int(len(vals)),
        n_clusters=int(n_clusters),
        method=method,
        cluster_col=None if aligned_clusters is None else str(aligned_clusters.name or "cluster"),
    )


def estimate_effect_from_frame(
    df: pd.DataFrame,
    *,
    value_col: str,
    cluster_col: Optional[str] = None,
    alpha: float = 0.05,
    use_bootstrap_ci: bool = True,
    n_boot: int = 1000,
) -> EffectEstimate:
    if df.empty or value_col not in df.columns:
        return estimate_effect(
            pd.Series(dtype=float), alpha=alpha, use_bootstrap_ci=use_bootstrap_ci, n_boot=n_boot
        )
    clusters = df[cluster_col] if cluster_col and cluster_col in df.columns else None
    return estimate_effect(
        df[value_col],
        clusters=clusters,
        alpha=alpha,
        use_bootstrap_ci=use_bootstrap_ci,
        n_boot=n_boot,
    )
