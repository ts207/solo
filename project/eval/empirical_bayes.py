"""Empirical Bayes shrinkage for discovery candidates (M2).

Fits a Normal-Normal hierarchical prior over net mean returns across all
candidates in a batch, then shrinks each candidate's observed mean toward the
prior mean by a factor that reflects per-candidate uncertainty.

Shrinkage formula (James-Stein):
    mu_post = (1 - B) * mu_obs + B * mu_prior
    B (shrinkage factor) = sigma2_obs / (sigma2_obs + tau2)

where tau2 is the across-candidate variance of true means, estimated from the
observed variance minus average per-candidate noise:
    tau2 = max(0, Var(mu_obs) - mean(sigma2_obs))

Usage:
    from project.eval.empirical_bayes import fit_prior, shrink, apply_shrinkage
    prior = fit_prior(candidates_df)
    candidates_df = apply_shrinkage(candidates_df, prior)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

_LOG = logging.getLogger(__name__)

_MU_COL = "mean_return_net_bps"
_SIGMA_COL = "t_stat_net"
_N_COL = "n"


@dataclass(frozen=True)
class EBPrior:
    mu_prior: float  # grand mean
    tau2: float      # between-candidate variance of true means
    n_candidates: int


def fit_prior(candidates: pd.DataFrame) -> EBPrior:
    """Fit a Normal-Normal empirical Bayes prior from a batch of candidates.

    Requires columns: mean_return_net_bps, t_stat_net, n.
    Falls back gracefully when data is sparse.
    """
    df = candidates.copy()
    for col in (_MU_COL, _SIGMA_COL, _N_COL):
        if col not in df.columns:
            _LOG.warning("empirical_bayes.fit_prior: missing column %s; returning null prior", col)
            return EBPrior(mu_prior=0.0, tau2=0.0, n_candidates=0)

    df = df[[_MU_COL, _SIGMA_COL, _N_COL]].apply(pd.to_numeric, errors="coerce").dropna()
    df = df[df[_N_COL] >= 2].copy()
    n_cands = len(df)

    if n_cands < 3:
        return EBPrior(mu_prior=0.0, tau2=0.0, n_candidates=n_cands)

    mu_obs = df[_MU_COL].to_numpy(dtype=float)
    t_stat = df[_SIGMA_COL].to_numpy(dtype=float)

    # Per-candidate variance of the mean: sigma2 = mu^2 / t^2
    # (since t = mu / se, se = sigma/sqrt(n), sigma2_mean = mu^2/t^2)
    # Avoid division by zero
    t_safe = np.where(np.abs(t_stat) > 1e-10, t_stat, 1e-10)
    sigma2_obs = (mu_obs / t_safe) ** 2  # variance of each candidate's mean estimate

    # Grand mean (unweighted — avoids feedback loops from winners)
    mu_prior = float(np.mean(mu_obs))

    # Between-candidate variance: Var(mu_obs) - E[sigma2_obs]
    var_mu = float(np.var(mu_obs, ddof=1)) if n_cands > 1 else 0.0
    mean_sigma2 = float(np.mean(sigma2_obs))
    tau2 = max(0.0, var_mu - mean_sigma2)

    return EBPrior(mu_prior=mu_prior, tau2=tau2, n_candidates=n_cands)


def shrink(
    mu_obs: float,
    t_stat: float,
    n: int,
    prior: EBPrior,
) -> tuple[float, float]:
    """Shrink one candidate's observed mean toward the prior.

    Returns (mu_post_bps, shrinkage_factor).
    shrinkage_factor = 0 means no shrinkage; 1 means full shrinkage to prior.
    """
    if prior.tau2 <= 0.0 or prior.n_candidates < 3:
        return mu_obs, 0.0

    t_safe = t_stat if abs(t_stat) > 1e-10 else 1e-10
    sigma2_obs = (mu_obs / t_safe) ** 2
    shrinkage_factor = sigma2_obs / (sigma2_obs + prior.tau2)
    shrinkage_factor = float(np.clip(shrinkage_factor, 0.0, 1.0))
    mu_post = (1.0 - shrinkage_factor) * mu_obs + shrinkage_factor * prior.mu_prior
    return float(mu_post), shrinkage_factor


def apply_shrinkage(candidates: pd.DataFrame, prior: EBPrior) -> pd.DataFrame:
    """Add mu_post_bps and shrinkage_factor columns to a candidates DataFrame."""
    out = candidates.copy()
    if prior.n_candidates < 3 or prior.tau2 <= 0.0:
        out["mu_post_bps"] = out.get(_MU_COL, pd.Series(0.0, index=out.index))
        out["shrinkage_factor"] = 0.0
        return out

    mu_posts = []
    sf_vals = []
    for _, row in out.iterrows():
        mu_obs = float(pd.to_numeric(row.get(_MU_COL, 0.0), errors="coerce") or 0.0)
        t_stat = float(pd.to_numeric(row.get(_SIGMA_COL, 1.0), errors="coerce") or 1.0)
        n = int(pd.to_numeric(row.get(_N_COL, 2), errors="coerce") or 2)
        mu_post, sf = shrink(mu_obs, t_stat, n, prior)
        mu_posts.append(round(mu_post, 4))
        sf_vals.append(round(sf, 4))

    out["mu_post_bps"] = mu_posts
    out["shrinkage_factor"] = sf_vals
    return out
