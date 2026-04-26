"""Joint promote-and-size under portfolio log-wealth (M3).

Replaces per-thesis threshold gating with greedy portfolio selection that
optimises marginal expected log-wealth contribution, naturally encoding
decorrelation, sizing, and promotion into one pass.

Usage:
    from project.promote.portfolio_selection import PortfolioSelector
    selector = PortfolioSelector(min_marginal_lwc_bps=0.5)
    selected, kelly_fractions = selector.select(candidates_df)
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any

import pandas as pd

_LOG = logging.getLogger(__name__)

_DEFAULT_MIN_MARGINAL_LWC_BPS = 0.5
_DEFAULT_KELLY_FRACTION = 0.5
_DEFAULT_MAX_PORTFOLIO_SIZE = 20
_DEFAULT_MAX_PAIRWISE_CORR = 0.85


@dataclass
class PortfolioSelectorConfig:
    min_marginal_lwc_bps: float = _DEFAULT_MIN_MARGINAL_LWC_BPS
    base_kelly_fraction: float = _DEFAULT_KELLY_FRACTION
    max_portfolio_size: int = _DEFAULT_MAX_PORTFOLIO_SIZE
    max_pairwise_correlation: float = _DEFAULT_MAX_PAIRWISE_CORR
    lambda_var: float = 0.5  # variance penalty for parameter uncertainty

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> PortfolioSelectorConfig:
        return cls(
            min_marginal_lwc_bps=float(
                raw.get("min_marginal_lwc_bps", _DEFAULT_MIN_MARGINAL_LWC_BPS)
            ),
            base_kelly_fraction=float(
                raw.get("base_kelly_fraction", _DEFAULT_KELLY_FRACTION)
            ),
            max_portfolio_size=int(
                raw.get("max_portfolio_size", _DEFAULT_MAX_PORTFOLIO_SIZE)
            ),
            max_pairwise_correlation=float(
                raw.get("max_pairwise_correlation", _DEFAULT_MAX_PAIRWISE_CORR)
            ),
            lambda_var=float(raw.get("lambda_var", 0.5)),
        )


def _kelly_fraction_for_candidate(
    mu_net: float,
    sigma2_net: float,
    base_fraction: float,
    drift_ratio: float | None = None,
) -> float:
    """Fractional Kelly with drift-ratio adjustment (T1.5 extension)."""
    if sigma2_net < 1e-12:
        return 0.0
    optimal_f = mu_net / sigma2_net
    adjusted = base_fraction * max(
        0.1, 1.0 - (drift_ratio or 0.0)
    )
    return float(min(adjusted, optimal_f, base_fraction))


def _expected_log_wealth(
    mu_net_bps: float,
    sigma2_net_bps2: float,
    f: float,
    lambda_var: float,
) -> float:
    """Taylor-expanded E[log(1 + f * r)] - lambda * Var[log(1 + f * r)]."""
    mu = mu_net_bps / 1e4
    sigma2 = sigma2_net_bps2 / 1e8
    elw = f * mu - 0.5 * f**2 * sigma2 - lambda_var * f**2 * sigma2
    return elw * 1e4  # back to bps scale



class PortfolioSelector:
    """Greedy portfolio selection under expected log-wealth."""

    def __init__(self, config: PortfolioSelectorConfig | None = None):
        self.config = config or PortfolioSelectorConfig()

    def _return_series_matrix(self, candidates_df: pd.DataFrame) -> pd.DataFrame | None:
        """Extract per-candidate return series for correlation estimation."""
        series_cols = [c for c in candidates_df.columns if c.startswith("fold_return_")]
        if not series_cols:
            return None
        try:
            mat = candidates_df[series_cols].apply(
                pd.to_numeric, errors="coerce"
            ).T
            return mat
        except Exception:
            return None

    def _estimate_pairwise_corr(
        self,
        candidates_df: pd.DataFrame,
        idx_i: int,
        idx_j: int,
    ) -> float:
        """Estimate pairwise correlation between two candidates."""
        mat = self._return_series_matrix(candidates_df)
        if mat is None or mat.shape[1] < 4:
            return 0.0
        try:
            r_i = mat.iloc[:, idx_i].dropna()
            r_j = mat.iloc[:, idx_j].dropna()
            aligned = pd.concat([r_i, r_j], axis=1).dropna()
            if len(aligned) < 4:
                return 0.0
            corr = float(aligned.iloc[:, 0].corr(aligned.iloc[:, 1]))
            return 0.0 if math.isnan(corr) else corr
        except Exception:
            return 0.0

    def select(
        self,
        candidates_df: pd.DataFrame,
        *,
        profile: str = "research",
    ) -> tuple[pd.DataFrame, dict[str, float]]:
        """Greedy portfolio selection.

        Returns:
            (selected_df, kelly_fractions) where kelly_fractions maps
            hypothesis_id → kelly_fraction.
        """
        cfg = self.config
        min_lwc = cfg.min_marginal_lwc_bps
        if profile == "deploy":
            min_lwc = max(min_lwc, 1.0)

        if candidates_df.empty:
            return candidates_df.iloc[0:0], {}

        # Pre-sort by log_wealth_contribution_bps descending for greedy ordering
        lwc_col = "log_wealth_contribution_bps"
        mu_col = "mean_return_net_bps"

        if lwc_col not in candidates_df.columns:
            _LOG.warning("log_wealth_contribution_bps not in candidates; falling back to t_stat_net sort")
            sort_col = "t_stat_net" if "t_stat_net" in candidates_df.columns else candidates_df.columns[0]
        else:
            sort_col = lwc_col

        ranked = candidates_df.sort_values(sort_col, ascending=False).reset_index(drop=True)

        selected_indices: list[int] = []
        selected_ids: set[str] = set()
        kelly_fractions: dict[str, float] = {}
        id_col = "hypothesis_id" if "hypothesis_id" in ranked.columns else ranked.columns[0]

        for i, row in ranked.iterrows():
            if len(selected_indices) >= cfg.max_portfolio_size:
                break

            h_id = str(row.get(id_col, i))
            mu_net = float(row.get(mu_col, 0.0) or 0.0)
            if lwc_col in row and not math.isnan(float(row[lwc_col] or 0.0)):
                candidate_lwc = float(row[lwc_col])
            else:
                candidate_lwc = mu_net  # fallback

            if candidate_lwc < min_lwc:
                break  # sorted descending, nothing below will pass

            # Pairwise correlation check against already selected
            correlated = False
            for j in selected_indices:
                corr = self._estimate_pairwise_corr(ranked, i if isinstance(i, int) else int(str(i)), j)
                if abs(corr) > cfg.max_pairwise_correlation:
                    correlated = True
                    break
            if correlated:
                continue

            # Compute per-candidate sigma^2 from t_stat and n
            n = max(int(row.get("n", 30) or 30), 2)
            t_net = float(row.get("t_stat_net", row.get("t_stat", 1.0)) or 1.0)
            if abs(t_net) > 1e-10:
                sigma_bps = abs(mu_net) / abs(t_net) * math.sqrt(n)
            else:
                sigma_bps = abs(mu_net) * 10.0
            sigma2_bps2 = sigma_bps**2

            drift_ratio = float(row.get("oos_is_drift_ratio", 0.0) or 0.0)
            f = _kelly_fraction_for_candidate(
                mu_net / 1e4,
                sigma2_bps2 / 1e8,
                cfg.base_kelly_fraction,
                drift_ratio,
            )

            marginal_u = _expected_log_wealth(mu_net, sigma2_bps2, f, cfg.lambda_var)
            if marginal_u < min_lwc:
                continue

            selected_indices.append(int(i))
            selected_ids.add(h_id)
            kelly_fractions[h_id] = round(f, 4)

        selected_df = ranked.loc[selected_indices].reset_index(drop=True)
        return selected_df, kelly_fractions
