from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class MarginalRiskEstimate:
    downside_bps: float
    marginal_volatility: float
    marginal_drawdown_contribution: float

    @property
    def worst_component(self) -> float:
        return max(
            0.0,
            float(self.downside_bps),
            float(self.marginal_volatility) * 10_000.0,
            float(self.marginal_drawdown_contribution) * 10_000.0,
        )


def estimate_marginal_risk(
    *,
    downside_bps: float | None = None,
    marginal_volatility: float | None = None,
    marginal_drawdown_contribution: float | None = None,
) -> MarginalRiskEstimate:
    return MarginalRiskEstimate(
        downside_bps=max(0.0, float(downside_bps or 0.0)),
        marginal_volatility=max(0.0, float(marginal_volatility or 0.0)),
        marginal_drawdown_contribution=max(0.0, float(marginal_drawdown_contribution or 0.0)),
    )


def marginal_risk_multiplier(
    estimate: MarginalRiskEstimate,
    *,
    max_downside_bps: float = 100.0,
    max_marginal_volatility: float = 0.25,
    max_drawdown_contribution: float = 0.08,
    min_multiplier: float = 0.10,
) -> float:
    """Return a 0-1 multiplier that shrinks as marginal risk consumes budget."""

    downside_ratio = max_downside_bps / max(float(estimate.downside_bps), 1e-9)
    volatility_ratio = max_marginal_volatility / max(float(estimate.marginal_volatility), 1e-9)
    drawdown_ratio = max_drawdown_contribution / max(
        float(estimate.marginal_drawdown_contribution), 1e-9
    )
    multiplier = min(1.0, downside_ratio, volatility_ratio, drawdown_ratio)
    return float(np.clip(multiplier, min_multiplier, 1.0))
