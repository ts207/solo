from __future__ import annotations

import logging
from typing import Dict

import numpy as np

_LOG = logging.getLogger(__name__)


def calculate_portfolio_risk_multiplier(
    gross_exposure: float,
    max_gross_leverage: float,
    target_vol: float,
    current_vol: float,
) -> float:
    """
    Calculate risk multiplier based on portfolio constraints and volatility.
    """
    if max_gross_leverage <= 0.0:
        return 0.0

    # Hard leverage gate: stay fully sized until the soft-cap region is reached,
    # then stop adding risk instead of linearly tapering to zero.
    soft_cap_ratio = 0.90
    exposure_ratio = max(0.0, abs(float(gross_exposure)) / max_gross_leverage)
    leverage_cap = 1.0 if exposure_ratio <= soft_cap_ratio else 0.0

    # Volatility scaling
    vol_scale = target_vol / max(1e-6, current_vol)

    risk_mult = min(1.0, leverage_cap, vol_scale)
    return float(np.clip(risk_mult, 0.0, 1.0))


def get_asset_correlation_adjustment(
    asset_bucket: str,
    bucket_exposures: Dict[str, float],
    correlation_limit: float = 0.5,
) -> float:
    """
    Reduce sizing if correlated exposure is already high.
    """
    current_bucket_exposure = abs(float(bucket_exposures.get(asset_bucket, 0.0)))
    if current_bucket_exposure > correlation_limit:
        return 0.5  # Simple 50% reduction
    return 1.0


def calculate_cluster_risk_multiplier(
    cluster_id: int,
    active_cluster_counts: Dict[int, int],
    max_strategies_per_cluster: int = 3,
) -> float:
    """
    Scale down risk if too many strategies from the same alpha cluster are active.

    This enforces the 'Portfolio Matrix' gating logic where redundant alphas
    (identified by PnL/Trigger clustering) share a total risk budget.
    """
    count = active_cluster_counts.get(cluster_id, active_cluster_counts.get(str(cluster_id), 0))
    if count <= 1:
        return 1.0

    # Inverse square root scaling for clusters (1 -> 1.0, 2 -> 0.707, 3 -> 0.577)
    # This ensures the total risk of the cluster grows sub-linearly.
    multiplier = 1.0 / np.sqrt(count)

    # Smooth exponential decay beyond the cluster cap (replaces hard 0.5 step).
    # At cap+1: ~0.70×, at cap+2: ~0.50×, at cap+3: ~0.35× — avoids cliff effects.
    if count > max_strategies_per_cluster:
        excess = count - max_strategies_per_cluster
        multiplier *= float(np.exp(-0.35 * excess))

    return float(np.clip(multiplier, 0.1, 1.0))


def calculate_edge_risk_multiplier(
    *,
    expected_net_edge_bps: float,
    expected_downside_bps: float,
    fill_probability: float,
    edge_confidence: float,
    target_reward_to_risk: float = 0.25,
    min_multiplier: float = 0.0,
) -> float:
    """Scale risk from expected post-cost edge instead of support buckets."""

    net_edge = float(expected_net_edge_bps)
    if net_edge <= 0.0:
        return 0.0
    downside = max(1.0, abs(float(expected_downside_bps)))
    reward_to_risk = net_edge / downside
    rr_scale = reward_to_risk / max(float(target_reward_to_risk), 1e-9)
    probability_scale = (float(fill_probability) - 0.50) / 0.15
    confidence_scale = float(edge_confidence)
    multiplier = min(1.0, rr_scale, probability_scale, confidence_scale)
    return float(np.clip(multiplier, min_multiplier, 1.0))


def calculate_execution_quality_multiplier(
    *,
    realized_slippage_bps: float | None = None,
    slippage_budget_bps: float | None = None,
    fill_rate: float | None = None,
    min_fill_rate: float | None = None,
    explicit_quality: float | None = None,
    min_multiplier: float = 0.10,
) -> float:
    """Return a risk scale from current realized execution quality."""

    if explicit_quality is not None:
        return float(np.clip(float(explicit_quality), min_multiplier, 1.0))

    multiplier = 1.0
    if (
        realized_slippage_bps is not None
        and slippage_budget_bps is not None
        and float(slippage_budget_bps) > 0.0
    ):
        realized = max(0.0, float(realized_slippage_bps))
        budget = float(slippage_budget_bps)
        if realized > budget:
            multiplier = min(multiplier, budget / max(realized, 1e-9))

    if fill_rate is not None and min_fill_rate is not None and float(min_fill_rate) > 0.0:
        observed = float(np.clip(float(fill_rate), 0.0, 1.0))
        required = float(np.clip(float(min_fill_rate), 1e-9, 1.0))
        if observed < required:
            multiplier = min(multiplier, observed / required)

    return float(np.clip(multiplier, min_multiplier, 1.0))
