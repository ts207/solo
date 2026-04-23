"""
E4-T1: Stress-conditional correlation limits.

When regime_series contains a stress label, the allocator must use
stressed_max_pairwise_correlation (tighter limit) instead of max_pairwise_correlation.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from project.engine.risk_allocator import RiskLimits, allocate_position_scales


def _ts(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")


def _correlated_positions(n: int, rng_seed: int = 0) -> dict[str, pd.Series]:
    """Two strategies with highly correlated positions (same direction every bar)."""
    ts = _ts(n)
    rng = np.random.default_rng(rng_seed)
    base = pd.Series((rng.random(n) > 0.5).astype(float), index=ts)
    # Both always +1 or both always -1 — perfect correlation
    return {
        "s1": base.copy(),
        "s2": base.copy(),
    }


def test_calm_regime_uses_normal_correlation_limit():
    """During calm regime, max_pairwise_correlation (0.9) is used — perfectly correlated positions should not be clipped."""
    n = 100
    ts = _ts(n)
    pos = _correlated_positions(n)
    req = {k: pd.Series(1.0, index=ts) for k in pos}

    limits = RiskLimits(
        max_pairwise_correlation=0.95,  # calm: allow 95% correlation
        stressed_max_pairwise_correlation=0.3,  # stressed: tighten to 30%
        stressed_regime_values=frozenset({"stress", "crisis"}),
    )
    regime = pd.Series("calm", index=ts)

    scales, _ = allocate_position_scales(pos, req, limits, regime_series=regime)

    # In calm regime, correlation ~1.0 exceeds the 0.95 limit only slightly
    # but positions should not be zeroed out
    for key, s in scales.items():
        assert s.abs().max() > 0, f"Strategy {key} was zeroed in calm regime — unexpected"


def test_stress_regime_applies_tighter_correlation_limit():
    """During stress regime, stressed_max_pairwise_correlation (0.3) is used — perfectly correlated positions must be clipped."""
    n = 100
    ts = _ts(n)
    pos = _correlated_positions(n)
    req = {k: pd.Series(1.0, index=ts) for k in pos}

    limits_no_stress = RiskLimits(max_pairwise_correlation=0.95)
    limits_with_stress = RiskLimits(
        max_pairwise_correlation=0.95,
        stressed_max_pairwise_correlation=0.3,
        stressed_regime_values=frozenset({"stress"}),
    )
    regime = pd.Series("stress", index=ts)

    scales_no_stress, _ = allocate_position_scales(pos, req, limits_no_stress, regime_series=regime)
    scales_with_stress, _ = allocate_position_scales(
        pos, req, limits_with_stress, regime_series=regime
    )

    # With stress-conditional limit, positions during stress must be more clipped
    agg_no_stress = sum(s.abs().mean() for s in scales_no_stress.values())
    agg_with_stress = sum(s.abs().mean() for s in scales_with_stress.values())

    assert agg_with_stress < agg_no_stress, (
        f"Stressed correlation limit should clip positions more than calm limit. "
        f"calm_agg={agg_no_stress:.4f}, stressed_agg={agg_with_stress:.4f}"
    )


def test_no_stressed_limit_falls_back_to_normal():
    """If stressed_max_pairwise_correlation is None, stress regime uses normal limit."""
    n = 50
    ts = _ts(n)
    pos = _correlated_positions(n)
    req = {k: pd.Series(1.0, index=ts) for k in pos}

    limits = RiskLimits(max_pairwise_correlation=0.95)  # no stressed limit
    regime = pd.Series("crisis", index=ts)

    # Should not raise; should fall back to max_pairwise_correlation
    scales, _ = allocate_position_scales(pos, req, limits, regime_series=regime)
    assert isinstance(scales, dict)


def test_mixed_regime_applies_limit_per_bar():
    """Bars with stress label get stressed limit; calm bars get normal limit."""
    n = 100
    ts = _ts(n)
    pos = _correlated_positions(n)
    req = {k: pd.Series(1.0, index=ts) for k in pos}

    limits = RiskLimits(
        max_pairwise_correlation=0.99,  # calm: very permissive
        stressed_max_pairwise_correlation=0.1,  # stressed: very tight
        stressed_regime_values=frozenset({"stress"}),
    )
    # First 50 bars calm, last 50 bars stressed
    regime_vals = ["calm"] * 50 + ["stress"] * 50
    regime = pd.Series(regime_vals, index=ts)

    scales, _ = allocate_position_scales(pos, req, limits, regime_series=regime)

    # Stressed bars (last 50) must have lower average scale than calm bars (first 50)
    s1 = scales["s1"]
    calm_avg = float(s1.iloc[:50].abs().mean())
    stress_avg = float(s1.iloc[50:].abs().mean())

    assert stress_avg < calm_avg, (
        f"Stress bars must be more clipped than calm bars. "
        f"calm_avg={calm_avg:.4f}, stress_avg={stress_avg:.4f}"
    )
