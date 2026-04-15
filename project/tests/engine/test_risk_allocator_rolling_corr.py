from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from project.engine.risk_allocator import RiskLimits, allocate_position_scales

def _ts(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")

def test_rolling_correlation_adapts_to_time_varying_correlation():
    """
    Test that the correlation constraint adapts to time-varying correlation.
    
    In this test:
    - First 100 bars: s1 and s2 are perfectly correlated.
    - Next 100 bars: s1 and s2 are uncorrelated.
    
    If rolling correlation is working, the first 100 bars should be clipped,
    and the next 100 bars should NOT be clipped (or clipped less).
    """
    n = 200
    ts = _ts(n)
    
    # s1 is random
    rng1 = np.random.default_rng(42)
    s1_vals = rng1.choice([-1.0, 1.0], size=n)
    s1_pos = pd.Series(s1_vals, index=ts)
    
    # s2 matches s1 for first 100 bars (correlated with s1)
    # s2 is independent random for next 100 bars (uncorrelated with s1)
    rng2 = np.random.default_rng(43)
    s2_vals = s1_vals.copy()
    s2_vals[100:] = rng2.choice([-1.0, 1.0], size=100)
    s2_pos = pd.Series(s2_vals, index=ts)
    
    pos = {"s1": s1_pos, "s2": s2_pos}
    req = {k: pd.Series(1.0, index=ts) for k in pos}
    
    # Limit correlation to 0.5, but allow high gross
    limits = RiskLimits(
        max_pairwise_correlation=0.5,
        max_portfolio_gross=10.0,
        max_strategy_gross=10.0,
        max_symbol_gross=10.0
    )
    
    scales, _ = allocate_position_scales(pos, req, limits)
    
    s1_scale = scales["s1"]
    
    first_half_avg = float(s1_scale.iloc[:100].abs().mean())
    second_half_avg = float(s1_scale.iloc[100:].abs().mean())
    
    print(f"First half avg scale: {first_half_avg}")
    print(f"Second half avg scale: {second_half_avg}")
    
    # With the CURRENT implementation (single tail correlation), 
    # if the tail (last 100 bars) has low correlation, NO clipping will be applied to the WHOLE series.
    # If the tail has high correlation, the WHOLE series will be clipped.
    
    # In our case, the tail (100:200) has low correlation.
    # Current code will see max_corr ~ 0 (or low), so scale_factor will be 1.0.
    # Thus both halves will have avg scale ~ 1.0.
    
    # We WANT the first half to be clipped (scale ~ 0.5) and second half to be unclipped (scale ~ 1.0).
    assert first_half_avg < 0.8, "First half should be clipped due to high correlation"
    assert second_half_avg > 0.9, "Second half should not be clipped due to low correlation"

if __name__ == "__main__":
    test_rolling_correlation_adapts_to_time_varying_correlation()
