import pandas as pd
import numpy as np
from project.research.gating import calculate_expectancy_stats

def test_newey_west_t_stat_autocorrelation():
    # Create highly autocorrelated returns
    # 1, 1, 1, 1, 1, -1, -1, -1, -1, -1, ...
    returns = np.array([1.0, 1.0, 1.0, 1.0, 1.0] * 20)
    
    # Mock features and events
    idx = pd.date_range("2025-01-01", periods=len(returns), freq="1h")
    features_df = pd.DataFrame({
        "timestamp": idx,
        "close": np.cumsum(returns) + 100.0
    }, index=idx)
    
    sym_events = pd.DataFrame({
        "timestamp": idx,
        "symbol": ["BTC"] * len(returns),
        "rule": ["test_rule"] * len(returns),
        "direction": [1] * len(returns)
    }, index=idx)
    
    # Current behavior uses simple t-stat: mean / (std / sqrt(n))
    # For this series, mean is 1.0, std is 0.0 (oops, let's add some noise)
    
    returns = np.array([1.1, 0.9, 1.1, 0.9, 1.1] * 20) # Mean 1.0, some variance
    features_df["close"] = np.cumsum(returns) + 100.0
    
    stats = calculate_expectancy_stats(
        sym_events,
        features_df,
        rule="test_rule",
        horizon="1m",
        min_samples=10
    )
    
    t_stat_simple = stats["t_stat"]
    
    # If we have positive autocorrelation, Newey-West t-stat should be LOWER
    # than simple t-stat because it accounts for the fact that we have fewer
    # "independent" samples.
    
    # In this case, 1.1, 0.9, 1.1, 0.9 has negative autocorrelation at lag 1.
    # Let's use 1.1, 1.1, 1.1, 1.1, 0.9, 0.9, 0.9, 0.9...
    
    returns = np.array([1.1] * 50 + [0.9] * 50)
    features_df = pd.DataFrame({
        "timestamp": idx,
        "close": np.cumsum(returns) + 100.0
    }, index=idx)
    
    stats = calculate_expectancy_stats(
        sym_events,
        features_df,
        rule="test_rule",
        horizon="1m",
        min_samples=10
    )
    
    t_stat_hac = stats["t_stat"]
    
    # Before the fix, t_stat_hac will likely be the same as t_stat_simple
    # (or rather, it will be the simple t-stat calculation).
    
    # If we have positive autocorrelation, Newey-West t-stat should be LOWER
    # than simple t-stat because it accounts for the fact that we have fewer
    # "independent" samples.
    
    # For this series, IID t-stat would be ~100. Newey-West should be much lower.
    print(f"Stats t-stat: {stats['t_stat']}")
    assert 0.0 < stats["t_stat"] < 50.0

