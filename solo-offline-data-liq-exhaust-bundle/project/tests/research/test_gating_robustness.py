
import pandas as pd
import numpy as np
from project.research.gating import calculate_expectancy_stats

def test_unsorted_features_bug():
    # Create features DF that is unsorted
    # t=0: close=100
    # t=1: close=110
    # t=2: close=120
    
    # We provide them in reverse order: t=2, t=1, t=0
    # timestamps: 12:00, 11:00, 10:00
    
    ts_10 = pd.Timestamp("2023-01-01 10:00", tz="UTC")
    ts_11 = pd.Timestamp("2023-01-01 11:00", tz="UTC")
    ts_12 = pd.Timestamp("2023-01-01 12:00", tz="UTC")
    
    features_df = pd.DataFrame({
        "timestamp": [ts_12, ts_11, ts_10],
        "close": [120.0, 110.0, 100.0]
    })
    
    # Event at t=10:00 (match t=0).
    # Expected return (horizon=1 bar): (110 - 100)/100 = 0.10
    
    events_df = pd.DataFrame({
        "timestamp": [ts_10],
        "event_type": ["TEST"]
    })
    
    # Run calc
    # join_events_to_features will sort features internally -> 10:00 is idx 0.
    # So _feature_pos for 10:00 will be 0.
    # entry_lag=0 for simplicity (if supported? code says >=1). Let's use entry_lag=1.
    # If entry_lag=1, entry is at t=1 (11:00, idx 1). Close=110.
    # Horizon=1. Exit at t=2 (12:00, idx 2). Close=120.
    # Return = (120-110)/110 = 0.0909...
    
    # However, if it uses the unsorted array:
    # features_df.values: [120, 110, 100]
    # idx 0 (from _feature_pos) -> 120 (Actual t=2 close!)
    # idx 1 (entry) -> 110 (Actual t=1 close)
    # idx 2 (exit) -> 100 (Actual t=0 close)
    # So it calculates return from 110 to 100 -> -0.0909...
    
    # So if we get negative return, the bug is present.
    # If positive, it's fixed (or lucky).
    
    stats = calculate_expectancy_stats(
        events_df, 
        features_df, 
        rule="TEST", 
        horizon="1h", 
        entry_lag_bars=1, 
        min_samples=1,
        horizon_bars_override=1
    )
    
    print(f"Mean Return: {stats['mean_return']}")
    
    # The expected return if correct: ~0.09 (110->120)
    # The expected return if buggy: ~-0.09 (110->100) or garbage
    
    # Let's see what happens.
    assert stats["mean_return"] > 0
