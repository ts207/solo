import pandas as pd
import numpy as np
import logging
from project.features.liquidity_vacuum import detect_liquidity_vacuum_events


def run_stress_tests():
    logging.basicConfig(level=logging.INFO)
    print("Running Data Pipeline Stress Tests...")

    n = 1000
    base_df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "close": 100.0,
            "high": 100.1,
            "low": 99.9,
            "volume": 100.0,
        }
    )

    # 1. Handling Missing KLines
    print("\nScenario 1: Massive Data Gaps")
    df_gaps = base_df.copy()
    # Remove 20% of data randomly
    indices = np.random.choice(base_df.index, size=int(n * 0.2), replace=False)
    df_gaps = df_gaps.drop(indices).sort_values("timestamp")

    try:
        events = detect_liquidity_vacuum_events(df_gaps, "STRESS_GAPS")
        print(f"  Success: Handled data gaps. Events found: {len(events)}")
    except Exception as e:
        print(f"  FAILURE: Crashed on data gaps: {e}")

    # 2. Timestamp Desync (Duplicates)
    print("\nScenario 2: Duplicate Timestamps")
    df_dups = pd.concat([base_df, base_df.iloc[500:510]]).sort_values("timestamp")
    try:
        events = detect_liquidity_vacuum_events(df_dups, "STRESS_DUPS")
        print(f"  Success: Handled duplicate timestamps. Events found: {len(events)}")
    except Exception as e:
        print(f"  FAILURE: Crashed on duplicate timestamps: {e}")

    # 3. Extreme Volatility (Inf/NaN)
    print("\nScenario 3: Zero Volume and Price Gaps")
    df_extreme = base_df.copy()
    df_extreme.loc[100, "close"] = 0.0  # Division by zero potential
    df_extreme.loc[200, "volume"] = 0.0  # Median of zeros potential
    try:
        events = detect_liquidity_vacuum_events(df_extreme, "STRESS_EXTREME")
        print(f"  Success: Handled zero volume/price. Events found: {len(events)}")
    except Exception as e:
        print(f"  FAILURE: Crashed on zero volume/price: {e}")


if __name__ == "__main__":
    run_stress_tests()
