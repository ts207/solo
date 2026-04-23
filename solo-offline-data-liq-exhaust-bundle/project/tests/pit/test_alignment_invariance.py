import numpy as np
import pandas as pd
import pytest
from project.features.alignment import align_funding_to_bars


def test_alignment_pit_invariance():
    """
    Verify that adding future funding records doesn't change past alignment.
    """
    bars = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=10, freq="1h", tz="UTC"),
            "close": np.random.randn(10),
        }
    )

    funding = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=5, freq="8h", tz="UTC"),
            "funding_rate_scaled": np.random.randn(5),
        }
    )

    # 1. Base alignment
    res_base = align_funding_to_bars(bars, funding)

    # 2. Add future funding
    future_funding = pd.DataFrame(
        {
            "timestamp": [bars["timestamp"].iloc[-1] + pd.Timedelta("1h")],
            "funding_rate_scaled": [999.0],
        }
    )
    funding_extended = pd.concat([funding, future_funding]).reset_index(drop=True)

    # 3. Extended alignment
    res_ext = align_funding_to_bars(bars, funding_extended)

    # 4. Assert past remains identical
    pd.testing.assert_frame_equal(res_base, res_ext)


def test_alignment_timestamp_shift_invariance():
    """
    Verify that shifting future funding timestamps doesn't change past alignment.
    """
    bars = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=10, freq="1h", tz="UTC"),
            "close": np.random.randn(10),
        }
    )

    funding = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=5, freq="8h", tz="UTC"),
            "funding_rate_scaled": np.random.randn(5),
        }
    )

    # 1. Base alignment
    res_base = align_funding_to_bars(bars, funding)

    # 2. Shift future funding timestamp (after cutoff)
    cutoff = bars["timestamp"].iloc[5]
    funding_shifted = funding.copy()
    funding_shifted.loc[funding_shifted["timestamp"] > cutoff, "timestamp"] += pd.Timedelta("10min")

    # 3. Shifted alignment
    res_shifted = align_funding_to_bars(bars, funding_shifted)

    # 4. Assert prefix up to cutoff remains identical
    res_base_prefix = res_base[res_base["timestamp"] <= cutoff]
    res_shifted_prefix = res_shifted[res_shifted["timestamp"] <= cutoff]

    pd.testing.assert_frame_equal(res_base_prefix, res_shifted_prefix)
