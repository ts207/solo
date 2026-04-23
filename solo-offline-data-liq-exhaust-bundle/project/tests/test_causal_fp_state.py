import pytest
import pandas as pd
import numpy as np
from project.features.funding_persistence import (
    build_funding_persistence_state,
    FundingPersistenceConfig,
)


def _normalize_missing(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for col in out.select_dtypes(include=["object"]).columns:
        out[col] = out[col].map(lambda value: np.nan if value is None else value)
    return out


def test_prefix_invariance():
    """
    Mechanical Check 1: Prefix Invariance Test
    A causal feature must be a function only of past data.
    Truncating the future cannot change past outputs.
    """
    periods = 200
    timestamps = pd.date_range("2024-01-01", periods=periods, freq="5min", tz="UTC")

    # Funding rate scaled such that it sometimes crosses the percentile threshold
    rates = np.sin(np.linspace(0, 4 * np.pi, periods)) * 100

    df = pd.DataFrame({"timestamp": timestamps, "funding_rate_scaled": rates})

    config = FundingPersistenceConfig(persistence_min_bars=8, norm_due_bars=20)

    # Compute on full dataset
    full_result = build_funding_persistence_state(df, "TEST", config)

    # For multiple cutoff points
    for cutoff_idx in [50, 100, 150]:
        truncated_df = df.iloc[:cutoff_idx].copy()
        truncated_result = build_funding_persistence_state(truncated_df, "TEST", config)

        # Normalize None/NaN in object columns for comparison without relying on
        # object-dtype fillna downcasting behavior.
        expected = _normalize_missing(full_result.iloc[:cutoff_idx].reset_index(drop=True))
        actual = _normalize_missing(truncated_result.reset_index(drop=True))

        # Compare rows up to cutoff
        pd.testing.assert_frame_equal(expected, actual, check_dtype=False, check_categorical=False)
