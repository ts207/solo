from __future__ import annotations

import numpy as np
import pandas as pd

from project.features.context_guards import optional_state, state_at_least, state_at_most, state_in


def test_optional_state_returns_float_nan_series_when_absent():
    df = pd.DataFrame(index=pd.RangeIndex(3))

    state = optional_state(df, "ms_vol_state")

    assert state.dtype == float
    assert state.isna().all()


def test_state_at_least_defaults_false_when_absent():
    df = pd.DataFrame(index=pd.RangeIndex(3))

    guard = state_at_least(df, "ms_vol_state", 2.0)

    assert not guard.any()


def test_state_at_most_uses_present_values():
    df = pd.DataFrame({"ms_oi_state": [2.0, 0.0, np.nan]})

    guard = state_at_most(df, "ms_oi_state", 0.0)

    assert guard.tolist() == [False, False, True]


def test_state_in_supports_lagged_guards():
    df = pd.DataFrame({"ms_vol_state": [0.0, 3.0, 1.0]})

    guard = state_in(df, "ms_vol_state", [2.0, 3.0], lag=1)

    assert guard.tolist() == [False, False, True]


def test_optional_state_suppresses_low_confidence_context() -> None:
    df = pd.DataFrame(
        {
            "ms_vol_state": [3.0, 3.0],
            "ms_vol_confidence": [0.40, 0.80],
            "ms_vol_entropy": [0.10, 0.10],
        }
    )

    state = optional_state(df, "ms_vol_state", min_confidence=0.55, max_entropy=0.90)

    assert state.isna().tolist() == [True, False]


def test_optional_state_cold_start_nan_confidence_rejects() -> None:
    df = pd.DataFrame(
        {
            "ms_vol_state": [3.0, 3.0],
            "ms_vol_confidence": [np.nan, np.nan],
            "ms_vol_entropy": [np.nan, np.nan],
        }
    )

    state = optional_state(df, "ms_vol_state", min_confidence=0.55, max_entropy=0.90)

    assert state.isna().all()


def test_state_at_least_suppresses_high_entropy_context() -> None:
    df = pd.DataFrame(
        {
            "ms_spread_state": [1.0, 1.0],
            "ms_spread_confidence": [0.80, 0.80],
            "ms_spread_entropy": [0.95, 0.20],
        }
    )

    guard = state_at_least(
        df,
        "ms_spread_state",
        1.0,
        lag=0,
        min_confidence=0.55,
        max_entropy=0.90,
    )

    assert guard.tolist() == [False, True]
