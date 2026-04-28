import numpy as np
import pandas as pd
import pytest

from project.features.context_states import (
    calculate_ms_funding_probabilities,
    calculate_ms_funding_state,
    calculate_ms_liq_probabilities,
    calculate_ms_liq_state,
    calculate_ms_oi_probabilities,
    calculate_ms_oi_state,
    calculate_ms_spread_probabilities,
    calculate_ms_spread_state,
    calculate_ms_trend_probabilities,
    calculate_ms_trend_state,
    calculate_ms_vol_probabilities,
    calculate_ms_vol_state,
    encode_context_state_code,
)


def test_calculate_ms_vol_state():
    rv_pct = pd.Series([10.0, 50.0, 80.0, 98.0])
    states = calculate_ms_vol_state(rv_pct)
    assert states.iloc[0] == 0.0  # LOW
    assert states.iloc[1] == 1.0  # MID
    assert states.iloc[2] == 2.0  # HIGH
    assert states.iloc[3] == 3.0  # SHOCK


def test_calculate_ms_vol_probabilities_sum_to_one_and_expose_confidence():
    rv_pct = pd.Series([10.0, 50.0, 80.0, 98.0])
    probs = calculate_ms_vol_probabilities(rv_pct)

    assert np.allclose(
        probs[["prob_vol_low", "prob_vol_mid", "prob_vol_high", "prob_vol_shock"]].sum(axis=1),
        1.0,
    )
    assert probs["ms_vol_confidence"].between(0.0, 1.0).all()
    assert probs["ms_vol_entropy"].between(0.0, 1.0).all()


def test_calculate_ms_liq_state():
    # Sine wave ensures we hit all quantiles
    vol = pd.Series(np.sin(np.linspace(0, 10, 500)) + 2.0)
    states = calculate_ms_liq_state(vol, window=100)

    # Peak of sine is max -> FLUSH (2.0)
    # Trough of sine is min -> THIN (0.0)
    # Somewhere in between -> NORMAL (1.0)

    assert 2.0 in states.values
    assert 0.0 in states.values
    assert 1.0 in states.values


def test_calculate_ms_liq_probabilities_sum_to_one():
    vol = pd.Series(np.sin(np.linspace(0, 10, 500)) + 2.0)
    probs = calculate_ms_liq_probabilities(vol, window=100)

    valid = probs[["prob_liq_thin", "prob_liq_normal", "prob_liq_flush"]].dropna()
    assert np.allclose(valid.sum(axis=1), 1.0)


def test_calculate_ms_oi_state():
    # Constant OI delta -> z-score will be 0 (STABLE)
    oi_delta = pd.Series([10.0] * 100)
    states = calculate_ms_oi_state(oi_delta, window=50)
    assert states.iloc[-1] == 1.0

    # Large spike
    oi_delta.iloc[-1] = 1000.0
    states = calculate_ms_oi_state(oi_delta, window=50)
    assert states.iloc[-1] == 2.0  # ACCEL


def test_calculate_ms_oi_probabilities_expose_confidence_and_entropy():
    oi_delta = pd.Series([10.0] * 100)
    probs = calculate_ms_oi_probabilities(oi_delta, window=50)

    assert probs["ms_oi_confidence"].iloc[-1] >= 0.0
    assert 0.0 <= probs["ms_oi_entropy"].iloc[-1] <= 1.0


def test_calculate_ms_funding_state():
    # With insufficient long-window history, state should remain NEUTRAL.
    fnd = pd.Series([0.5] * 100)
    states = calculate_ms_funding_state(fnd, window=20)
    assert states.iloc[-1] == 0.0

    # Flat default funding should not saturate the persistent state once
    # long-window quantiles are available.
    states_ready = calculate_ms_funding_state(fnd, window=20, window_long=60)
    assert states_ready.iloc[-1] == 0.0

    # A fresh move from baseline funding into elevated funding is detected.
    fnd_ext = pd.Series([0.5] * 100 + [5.0] * 30)
    states_ext = calculate_ms_funding_state(fnd_ext, window=20, window_long=60)
    assert 1.0 in states_ext.tail(30).values  # PERSISTENT
    assert 2.0 in states_ext.tail(30).values  # EXTREME


def test_calculate_ms_funding_state_does_not_saturate_on_default_funding():
    fnd = pd.Series([1.0] * 500)
    states = calculate_ms_funding_state(fnd, window=20, window_long=60)
    ready = states.iloc[60:]

    assert (ready == 0.0).mean() >= 0.95


def test_calculate_ms_funding_probabilities_default_to_neutral_without_long_history():
    fnd = pd.Series([0.5] * 100)
    probs = calculate_ms_funding_probabilities(fnd, window=20)

    assert probs["prob_funding_neutral"].iloc[-1] == pytest.approx(1.0)
    assert probs["ms_funding_state"].iloc[-1] == 0.0


def test_calculate_ms_trend_state():
    # With insufficient long-window history, state should remain CHOP.
    assert calculate_ms_trend_state(pd.Series([0.005])).iloc[0] == 0.0
    assert calculate_ms_trend_state(pd.Series([-0.005])).iloc[0] == 0.0

    rv = pd.Series([0.001] * 120)

    # Bull once long-window quantiles are available.
    ret_bull = pd.Series(np.linspace(-0.002, 0.006, 120))
    bull_state = calculate_ms_trend_state(ret_bull, rv=rv, window_long=60)
    assert bull_state.iloc[-1] == 1.0

    # Bear once long-window quantiles are available.
    ret_bear = pd.Series(np.linspace(0.006, -0.002, 120))
    bear_state = calculate_ms_trend_state(ret_bear, rv=rv, window_long=60)
    assert bear_state.iloc[-1] == 2.0


def test_calculate_ms_trend_probabilities_expose_confidence_and_entropy():
    rv = pd.Series([0.001] * 120)
    ret_bull = pd.Series(np.linspace(-0.002, 0.006, 120))
    probs = calculate_ms_trend_probabilities(ret_bull, rv=rv, window_long=60)

    assert probs["prob_trend_bull"].iloc[-1] > probs["prob_trend_chop"].iloc[-1]
    assert probs["ms_trend_confidence"].iloc[-1] >= 0.0
    assert 0.0 <= probs["ms_trend_entropy"].iloc[-1] <= 1.0


def test_calculate_ms_spread_state():
    # Tight
    spread = pd.Series([0.1])
    assert calculate_ms_spread_state(spread).iloc[0] == 0.0
    # Wide
    spread = pd.Series([0.6])
    assert calculate_ms_spread_state(spread).iloc[0] == 1.0


def test_calculate_ms_spread_probabilities_sum_to_one():
    spread = pd.Series([0.1, 0.6])
    probs = calculate_ms_spread_probabilities(spread)

    assert np.allclose(probs[["prob_spread_tight", "prob_spread_wide"]].sum(axis=1), 1.0)


def test_encode_context_state_code():
    vol = pd.Series([3.0])
    liq = pd.Series([0.0])
    oi = pd.Series([2.0])
    fnd = pd.Series([1.0])
    trend = pd.Series([1.0])
    spread = pd.Series([0.0])
    code = encode_context_state_code(vol, liq, oi, fnd, trend, spread)
    # V L O F T S
    # 3 0 2 1 1 0
    assert code.iloc[0] == 302110.0
