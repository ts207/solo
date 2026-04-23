import pandas as pd

from project.strategy.templates.compiler import compile_positions
from project.strategy.templates.data_bundle import DataBundle
from project.strategy.templates.generator import generate_candidates
from project.strategy.templates.spec import StrategySpec
from project.strategy.templates.validation import check_closed_left_rolling, validate_pit_invariants


def test_deterministic_generator():
    grids = {"a": [1.0, 2.0], "b": [3.0, 4.0]}
    c1 = generate_candidates("TEST", {}, grids, 2, 42)
    c2 = generate_candidates("TEST", {}, grids, 2, 42)
    assert len(c1) == 2
    assert c1[0].primary_event_id == "TEST"
    assert c1[0].compat_event_family == "TEST"
    assert c1[0].strategy_id == c2[0].strategy_id
    assert c1[1].strategy_id == c2[1].strategy_id


def test_reordering_spec_normalization():
    s1 = StrategySpec("V", "en", "ex", params={"x": 1.0, "y": 2.0})
    s2 = StrategySpec("V", "en", "ex", params={"y": 2.0, "x": 1.0})
    assert s1.primary_event_id == "V"
    normalized = s1.normalize()
    assert normalized["primary_event_id"] == "V"
    assert normalized["compat_event_family"] == "V"
    assert normalized["event_family"] == "V"
    assert s1.strategy_id == s2.strategy_id


def test_compiler_respects_cap_and_cooldown():
    idx = pd.date_range("2024-01-01", periods=10, freq="5min")
    prices = pd.DataFrame({"close": 100.0}, index=idx)
    ev = pd.DataFrame(
        {
            "eval_bar_ts": [idx[1], idx[2]],
            "signal_ts": [idx[2], idx[3]],
            "event_type": ["TEST", "TEST"],
        }
    )
    bundle = DataBundle(prices, prices, ev)

    spec = StrategySpec("TEST", "enter", "exit", position_cap=0.5, cooldown_bars=2)
    pos, _ = compile_positions(spec, bundle)

    assert pos.max() <= 0.5


def test_validation_pit():
    idx = pd.date_range("2024-01-01", periods=10, freq="5min")
    series = pd.Series([1.0] * 10, index=idx)
    assert validate_pit_invariants(series)
    assert check_closed_left_rolling(series)


def test_compiler_stable_resolution():
    idx = pd.date_range("2024-01-01", periods=10, freq="5min")
    prices = pd.DataFrame({"close": 100.0}, index=idx)
    ev = pd.DataFrame(
        {
            "eval_bar_ts": [idx[1], idx[1]],
            "signal_ts": [idx[2], idx[2]],
            "event_type": ["TEST", "TEST"],
        }
    )
    bundle = DataBundle(prices, prices, ev)
    bundle.get_event_signal = lambda event_type, kind: pd.Series(
        [False, False, True, False, False, False, False, False, False, False], index=idx
    )

    spec = StrategySpec("TEST", "enter", "exit", position_cap=1.0, cooldown_bars=2)
    pos, _ = compile_positions(spec, bundle)

    assert pos.iloc[2] == 1.0
