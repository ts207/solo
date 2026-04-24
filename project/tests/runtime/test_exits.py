"""Direct-call test for exits.py. Guards against NameError from missing imports."""

import pandas as pd


def _make_bar(close: float = 100.0) -> pd.Series:
    return pd.Series({"close": close, "atr": 1.0})


def test_exits_module_imports_cleanly():
    """Importing exits must not raise NameError."""
    import project.strategy.runtime.exits  # noqa: F401


def test_check_exit_conditions_time_stop():
    from project.strategy.runtime.exits import check_exit_conditions

    bar = _make_bar()
    exited, reason = check_exit_conditions(
        bar=bar,
        position_entry_price=100.0,
        is_long=True,
        blueprint_exit={"time_stop_bars": 5},
        bars_held=5,
    )
    assert exited is True
    assert reason == "time_stop"


def test_check_exit_conditions_no_exit():
    from project.strategy.runtime.exits import check_exit_conditions

    bar = _make_bar(close=100.5)
    exited, reason = check_exit_conditions(
        bar=bar,
        position_entry_price=100.0,
        is_long=True,
        blueprint_exit={"time_stop_bars": 96, "target_value": 0.05, "stop_value": 0.03},
        bars_held=1,
    )
    assert exited is False
    assert reason == ""


def test_check_exit_conditions_stop_hit():
    from project.strategy.runtime.exits import check_exit_conditions

    bar = _make_bar(close=96.0)  # 4% down from 100 → exceeds 3% stop
    exited, reason = check_exit_conditions(
        bar=bar,
        position_entry_price=100.0,
        is_long=True,
        blueprint_exit={"time_stop_bars": 96, "target_value": 0.05, "stop_value": 0.03},
        bars_held=3,
    )
    assert exited is True
    assert reason == "stop_hit"
