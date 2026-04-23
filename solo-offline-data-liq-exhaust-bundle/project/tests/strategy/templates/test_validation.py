# tests/strategy/templates/test_validation.py
import pandas as pd
import pytest


def test_validate_pit_invariants_valid_series_passes():
    from project.strategy.templates.validation import validate_pit_invariants

    idx = pd.date_range("2024-01-01", periods=5, freq="5min", tz="UTC")
    signal = pd.Series([1, 2, 3, 4, 5], index=idx)
    assert validate_pit_invariants(signal) is True


def test_validate_pit_invariants_non_monotone_index_fails():
    """A series with a non-monotone index fails PIT validation."""
    from project.strategy.templates.validation import validate_pit_invariants

    idx = pd.to_datetime(["2024-01-01 00:05", "2024-01-01 00:00", "2024-01-01 00:10"], utc=True)
    signal = pd.Series([1, 2, 3], index=idx)
    assert validate_pit_invariants(signal) is False


def test_validate_pit_invariants_duplicate_index_fails():
    """A series with duplicate timestamps fails PIT validation (not strictly monotone)."""
    from project.strategy.templates.validation import validate_pit_invariants

    idx = pd.to_datetime(["2024-01-01 00:00", "2024-01-01 00:00", "2024-01-01 00:05"], utc=True)
    signal = pd.Series([1, 1, 2], index=idx)
    # duplicates make index non-strictly-monotone
    assert validate_pit_invariants(signal) is False


def test_validate_pit_invariants_empty_passes():
    from project.strategy.templates.validation import validate_pit_invariants

    assert validate_pit_invariants(pd.Series([], dtype=float)) is True


def test_check_closed_left_rolling_valid_passes():
    from project.strategy.templates.validation import check_closed_left_rolling

    idx = pd.date_range("2024-01-01", periods=10, freq="5min", tz="UTC")
    window = pd.Series(range(10), index=idx)
    assert check_closed_left_rolling(window) is True


def test_check_closed_left_rolling_non_monotone_fails():
    from project.strategy.templates.validation import check_closed_left_rolling

    idx = pd.to_datetime(["2024-01-01 00:10", "2024-01-01 00:05", "2024-01-01 00:15"], utc=True)
    window = pd.Series([3, 2, 4], index=idx)
    assert check_closed_left_rolling(window) is False


def test_check_closed_left_rolling_empty_passes():
    from project.strategy.templates.validation import check_closed_left_rolling

    assert check_closed_left_rolling(pd.Series([], dtype=float)) is True


def test_compiler_blocks_on_non_monotone_entry_signal(monkeypatch):
    """compile_positions must raise if the entry signal index is not monotone."""
    import pandas as pd
    from project.strategy.templates.spec import StrategySpec
    from project.strategy.templates import compiler

    idx_bad = pd.to_datetime(["2024-01-01 00:05", "2024-01-01 00:00", "2024-01-01 00:10"], utc=True)

    # Patch get_event_signal to return non-monotone entry signals
    class _BadBundle:
        prices = pd.DataFrame({"close": [1.0, 2.0, 3.0]}, index=idx_bad)

        def get_event_signal(self, family, signal):
            return pd.Series([True, False, True], index=idx_bad)

    spec = StrategySpec(
        event_family="TEST_FAM",
        entry_signal="enter",
        exit_signal="exit",
        position_cap=1.0,
        cooldown_bars=0,
        params={},
    )

    with pytest.raises(ValueError, match="PIT"):
        compiler.compile_positions(spec, _BadBundle())



def test_validate_template_stack_accepts_expression_plus_filter():
    from project.strategy.templates.validation import validate_template_stack

    assert validate_template_stack("continuation", filter_template_id="only_if_regime") == []


def test_validate_template_stack_rejects_filter_as_primary():
    from project.strategy.templates.validation import validate_template_stack

    errors = validate_template_stack("only_if_regime")
    assert any("expression templates" in err for err in errors)
