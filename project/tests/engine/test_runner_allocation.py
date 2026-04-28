from __future__ import annotations

from pathlib import Path
from typing import ClassVar

import pandas as pd
import pytest

from project.engine.runner import run_engine
from project.engine.strategy_executor import StrategyResult


class _DummyStrategy:
    required_features: ClassVar[list[str]] = []


def _make_frame(strategy_name: str, symbol: str) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=4, freq="5min", tz="UTC")
    frame = pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": symbol,
            "strategy": strategy_name,
            "signal_position": [0.0, 1.0, 1.0, 1.0],
            "requested_position_scale": [1.0, 1.0, 1.0, 1.0],
            "target_position": [0.0, 1.0, 1.0, 1.0],
            "executed_position": [0.0, 0.0, 1.0, 1.0],
            "prior_executed_position": [0.0, 0.0, 0.0, 1.0],
            "fill_mode": ["close"] * 4,
            "fill_price": [None, None, 101.0, None],
            "mark_price": [100.0, 101.0, 102.0, 103.0],
            "open": [100.0, 101.0, 102.0, 103.0],
            "close": [100.0, 101.0, 102.0, 103.0],
            "bar_return_close_to_close": [None, 0.01, 102.0 / 101.0 - 1.0, 103.0 / 102.0 - 1.0],
            "entry_return_next_open": [None, None, None, None],
            "holding_return": [None, 0.01, 102.0 / 101.0 - 1.0, 103.0 / 102.0 - 1.0],
            "turnover": [0.0, 0.0, 1.0, 0.0],
            "gross_pnl": [0.0, 0.0, 102.0 / 101.0 - 1.0, 103.0 / 102.0 - 1.0],
            "transaction_cost": [0.0, 0.0, 0.0, 0.0],
            "slippage_cost": [0.0, 0.0, 0.0, 0.0],
            "funding_pnl": [0.0, 0.0, 0.0, 0.0],
            "borrow_cost": [0.0, 0.0, 0.0, 0.0],
            "net_pnl": [0.0, 0.0, 102.0 / 101.0 - 1.0, 103.0 / 102.0 - 1.0],
            "gross_exposure": [0.0, 0.0, 1.0, 1.0],
            "net_exposure": [0.0, 0.0, 1.0, 1.0],
            "capital_base": [1.0, 1.0, 1.0, 1.0],
            "equity_return": [0.0, 0.0, 102.0 / 101.0 - 1.0, 103.0 / 102.0 - 1.0],
        }
    )
    return frame


def test_run_engine_applies_allocator_before_final_pnl(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_get_strategy(_name: str):
        return _DummyStrategy()

    def fake_load_symbol_raw_data(*args, **kwargs):
        ts = pd.date_range("2024-01-01", periods=4, freq="5min", tz="UTC")
        bars = pd.DataFrame(
            {
                "timestamp": ts,
                "open": [100.0, 101.0, 102.0, 103.0],
                "high": [101.0, 102.0, 103.0, 104.0],
                "low": [99.0, 100.0, 101.0, 102.0],
                "close": [100.0, 101.0, 102.0, 103.0],
                "volume": [1.0, 1.0, 1.0, 1.0],
            }
        )
        return bars, pd.DataFrame({"timestamp": ts})

    def fake_context(bars, features_raw, *args, **kwargs):
        return features_raw

    def fake_calc(
        symbol, bars, features, strategy_name, strategy_params, cost_bps, data_root, **kwargs
    ):
        frame = _make_frame(strategy_name, symbol)
        trace = frame[
            [
                "timestamp",
                "symbol",
                "strategy",
                "signal_position",
                "target_position",
                "executed_position",
                "prior_executed_position",
                "fill_mode",
                "gross_pnl",
                "net_pnl",
                "turnover",
            ]
        ].copy()
        return StrategyResult(
            strategy_name, frame, {}, {"engine_execution_lag_bars_used": 1}, trace
        )

    monkeypatch.setattr("project.engine.runner.get_strategy", fake_get_strategy)
    monkeypatch.setattr("project.engine.runner.load_symbol_raw_data", fake_load_symbol_raw_data)
    monkeypatch.setattr("project.engine.runner.assemble_symbol_context", fake_context)
    monkeypatch.setattr("project.engine.runner.calculate_strategy_returns", fake_calc)
    monkeypatch.setattr(
        "project.engine.runner.load_universe_snapshots", lambda *args, **kwargs: pd.DataFrame()
    )
    monkeypatch.setattr(
        "project.engine.runner.is_dsl_strategy", lambda *args, **kwargs: (False, None)
    )

    result = run_engine(
        run_id="alloc_test",
        symbols=["BTCUSDT"],
        strategies=["s1", "s2"],
        params={"max_portfolio_gross": 1.0, "portfolio_max_exposure": 10.0},
        cost_bps=0.0,
        data_root=tmp_path,
        memory_efficient=False,
    )

    s1 = result["strategy_frames"]["s1"].set_index("timestamp")
    s2 = result["strategy_frames"]["s2"].set_index("timestamp")
    bar = pd.Timestamp("2024-01-01 00:10:00", tz="UTC")

    assert s1.loc[bar, "allocation_scale"] == pytest.approx(0.5)
    assert s2.loc[bar, "allocation_scale"] == pytest.approx(0.5)
    assert s1.loc[bar, "executed_position"] == pytest.approx(0.5)
    assert s2.loc[bar, "executed_position"] == pytest.approx(0.5)
    assert "max_symbol_gross" in s1.loc[bar, "allocation_clip_reason"]

    diag = result["allocation_diagnostics"].set_index("timestamp")
    assert diag.loc[bar, "allocated_gross"] == pytest.approx(1.0)


def test_run_engine_drops_legacy_alias_columns(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_get_strategy(_name: str):
        return _DummyStrategy()

    def fake_load_symbol_raw_data(*args, **kwargs):
        ts = pd.date_range("2024-01-01", periods=4, freq="5min", tz="UTC")
        bars = pd.DataFrame(
            {
                "timestamp": ts,
                "open": [100.0, 101.0, 102.0, 103.0],
                "high": [101.0, 102.0, 103.0, 104.0],
                "low": [99.0, 100.0, 101.0, 102.0],
                "close": [100.0, 101.0, 102.0, 103.0],
                "volume": [1.0, 1.0, 1.0, 1.0],
            }
        )
        return bars, pd.DataFrame({"timestamp": ts})

    def fake_context(bars, features_raw, *args, **kwargs):
        return features_raw

    def fake_calc(
        symbol, bars, features, strategy_name, strategy_params, cost_bps, data_root, **kwargs
    ):
        frame = _make_frame(strategy_name, symbol)
        trace = frame[
            [
                "timestamp",
                "symbol",
                "strategy",
                "signal_position",
                "target_position",
                "executed_position",
                "prior_executed_position",
                "fill_mode",
                "gross_pnl",
                "net_pnl",
                "turnover",
            ]
        ].copy()
        return StrategyResult(
            strategy_name, frame, {}, {"engine_execution_lag_bars_used": 1}, trace
        )

    monkeypatch.setattr("project.engine.runner.get_strategy", fake_get_strategy)
    monkeypatch.setattr("project.engine.runner.load_symbol_raw_data", fake_load_symbol_raw_data)
    monkeypatch.setattr("project.engine.runner.assemble_symbol_context", fake_context)
    monkeypatch.setattr("project.engine.runner.calculate_strategy_returns", fake_calc)
    monkeypatch.setattr(
        "project.engine.runner.load_universe_snapshots", lambda *args, **kwargs: pd.DataFrame()
    )
    monkeypatch.setattr(
        "project.engine.runner.is_dsl_strategy", lambda *args, **kwargs: (False, None)
    )

    result = run_engine(
        run_id="alloc_alias_cleanup",
        symbols=["BTCUSDT"],
        strategies=["s1"],
        params={"max_portfolio_gross": 10.0, "portfolio_max_exposure": 10.0},
        cost_bps=0.0,
        data_root=tmp_path,
        memory_efficient=False,
    )

    frame = result["strategy_frames"]["s1"]
    assert "position" not in frame.columns
    assert "pnl" not in frame.columns
    assert "ret" not in frame.columns
