from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from project.engine.runner import run_engine
from project.engine.strategy_executor import StrategyResult


class _DummyStrategy:
    required_features: list[str] = []


def _make_frame(strategy_name: str, symbol: str) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=4, freq="5min", tz="UTC")
    return pd.DataFrame(
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


def test_run_engine_writes_versioned_manifest_and_inventory(
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
        run_id="manifest_test",
        symbols=["BTCUSDT"],
        strategies=["s1"],
        params={"max_portfolio_gross": 1.0, "portfolio_max_exposure": 10.0},
        cost_bps=2.5,
        data_root=tmp_path,
        memory_efficient=False,
    )

    manifest_path = result["manifest_path"]
    assert manifest_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["manifest_type"] == "engine_run_manifest"
    assert manifest["manifest_version"] == "engine_run_manifest_v1"
    assert manifest["engine_artifact_schema_version"] == "engine_artifact_v2"
    assert manifest["capital_model"]["name"] == "equity_curve_from_net_pnl"
    assert manifest["execution"]["fill_modes"] == ["close"]
    assert manifest["execution"]["input_cost_bps"] == pytest.approx(2.5)
    assert manifest["allocator"]["mode"] == "heuristic"
    assert manifest["allocator"]["config"]["max_portfolio_gross"] == pytest.approx(1.0)
    assert manifest["allocator"]["contract"]["schema_version"] == "allocation_contract_v1"
    assert manifest["allocator"]["contract"]["policy"]["mode"] == "heuristic"

    artifacts = {item["artifact_name"]: item for item in manifest["artifacts"]}
    assert "strategy_returns_s1" in artifacts
    assert "strategy_trace_s1" in artifacts
    assert "portfolio_returns" in artifacts

    for item in artifacts.values():
        assert Path(item["path"]).exists()
        assert item["storage_format"] in {"csv", "parquet"}
        assert item["rows"] >= 0
        assert isinstance(item["columns"], list)


def test_strategy_frame_artifact_uses_canonical_columns_only(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_get_strategy(_name: str):
        return _DummyStrategy()

    def fake_load_symbol_raw_data(*args, **kwargs):
        ts = pd.date_range("2024-01-01", periods=2, freq="5min", tz="UTC")
        bars = pd.DataFrame(
            {
                "timestamp": ts,
                "open": [100.0, 101.0],
                "high": [101.0, 102.0],
                "low": [99.0, 100.0],
                "close": [100.0, 101.0],
                "volume": [1.0, 1.0],
            }
        )
        return bars, pd.DataFrame({"timestamp": ts})

    def fake_context(bars, features_raw, *args, **kwargs):
        return features_raw

    def fake_calc(
        symbol, bars, features, strategy_name, strategy_params, cost_bps, data_root, **kwargs
    ):
        frame = _make_frame(strategy_name, symbol).iloc[:2].copy()
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
        return StrategyResult(strategy_name, frame, {}, {}, trace)

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
        run_id="canonical_columns_test",
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


def test_run_engine_resolves_default_data_root_at_call_time(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, Path] = {}

    def fake_get_strategy(_name: str):
        return _DummyStrategy()

    def fake_load_universe_snapshots(data_root: Path, run_id: str):
        captured["universe"] = data_root
        return pd.DataFrame()

    def fake_load_symbol_raw_data(data_root: Path, *args, **kwargs):
        captured["symbol_data"] = data_root
        raise RuntimeError("stop_after_data_root_capture")

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr("project.engine.runner.get_strategy", fake_get_strategy)
    monkeypatch.setattr(
        "project.engine.runner.load_universe_snapshots", fake_load_universe_snapshots
    )
    monkeypatch.setattr("project.engine.runner.load_symbol_raw_data", fake_load_symbol_raw_data)
    monkeypatch.setattr(
        "project.engine.runner.is_dsl_strategy", lambda *args, **kwargs: (False, None)
    )

    with pytest.raises(RuntimeError, match="stop_after_data_root_capture"):
        run_engine(
            run_id="dynamic_root_test",
            symbols=["BTCUSDT"],
            strategies=["s1"],
            params={},
            cost_bps=0.0,
        )

    assert captured["universe"] == tmp_path
    assert captured["symbol_data"] == tmp_path


def test_manifest_records_deterministic_optimizer_contract(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
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
        run_id="optimizer_manifest_test",
        symbols=["BTCUSDT"],
        strategies=["s1", "s2"],
        params={
            "allocator_mode": "deterministic_optimizer",
            "strategy_risk_budgets": {"s1": 3.0, "s2": 1.0},
            "max_portfolio_gross": 10.0,
            "portfolio_max_exposure": 10.0,
        },
        cost_bps=0.0,
        data_root=tmp_path,
        memory_efficient=False,
    )

    manifest = result["manifest"]
    assert manifest["allocator"]["mode"] == "deterministic_optimizer"
    assert manifest["allocator"]["contract"]["policy"]["strategy_risk_budgets"] == {
        "s1": pytest.approx(3.0),
        "s2": pytest.approx(1.0),
    }


def test_manifest_records_family_budget_contract(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
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
        run_id="family_budget_manifest_test",
        symbols=["BTCUSDT"],
        strategies=["s1", "s2"],
        params={
            "family_risk_budgets": {"trend": 1.0},
            "strategy_family_map": {"s1": "trend", "s2": "trend"},
            "max_portfolio_gross": 10.0,
            "portfolio_max_exposure": 10.0,
        },
        cost_bps=0.0,
        data_root=tmp_path,
        memory_efficient=False,
    )

    manifest = result["manifest"]
    assert manifest["allocator"]["contract"]["policy"]["family_risk_budgets"] == {
        "trend": pytest.approx(1.0),
    }
    assert manifest["allocator"]["contract"]["policy"]["strategy_family_map"] == {
        "s1": "trend",
        "s2": "trend",
    }
