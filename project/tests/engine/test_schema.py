from __future__ import annotations

import pandas as pd
import pytest

from project.engine.schema import (
    SchemaValidationError,
    validate_portfolio_frame_schema,
    validate_strategy_frame_schema,
    validate_trace_schema,
)


def _base_strategy_frame() -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=2, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": idx,
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "strategy": ["test", "test"],
            "signal_position": [0.0, 1.0],
            "requested_position_scale": [1.0, 1.0],
            "target_position": [0.0, 1.0],
            "executed_position": [0.0, 0.0],
            "prior_executed_position": [0.0, 0.0],
            "fill_mode": ["close", "close"],
            "gross_pnl": [0.0, 0.0],
            "transaction_cost": [0.0, 0.0],
            "slippage_cost": [0.0, 0.0],
            "funding_pnl": [0.0, 0.0],
            "borrow_cost": [0.0, 0.0],
            "net_pnl": [0.0, 0.0],
            "close": [100.0, 101.0],
            "bar_return_close_to_close": [float("nan"), 0.01],
            "turnover": [0.0, 0.0],
        }
    )


def test_validate_strategy_frame_schema_accepts_canonical_frame() -> None:
    validate_strategy_frame_schema(_base_strategy_frame())


def test_validate_strategy_frame_schema_rejects_missing_required_column() -> None:
    frame = _base_strategy_frame().drop(columns=["executed_position"])
    with pytest.raises(SchemaValidationError, match="executed_position"):
        validate_strategy_frame_schema(frame)


def test_validate_strategy_frame_schema_rejects_invalid_timestamp() -> None:
    frame = _base_strategy_frame().astype({"timestamp": object})
    frame.loc[1, "timestamp"] = "not-a-timestamp"
    with pytest.raises(SchemaValidationError, match="invalid timestamp"):
        validate_strategy_frame_schema(frame)


def test_validate_trace_schema_accepts_trace_subset() -> None:
    frame = _base_strategy_frame().assign(fill_price=[None, 100.0], mark_price=[100.0, 101.0])
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
    validate_trace_schema(trace)


def test_validate_portfolio_frame_schema_accepts_equity_based_portfolio_frame() -> None:
    idx = pd.date_range("2024-01-01", periods=2, freq="5min", tz="UTC")
    frame = pd.DataFrame(
        {
            "timestamp": idx,
            "portfolio_net_pnl": [0.0, 0.1],
            "portfolio_equity": [1.0, 1.1],
            "portfolio_equity_return": [0.0, 0.1],
        }
    )
    validate_portfolio_frame_schema(frame)


def test_validate_strategy_frame_schema_does_not_require_extended_detectors() -> None:
    frame = _base_strategy_frame()
    assert "position" not in frame.columns
    assert "pnl" not in frame.columns
    assert "ret" not in frame.columns
    validate_strategy_frame_schema(frame)
