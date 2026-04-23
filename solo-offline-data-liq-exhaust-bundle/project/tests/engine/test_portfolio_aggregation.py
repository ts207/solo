from __future__ import annotations

import pandas as pd
import pytest

from project.engine.portfolio_aggregator import (
    aggregate_strategy_results,
    build_strategy_contributions,
    build_symbol_contributions,
)
from project.engine.reporting_summarizer import summarize_portfolio_ledger
from project.engine.schema import validate_portfolio_frame_schema


def _make_frame(
    strategy: str, symbol: str, net_pnls: list[float], capital_base: float = 1.0
) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=len(net_pnls), freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": symbol,
            "strategy": strategy,
            "gross_pnl": net_pnls,
            "net_pnl": net_pnls,
            "transaction_cost": [0.0] * len(net_pnls),
            "slippage_cost": [0.0] * len(net_pnls),
            "funding_pnl": [0.0] * len(net_pnls),
            "borrow_cost": [0.0] * len(net_pnls),
            "gross_exposure": [1.0] * len(net_pnls),
            "net_exposure": [1.0] * len(net_pnls),
            "turnover": [0.0] * len(net_pnls),
            "capital_base": [capital_base] * len(net_pnls),
        }
    )


def test_aggregate_strategy_results_builds_equity_curve_from_net_pnl() -> None:
    strategy_frames = {
        "s1": _make_frame("s1", "BTCUSDT", [0.10, -0.05, 0.02]),
        "s2": _make_frame("s2", "ETHUSDT", [0.00, 0.03, -0.01]),
    }
    portfolio = aggregate_strategy_results(strategy_frames)
    validate_portfolio_frame_schema(portfolio)

    assert portfolio["portfolio_net_pnl"].tolist() == pytest.approx([0.10, -0.02, 0.01])
    assert portfolio["portfolio_equity"].tolist() == pytest.approx([2.10, 2.08, 2.09])
    assert portfolio["portfolio_equity_return"].iloc[0] == pytest.approx(0.10 / 2.0)
    assert portfolio["portfolio_equity_return"].iloc[1] == pytest.approx(-0.02 / 2.10)
    assert "portfolio_ret" not in portfolio.columns
    assert "portfolio_pnl" not in portfolio.columns


def test_contribution_tables_reconcile_to_portfolio_totals() -> None:
    strategy_frames = {
        "s1": _make_frame("s1", "BTCUSDT", [0.10, -0.05, 0.02]),
        "s2": _make_frame("s2", "ETHUSDT", [0.00, 0.03, -0.01]),
    }
    portfolio = aggregate_strategy_results(strategy_frames)
    strategy_contrib = build_strategy_contributions(strategy_frames, portfolio)
    symbol_contrib = build_symbol_contributions(strategy_frames, portfolio)

    by_ts_strategy = strategy_contrib.groupby("timestamp", sort=True)["strategy_net_pnl"].sum()
    by_ts_symbol = symbol_contrib.groupby("timestamp", sort=True)["symbol_net_pnl"].sum()

    expected = portfolio.set_index("timestamp")["portfolio_net_pnl"]
    assert by_ts_strategy.tolist() == pytest.approx(expected.tolist())
    assert by_ts_symbol.tolist() == pytest.approx(expected.tolist())

    expected_ret = portfolio.set_index("timestamp")["portfolio_equity_return"]
    by_ts_strategy_ret = strategy_contrib.groupby("timestamp", sort=True)[
        "equity_return_contribution"
    ].sum()
    by_ts_symbol_ret = symbol_contrib.groupby("timestamp", sort=True)[
        "equity_return_contribution"
    ].sum()
    assert by_ts_strategy_ret.tolist() == pytest.approx(expected_ret.tolist())
    assert by_ts_symbol_ret.tolist() == pytest.approx(expected_ret.tolist())


def test_summarize_portfolio_ledger_uses_equity_curve() -> None:
    strategy_frames = {
        "s1": _make_frame("s1", "BTCUSDT", [0.10, -0.05, 0.02]),
        "s2": _make_frame("s2", "ETHUSDT", [0.00, 0.03, -0.01]),
    }
    portfolio = aggregate_strategy_results(strategy_frames)
    summary = summarize_portfolio_ledger(portfolio)

    assert summary["starting_equity"] == pytest.approx(2.0)
    assert summary["ending_equity"] == pytest.approx(2.09)
    assert summary["total_pnl"] == pytest.approx(0.09)
    assert summary["max_drawdown"] == pytest.approx((2.08 / 2.10) - 1.0)


def test_summarize_portfolio_ledger_drawdown_includes_starting_equity() -> None:
    frame = pd.DataFrame(
        {
            "portfolio_net_pnl": [-0.10, -0.10, 0.05],
            "portfolio_equity": [0.90, 0.80, 0.85],
            "portfolio_equity_return": [-0.10, -0.111111, 0.0625],
            "portfolio_gross_exposure": [0.0, 0.0, 0.0],
            "portfolio_turnover": [0.0, 0.0, 0.0],
        }
    )

    summary = summarize_portfolio_ledger(frame)

    assert summary["starting_equity"] == pytest.approx(1.0)
    assert summary["max_drawdown"] == pytest.approx(-0.20)
