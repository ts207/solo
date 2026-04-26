from __future__ import annotations

from project.live.portfolio_circuit import (
    PortfolioCircuitBreaker,
    PortfolioCircuitConfig,
    PortfolioCircuitSnapshot,
)
from project.live.state import AccountState, PositionState


def test_portfolio_circuit_triggers_on_drawdown() -> None:
    circuit = PortfolioCircuitBreaker(
        PortfolioCircuitConfig(max_portfolio_dd_pct=0.05, min_samples=2)
    )
    circuit.record_snapshot(PortfolioCircuitSnapshot("t0", equity=1000.0, gross_exposure=0.0))
    circuit.record_snapshot(PortfolioCircuitSnapshot("t1", equity=940.0, gross_exposure=0.0))

    verdict = circuit.evaluate_current()

    assert verdict.triggered is True
    assert verdict.reason == "portfolio_drawdown"
    assert verdict.metrics["portfolio_drawdown_pct"] >= 0.05


def test_portfolio_circuit_triggers_on_symbol_concentration() -> None:
    account = AccountState(wallet_balance=1000.0)
    account.update_position(
        PositionState(
            symbol="BTCUSDT",
            side="LONG",
            quantity=1.0,
            entry_price=100.0,
            mark_price=100.0,
            unrealized_pnl=0.0,
        )
    )
    circuit = PortfolioCircuitBreaker(
        PortfolioCircuitConfig(
            max_portfolio_dd_pct=1.0,
            concentration_cap_pct=0.05,
            concentration_breach_multiplier=1.5,
            min_samples=1,
        )
    )

    verdict = circuit.evaluate_account(account)

    assert verdict.triggered is True
    assert verdict.reason == "portfolio_concentration"
    assert verdict.metrics["max_symbol"] == "BTCUSDT"


def test_portfolio_circuit_no_trigger_when_disabled() -> None:
    circuit = PortfolioCircuitBreaker(PortfolioCircuitConfig(enabled=False, min_samples=2))
    circuit.record_snapshot(PortfolioCircuitSnapshot("t0", equity=1000.0, gross_exposure=0.0))
    circuit.record_snapshot(PortfolioCircuitSnapshot("t1", equity=1.0, gross_exposure=0.0))

    assert circuit.evaluate_current().triggered is False
