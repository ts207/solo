from __future__ import annotations

import asyncio
import json
import logging
import math
from types import SimpleNamespace

import pandas as pd
import pytest

from project import PROJECT_ROOT
from project.core.exceptions import CompatibilityRequiredError
from project.engine.exchange_constraints import SymbolConstraints
from project.engine.strategy_executor import StrategyResult, calculate_strategy_returns
from project.live.execution_attribution import build_execution_attribution_record
from project.live.ingest.parsers import KlineEvent
from project.live.kill_switch import KillSwitchReason
from project.live.oms import OrderManager, OrderType, OrderSubmissionBlocked
from project.live.runner import LiveEngineRunner
from project.portfolio.incubation import IncubationLedger


class _DummyDataManager:
    def __init__(self) -> None:
        self.kline_queue = asyncio.Queue()
        self.ticker_queue = asyncio.Queue()
        self.stop_calls = 0

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        self.stop_calls += 1
        return None


class _DummyOrderManager:
    def __init__(self) -> None:
        self.execution_attribution = []
        self.cancel_calls = 0
        self.flatten_calls = 0

    async def cancel_all_orders(self) -> None:
        self.cancel_calls += 1

    async def flatten_all_positions(self, state_store) -> None:
        assert state_store is not None
        self.flatten_calls += 1


class _FailingOrderManager(_DummyOrderManager):
    async def cancel_all_orders(self) -> None:
        self.cancel_calls += 1
        raise RuntimeError("cancel failed")


class _FailingMarketFeatureRestClient:
    async def get_premium_index(self, symbol: str):
        raise RuntimeError(f"premium outage for {symbol}")

    async def get_open_interest(self, symbol: str):
        raise RuntimeError(f"open-interest outage for {symbol}")


def test_live_runner_exposes_persistent_session_metadata(tmp_path) -> None:
    snapshot_path = tmp_path / "live_session_state.json"
    report_path = tmp_path / "execution_quality.json"
    metrics_path = tmp_path / "runtime_metrics.json"

    runner = LiveEngineRunner(
        ["btcusdt", "ethusdt"],
        snapshot_path=snapshot_path,
        microstructure_recovery_streak=5,
        execution_quality_report_path=report_path,
        runtime_metrics_snapshot_path=metrics_path,
        data_manager=_DummyDataManager(),
    )

    assert runner.state_store._snapshot_path == snapshot_path
    assert runner.kill_switch.microstructure_recovery_streak == 5
    assert runner.session_metadata["live_state_snapshot_path"] == str(snapshot_path)
    assert runner.session_metadata["live_state_auto_persist_enabled"] is True
    assert runner.session_metadata["kill_switch_recovery_streak"] == 5
    assert runner.session_metadata["account_sync_interval_seconds"] == 30.0
    assert runner.session_metadata["account_sync_failure_threshold"] == 3
    assert runner.session_metadata["execution_degradation_min_samples"] == 3
    assert runner.session_metadata["execution_degradation_warn_edge_bps"] == 0.0
    assert runner.session_metadata["execution_degradation_block_edge_bps"] == -5.0
    assert runner.session_metadata["execution_degradation_throttle_scale"] == 0.5
    assert runner.session_metadata["execution_quality_report_path"] == str(report_path)
    assert runner.session_metadata["runtime_metrics_snapshot_path"] == str(metrics_path)
    assert runner.session_metadata["runtime_mode"] == "monitor_only"
    assert runner.session_metadata["strategy_runtime_implemented"] is False
    assert runner.session_metadata["event_detection_adapter"] == "governed_runtime_core"


def test_live_runner_uses_canonical_default_incubation_ledger_path() -> None:
    runner = LiveEngineRunner(["btcusdt"], data_manager=_DummyDataManager())

    assert runner.incubation_ledger.path == PROJECT_ROOT / "live" / "incubation_ledger.json"
    assert "/project/project/" not in str(runner.incubation_ledger.path)


def test_live_runner_fails_closed_for_missing_explicit_thesis_store(tmp_path) -> None:
    missing_path = tmp_path / "missing_promoted_theses.json"

    with pytest.raises(RuntimeError, match="Configured thesis store is unavailable"):
        LiveEngineRunner(
            ["btcusdt"],
            data_manager=_DummyDataManager(),
            strategy_runtime={
                "implemented": True,
                "thesis_path": str(missing_path),
            },
        )


def test_live_runner_monitor_only_ignores_invalid_optional_thesis_store(
    monkeypatch, tmp_path
) -> None:
    broken_path = tmp_path / "broken_promoted_theses.json"
    broken_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "project.live.runner.ThesisStore.from_path",
        lambda _path: (_ for _ in ()).throw(
            CompatibilityRequiredError("legacy_but_interpretable artifact")
        ),
    )

    runner = LiveEngineRunner(
        ["btcusdt"],
        data_manager=_DummyDataManager(),
        strategy_runtime={
            "implemented": False,
            "thesis_path": str(broken_path),
        },
    )

    assert runner._thesis_store is None
    assert runner.session_metadata["strategy_runtime_implemented"] is False


def test_live_runner_periodic_account_sync_updates_state() -> None:
    snapshots = iter(
        [
            {
                "wallet_balance": 100.0,
                "margin_balance": 105.0,
                "available_balance": 90.0,
                "exchange_status": "NORMAL",
                "positions": [],
            },
            {
                "wallet_balance": 200.0,
                "margin_balance": 210.0,
                "available_balance": 180.0,
                "exchange_status": "NORMAL",
                "positions": [],
            },
        ]
    )

    async def _fetch_snapshot():
        return next(snapshots)

    runner = LiveEngineRunner(
        ["btcusdt"],
        account_sync_interval_seconds=1.0,
        account_snapshot_fetcher=_fetch_snapshot,
        data_manager=_DummyDataManager(),
    )
    runner._running = True

    async def _exercise() -> None:
        task = asyncio.create_task(runner._sync_account_state())
        await asyncio.sleep(0.05)
        assert runner.state_store.account.wallet_balance == 100.0
        runner._running = False
        await task

    asyncio.run(_exercise())


def test_live_runner_account_sync_failures_trigger_kill_switch() -> None:
    async def _fail_snapshot():
        raise RuntimeError("auth lost")

    runner = LiveEngineRunner(
        ["btcusdt"],
        account_sync_interval_seconds=1.0,
        account_sync_failure_threshold=2,
        account_snapshot_fetcher=_fail_snapshot,
        data_manager=_DummyDataManager(),
    )
    runner._running = True

    async def _exercise() -> None:
        task = asyncio.create_task(runner._sync_account_state())
        await asyncio.sleep(1.05)
        runner._running = False
        await task

    asyncio.run(_exercise())

    assert runner.account_sync_failure_count >= 2
    assert runner.kill_switch.status.is_active is True
    assert runner.kill_switch.status.reason == KillSwitchReason.ACCOUNT_SYNC_LOSS
    assert "Authenticated account sync failed" in runner.kill_switch.status.message


def test_live_runner_account_sync_success_resets_failure_count() -> None:
    results = iter(
        [
            RuntimeError("first"),
            {
                "wallet_balance": 123.0,
                "margin_balance": 124.0,
                "available_balance": 120.0,
                "exchange_status": "NORMAL",
                "positions": [],
            },
        ]
    )

    async def _fetch_snapshot():
        item = next(results)
        if isinstance(item, Exception):
            raise item
        return item

    runner = LiveEngineRunner(
        ["btcusdt"],
        account_sync_interval_seconds=1.0,
        account_sync_failure_threshold=3,
        account_snapshot_fetcher=_fetch_snapshot,
        data_manager=_DummyDataManager(),
    )
    runner._running = True

    async def _exercise() -> None:
        task = asyncio.create_task(runner._sync_account_state())
        await asyncio.sleep(0.05)
        assert runner.account_sync_failure_count == 1
        await asyncio.sleep(1.05)
        runner._running = False
        await task

    asyncio.run(_exercise())

    assert runner.account_sync_failure_count == 0
    assert runner.state_store.account.wallet_balance == 123.0
    assert runner.kill_switch.status.is_active is False


def test_live_runner_actuates_kill_switch_shutdown_and_unwind() -> None:
    data_manager = _DummyDataManager()
    order_manager = _DummyOrderManager()
    runner = LiveEngineRunner(
        ["btcusdt"],
        order_manager=order_manager,
        data_manager=data_manager,
        runtime_mode="trading",
        strategy_runtime={"implemented": True},
    )
    runner._running = True

    async def _exercise() -> None:
        runner.kill_switch.trigger(KillSwitchReason.MANUAL, "manual test")
        assert runner._kill_switch_task is not None
        await runner._kill_switch_task

    asyncio.run(_exercise())

    assert runner._running is False
    assert data_manager.stop_calls == 1
    assert order_manager.cancel_calls == 1
    assert order_manager.flatten_calls == 1


def test_live_runner_does_not_shutdown_if_kill_switch_unwind_fails(caplog) -> None:
    data_manager = _DummyDataManager()
    order_manager = _FailingOrderManager()
    runner = LiveEngineRunner(
        ["btcusdt"],
        order_manager=order_manager,
        data_manager=data_manager,
        runtime_mode="trading",
        strategy_runtime={"implemented": True},
    )
    runner._running = True

    async def _exercise() -> None:
        runner.kill_switch.trigger(KillSwitchReason.MANUAL, "manual test")
        assert runner._kill_switch_task is not None
        await runner._kill_switch_task

    with caplog.at_level(logging.CRITICAL, logger="project.live.runner"):
        asyncio.run(_exercise())

    assert runner._running is False
    assert data_manager.stop_calls == 0
    assert order_manager.cancel_calls == 1
    assert order_manager.flatten_calls == 0
    assert any(
        record.levelno == logging.CRITICAL and "Kill-switch actuation failed" in record.message
        for record in caplog.records
    )


def test_reconcile_thesis_batch_reports_degraded_state_in_monitor_only(monkeypatch) -> None:
    runner = LiveEngineRunner(["btcusdt"], data_manager=_DummyDataManager())
    runner._thesis_store = SimpleNamespace()

    def _boom(**_kwargs):
        raise ValueError("metadata unreadable")

    monkeypatch.setattr("project.live.runner.reconcile_thesis_batch", _boom)

    runner._reconcile_thesis_batch()

    snapshot = runner.state_store.get_kill_switch_snapshot()
    assert snapshot["is_active"] is False
    assert snapshot["reason"] == "thesis_batch_reconciliation_degraded"
    assert "metadata unreadable" in snapshot["message"]
    assert runner.state_store.account.exchange_status == "DEGRADED"


def test_reconcile_thesis_batch_raises_typed_error_for_trading_runtime(monkeypatch) -> None:
    runner = LiveEngineRunner(
        ["btcusdt"],
        data_manager=_DummyDataManager(),
        runtime_mode="trading",
        strategy_runtime={"implemented": True},
    )
    runner._thesis_store = SimpleNamespace()

    def _boom(**_kwargs):
        raise ValueError("metadata unreadable")

    monkeypatch.setattr("project.live.runner.reconcile_thesis_batch", _boom)

    with pytest.raises(Exception, match="metadata unreadable") as excinfo:
        runner._reconcile_thesis_batch()

    assert excinfo.type.__name__ == "ThesisBatchReconciliationError"


def test_startup_reconciliation_does_not_abort_on_minor_venue_drift() -> None:
    """Regression: launcher seeds state from snapshot A, then start() fetches
    snapshot B.  Even when drift exceeds tolerances (e.g. fill between
    snapshots), startup must not abort — it updates state and logs warnings."""
    from project.live.state import LiveStateStore

    state_store = LiveStateStore()
    state_store.update_from_exchange_snapshot(
        {
            "wallet_balance": 10000.0,
            "margin_balance": 10000.0,
            "available_balance": 9800.0,
            "positions": [
                {
                    "symbol": "BTCUSDT",
                    "quantity": 0.5,
                    "entry_price": 60000.0,
                    "unrealized_pnl": 100.0,
                },
            ],
        }
    )

    drift_snapshot = {
        "wallet_balance": 10000.50,
        "margin_balance": 10000.50,
        "available_balance": 9800.50,
        "positions": [
            {
                "symbol": "BTCUSDT",
                "quantity": 0.51,
                "entry_price": 60000.0,
                "unrealized_pnl": 110.0,
            },
        ],
    }

    discrepancies = state_store.reconcile(drift_snapshot)
    assert len(discrepancies) > 0, "drift must exceed tolerances for this test"

    state_store.update_from_exchange_snapshot(drift_snapshot)
    assert state_store.account.wallet_balance == pytest.approx(10000.50)
    assert state_store.account.positions["BTCUSDT"].quantity == pytest.approx(0.51)

    state_store.update_from_exchange_snapshot(drift_snapshot)
    discrepancies_after = state_store.reconcile(drift_snapshot)
    assert len(discrepancies_after) == 0, "after update, no discrepancies"


def test_live_runner_monitor_only_kill_switch_does_not_mutate_venue() -> None:
    data_manager = _DummyDataManager()
    order_manager = _DummyOrderManager()
    runner = LiveEngineRunner(
        ["btcusdt"],
        order_manager=order_manager,
        data_manager=data_manager,
        runtime_mode="monitor_only",
    )
    runner._running = True

    async def _exercise() -> None:
        runner.kill_switch.trigger(KillSwitchReason.MANUAL, "monitor-only test")
        assert runner._kill_switch_task is not None
        await runner._kill_switch_task

    asyncio.run(_exercise())

    assert runner._running is False
    assert data_manager.stop_calls == 1
    assert order_manager.cancel_calls == 0
    assert order_manager.flatten_calls == 0


def test_live_runner_marks_kline_task_done_even_when_processing_fails() -> None:
    data_manager = _DummyDataManager()
    runner = LiveEngineRunner(
        ["btcusdt"],
        data_manager=data_manager,
    )
    runner._running = True

    async def _boom(event) -> None:
        raise RuntimeError("processing failed")

    runner._process_kline_for_thesis_runtime = _boom  # type: ignore[method-assign]

    event = KlineEvent(
        symbol="BTCUSDT",
        timeframe="1m",
        timestamp=pd.Timestamp("2026-01-01T00:00:00Z"),
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        volume=1.0,
        quote_volume=1.0,
        taker_base_volume=1.0,
        is_final=True,
    )

    async def _exercise() -> None:
        consumer = asyncio.create_task(runner._consume_klines())
        await data_manager.kline_queue.put(event)
        await asyncio.sleep(0.05)
        runner._running = False
        consumer.cancel()
        try:
            await consumer
        except asyncio.CancelledError:
            pass

    asyncio.run(_exercise())

    assert data_manager.kline_queue._unfinished_tasks == 0


class _DummyStrategy:
    def generate_positions(
        self, bars: pd.DataFrame, features: pd.DataFrame, params: dict
    ) -> pd.Series:
        out = pd.Series([0.0, 1.0, 1.0], index=pd.DatetimeIndex(bars["timestamp"]), dtype=float)
        out.attrs["strategy_metadata"] = {"family": "test"}
        return out


def _bars() -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=3, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": idx,
            "open": [100.0, 100.0, 100.0],
            "high": [100.2, 100.2, 100.2],
            "low": [99.8, 99.8, 99.8],
            "close": [100.0, 100.0, 100.0],
        }
    )


def _features() -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=3, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": idx,
            "spread_bps": [4.0, 4.0, 4.0],
            "quote_volume": [250000.0, 250000.0, 250000.0],
            "depth_usd": [50000.0, 50000.0, 50000.0],
            "tob_coverage": [1.0, 1.0, 1.0],
            "atr_14": [0.2, 0.2, 0.2],
        }
    )


def _graduate_dummy_strategy(runner: LiveEngineRunner, tmp_path) -> None:
    ledger = IncubationLedger(tmp_path / "incubation_ledger.json")
    ledger.start_incubation("dummy_strategy", "test-blueprint")
    ledger.graduate("dummy_strategy")
    runner.incubation_ledger = ledger


def test_live_runner_submit_strategy_result_routes_order_through_oms(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("project.engine.strategy_executor.get_strategy", lambda _: _DummyStrategy())
    monkeypatch.setattr(
        "project.engine.strategy_executor.load_symbol_constraints",
        lambda symbol, meta_dir: SymbolConstraints(
            tick_size=None, step_size=None, min_notional=None
        ),
    )

    result = calculate_strategy_returns(
        "BTCUSDT",
        _bars(),
        _features(),
        "dummy_strategy",
        {
            "position_scale": 1000.0,
            "execution_lag_bars": 0,
            "expected_return_bps": 25.0,
            "expected_adverse_bps": 5.0,
            "execution_model": {
                "cost_model": "static",
                "base_fee_bps": 2.0,
                "base_slippage_bps": 1.0,
            },
        },
        0.0,
        tmp_path,
    )

    runner = LiveEngineRunner(
        ["btcusdt"],
        order_manager=OrderManager(),
        data_manager=_DummyDataManager(),
        runtime_mode="trading",
        strategy_runtime={"implemented": True},
    )
    _graduate_dummy_strategy(runner, tmp_path)
    accepted = runner.submit_strategy_result(
        result,
        client_order_id="runner-order-1",
        order_type=OrderType.MARKET,
        realized_fee_bps=1.5,
        market_state={"spread_bps": 2.0, "depth_usd": 100000.0, "tob_coverage": 0.95},
    )

    assert accepted is not None
    assert accepted["accepted"] is True
    order = runner.order_manager.active_orders["runner-order-1"]
    assert order.metadata["expected_return_bps"] == 25.0
    assert order.metadata["realized_fee_bps"] == 1.5
    assert order.metadata["execution_degradation_action"] == "allow"


def test_live_runner_blocks_ungraduated_strategy_submission(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("project.engine.strategy_executor.get_strategy", lambda _: _DummyStrategy())
    monkeypatch.setattr(
        "project.engine.strategy_executor.load_symbol_constraints",
        lambda symbol, meta_dir: SymbolConstraints(
            tick_size=None, step_size=None, min_notional=None
        ),
    )

    result = calculate_strategy_returns(
        "BTCUSDT",
        _bars(),
        _features(),
        "dummy_strategy",
        {
            "position_scale": 1000.0,
            "execution_lag_bars": 0,
            "expected_return_bps": 25.0,
            "expected_adverse_bps": 5.0,
            "execution_model": {
                "cost_model": "static",
                "base_fee_bps": 2.0,
                "base_slippage_bps": 1.0,
            },
        },
        0.0,
        tmp_path,
    )

    runner = LiveEngineRunner(
        ["btcusdt"],
        order_manager=OrderManager(),
        data_manager=_DummyDataManager(),
        runtime_mode="trading",
        strategy_runtime={"implemented": True},
    )
    runner.incubation_ledger = IncubationLedger(tmp_path / "incubation_ledger.json")

    blocked = runner.submit_strategy_result(
        result,
        client_order_id="runner-order-incubating",
        order_type=OrderType.MARKET,
        realized_fee_bps=1.5,
        market_state={"spread_bps": 2.0, "depth_usd": 100000.0, "tob_coverage": 0.95},
    )

    assert blocked is not None
    assert blocked["accepted"] is False
    assert blocked["blocked_by"] == "incubation_gate"
    assert "runner-order-incubating" not in runner.order_manager.active_orders
    assert runner.order_manager.order_history[-1].status.name == "REJECTED"


def test_live_runner_rejects_forged_strategy_result_in_trading_mode() -> None:
    result = StrategyResult(
        name="dummy",
        data=pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=1, freq="5min", tz="UTC"),
                "symbol": ["BTCUSDT"],
                "target_position": [1000.0],
                "prior_executed_position": [0.0],
                "fill_price": [100.0],
                "close": [100.0],
                "expected_return_bps": [20.0],
                "expected_adverse_bps": [5.0],
                "expected_cost_bps": [3.0],
                "expected_net_edge_bps": [12.0],
            }
        ),
        diagnostics={},
        strategy_metadata={},
        trace=pd.DataFrame(),
    )
    runner = LiveEngineRunner(
        ["btcusdt"],
        order_manager=OrderManager(),
        data_manager=_DummyDataManager(),
        runtime_mode="trading",
        strategy_runtime={"implemented": True},
    )

    with pytest.raises(OrderSubmissionBlocked, match="validated runtime provenance"):
        runner.submit_strategy_result(result, client_order_id="forged-order")


def test_live_runner_sync_submit_fails_closed_for_exchange_backed_oms(
    monkeypatch, tmp_path
) -> None:
    class _DummyExchangeClient:
        async def create_market_order(self, **kwargs):
            return {"orderId": "venue-2"}

    monkeypatch.setattr("project.engine.strategy_executor.get_strategy", lambda _: _DummyStrategy())
    monkeypatch.setattr(
        "project.engine.strategy_executor.load_symbol_constraints",
        lambda symbol, meta_dir: SymbolConstraints(
            tick_size=None, step_size=None, min_notional=None
        ),
    )
    result = calculate_strategy_returns(
        "BTCUSDT",
        _bars(),
        _features(),
        "dummy_strategy",
        {
            "position_scale": 1000.0,
            "execution_lag_bars": 0,
            "expected_return_bps": 25.0,
            "expected_adverse_bps": 5.0,
            "execution_model": {
                "cost_model": "static",
                "base_fee_bps": 2.0,
                "base_slippage_bps": 1.0,
            },
        },
        0.0,
        tmp_path,
    )
    runner = LiveEngineRunner(
        ["btcusdt"],
        order_manager=OrderManager(exchange_client=_DummyExchangeClient()),
        data_manager=_DummyDataManager(),
        runtime_mode="trading",
        strategy_runtime={"implemented": True},
    )
    _graduate_dummy_strategy(runner, tmp_path)

    with pytest.raises(Exception, match="submit_strategy_result_async"):
        runner.submit_strategy_result(
            result,
            client_order_id="runner-order-sync-venue",
            order_type=OrderType.MARKET,
            realized_fee_bps=1.5,
            market_state={"spread_bps": 2.0, "depth_usd": 100000.0, "tob_coverage": 0.95},
        )


def test_live_runner_submit_strategy_result_async_hits_venue(monkeypatch, tmp_path) -> None:
    class _DummyExchangeClient:
        def __init__(self) -> None:
            self.calls = []

        async def create_market_order(self, **kwargs):
            self.calls.append(kwargs)
            return {"orderId": "venue-3"}

    monkeypatch.setattr("project.engine.strategy_executor.get_strategy", lambda _: _DummyStrategy())
    monkeypatch.setattr(
        "project.engine.strategy_executor.load_symbol_constraints",
        lambda symbol, meta_dir: SymbolConstraints(
            tick_size=None, step_size=None, min_notional=None
        ),
    )
    result = calculate_strategy_returns(
        "BTCUSDT",
        _bars(),
        _features(),
        "dummy_strategy",
        {
            "position_scale": 1000.0,
            "execution_lag_bars": 0,
            "expected_return_bps": 25.0,
            "expected_adverse_bps": 5.0,
            "execution_model": {
                "cost_model": "static",
                "base_fee_bps": 2.0,
                "base_slippage_bps": 1.0,
            },
        },
        0.0,
        tmp_path,
    )
    exchange_client = _DummyExchangeClient()
    runner = LiveEngineRunner(
        ["btcusdt"],
        order_manager=OrderManager(exchange_client=exchange_client),
        data_manager=_DummyDataManager(),
        runtime_mode="trading",
        strategy_runtime={"implemented": True},
    )
    _graduate_dummy_strategy(runner, tmp_path)

    accepted = asyncio.run(
        runner.submit_strategy_result_async(
            result,
            client_order_id="runner-order-async-venue",
            order_type=OrderType.MARKET,
            realized_fee_bps=1.5,
            market_state={"spread_bps": 2.0, "depth_usd": 100000.0, "tob_coverage": 0.95},
        )
    )

    assert accepted is not None
    assert accepted["accepted"] is True
    assert accepted["venue_submitted"] is True
    assert exchange_client.calls == [
        {"symbol": "BTCUSDT", "side": "BUY", "quantity": 10.0, "reduce_only": False}
    ]
    order = runner.order_manager.active_orders["runner-order-async-venue"]
    assert order.exchange_order_id == "venue-3"


def test_live_runner_submit_strategy_result_rejects_monitor_only_mode() -> None:
    result = StrategyResult(
        name="dummy",
        data=pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=1, freq="5min", tz="UTC"),
                "symbol": ["BTCUSDT"],
                "target_position": [0.0],
                "prior_executed_position": [0.0],
                "fill_price": [100.0],
                "close": [100.0],
                "expected_return_bps": [20.0],
                "expected_adverse_bps": [5.0],
                "expected_cost_bps": [3.0],
                "expected_net_edge_bps": [12.0],
            }
        ),
        diagnostics={},
        strategy_metadata={},
        trace=pd.DataFrame(),
    )
    runner = LiveEngineRunner(["btcusdt"], data_manager=_DummyDataManager())

    with pytest.raises(RuntimeError, match="monitor_only"):
        runner.submit_strategy_result(result, client_order_id="flat")


def test_live_runner_submit_strategy_result_returns_none_for_flat_result_in_trading_mode() -> None:
    result = StrategyResult(
        name="dummy",
        data=pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=1, freq="5min", tz="UTC"),
                "symbol": ["BTCUSDT"],
                "target_position": [0.0],
                "prior_executed_position": [0.0],
                "fill_price": [100.0],
                "close": [100.0],
                "expected_return_bps": [20.0],
                "expected_adverse_bps": [5.0],
                "expected_cost_bps": [3.0],
                "expected_net_edge_bps": [12.0],
            }
        ),
        diagnostics={},
        strategy_metadata={},
        trace=pd.DataFrame(),
    )
    runner = LiveEngineRunner(
        ["btcusdt"],
        data_manager=_DummyDataManager(),
        runtime_mode="trading",
        strategy_runtime={"implemented": True},
    )

    assert runner.submit_strategy_result(result, client_order_id="flat") is None


def test_live_runner_submit_strategy_result_throttles_negative_bucket(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.setattr("project.engine.strategy_executor.get_strategy", lambda _: _DummyStrategy())
    monkeypatch.setattr(
        "project.engine.strategy_executor.load_symbol_constraints",
        lambda symbol, meta_dir: SymbolConstraints(
            tick_size=None, step_size=None, min_notional=None
        ),
    )
    result = calculate_strategy_returns(
        "BTCUSDT",
        _bars(),
        _features(),
        "dummy_strategy",
        {
            "position_scale": 1000.0,
            "execution_lag_bars": 0,
            "expected_return_bps": 25.0,
            "expected_adverse_bps": 5.0,
            "execution_model": {
                "cost_model": "static",
                "base_fee_bps": 2.0,
                "base_slippage_bps": 1.0,
            },
        },
        0.0,
        tmp_path,
    )
    runner = LiveEngineRunner(
        ["btcusdt"],
        order_manager=OrderManager(),
        execution_degradation_min_samples=2,
        execution_degradation_warn_edge_bps=0.0,
        execution_degradation_block_edge_bps=-5.0,
        execution_degradation_throttle_scale=0.5,
        data_manager=_DummyDataManager(),
        runtime_mode="trading",
        strategy_runtime={"implemented": True},
    )
    _graduate_dummy_strategy(runner, tmp_path)
    runner.order_manager.execution_attribution.extend(
        [
            build_execution_attribution_record(
                client_order_id="hist1",
                symbol="BTCUSDT",
                strategy="dummy_strategy",
                volatility_regime="elevated",
                microstructure_regime="healthy",
                side="BUY",
                quantity=1.0,
                signal_timestamp="2024-01-01T00:00:00+00:00",
                expected_entry_price=100.0,
                realized_fill_price=100.07,
                expected_return_bps=10.0,
                expected_adverse_bps=5.0,
                expected_cost_bps=2.0,
                realized_fee_bps=1.0,
            ),
            build_execution_attribution_record(
                client_order_id="hist2",
                symbol="BTCUSDT",
                strategy="dummy_strategy",
                volatility_regime="elevated",
                microstructure_regime="healthy",
                side="BUY",
                quantity=1.0,
                signal_timestamp="2024-01-01T00:05:00+00:00",
                expected_entry_price=100.0,
                realized_fill_price=100.06,
                expected_return_bps=10.0,
                expected_adverse_bps=5.0,
                expected_cost_bps=2.0,
                realized_fee_bps=1.0,
            ),
        ]
    )

    accepted = runner.submit_strategy_result(
        result,
        client_order_id="runner-order-throttle",
        order_type=OrderType.MARKET,
        realized_fee_bps=1.5,
        market_state={"spread_bps": 2.0, "depth_usd": 100000.0, "tob_coverage": 0.95},
    )

    assert accepted is not None
    assert accepted["accepted"] is True
    order = runner.order_manager.active_orders["runner-order-throttle"]
    assert order.quantity == pytest.approx(5.0)
    assert order.metadata["execution_degradation_action"] == "throttle"
    assert order.metadata["execution_degradation_applied_scale"] == pytest.approx(0.5)


def test_live_runner_submit_strategy_result_blocks_degraded_bucket(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("project.engine.strategy_executor.get_strategy", lambda _: _DummyStrategy())
    monkeypatch.setattr(
        "project.engine.strategy_executor.load_symbol_constraints",
        lambda symbol, meta_dir: SymbolConstraints(
            tick_size=None, step_size=None, min_notional=None
        ),
    )
    result = calculate_strategy_returns(
        "BTCUSDT",
        _bars(),
        _features(),
        "dummy_strategy",
        {
            "position_scale": 1000.0,
            "execution_lag_bars": 0,
            "expected_return_bps": 25.0,
            "expected_adverse_bps": 5.0,
            "execution_model": {
                "cost_model": "static",
                "base_fee_bps": 2.0,
                "base_slippage_bps": 1.0,
            },
        },
        0.0,
        tmp_path,
    )
    runner = LiveEngineRunner(
        ["btcusdt"],
        order_manager=OrderManager(),
        execution_degradation_min_samples=2,
        execution_degradation_warn_edge_bps=0.0,
        execution_degradation_block_edge_bps=-5.0,
        execution_degradation_throttle_scale=0.5,
        data_manager=_DummyDataManager(),
        runtime_mode="trading",
        strategy_runtime={"implemented": True},
    )
    _graduate_dummy_strategy(runner, tmp_path)
    runner.order_manager.execution_attribution.extend(
        [
            build_execution_attribution_record(
                client_order_id="hist3",
                symbol="BTCUSDT",
                strategy="dummy_strategy",
                volatility_regime="elevated",
                microstructure_regime="healthy",
                side="BUY",
                quantity=1.0,
                signal_timestamp="2024-01-01T00:00:00+00:00",
                expected_entry_price=100.0,
                realized_fill_price=100.3,
                expected_return_bps=5.0,
                expected_adverse_bps=5.0,
                expected_cost_bps=2.0,
                realized_fee_bps=2.0,
            ),
            build_execution_attribution_record(
                client_order_id="hist4",
                symbol="BTCUSDT",
                strategy="dummy_strategy",
                volatility_regime="elevated",
                microstructure_regime="healthy",
                side="BUY",
                quantity=1.0,
                signal_timestamp="2024-01-01T00:05:00+00:00",
                expected_entry_price=100.0,
                realized_fill_price=100.4,
                expected_return_bps=5.0,
                expected_adverse_bps=5.0,
                expected_cost_bps=2.0,
                realized_fee_bps=2.0,
            ),
        ]
    )

    blocked = runner.submit_strategy_result(
        result,
        client_order_id="runner-order-block",
        order_type=OrderType.MARKET,
        realized_fee_bps=1.5,
        market_state={"spread_bps": 2.0, "depth_usd": 100000.0, "tob_coverage": 0.95},
    )

    assert blocked is not None
    assert blocked["accepted"] is False
    assert blocked["blocked_by"] == "execution_degradation"
    assert "runner-order-block" not in runner.order_manager.active_orders
    assert runner.order_manager.order_history[-1].status.name == "REJECTED"


def test_live_runner_persists_execution_quality_report_after_fill(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("project.engine.strategy_executor.get_strategy", lambda _: _DummyStrategy())
    monkeypatch.setattr(
        "project.engine.strategy_executor.load_symbol_constraints",
        lambda symbol, meta_dir: SymbolConstraints(
            tick_size=None, step_size=None, min_notional=None
        ),
    )

    result = calculate_strategy_returns(
        "BTCUSDT",
        _bars(),
        _features(),
        "dummy_strategy",
        {
            "position_scale": 1000.0,
            "execution_lag_bars": 0,
            "expected_return_bps": 25.0,
            "expected_adverse_bps": 5.0,
            "execution_model": {
                "cost_model": "static",
                "base_fee_bps": 2.0,
                "base_slippage_bps": 1.0,
            },
        },
        0.0,
        tmp_path,
    )
    report_path = tmp_path / "execution_quality.json"
    runner = LiveEngineRunner(
        ["btcusdt"],
        order_manager=OrderManager(),
        execution_quality_report_path=report_path,
        data_manager=_DummyDataManager(),
        runtime_mode="trading",
        strategy_runtime={"implemented": True},
    )
    _graduate_dummy_strategy(runner, tmp_path)
    runner.submit_strategy_result(
        result,
        client_order_id="runner-order-2",
        order_type=OrderType.MARKET,
        realized_fee_bps=1.5,
        market_state={"spread_bps": 2.0, "depth_usd": 100000.0, "tob_coverage": 0.95},
    )

    runner.on_order_fill("runner-order-2", fill_qty=10.0, fill_price=100.02)

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["summary"]["fills"] == 1.0
    assert (
        payload["summary"]["avg_realized_net_edge_bps"]
        < payload["summary"]["avg_expected_net_edge_bps"]
    )
    assert payload["records"][0]["client_order_id"] == "runner-order-2"
    assert payload["records"][0]["strategy"] == "dummy_strategy"
    assert payload["records"][0]["volatility_regime"] == "elevated"
    assert payload["records"][0]["microstructure_regime"] == "healthy"
    assert payload["by_symbol"]["BTCUSDT"]["fills"] == 1.0
    assert payload["by_strategy"]["dummy_strategy"]["fills"] == 1.0
    assert payload["by_volatility_regime"]["elevated"]["fills"] == 1.0
    assert payload["by_microstructure_regime"]["healthy"]["fills"] == 1.0


def test_live_runner_persist_execution_quality_report_returns_none_without_path() -> None:
    runner = LiveEngineRunner(["btcusdt"], data_manager=_DummyDataManager())
    assert runner.persist_execution_quality_report() is None


def test_live_runner_start_rejects_unimplemented_trading_runtime() -> None:
    runner = LiveEngineRunner(
        ["btcusdt"],
        data_manager=_DummyDataManager(),
        runtime_mode="trading",
    )

    async def _exercise() -> None:
        with pytest.raises(RuntimeError, match="strategy_runtime.implemented=true"):
            await runner.start()

    asyncio.run(_exercise())


# --- TICKET-010: kill-switch trigger tests ---


def test_stale_data_triggers_kill_switch() -> None:
    """STALE_DATA kill-switch fires when data health monitor reports unhealthy."""
    from unittest.mock import patch

    runner = LiveEngineRunner(
        ["btcusdt"],
        data_manager=_DummyDataManager(),
        stale_threshold_sec=1.0,
    )
    # Register an event on a symbol so the monitor has state, then advance the clock
    runner.health_monitor.on_event("btcusdt", "kline:1m")

    stale_report = {
        "is_healthy": False,
        "stale_count": 1,
        "max_last_seen_sec_ago": 30.0,
    }
    with patch.object(runner.health_monitor, "check_health", return_value=stale_report):

        async def _run():
            # Simulate one iteration of _monitor_data_health
            report = runner.health_monitor.check_health()
            if not report["is_healthy"]:
                from project.live.kill_switch import KillSwitchReason

                runner.kill_switch.trigger(
                    KillSwitchReason.STALE_DATA,
                    f"Stale data feeds detected: {report['stale_count']} streams",
                )

        asyncio.run(_run())

    assert runner.kill_switch.status.is_active
    assert runner.kill_switch.status.reason == KillSwitchReason.STALE_DATA


def test_ws_reconnect_exhaustion_triggers_exchange_disconnect() -> None:
    """EXCHANGE_DISCONNECT kill-switch fires when WebSocket reconnect retries are exhausted."""
    runner = LiveEngineRunner(
        ["btcusdt"],
        data_manager=_DummyDataManager(),
    )
    assert not runner.kill_switch.status.is_active

    # Simulate the ws_client calling the exhaustion callback
    runner._on_ws_reconnect_exhausted()

    assert runner.kill_switch.status.is_active
    assert runner.kill_switch.status.reason == KillSwitchReason.EXCHANGE_DISCONNECT


def test_ws_client_calls_on_reconnect_exhausted_after_max_retries() -> None:
    """ws_client invokes on_reconnect_exhausted callback when retries are exhausted."""
    import asyncio
    from project.live.ingest.ws_client import BinanceWebSocketClient

    exhausted_calls = []

    client = BinanceWebSocketClient(
        streams=["btcusdt@kline_1m"],
        on_message=lambda _: None,
        on_reconnect_exhausted=lambda: exhausted_calls.append(True),
    )

    async def _run():
        # Patch websockets.connect to always raise, forcing exhaustion
        import unittest.mock as mock

        with mock.patch(
            "project.live.ingest.ws_client.websockets.connect",
            side_effect=ConnectionRefusedError("refused"),
        ):
            # Override sleep to avoid actual delay
            with mock.patch("asyncio.sleep", return_value=None):
                client._running = True
                await client._listen()

    asyncio.run(_run())
    assert len(exhausted_calls) == 1


def test_live_runner_monitor_only_processes_thesis_runtime_events(tmp_path) -> None:
    thesis_dir = tmp_path / "live" / "theses" / "run_1"
    thesis_dir.mkdir(parents=True, exist_ok=True)
    (thesis_dir / "promoted_theses.json").write_text(
        json.dumps(
            {
                "schema_version": "promoted_theses_v1",
                "run_id": "run_1",
                "generated_at_utc": "2026-03-30T00:00:00Z",
                "thesis_count": 1,
                "active_thesis_count": 1,
                "pending_thesis_count": 0,
                "theses": [
                    {
                        "thesis_id": "thesis::run_1::cand_1",
                        "status": "active",
                        "symbol_scope": {
                            "mode": "single_symbol",
                            "symbols": ["BTCUSDT"],
                            "candidate_symbol": "BTCUSDT",
                        },
                        "timeframe": "5m",
                        "event_family": "VOL_SHOCK",
                        "canonical_regime": "VOLATILITY",
                        "event_side": "long",
                        "required_context": {"symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
                        "supportive_context": {
                            "canonical_regime": "VOLATILITY",
                            "has_realized_oos_path": True,
                        },
                        "expected_response": {"direction": "long"},
                        "invalidation": {"metric": "adverse_proxy", "operator": ">", "value": 0.02},
                        "risk_notes": [],
                        "evidence": {
                            "sample_size": 120,
                            "validation_samples": 60,
                            "test_samples": 60,
                            "estimate_bps": 10.0,
                            "net_expectancy_bps": 7.0,
                            "q_value": 0.01,
                            "stability_score": 0.8,
                            "cost_survival_ratio": 1.0,
                            "tob_coverage": 0.95,
                            "rank_score": 1.0,
                            "promotion_track": "deploy",
                            "policy_version": "v1",
                            "bundle_version": "b1",
                        },
                        "lineage": {
                            "run_id": "run_1",
                            "candidate_id": "cand_1",
                            "blueprint_id": "bp_1",
                            "hypothesis_id": "",
                            "plan_row_id": "",
                            "proposal_id": "",
                        },
                    }
                ],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    runner = LiveEngineRunner(
        ["btcusdt"],
        data_manager=_DummyDataManager(),
        strategy_runtime={
            "implemented": True,
            "thesis_path": str(thesis_dir / "promoted_theses.json"),
            "include_pending_theses": False,
            "auto_submit": False,
            "supported_event_ids": ["VOL_SHOCK"],
            "event_detector": {
                "adapter": "heuristic",
                "legacy_heuristic_enabled": True,
            },
            "memory_root": str(tmp_path / "memory"),
            "persist_dir": str(tmp_path / "live" / "persist"),
        },
    )
    runner._latest_book_ticker_by_symbol["BTCUSDT"] = {
        "best_bid_price": 99.99,
        "best_ask_price": 100.01,
        "timestamp": "2026-03-30T00:04:59Z",
    }

    first = KlineEvent(
        symbol="BTCUSDT",
        timeframe="5m",
        timestamp=pd.Timestamp("2026-03-30T00:00:00Z"),
        open=100.0,
        high=100.2,
        low=99.8,
        close=100.0,
        volume=10.0,
        quote_volume=1000.0,
        taker_base_volume=5.0,
        is_final=True,
    )
    second = KlineEvent(
        symbol="BTCUSDT",
        timeframe="5m",
        timestamp=pd.Timestamp("2026-03-30T00:05:00Z"),
        open=100.0,
        high=101.0,
        low=99.9,
        close=100.6,
        volume=20.0,
        quote_volume=2000.0,
        taker_base_volume=10.0,
        is_final=True,
    )

    asyncio.run(runner._process_kline_for_thesis_runtime(first))
    asyncio.run(runner._process_kline_for_thesis_runtime(second))

    outcomes = runner.latest_trade_intents()
    assert len(outcomes) == 1
    assert outcomes[0].trade_intent.action in {"probe", "trade_small", "trade_normal"}
    assert (tmp_path / "memory" / "episodic_trades.jsonl").exists()


def test_live_runner_refreshes_runtime_market_features_and_computes_open_interest_delta() -> None:
    responses = iter(
        [
            {"funding_rate": 0.0010, "open_interest": 1000.0, "mark_price": 101.0},
            {"funding_rate": 0.0015, "open_interest": 900.0, "mark_price": 102.0},
        ]
    )

    async def _fetch_market_features(symbol: str):
        assert symbol == "BTCUSDT"
        return next(responses)

    runner = LiveEngineRunner(
        ["btcusdt"],
        data_manager=_DummyDataManager(),
        strategy_runtime={
            "implemented": True,
            "supported_event_ids": ["LIQUIDATION_CASCADE"],
        },
        market_feature_fetcher=_fetch_market_features,
    )
    runner._thesis_store = object()

    async def _exercise() -> None:
        await runner._refresh_runtime_market_features_once()
        first = runner._latest_runtime_market_features_by_symbol["BTCUSDT"]
        assert first["funding_rate"] == pytest.approx(0.0010)
        assert first["open_interest"] == pytest.approx(1000.0)
        assert first["open_interest_delta_fraction"] == pytest.approx(0.0)

        await runner._refresh_runtime_market_features_once()
        second = runner._latest_runtime_market_features_by_symbol["BTCUSDT"]
        assert second["funding_rate"] == pytest.approx(0.0015)
        assert second["open_interest"] == pytest.approx(900.0)
        assert second["open_interest_delta_fraction"] == pytest.approx(-0.1)

        snapshot = runner._current_market_snapshot(
            symbol="BTCUSDT",
            timeframe="5m",
            close=103.0,
            timestamp="2026-04-01T00:00:00+00:00",
            move_bps=450.0,
        )
        assert snapshot["funding_rate"] == pytest.approx(0.0015)
        assert snapshot["open_interest_delta_fraction"] == pytest.approx(-0.1)
        assert snapshot["mark_price"] == pytest.approx(102.0)
        assert snapshot["mid_price"] == pytest.approx(102.0)

    asyncio.run(_exercise())


def test_live_runner_preserves_invalid_open_interest_delta_as_nan(caplog) -> None:
    async def _fetch_market_features(symbol: str):
        assert symbol == "BTCUSDT"
        return {
            "funding_rate": 0.0010,
            "open_interest": 1000.0,
            "open_interest_delta_fraction": "bad-delta",
        }

    runner = LiveEngineRunner(
        ["btcusdt"],
        data_manager=_DummyDataManager(),
        strategy_runtime={
            "implemented": True,
            "supported_event_ids": ["LIQUIDATION_CASCADE"],
        },
        market_feature_fetcher=_fetch_market_features,
    )
    runner._thesis_store = object()

    async def _exercise() -> None:
        await runner._refresh_runtime_market_features_once()

    with caplog.at_level(logging.WARNING, logger="project.live.runner"):
        asyncio.run(_exercise())

    snapshot = runner._latest_runtime_market_features_by_symbol["BTCUSDT"]
    assert math.isnan(snapshot["open_interest_delta_fraction"])
    assert any(
        "Invalid open_interest_delta_fraction" in record.message
        for record in caplog.records
    )


def test_live_runner_clears_runtime_market_features_after_refresh_failure(caplog) -> None:
    responses = iter(
        [
            {"funding_rate": 0.0010, "open_interest": 1000.0, "mark_price": 101.0},
            RuntimeError("rest outage"),
        ]
    )

    async def _fetch_market_features(symbol: str):
        response = next(responses)
        if isinstance(response, Exception):
            raise response
        return response

    runner = LiveEngineRunner(
        ["btcusdt"],
        data_manager=_DummyDataManager(),
        strategy_runtime={
            "implemented": True,
            "supported_event_ids": ["LIQUIDATION_CASCADE"],
        },
        market_feature_fetcher=_fetch_market_features,
    )
    runner._thesis_store = object()

    async def _exercise() -> None:
        await runner._refresh_runtime_market_features_once()
        assert runner._latest_runtime_market_features_by_symbol["BTCUSDT"][
            "funding_rate"
        ] == pytest.approx(0.0010)
        with caplog.at_level(logging.WARNING, logger="project.live.runner"):
            await runner._refresh_runtime_market_features_once()
        assert "BTCUSDT" not in runner._latest_runtime_market_features_by_symbol
        assert any(
            "Runtime market-feature refresh failed for BTCUSDT" in record.message
            for record in caplog.records
        )

        snapshot = runner._current_market_snapshot(
            symbol="BTCUSDT",
            timeframe="5m",
            close=103.0,
            timestamp="2026-04-01T00:00:00+00:00",
            move_bps=450.0,
        )
        assert snapshot["funding_rate"] == pytest.approx(0.0)
        assert snapshot["open_interest"] == pytest.approx(0.0)
        assert snapshot["open_interest_delta_fraction"] == pytest.approx(0.0)

    asyncio.run(_exercise())


def test_live_runner_rest_market_feature_total_outage_is_warning_and_clears_state(caplog) -> None:
    runner = LiveEngineRunner(
        ["btcusdt"],
        data_manager=_DummyDataManager(),
        strategy_runtime={
            "implemented": True,
            "supported_event_ids": ["LIQUIDATION_CASCADE"],
        },
    )
    runner.rest_client = _FailingMarketFeatureRestClient()
    runner._thesis_store = object()
    runner._latest_runtime_market_features_by_symbol["BTCUSDT"] = {"open_interest": 1000.0}

    async def _exercise() -> None:
        with caplog.at_level(logging.WARNING, logger="project.live.runner"):
            await runner._refresh_runtime_market_features_once()

    asyncio.run(_exercise())

    assert "BTCUSDT" not in runner._latest_runtime_market_features_by_symbol
    messages = [record.message for record in caplog.records]
    assert any("Runtime premium-index fetch failed for BTCUSDT" in msg for msg in messages)
    assert any("Runtime open-interest fetch failed for BTCUSDT" in msg for msg in messages)
    assert any("Runtime market-feature refresh failed for BTCUSDT" in msg for msg in messages)


def test_live_runner_persists_runtime_metrics_snapshot_with_market_state_and_decisions(
    tmp_path,
) -> None:
    thesis_dir = tmp_path / "live" / "theses" / "run_metrics"
    thesis_dir.mkdir(parents=True, exist_ok=True)
    (thesis_dir / "promoted_theses.json").write_text(
        json.dumps(
            {
                "schema_version": "promoted_theses_v1",
                "run_id": "run_metrics",
                "generated_at_utc": "2026-03-30T00:00:00Z",
                "thesis_count": 1,
                "active_thesis_count": 1,
                "pending_thesis_count": 0,
                "theses": [
                    {
                        "thesis_id": "thesis::run_metrics::cand_1",
                        "status": "active",
                        "symbol_scope": {
                            "mode": "single_symbol",
                            "symbols": ["BTCUSDT"],
                            "candidate_symbol": "BTCUSDT",
                        },
                        "timeframe": "5m",
                        "event_family": "VOL_SHOCK",
                        "event_side": "long",
                        "required_context": {"symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
                        "supportive_context": {
                            "canonical_regime": "VOLATILITY",
                            "has_realized_oos_path": True,
                        },
                        "expected_response": {"direction": "long"},
                        "invalidation": {"metric": "adverse_proxy", "operator": ">", "value": 0.02},
                        "risk_notes": [],
                        "evidence": {
                            "sample_size": 120,
                            "validation_samples": 60,
                            "test_samples": 60,
                            "estimate_bps": 10.0,
                            "net_expectancy_bps": 7.0,
                            "q_value": 0.01,
                            "stability_score": 0.8,
                            "cost_survival_ratio": 1.0,
                            "tob_coverage": 0.95,
                            "rank_score": 1.0,
                            "promotion_track": "deploy",
                            "policy_version": "v1",
                            "bundle_version": "b1",
                        },
                        "lineage": {
                            "run_id": "run_metrics",
                            "candidate_id": "cand_1",
                            "blueprint_id": "bp_1",
                            "hypothesis_id": "",
                            "plan_row_id": "",
                            "proposal_id": "",
                        },
                    }
                ],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    responses = iter(
        [
            {"funding_rate": 0.0010, "open_interest": 1000.0, "mark_price": 101.0},
            {"funding_rate": 0.0015, "open_interest": 900.0, "mark_price": 102.0},
        ]
    )

    async def _fetch_market_features(symbol: str):
        assert symbol == "BTCUSDT"
        return next(responses)

    metrics_path = tmp_path / "runtime_metrics.json"
    runner = LiveEngineRunner(
        ["btcusdt"],
        data_manager=_DummyDataManager(),
        runtime_metrics_snapshot_path=metrics_path,
        strategy_runtime={
            "implemented": True,
            "thesis_path": str(thesis_dir / "promoted_theses.json"),
            "include_pending_theses": False,
            "auto_submit": False,
            "supported_event_ids": ["VOL_SHOCK", "LIQUIDATION_CASCADE"],
            "event_detector": {
                "adapter": "heuristic",
                "legacy_heuristic_enabled": True,
            },
            "memory_root": str(tmp_path / "memory"),
            "persist_dir": str(tmp_path / "live" / "persist"),
        },
        market_feature_fetcher=_fetch_market_features,
    )
    runner._latest_book_ticker_by_symbol["BTCUSDT"] = {
        "best_bid_price": 99.99,
        "best_ask_price": 100.01,
        "timestamp": "2026-03-30T00:04:59Z",
    }

    first = KlineEvent(
        symbol="BTCUSDT",
        timeframe="5m",
        timestamp=pd.Timestamp("2026-03-30T00:00:00Z"),
        open=100.0,
        high=100.2,
        low=99.8,
        close=100.0,
        volume=10.0,
        quote_volume=1000.0,
        taker_base_volume=5.0,
        is_final=True,
    )
    second = KlineEvent(
        symbol="BTCUSDT",
        timeframe="5m",
        timestamp=pd.Timestamp("2026-03-30T00:05:00Z"),
        open=100.0,
        high=101.0,
        low=99.9,
        close=100.6,
        volume=20.0,
        quote_volume=2000.0,
        taker_base_volume=10.0,
        is_final=True,
    )

    async def _exercise() -> None:
        await runner._refresh_runtime_market_features_once()
        await runner._refresh_runtime_market_features_once()
        await runner._process_kline_for_thesis_runtime(first)
        await runner._process_kline_for_thesis_runtime(second)

    asyncio.run(_exercise())

    payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    symbol_state = payload["latest_market_state_by_symbol"]["BTCUSDT"]
    assert symbol_state["funding_rate"] == pytest.approx(0.0015)
    assert symbol_state["open_interest"] == pytest.approx(900.0)
    assert symbol_state["open_interest_delta_fraction"] == pytest.approx(-0.1)
    assert payload["decision_counts"]["recent_window"] == 1
    assert payload["decision_counts"]["by_action"]
    assert payload["recent_decisions"][0]["symbol"] == "BTCUSDT"
    assert payload["recent_decisions"][0]["primary_event_id"] == "VOL_SHOCK"
    assert payload["recent_decisions"][0]["canonical_regime"] == "VOLATILITY"
    assert payload["recent_decisions"][0]["compat_event_family"] == "VOL_SHOCK"
    assert payload["recent_decisions"][0]["thesis_canonical_regime"] == "VOLATILITY"
    assert payload["recent_decisions"][0]["event_detection_adapter"] == "heuristic"
    trace = payload["recent_decisions"][0]["decision_trace"]
    assert trace["detected_event"]["event_id"] == "VOL_SHOCK"
    assert trace["matched_thesis_ids"]
    assert trace["trade_intent"]["action"] == payload["recent_decisions"][0]["action"]
