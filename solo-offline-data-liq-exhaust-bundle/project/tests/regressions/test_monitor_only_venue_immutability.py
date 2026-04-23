from __future__ import annotations

import asyncio

from project.live.kill_switch import KillSwitchReason
from project.live.runner import LiveEngineRunner


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
        self.flatten_calls += 1


def test_monitor_only_kill_switch_never_mutates_venue() -> None:
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
        runner.kill_switch.trigger(KillSwitchReason.MANUAL, "monitor-only canary")
        assert runner._kill_switch_task is not None
        await runner._kill_switch_task

    asyncio.run(_exercise())

    assert runner._running is False
    assert data_manager.stop_calls == 1
    assert order_manager.cancel_calls == 0
    assert order_manager.flatten_calls == 0
