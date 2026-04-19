"""
E6-T3: Live Kill-switch and Unwind.

Verify that the KillSwitchManager correctly detects risk and triggers.
"""

from __future__ import annotations

import asyncio

import pytest

from project.live.kill_switch import (
    KillSwitchManager,
    KillSwitchReason,
    UnwindOrchestrator,
)
from project.live.state import LiveStateStore, PositionState


def test_drawdown_trigger():
    store = LiveStateStore()
    mgr = KillSwitchManager(store)

    # 1. Establish peak at $1000
    store.account.wallet_balance = 1000.0
    mgr.check_drawdown(max_drawdown_pct=0.15)
    assert mgr.status.peak_equity == 1000.0
    assert not mgr.status.is_active

    # 2. Drawdown to $800 (20%)
    store.account.update_position(
        PositionState(
            symbol="BTCUSDT",
            side="LONG",
            quantity=1.0,
            entry_price=60000.0,
            mark_price=59800.0,
            unrealized_pnl=-200.0,
        )
    )

    mgr.check_drawdown(max_drawdown_pct=0.15)

    assert mgr.status.is_active
    assert mgr.status.reason == KillSwitchReason.EXCESSIVE_DRAWDOWN
    assert "20.00%" in mgr.status.message


def test_callback_triggered():
    store = LiveStateStore()
    mgr = KillSwitchManager(store)

    triggered_count = 0

    def my_cb(reason, msg):
        nonlocal triggered_count
        triggered_count += 1

    mgr.register_callback(my_cb)
    mgr.trigger(KillSwitchReason.MANUAL, "test message")

    assert triggered_count == 1
    assert mgr.status.is_active

    # Should not trigger again if already active
    mgr.trigger(KillSwitchReason.FEATURE_DRIFT, "should not see this")
    assert triggered_count == 1


def test_live_quality_gate_disable_can_trigger_kill_switch():
    store = LiveStateStore()
    mgr = KillSwitchManager(store)

    result = mgr.check_live_quality_gate(
        {
            "action": "disable",
            "reason_codes": ["edge_divergence_disable"],
        }
    )

    assert result["triggered"] is True
    assert mgr.status.is_active
    assert mgr.status.reason == KillSwitchReason.LIVE_QUALITY_DEGRADATION


def test_reset():
    store = LiveStateStore()
    mgr = KillSwitchManager(store)

    mgr.trigger(KillSwitchReason.MANUAL)
    assert mgr.status.is_active

    mgr.reset()
    assert not mgr.status.is_active
    assert mgr.status.reason is None


def test_microstructure_breakdown_trigger() -> None:
    store = LiveStateStore()
    mgr = KillSwitchManager(store)

    gate = mgr.check_microstructure(
        spread_bps=15.0,
        depth_usd=10_000.0,
        tob_coverage=0.50,
        max_spread_bps=5.0,
        min_depth_usd=25_000.0,
        min_tob_coverage=0.80,
    )

    assert gate["is_tradable"] is False
    assert mgr.status.is_active
    assert mgr.status.reason == KillSwitchReason.MICROSTRUCTURE_BREAKDOWN
    assert "spread_blowout" in mgr.status.message
    assert "depth_collapse" in mgr.status.message
    assert "cost_model_invalid" in mgr.status.message


def test_microstructure_check_does_not_trigger_when_safe() -> None:
    store = LiveStateStore()
    mgr = KillSwitchManager(store)

    gate = mgr.check_microstructure(
        spread_bps=2.0,
        depth_usd=100_000.0,
        tob_coverage=0.95,
        max_spread_bps=5.0,
        min_depth_usd=25_000.0,
        min_tob_coverage=0.80,
    )

    assert gate["is_tradable"] is True
    assert mgr.status.is_active is False


def test_microstructure_recovery_requires_healthy_streak() -> None:
    store = LiveStateStore()
    mgr = KillSwitchManager(store, microstructure_recovery_streak=3)

    failing = mgr.check_microstructure(
        spread_bps=15.0,
        depth_usd=10_000.0,
        tob_coverage=0.50,
        max_spread_bps=5.0,
        min_depth_usd=25_000.0,
        min_tob_coverage=0.80,
    )
    first_recovery = mgr.check_microstructure(
        spread_bps=2.0,
        depth_usd=100_000.0,
        tob_coverage=0.95,
        max_spread_bps=5.0,
        min_depth_usd=25_000.0,
        min_tob_coverage=0.80,
    )
    second_recovery = mgr.check_microstructure(
        spread_bps=2.0,
        depth_usd=100_000.0,
        tob_coverage=0.95,
        max_spread_bps=5.0,
        min_depth_usd=25_000.0,
        min_tob_coverage=0.80,
    )
    final_recovery = mgr.check_microstructure(
        spread_bps=2.0,
        depth_usd=100_000.0,
        tob_coverage=0.95,
        max_spread_bps=5.0,
        min_depth_usd=25_000.0,
        min_tob_coverage=0.80,
    )

    assert failing["is_tradable"] is False
    assert first_recovery["is_tradable"] is False
    assert first_recovery["reasons"] == ["microstructure_cooldown"]
    assert first_recovery["recovery_streak"] == 1
    assert second_recovery["is_tradable"] is False
    assert second_recovery["recovery_streak"] == 2
    assert final_recovery["is_tradable"] is True
    assert final_recovery["recovered"] is True
    assert mgr.status.is_active is False


def test_non_microstructure_kill_switch_keeps_gate_blocked() -> None:
    store = LiveStateStore()
    mgr = KillSwitchManager(store)
    mgr.trigger(KillSwitchReason.MANUAL, "manual halt")

    gate = mgr.check_microstructure(
        spread_bps=2.0,
        depth_usd=100_000.0,
        tob_coverage=0.95,
        max_spread_bps=5.0,
        min_depth_usd=25_000.0,
        min_tob_coverage=0.80,
    )

    assert gate["is_tradable"] is False
    assert "kill_switch_active" in gate["reasons"]


def test_kill_switch_state_persists_across_manager_restart() -> None:
    store = LiveStateStore()
    mgr = KillSwitchManager(store, microstructure_recovery_streak=2)

    mgr.check_microstructure(
        spread_bps=15.0,
        depth_usd=10_000.0,
        tob_coverage=0.50,
        max_spread_bps=5.0,
        min_depth_usd=25_000.0,
        min_tob_coverage=0.80,
    )
    mgr.check_microstructure(
        spread_bps=2.0,
        depth_usd=100_000.0,
        tob_coverage=0.95,
        max_spread_bps=5.0,
        min_depth_usd=25_000.0,
        min_tob_coverage=0.80,
    )

    restarted = KillSwitchManager(store, microstructure_recovery_streak=2)
    gate = restarted.check_microstructure(
        spread_bps=2.0,
        depth_usd=100_000.0,
        tob_coverage=0.95,
        max_spread_bps=5.0,
        min_depth_usd=25_000.0,
        min_tob_coverage=0.80,
    )

    assert restarted.status.is_active is False
    assert gate["is_tradable"] is True
    assert gate["recovered"] is True


def test_kill_switch_state_persists_across_disk_snapshot(tmp_path) -> None:
    store = LiveStateStore()
    mgr = KillSwitchManager(store, microstructure_recovery_streak=2)

    mgr.check_microstructure(
        spread_bps=15.0,
        depth_usd=10_000.0,
        tob_coverage=0.50,
        max_spread_bps=5.0,
        min_depth_usd=25_000.0,
        min_tob_coverage=0.80,
    )
    mgr.check_microstructure(
        spread_bps=2.0,
        depth_usd=100_000.0,
        tob_coverage=0.95,
        max_spread_bps=5.0,
        min_depth_usd=25_000.0,
        min_tob_coverage=0.80,
    )

    snapshot_path = store.save_snapshot(tmp_path / "live_state.json")
    restored_store = LiveStateStore.load_snapshot(snapshot_path)
    restarted = KillSwitchManager(restored_store, microstructure_recovery_streak=2)

    gate = restarted.check_microstructure(
        spread_bps=2.0,
        depth_usd=100_000.0,
        tob_coverage=0.95,
        max_spread_bps=5.0,
        min_depth_usd=25_000.0,
        min_tob_coverage=0.80,
    )

    assert restarted.status.is_active is False
    assert gate["is_tradable"] is True
    assert gate["recovered"] is True


def test_kill_switch_auto_persists_via_state_store_snapshot_path(tmp_path) -> None:
    snapshot_path = tmp_path / "live_state.json"
    store = LiveStateStore(snapshot_path=snapshot_path)
    mgr = KillSwitchManager(store)

    mgr.check_microstructure(
        spread_bps=15.0,
        depth_usd=10_000.0,
        tob_coverage=0.50,
        max_spread_bps=5.0,
        min_depth_usd=25_000.0,
        min_tob_coverage=0.80,
    )

    restored_store = LiveStateStore.load_snapshot(snapshot_path)
    assert restored_store.get_kill_switch_snapshot()["is_active"] is True
    assert restored_store.get_kill_switch_snapshot()["reason"] == "MICROSTRUCTURE_BREAKDOWN"


# ---------------------------------------------------------------------------
# Regression: Bug 1.1 — peak equity must survive trigger() and reset()
# ---------------------------------------------------------------------------


def test_peak_equity_preserved_after_trigger():
    """Kill-switch trigger() and reset() must not erase the high-water mark."""
    store = LiveStateStore()
    mgr = KillSwitchManager(store)

    # 1. Establish peak at $1000
    store.account.wallet_balance = 1000.0
    store.account.total_unrealized_pnl = 0.0
    mgr.check_drawdown(max_drawdown_pct=0.15)
    assert mgr.status.peak_equity == pytest.approx(1000.0)

    # 2. Trigger kill-switch manually — peak must survive
    mgr.trigger(KillSwitchReason.MANUAL, "regression test")
    assert mgr.status.is_active
    assert mgr.status.peak_equity == pytest.approx(1000.0), (
        "peak_equity was erased by trigger() — Bug 1.1 regression"
    )

    # 3. Reset — peak must still survive
    mgr.reset()
    assert not mgr.status.is_active
    assert mgr.status.peak_equity == pytest.approx(1000.0), (
        "peak_equity was erased by reset() — Bug 1.1 regression"
    )

    # 4. A second drawdown event must still detect drawdown relative to $1000
    store.account.wallet_balance = 800.0  # 20% drawdown from $1000
    mgr.check_drawdown(max_drawdown_pct=0.15)
    assert mgr.status.is_active, (
        "Drawdown check did not trigger after reset — peak must still be $1000"
    )
    assert mgr.status.reason == KillSwitchReason.EXCESSIVE_DRAWDOWN


# ---------------------------------------------------------------------------
# Regression: Bug 1.3 — unwind lock must prevent double-execution
# ---------------------------------------------------------------------------


def test_unwind_lock_prevents_double_execution():
    """Two concurrent calls to unwind_all() must not both proceed."""
    call_count = 0

    class FakeOms:
        async def cancel_all_orders(self):
            nonlocal call_count
            call_count += 1
            # Simulate slow cancellation so the second caller arrives while busy
            await asyncio.sleep(0.05)

    store = LiveStateStore()
    orchestrator = UnwindOrchestrator(state_store=store, oms_manager=FakeOms())

    async def run():
        await asyncio.gather(
            orchestrator.unwind_all(),
            orchestrator.unwind_all(),
        )

    asyncio.run(run())
    # Only one of the two concurrent calls should have proceeded past the guard
    assert call_count == 1, (
        f"cancel_all_orders() called {call_count} times — Bug 1.3 double-unwind regression"
    )
