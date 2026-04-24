"""
E6-T1: Live state tracking (Account/Position).

LiveStateStore must correctly parse exchange snapshots and update
AccountState/PositionState.
"""

from __future__ import annotations

import pytest

from project.core.exceptions import DataIntegrityError
from project.live.state import LiveStateStore


def test_initial_state_empty():
    store = LiveStateStore()
    assert store.account.wallet_balance == 0.0
    assert len(store.account.positions) == 0


def test_update_from_snapshot():
    store = LiveStateStore()
    snapshot = {
        "wallet_balance": 10000.0,
        "margin_balance": 10500.0,
        "available_balance": 9800.0,
        "exchange_status": "NORMAL",
        "positions": [
            {
                "symbol": "BTCUSDT",
                "quantity": 0.5,
                "entry_price": 60000.0,
                "mark_price": 61000.0,
                "unrealized_pnl": 500.0,
            },
            {
                "symbol": "ETHUSDT",
                "quantity": -2.0,
                "entry_price": 3000.0,
                "mark_price": 3100.0,
                "unrealized_pnl": -200.0,
            },
        ],
    }

    store.update_from_exchange_snapshot(snapshot)

    assert store.account.wallet_balance == 10000.0
    assert store.account.available_balance == 9800.0
    assert store.account.exchange_status == "NORMAL"
    assert store.account.total_unrealized_pnl == 300.0  # 500 - 200

    # Check BTC position
    btc_pos = store.account.positions["BTCUSDT"]
    assert btc_pos.side == "LONG"
    assert btc_pos.quantity == 0.5

    # Check ETH position
    eth_pos = store.account.positions["ETHUSDT"]
    assert eth_pos.side == "SHORT"
    assert eth_pos.quantity == 2.0


def test_closing_position_removes_it():
    store = LiveStateStore()
    # First, open a position
    store.update_from_exchange_snapshot(
        {"positions": [{"symbol": "BTCUSDT", "quantity": 0.5, "unrealized_pnl": 10.0}]}
    )
    assert "BTCUSDT" in store.account.positions

    # Now, snapshot with quantity 0
    store.update_from_exchange_snapshot(
        {"positions": [{"symbol": "BTCUSDT", "quantity": 0.0, "unrealized_pnl": 0.0}]}
    )
    assert "BTCUSDT" not in store.account.positions
    assert store.account.total_unrealized_pnl == 0.0


def test_kill_switch_snapshot_round_trip():
    store = LiveStateStore()
    snapshot = {
        "is_active": True,
        "reason": "MICROSTRUCTURE_BREAKDOWN",
        "triggered_at": "2026-03-13T12:00:00+00:00",
        "message": "halted",
        "recovery_streak": 2,
    }

    store.set_kill_switch_snapshot(snapshot)

    assert store.get_kill_switch_snapshot() == snapshot


def test_live_state_snapshot_save_and_load(tmp_path):
    store = LiveStateStore()
    store.update_from_exchange_snapshot(
        {
            "wallet_balance": 10000.0,
            "margin_balance": 10250.0,
            "positions": [
                {
                    "symbol": "BTCUSDT",
                    "quantity": 0.5,
                    "entry_price": 60000.0,
                    "mark_price": 60500.0,
                    "unrealized_pnl": 250.0,
                }
            ],
        }
    )
    store.set_kill_switch_snapshot(
        {
            "is_active": True,
            "reason": "MICROSTRUCTURE_BREAKDOWN",
            "triggered_at": "2026-03-13T12:00:00+00:00",
            "message": "halted",
            "recovery_streak": 1,
        }
    )

    snapshot_path = store.save_snapshot(tmp_path / "live_state_snapshot.json")
    restored = LiveStateStore.load_snapshot(snapshot_path)

    assert restored.account.wallet_balance == pytest.approx(10000.0)
    assert "BTCUSDT" in restored.account.positions
    assert restored.get_kill_switch_snapshot()["reason"] == "MICROSTRUCTURE_BREAKDOWN"
    assert restored.get_kill_switch_snapshot()["recovery_streak"] == 1


def test_live_state_auto_persists_account_updates(tmp_path):
    snapshot_path = tmp_path / "auto_live_state.json"
    store = LiveStateStore(snapshot_path=snapshot_path)

    store.update_from_exchange_snapshot(
        {
            "wallet_balance": 1234.0,
            "positions": [{"symbol": "BTCUSDT", "quantity": 0.25, "unrealized_pnl": 12.0}],
        }
    )

    restored = LiveStateStore.load_snapshot(snapshot_path)
    assert restored.account.wallet_balance == pytest.approx(1234.0)
    assert "BTCUSDT" in restored.account.positions


def test_live_state_load_snapshot_raises_on_malformed_json(tmp_path):
    snapshot_path = tmp_path / "broken_snapshot.json"
    snapshot_path.write_text("{not valid json", encoding="utf-8")

    with pytest.raises(DataIntegrityError, match="Failed to read live state snapshot"):
        LiveStateStore.load_snapshot(snapshot_path)
