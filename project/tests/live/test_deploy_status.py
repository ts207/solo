from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from project.live.deploy_status import deployment_status, inspect_deployment


def _write_thesis_bundle(data_root: Path, run_id: str) -> Path:
    thesis_dir = data_root / "live" / "theses" / run_id
    thesis_dir.mkdir(parents=True)
    thesis_path = thesis_dir / "promoted_theses.json"
    thesis_path.write_text(
        json.dumps(
            {
                "schema_version": "promoted_theses_v1",
                "run_id": run_id,
                "generated_at_utc": "2026-04-19T00:00:00+00:00",
                "thesis_count": 1,
                "active_thesis_count": 1,
                "pending_thesis_count": 0,
                "theses": [
                    {
                        "thesis_id": f"thesis::{run_id}::BTCUSDT",
                        "status": "active",
                        "deployment_state": "live_eligible",
                        "deployment_mode_allowed": "live_eligible",
                        "primary_event_id": "VOL_SHOCK",
                        "event_family": "VOLATILITY_TRANSITION",
                        "symbol_scope": {
                            "candidate_symbol": "BTCUSDT",
                            "symbols": ["BTCUSDT"],
                        },
                        "requirements": {"trigger_events": ["VOL_SHOCK"]},
                        "live_approval": {"live_approval_status": "approved"},
                        "cap_profile": {
                            "max_notional": 1000.0,
                            "max_daily_loss": 50.0,
                            "max_active_orders": 2,
                            "kill_switch_scope": "thesis",
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return thesis_path


def test_inspect_deployment_returns_runtime_thesis_and_approval_state(tmp_path: Path) -> None:
    run_id = "unit_run"
    data_root = tmp_path / "data"
    _write_thesis_bundle(data_root, run_id)
    config_path = tmp_path / "live_unit.yaml"
    config_path.write_text(
        "\n".join(
            [
                "workflow_id: live_paper_unit",
                "runtime_mode: trading",
                "execution_mode: measured",
                "venue: bybit",
                "oms_lineage:",
                "  order_source: paper_oms",
                "strategy_runtime:",
                "  implemented: true",
                f"  thesis_run_id: {run_id}",
                "  auto_submit: true",
                "  include_pending_theses: false",
                "  supported_event_families:",
                "    - VOL_SHOCK",
                "  allowed_actions:",
                "    - probe",
                "  max_notional_fraction: 0.03",
                "  max_spread_bps: 5.0",
                "  min_depth_usd: 50000.0",
                "  min_tob_coverage: 0.9",
                "  execution_model:",
                "    cost_model: execution_simulator_v2",
                "    base_slippage_bps: 1.5",
                "  live_quality_gate:",
                "    max_slippage_drift_bps: 4.0",
                "    disable_slippage_drift_bps: 12.0",
                "    kill_on_disable: true",
                "  portfolio_candidate_batch_size: 4",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    payload = inspect_deployment(run_id, data_root=data_root, config_path=config_path)

    assert payload["kind"] == "deploy_inspect"
    assert payload["thesis_batch"]["thesis_count"] == 1
    assert payload["thesis_batch"]["hash_short"]
    assert payload["symbols"] == ["BTCUSDT"]
    assert payload["event_families"] == ["VOLATILITY_TRANSITION", "VOL_SHOCK"]
    assert payload["runtime"]["runtime_mode"] == "trading"
    assert payload["runtime"]["venue"] == "bybit"
    assert payload["strategy_runtime"]["implemented"] is True
    assert payload["strategy_runtime"]["execution_model"]["cost_model"] == "execution_simulator_v2"
    assert payload["strategy_runtime"]["execution_model_family"] == "execution_simulator_v2"
    assert payload["strategy_runtime"]["live_quality_gate"]["kill_on_disable"] is True
    assert payload["strategy_runtime"]["live_quality_kill_on_disable"] is True
    assert payload["strategy_runtime"]["portfolio_candidate_batch_size"] == 4
    assert payload["strategy_runtime"]["synthetic_microstructure_defaults_present"] == []
    assert payload["risk_caps"]["configured_count"] == 1
    assert payload["approval_state"]["approved_required_count"] == 1


def test_deployment_status_returns_heartbeat_feed_positions_and_kill_switch(
    tmp_path: Path,
) -> None:
    run_id = "unit_run"
    data_root = tmp_path / "data"
    _write_thesis_bundle(data_root, run_id)
    now = datetime.now(UTC).isoformat()
    snapshot_path = tmp_path / "live_state.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "account": {
                    "wallet_balance": 1000.0,
                    "margin_balance": 1000.0,
                    "available_balance": 900.0,
                    "total_unrealized_pnl": 5.0,
                    "exchange_status": "NORMAL",
                    "update_time": now,
                    "positions": [
                        {
                            "symbol": "BTCUSDT",
                            "side": "LONG",
                            "quantity": 0.1,
                            "entry_price": 60000.0,
                            "mark_price": 60100.0,
                            "unrealized_pnl": 10.0,
                        }
                    ],
                },
                "kill_switch": {
                    "is_active": False,
                    "reason": None,
                    "message": "",
                    "recovery_streak": 0,
                },
            }
        ),
        encoding="utf-8",
    )
    metrics_path = tmp_path / "metrics.json"
    metrics_path.write_text(
        json.dumps(
            {
                "generated_at": now,
                "latest_market_state_by_symbol": {
                    "BTCUSDT": {
                        "ticker_timestamp": now,
                        "funding_timestamp": now,
                        "open_interest_timestamp": now,
                    }
                },
                "active_orders": [{"client_order_id": "order-1", "symbol": "BTCUSDT"}],
            }
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "live_unit.yaml"
    config_path.write_text(
        "\n".join(
            [
                "runtime_mode: trading",
                "venue: binance",
                f"live_state_snapshot_path: {snapshot_path}",
                f"runtime_metrics_snapshot_path: {metrics_path}",
                "strategy_runtime:",
                "  implemented: true",
                f"  thesis_run_id: {run_id}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    payload = deployment_status(run_id, data_root=data_root, config_path=config_path)

    assert payload["kind"] == "deploy_status"
    assert payload["engine_heartbeat"]["state"] == "fresh"
    assert payload["feed_freshness"]["BTCUSDT"]["state"] == "fresh"
    assert payload["active_thesis_batch"]["hash_short"]
    assert payload["active_position_count"] == 1
    assert payload["active_order_count"] == 1
    assert payload["kill_switch"]["is_active"] is False
