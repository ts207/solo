from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from project.scripts import watch_live_runtime_alerts


def test_load_runtime_alert_settings_reads_defaults_from_live_config(tmp_path: Path) -> None:
    config_path = tmp_path / "live.yaml"
    config_path.write_text(
        "\n".join(
            [
                "runtime_metrics_snapshot_path: artifacts/runtime_metrics.json",
                "runtime_alerts:",
                "  poll_interval_seconds: 7",
                "  snapshot_max_age_seconds: 90",
                "  decision_drought_seconds: 1200",
                "  alert_log_path: artifacts/runtime_alerts.jsonl",
                "  trade_small_probe_ratio_baseline: 0.8",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    settings = watch_live_runtime_alerts.load_runtime_alert_settings(config_path)

    assert settings["metrics_path"] == "artifacts/runtime_metrics.json"
    assert settings["poll_interval_seconds"] == 7.0
    assert settings["snapshot_max_age_seconds"] == 90.0
    assert settings["decision_drought_seconds"] == 1200.0
    assert settings["alert_log_path"] == "artifacts/runtime_alerts.jsonl"
    assert settings["trade_small_probe_ratio_baseline"] == 0.8


def test_evaluate_runtime_alerts_detects_operational_problems_and_regime_shift() -> None:
    now = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
    previous_snapshot = {
        "generated_at": (now - timedelta(seconds=20)).isoformat(),
        "latest_market_state_by_symbol": {
            "BTCUSDT": {
                "funding_rate": 0.0001,
                "open_interest_delta_fraction": 0.0,
            }
        },
    }
    snapshot = {
        "generated_at": (now - timedelta(seconds=400)).isoformat(),
        "strategy_runtime_enabled": True,
        "health": {
            "freshness_status": "stale",
            "stale_count": 1,
            "stale_streams": [{"stream": "BTCUSDT:kline_5m", "last_seen_sec_ago": 400.0}],
            "max_last_seen_sec_ago": 400.0,
        },
        "kill_switch": {
            "is_active": True,
            "reason": "STALE_DATA",
            "triggered_at": (now - timedelta(seconds=30)).isoformat(),
            "message": "stale feed",
        },
        "decision_counts": {
            "by_action": {"probe": 2, "trade_small": 8},
        },
        "recent_decisions": [
            {"timestamp": (now - timedelta(seconds=5000)).isoformat(), "symbol": "BTCUSDT"},
        ],
        "latest_market_state_by_symbol": {
            "BTCUSDT": {
                "funding_rate": 0.0007,
                "open_interest_delta_fraction": -0.05,
            }
        },
    }

    alerts = watch_live_runtime_alerts.evaluate_runtime_alerts(
        snapshot,
        previous_snapshot=previous_snapshot,
        now=now,
        snapshot_max_age_seconds=180.0,
        decision_drought_seconds=3600.0,
        ratio_min_total=4,
        trade_small_probe_ratio_baseline=1.0,
        trade_small_probe_ratio_tolerance_fraction=0.5,
    )
    keys = {alert.key for alert in alerts}

    assert "snapshot_stale" in keys
    assert "stale_feeds" in keys
    assert "kill_switch_active" in keys
    assert "decision_drought" in keys
    assert "trade_small_probe_ratio_drift" in keys
    assert "regime_shift::BTCUSDT" in keys


def test_main_once_emits_alerts_and_nonzero_on_fail(tmp_path: Path, capsys) -> None:
    metrics_path = tmp_path / "runtime_metrics.json"
    alert_log_path = tmp_path / "runtime_alerts.jsonl"
    snapshot = {
        "generated_at": datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc).isoformat(),
        "strategy_runtime_enabled": True,
        "health": {"freshness_status": "healthy", "stale_count": 0, "stale_streams": []},
        "kill_switch": {"is_active": True, "reason": "MANUAL"},
        "decision_counts": {"by_action": {}},
        "recent_decisions": [],
        "latest_market_state_by_symbol": {},
    }
    metrics_path.write_text(json.dumps(snapshot), encoding="utf-8")
    config_path = tmp_path / "live.yaml"
    config_path.write_text(
        "\n".join(
            [
                f"runtime_metrics_snapshot_path: {metrics_path}",
                "runtime_alerts:",
                f"  alert_log_path: {alert_log_path}",
                "  snapshot_max_age_seconds: 999999",
                "  decision_drought_seconds: 999999",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    code = watch_live_runtime_alerts.main(
        [
            "--config",
            str(config_path),
            "--once",
            "--fail-on-alert",
        ]
    )

    assert code == 1
    stdout = capsys.readouterr().out.strip().splitlines()
    assert stdout
    payload = json.loads(stdout[0])
    assert payload["key"] in ("kill_switch_active", "snapshot_stale")
    log_lines = alert_log_path.read_text(encoding="utf-8").strip().splitlines()
    assert log_lines
    assert json.loads(log_lines[0])["key"] in ("kill_switch_active", "snapshot_stale")
