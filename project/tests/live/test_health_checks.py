"""
E6-T4: Live health checks and stale-feed supervision.

Verify that DataHealthMonitor correctly identifies stale data streams.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from project.live.health_checks import (
    DataHealthMonitor,
    build_runtime_certification_manifest,
    check_kill_switch_triggers,
    evaluate_market_state_components,
    evaluate_pretrade_microstructure_gate,
)


def test_stale_feed_detection():
    current_time = datetime(2026, 1, 1, tzinfo=UTC)
    monitor = DataHealthMonitor(stale_threshold_sec=0.1, now_fn=lambda: current_time)

    # 1. Initially healthy
    monitor.on_event("BTCUSDT", "kline_1m")
    health = monitor.check_health()
    assert health["is_healthy"]

    # 2. Becomes stale
    current_time += timedelta(milliseconds=150)
    health = monitor.check_health()
    assert not health["is_healthy"]
    assert health["freshness_status"] == "stale"
    assert health["stale_count"] == 1
    assert health["stale_streams"][0]["stream"] == "BTCUSDT:kline_1m"

    # 3. Recovered
    monitor.on_event("BTCUSDT", "kline_1m")
    health = monitor.check_health()
    assert health["is_healthy"]


def test_stale_data_with_frozen_time():
    monitor = DataHealthMonitor(stale_threshold_sec=60.0)

    # Mock some data seen 10 mins ago
    past_time = datetime.now(UTC) - timedelta(minutes=10)
    monitor.last_update_times["ETHUSDT:ticker"] = past_time

    health = monitor.check_health()
    assert not health["is_healthy"]
    assert health["stale_count"] == 1
    assert health["stale_streams"][0]["last_seen_sec_ago"] >= 600.0


def test_registered_but_never_seen_stream_becomes_disconnected() -> None:
    current_time = datetime(2026, 1, 1, tzinfo=UTC)
    monitor = DataHealthMonitor(stale_threshold_sec=1.0, now_fn=lambda: current_time)
    monitor.register_stream("BTCUSDT", "ticker")

    current_time += timedelta(seconds=2)
    health = monitor.check_health()

    assert health["is_healthy"] is False
    assert health["stale_count"] == 1
    assert health["stale_streams"][0]["stream"] == "BTCUSDT:ticker"
    assert monitor.status["BTCUSDT:ticker"] == "DISCONNECTED"


def test_build_runtime_certification_manifest() -> None:
    manifest = build_runtime_certification_manifest(
        postflight_audit={
            "status": "pass",
            "watermark_violation_count": 0,
            "watermark_violations_by_type": {},
            "replay_digest": "blake2b_256:test",
        },
        health_report={
            "is_healthy": True,
            "stale_count": 0,
            "stale_streams": [],
        },
        kill_switch_status={"is_active": False, "reason": None, "message": ""},
        oms_lineage={"order_source": "binance_oms", "session_id": "sess-1"},
        replay_status={"status": "pass", "replay_digest": "blake2b_256:test"},
        live_state_status={"snapshot_path": "/tmp/live_state.json", "auto_persist_enabled": True},
    )

    assert manifest["status"] == "pass"
    assert manifest["certification_checks"]["postflight_passed"]
    assert manifest["certification_checks"]["feeds_healthy"]
    assert manifest["certification_checks"]["kill_switch_inactive"]
    assert manifest["certification_checks"]["oms_lineage_present"]
    assert manifest["certification_checks"]["live_state_snapshot_present"]
    assert manifest["certification_checks"]["replay_digest_present"]
    assert manifest["live_state"]["snapshot_path"] == "/tmp/live_state.json"


def test_pretrade_microstructure_gate_blocks_spread_depth_and_coverage() -> None:
    gate = evaluate_pretrade_microstructure_gate(
        spread_bps=12.0,
        depth_usd=5_000.0,
        tob_coverage=0.40,
        max_spread_bps=5.0,
        min_depth_usd=25_000.0,
        min_tob_coverage=0.80,
    )

    assert gate["is_tradable"] is False
    assert gate["spread_ok"] is False
    assert gate["depth_ok"] is False
    assert gate["coverage_ok"] is False
    assert gate["reasons"] == ["spread_blowout", "depth_collapse", "cost_model_invalid"]


def test_pretrade_microstructure_gate_passes_when_inputs_are_safe() -> None:
    gate = evaluate_pretrade_microstructure_gate(
        spread_bps=2.0,
        depth_usd=100_000.0,
        tob_coverage=0.95,
        max_spread_bps=5.0,
        min_depth_usd=25_000.0,
        min_tob_coverage=0.80,
    )

    assert gate["is_tradable"] is True
    assert gate["reasons"] == []


def test_market_state_component_health_reports_stale_components() -> None:
    report = evaluate_market_state_components(
        {
            "ticker_fresh": False,
            "ticker_age_seconds": 45.0,
            "funding_rate_source": "runtime_market_features",
            "funding_fresh": False,
            "funding_age_seconds": 120.0,
            "open_interest_source": "runtime_market_features",
            "open_interest_fresh": True,
        },
        max_ticker_stale_seconds=30.0,
        runtime_feature_stale_after_seconds=60.0,
    )

    assert report["is_healthy"] is False
    assert report["freshness_status"] == "degraded"
    assert [item["component"] for item in report["stale_components"]] == [
        "ticker",
        "funding",
    ]


def test_kill_switch_does_not_fire_when_live_expectancy_is_less_negative_than_research() -> None:
    result = check_kill_switch_triggers(
        live_performance_expectancy=-5.0,
        research_mean_expectancy=-10.0,
        max_drawdown_limit=100.0,
        current_drawdown=20.0,
        recent_invalidation_streak=0,
    )

    assert result["should_kill"] is False
    assert "low_expectancy" not in result["reasons"]
