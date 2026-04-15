from __future__ import annotations

from pathlib import Path

from project.scripts.run_live_engine import load_live_engine_config


def test_golden_certification_config_declares_live_engine_persistence_contract() -> None:
    config = load_live_engine_config(Path("project/configs/golden_certification.yaml"))

    assert config["runtime_mode"] == "monitor_only"
    assert config["live_state_snapshot_path"] == "reliability/live_state.json"
    assert int(config["microstructure_recovery_streak"]) >= 1
    assert float(config["account_sync_interval_seconds"]) >= 1.0
    assert int(config["account_sync_failure_threshold"]) >= 1
    assert int(config["execution_degradation_min_samples"]) >= 1
    assert float(config["execution_degradation_throttle_scale"]) > 0.0
    assert float(config["execution_degradation_throttle_scale"]) <= 1.0

    streams = list(config.get("freshness_streams", []))
    assert streams
    assert all(str(item.get("symbol", "")).strip() for item in streams)
