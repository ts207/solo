from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from project.live.deploy_status import SYNTHETIC_MICROSTRUCTURE_DEFAULT_KEYS
from project.scripts import run_live_engine
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


def test_trading_configs_do_not_carry_synthetic_microstructure_defaults() -> None:
    offenders: dict[str, list[str]] = {}
    for path in sorted(Path("project/configs").glob("live*.yaml")):
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(payload, dict):
            continue
        if str(payload.get("runtime_mode", "")).strip().lower() != "trading":
            continue
        strategy_runtime = payload.get("strategy_runtime", {})
        if not isinstance(strategy_runtime, dict):
            continue
        present = sorted(
            key for key in SYNTHETIC_MICROSTRUCTURE_DEFAULT_KEYS if key in strategy_runtime
        )
        if present:
            offenders[str(path)] = present

    assert offenders == {}


def test_live_engine_rejects_trading_config_with_synthetic_microstructure_defaults(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "live_trading.yaml"
    config_path.write_text(
        "\n".join(
            [
                "runtime_mode: trading",
                "strategy_runtime:",
                "  implemented: true",
                "  thesis_run_id: run_1",
                "  default_depth_usd: 75000.0",
                "  default_tob_coverage: 0.97",
                "  default_expected_cost_bps: 3.0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(run_live_engine.LiveRuntimeConfigError) as exc:
        run_live_engine.load_live_engine_config(config_path)

    assert "synthetic microstructure defaults" in str(exc.value)


def test_simulation_config_may_carry_explicit_synthetic_microstructure_defaults(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "live_simulation.yaml"
    config_path.write_text(
        "\n".join(
            [
                "runtime_mode: simulation",
                "strategy_runtime:",
                "  implemented: false",
                "  default_depth_usd: 75000.0",
                "  default_tob_coverage: 0.97",
                "  default_expected_cost_bps: 3.0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    payload = run_live_engine.load_live_engine_config(config_path)

    assert payload["runtime_mode"] == "simulation"
    assert payload["strategy_runtime"]["default_depth_usd"] == 75000.0
