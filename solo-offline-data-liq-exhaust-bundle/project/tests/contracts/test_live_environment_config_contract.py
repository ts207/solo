from __future__ import annotations

from pathlib import Path

from project.scripts.run_live_engine import load_live_engine_config


def test_live_environment_configs_use_distinct_order_sources_and_snapshot_paths() -> None:
    paper = load_live_engine_config(Path("project/configs/live_paper.yaml"))
    production = load_live_engine_config(Path("project/configs/live_production.yaml"))
    paper_trading = load_live_engine_config(Path("project/configs/live_paper_btc_thesis_v1.yaml"))

    assert paper["oms_lineage"]["order_source"] == "paper_oms"
    assert production["oms_lineage"]["order_source"] == "production_oms"
    assert paper["runtime_mode"] == "monitor_only"
    assert production["runtime_mode"] == "monitor_only"
    assert paper_trading["runtime_mode"] == "trading"
    assert paper_trading["strategy_runtime"]["implemented"] is True
    assert paper_trading["strategy_runtime"]["auto_submit"] is True
    assert paper["live_state_snapshot_path"] == "artifacts/live_state_paper.json"
    assert production["live_state_snapshot_path"] == "artifacts/live_state_production.json"
    assert paper["live_state_snapshot_path"] != production["live_state_snapshot_path"]
    assert int(paper["microstructure_recovery_streak"]) >= 1
    assert int(production["microstructure_recovery_streak"]) >= 1
    assert float(paper["account_sync_interval_seconds"]) > float(
        production["account_sync_interval_seconds"]
    )
    assert int(paper["account_sync_failure_threshold"]) > int(
        production["account_sync_failure_threshold"]
    )
    assert int(paper["execution_degradation_min_samples"]) > int(
        production["execution_degradation_min_samples"]
    )
    assert float(paper["execution_degradation_block_edge_bps"]) < float(
        production["execution_degradation_block_edge_bps"]
    )
    assert float(paper["execution_degradation_throttle_scale"]) > float(
        production["execution_degradation_throttle_scale"]
    )


def test_runtime_facing_configs_use_explicit_thesis_lineage() -> None:
    paper = load_live_engine_config(Path("project/configs/live_paper.yaml"))
    production = load_live_engine_config(Path("project/configs/live_production.yaml"))
    paper_trading = load_live_engine_config(Path("project/configs/live_paper_btc_thesis_v1.yaml"))

    for config in (paper, production):
        strategy_runtime = dict(config.get("strategy_runtime", {}))
        assert "load_latest_theses" not in strategy_runtime
        assert not str(strategy_runtime.get("thesis_run_id", "")).strip()
        assert not str(strategy_runtime.get("thesis_path", "")).strip()

    strategy_runtime = dict(paper_trading.get("strategy_runtime", {}))
    thesis_run_id = str(strategy_runtime.get("thesis_run_id", "")).strip()
    thesis_path = str(strategy_runtime.get("thesis_path", "")).strip()
    assert bool(thesis_run_id) ^ bool(thesis_path)
    assert "load_latest_theses" not in strategy_runtime
    assert "seed" not in thesis_run_id.lower()
    assert "bootstrap" not in thesis_run_id.lower()
    assert "latest" not in thesis_run_id.lower()
