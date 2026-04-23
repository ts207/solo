from __future__ import annotations

from pathlib import Path


def test_environment_specific_services_pin_distinct_configs_and_snapshot_paths() -> None:
    paper = Path("deploy/systemd/edge-live-engine-paper.service").read_text(encoding="utf-8")
    production = Path("deploy/systemd/edge-live-engine-production.service").read_text(
        encoding="utf-8"
    )

    assert "EnvironmentFile=/etc/edge/edge-live-engine-paper.env" in paper
    assert "EnvironmentFile=/etc/edge/edge-live-engine-production.env" in production
    assert (
        "ExecStart=/opt/edge/.venv/bin/edge-live-engine --config ${EDGE_LIVE_CONFIG} --snapshot_path ${EDGE_LIVE_SNAPSHOT_PATH}"
        in paper
    )
    assert (
        "ExecStart=/opt/edge/.venv/bin/edge-live-engine --config ${EDGE_LIVE_CONFIG} --snapshot_path ${EDGE_LIVE_SNAPSHOT_PATH}"
        in production
    )


def test_environment_example_files_pin_distinct_credentials_and_runtime_paths() -> None:
    paper = Path("deploy/env/edge-live-engine-paper.env.example").read_text(encoding="utf-8")
    production = Path("deploy/env/edge-live-engine-production.env.example").read_text(
        encoding="utf-8"
    )

    assert "EDGE_ENVIRONMENT=paper" in paper
    assert "EDGE_BINANCE_PAPER_API_KEY=" in paper
    assert "EDGE_BINANCE_PAPER_API_SECRET=" in paper
    assert "EDGE_BINANCE_PAPER_API_BASE=https://testnet.binancefuture.com" in paper
    assert "EDGE_LIVE_CONFIG=/opt/edge/project/configs/live_paper.yaml" in paper
    assert "EDGE_LIVE_SNAPSHOT_PATH=/var/lib/edge/live_state_paper.json" in paper

    assert "EDGE_ENVIRONMENT=production" in production
    assert "EDGE_BINANCE_API_KEY=" in production
    assert "EDGE_BINANCE_API_SECRET=" in production
    assert "EDGE_BINANCE_API_BASE=https://fapi.binance.com" in production
    assert "EDGE_LIVE_CONFIG=/opt/edge/project/configs/live_production.yaml" in production
    assert "EDGE_LIVE_SNAPSHOT_PATH=/var/lib/edge/live_state_production.json" in production
