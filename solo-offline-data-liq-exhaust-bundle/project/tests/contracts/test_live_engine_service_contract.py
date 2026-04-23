from __future__ import annotations

from pathlib import Path


def test_systemd_service_pins_live_engine_command_and_snapshot_path() -> None:
    service_path = Path("deploy/systemd/edge-live-engine.service")
    content = service_path.read_text(encoding="utf-8")

    assert "WorkingDirectory=/opt/edge" in content
    assert (
        "ExecStart=/opt/edge/.venv/bin/edge-live-engine --config "
        "/opt/edge/project/configs/golden_certification.yaml --snapshot_path "
        "/var/lib/edge/live_state.json"
    ) in content
    assert "Restart=on-failure" in content
