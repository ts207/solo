from __future__ import annotations

from pathlib import Path

from project.reliability.cli_smoke import run_smoke_cli


def test_engine_smoke(tmp_path: Path):
    summary = run_smoke_cli("engine", root=tmp_path, storage_mode="auto")
    assert "engine" in summary
    assert summary["engine"]["trace_count"] >= 1
