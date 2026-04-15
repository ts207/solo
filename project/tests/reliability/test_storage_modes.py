from __future__ import annotations

from pathlib import Path

from project.reliability.cli_smoke import run_smoke_cli


def test_smoke_runner_supports_csv_fallback(tmp_path: Path, monkeypatch):
    summary = run_smoke_cli("engine", root=tmp_path, storage_mode="csv-fallback")
    assert summary["environment"]["storage_mode"] == "csv-fallback"
