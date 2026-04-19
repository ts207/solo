from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import project.cli as cli


def test_paper_run_and_live_run_invoke_same_canonical_runtime_entry(
    monkeypatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "live.yaml"
    config_path.write_text("runtime_mode: monitor_only\n", encoding="utf-8")
    calls: list[list[str]] = []

    def _fake_run(cmd):
        calls.append(list(cmd))
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(subprocess, "run", _fake_run)

    assert cli.main(["deploy", "paper-run", "--config", str(config_path)]) == 0
    assert cli.main(["deploy", "live-run", "--config", str(config_path)]) == 0

    assert len(calls) == 2
    assert calls[0] == calls[1]
    assert calls[0][0] == sys.executable
    assert calls[0][1].endswith("project/scripts/run_live_engine.py")
    assert calls[0][2:] == ["--config", str(config_path)]
