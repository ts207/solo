from __future__ import annotations

import json

from project.reliability import cli_smoke


def test_cli_smoke_main_writes_summary_and_returns_zero(tmp_path, capsys):
    exit_code = cli_smoke.main(["--mode", "research", "--root", str(tmp_path)])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert payload["mode"] == "research"
    assert (tmp_path / "reliability" / "smoke_summary.json").exists()
