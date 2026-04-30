from __future__ import annotations

import json

from project.scripts import write_mechanism_decision


def test_write_mechanism_decision_script(tmp_path) -> None:
    exit_code = write_mechanism_decision.main(
        [
            "--mechanism-id",
            "forced_flow_reversal",
            "--data-root",
            str(tmp_path / "data"),
        ]
    )

    path = tmp_path / "data" / "reports" / "mechanisms" / "forced_flow_reversal" / "decision.json"
    assert exit_code == 0
    assert json.loads(path.read_text(encoding="utf-8"))["decision"] == "pause"
