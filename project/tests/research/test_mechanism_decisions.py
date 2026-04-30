from __future__ import annotations

import json
from pathlib import Path

from project.research.mechanism_decisions import (
    forced_flow_reversal_pause_decision,
    write_mechanism_decision,
)


def test_write_forced_flow_decision_artifacts(tmp_path: Path) -> None:
    decision = forced_flow_reversal_pause_decision()

    paths = write_mechanism_decision(decision, data_root=tmp_path / "data")

    json_path = Path(paths["json_path"])
    md_path = Path(paths["markdown_path"])
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    markdown = md_path.read_text(encoding="utf-8")
    assert payload["schema_version"] == "mechanism_decision_v1"
    assert payload["decision"] == "pause"
    assert payload["mechanism_id"] == "forced_flow_reversal"
    assert "PRICE_DOWN_OI_DOWN" in markdown
    assert "OI_FLUSH" in markdown

