from __future__ import annotations

import json
from pathlib import Path

from project.scripts import extract_control_traces
from project.tests.research.test_control_traces import _write_bars, _write_base_trace


def test_script_writes_json_and_parquet(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    run_id = "run"
    _write_base_trace(data_root, run_id, "cand")
    _write_bars(data_root, run_id)

    exit_code = extract_control_traces.main(
        [
            "--run-id",
            run_id,
            "--candidate-id",
            "cand",
            "--data-root",
            str(data_root),
        ]
    )

    base = data_root / "reports" / "candidate_traces" / run_id
    payload = json.loads((base / "cand_control_traces.json").read_text(encoding="utf-8"))
    assert exit_code == 0
    assert (base / "cand_control_traces.parquet").exists()
    assert payload["status"] == "pass"
    assert payload["rows_by_control"]["base"] == 2
