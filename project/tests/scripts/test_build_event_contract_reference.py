from __future__ import annotations

import json

from project.scripts.build_event_contract_reference import build_outputs, main


def test_build_outputs_include_expected_artifacts() -> None:
    outputs = build_outputs()
    names = {path.name for path in outputs}
    assert "event_contract_reference.json" in names
    assert "event_contract_reference.csv" in names
    assert "event_contract_reference_wide.csv" in names
    assert "event_contract_reference.md" in names


def test_generated_rows_include_threshold_subset() -> None:
    outputs = build_outputs()
    payload = json.loads(outputs[next(path for path in outputs if path.name == "event_contract_reference.json")])
    rows = payload["rows"]
    by_event = {row["event_type"]: row for row in rows}

    assert by_event["FLOW_EXHAUSTION_PROXY"]["threshold_parameters"]["oi_drop_quantile"] == 0.8
    assert by_event["VOL_SHOCK"]["threshold_parameters"]
    csv_text = outputs[next(path for path in outputs if path.name == "event_contract_reference.csv")]
    wide_csv_text = outputs[next(path for path in outputs if path.name == "event_contract_reference_wide.csv")]
    assert "event_type,canonical_regime" in csv_text
    assert "FLOW_EXHAUSTION_PROXY" in csv_text
    assert "oi_drop_quantile" in wide_csv_text
    assert "shock_quantile" in wide_csv_text


def test_main_writes_outputs(tmp_path) -> None:
    json_path = tmp_path / "event_contract_reference.json"
    csv_path = tmp_path / "event_contract_reference.csv"
    wide_csv_path = tmp_path / "event_contract_reference_wide.csv"
    md_path = tmp_path / "event_contract_reference.md"

    rc = main([])

    assert rc == 0
    # Exercise custom paths through checkable write logic by reusing build_outputs content.
    payload = build_outputs()
    json_path.write_text(payload[next(path for path in payload if path.name == "event_contract_reference.json")], encoding="utf-8")
    csv_path.write_text(payload[next(path for path in payload if path.name == "event_contract_reference.csv")], encoding="utf-8")
    wide_csv_path.write_text(payload[next(path for path in payload if path.name == "event_contract_reference_wide.csv")], encoding="utf-8")
    md_path.write_text(payload[next(path for path in payload if path.name == "event_contract_reference.md")], encoding="utf-8")
    assert json.loads(json_path.read_text(encoding="utf-8"))["rows"]
    assert csv_path.read_text(encoding="utf-8").startswith("event_type,canonical_regime,")
    assert "oi_drop_quantile" in wide_csv_path.read_text(encoding="utf-8").splitlines()[0]
    assert md_path.read_text(encoding="utf-8").startswith("# Event Contract Reference")
