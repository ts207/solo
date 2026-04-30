from __future__ import annotations

import json

from project.scripts.update_research_decision_snapshot import main


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_update_research_decision_snapshot_writes_outputs(tmp_path):
    _write_json(
        tmp_path / "reports" / "regime_baselines" / "regime_scorecard.json",
        {"rows": [{"decision": "reject_directional"} for _ in range(9)]},
    )
    _write_json(
        tmp_path / "reports" / "data_quality_audit" / "run" / "mechanism_data_quality.json",
        {
            "mechanisms": [
                {
                    "mechanism_id": "funding_squeeze",
                    "data_quality_decision": "paper_blocked",
                    "blocked_fields": [],
                    "proxy_fields": ["basis_zscore"],
                }
            ]
        },
    )
    _write_json(
        tmp_path / "reports" / "regime_event_inventory" / "mechanism_inventory.json",
        {
            "rows": [
                {
                    "id": "funding_squeeze",
                    "enabled": True,
                    "classification": "eligible_for_event_lift_test",
                }
            ]
        },
    )
    out_dir = tmp_path / "reports" / "research_decision_snapshot"

    rc = main(
        [
            "--data-root",
            str(tmp_path),
            "--output-dir",
            str(out_dir),
        ]
    )

    assert rc == 0
    json_path = out_dir / "research_decision_snapshot.json"
    md_path = out_dir / "research_decision_snapshot.md"
    assert json_path.exists()
    assert md_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "research_decision_snapshot_v1"
    assert payload["decision"] == "park"
    assert payload["proposal_allowed"] is False
    assert payload["paper_allowed"] is False
