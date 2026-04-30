from __future__ import annotations

import json

from project.research.research_decision_snapshot import (
    ResearchDecisionSnapshotRequest,
    build_research_decision_snapshot,
)


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_inputs(tmp_path):
    _write_json(
        tmp_path / "reports" / "regime_baselines" / "regime_scorecard.json",
        {
            "rows": [
                {"decision": "reject_directional"},
                {"decision": "reject_directional"},
                {"decision": "allow_event_lift"},
            ]
        },
    )
    _write_json(
        tmp_path / "reports" / "data_quality_audit" / "latest" / "mechanism_data_quality.json",
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


def test_snapshot_parks_when_no_passing_event_lift(tmp_path):
    _write_inputs(tmp_path)

    snapshot = build_research_decision_snapshot(
        ResearchDecisionSnapshotRequest(
            data_root=tmp_path,
            mechanism_id="funding_squeeze",
            generated_at="2026-04-30T00:00:00Z",
        )
    )

    assert snapshot["mechanism_status"] == "active"
    assert snapshot["data_quality_decision"] == "paper_blocked"
    assert snapshot["data_quality_proxy_fields"] == ["basis_zscore"]
    assert snapshot["regime_decision_summary"] == {
        "allow_event_lift": 1,
        "reject_directional": 2,
    }
    assert snapshot["event_lift_passing_count"] == 0
    assert snapshot["proposal_allowed"] is False
    assert snapshot["paper_allowed"] is False
    assert snapshot["decision"] == "park"


def test_snapshot_current_no_go_shape_with_negative_regimes(tmp_path):
    _write_inputs(tmp_path)
    _write_json(
        tmp_path / "reports" / "regime_baselines" / "regime_scorecard.json",
        {"rows": [{"decision": "reject_directional"} for _ in range(9)]},
    )

    snapshot = build_research_decision_snapshot(
        ResearchDecisionSnapshotRequest(data_root=tmp_path, mechanism_id="funding_squeeze")
    )

    assert snapshot["regime_decision_summary"] == {
        "allow_event_lift": 0,
        "reject_directional": 9,
    }
    assert snapshot["decision"] == "park"
    assert snapshot["reason"] == (
        "data is research-usable but regimes are directionally negative; "
        "paper remains blocked by proxy basis_zscore"
    )
