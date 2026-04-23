from __future__ import annotations

import json

import pandas as pd

import project.research.generate_negative_control_summary as stage


def test_generate_negative_control_summary_aggregates_control_rates(monkeypatch, tmp_path):
    monkeypatch.setattr(stage, "get_data_root", lambda: tmp_path)

    edge_dir = tmp_path / "reports" / "edge_candidates" / "r1"
    edge_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"candidate_id": "c1", "event_type": "VOL_SHOCK", "control_pass_rate": 0.02},
            {"candidate_id": "c2", "event_type": "VOL_SHOCK", "control_pass_rate": 0.00},
            {"candidate_id": "c3", "event_type": "OI_FLUSH", "control_pass_rate": 0.10},
        ]
    ).to_parquet(edge_dir / "edge_candidates_normalized.parquet", index=False)

    rc = stage.main(["--run_id", "r1", "--symbols", "BTCUSDT"])
    assert rc == 0

    payload = json.loads(
        (
            tmp_path
            / "reports"
            / "negative_control"
            / "r1"
            / "negative_control_summary.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["global"]["has_control_evidence"] is True
    assert payload["global"]["pass_rate_after_bh"] == 0.04
    assert payload["by_event"]["VOL_SHOCK"]["pass_rate_after_bh"] == 0.01
    assert payload["by_event"]["OI_FLUSH"]["pass_rate_after_bh"] == 0.10


def test_generate_negative_control_summary_records_missing_evidence(monkeypatch, tmp_path):
    monkeypatch.setattr(stage, "get_data_root", lambda: tmp_path)

    edge_dir = tmp_path / "reports" / "edge_candidates" / "r1"
    edge_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"candidate_id": "c1", "event_type": "VOL_SHOCK"},
            {"candidate_id": "c2", "event_type": "OI_FLUSH"},
        ]
    ).to_parquet(edge_dir / "edge_candidates_normalized.parquet", index=False)

    rc = stage.main(["--run_id", "r1", "--symbols", "BTCUSDT"])
    assert rc == 0

    payload = json.loads(
        (
            tmp_path
            / "reports"
            / "negative_control"
            / "r1"
            / "negative_control_summary.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["global"]["has_control_evidence"] is False
    assert "pass_rate_after_bh" not in payload["global"]
    assert payload["by_event"]["VOL_SHOCK"]["has_control_evidence"] is False
