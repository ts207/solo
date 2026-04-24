from __future__ import annotations

import json

import pandas as pd

import project.research.analyze_conditional_expectancy as expectancy


def test_analyze_conditional_expectancy_writes_payload_from_edge_registry(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    monkeypatch.setattr(expectancy, "get_data_root", lambda: data_root)

    run_id = "r_expectancy"
    registry_dir = data_root / "runs" / run_id / "research"
    registry_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "edge_id": "e1",
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "template_id": "continuation",
                "direction_rule": "trend_following",
                "promotion_decision": "promoted",
                "times_tested": 4,
                "times_promoted": 2,
                "median_effect": 0.12,
                "stability_median": 0.70,
                "first_seen_run": "r0",
                "last_seen_run": run_id,
            },
            {
                "edge_id": "e2",
                "candidate_id": "cand_2",
                "event_type": "LIQUIDITY_VACUUM",
                "template_id": "mean_reversion",
                "direction_rule": "contrarian",
                "promotion_decision": "rejected",
                "times_tested": 2,
                "times_promoted": 0,
                "median_effect": 0.03,
                "stability_median": 0.40,
                "first_seen_run": "r0",
                "last_seen_run": run_id,
            },
        ]
    ).to_parquet(registry_dir / "edge_registry.parquet", index=False)

    rc = expectancy.main(["--run_id", run_id, "--symbols", "BTCUSDT"])

    assert rc == 0
    payload = json.loads(
        (data_root / "reports" / "expectancy" / run_id / "conditional_expectancy.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["expectancy_exists"] is True
    assert payload["registry_exists"] is True
    assert payload["skip_reason"] == ""
    assert payload["edge_count"] == 2
    assert payload["summary"]["promoted_edges"] == 1
    assert payload["expectancy_evidence"][0]["edge_id"] == "e1"


def test_analyze_conditional_expectancy_handles_missing_registry(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    monkeypatch.setattr(expectancy, "get_data_root", lambda: data_root)

    run_id = "r_missing"
    rc = expectancy.main(["--run_id", run_id, "--symbols", "BTCUSDT"])

    assert rc == 0
    payload = json.loads(
        (data_root / "reports" / "expectancy" / run_id / "conditional_expectancy.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["expectancy_exists"] is False
    assert payload["registry_exists"] is False
    assert payload["skip_reason"] == "missing_edge_registry"
    assert payload["edge_count"] == 0
    assert payload["expectancy_evidence"] == []
