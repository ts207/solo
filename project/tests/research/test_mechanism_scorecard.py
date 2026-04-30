from __future__ import annotations

import json
from pathlib import Path

from project.research import mechanism_scorecard


def _write_results(root: Path) -> None:
    results_dir = root / "data" / "reports" / "results"
    results_dir.mkdir(parents=True)
    rows = [
        {
            "run_id": "run_mech",
            "candidate_id": "hyp_abc",
            "methodology_epoch": "mechanism_backed",
            "mechanism_id": "forced_flow_reversal",
            "mechanism_version": "v1",
            "mechanism_preflight_status": "pass",
            "mechanism_classification": "mechanism_backed",
            "event_id": "PRICE_DOWN_OI_DOWN",
            "context": "VOL_REGIME=HIGH",
            "direction": "long",
            "horizon_bars": 24,
            "symbol": "BTCUSDT",
            "evidence_class": "validate_ready",
            "decision": "park",
            "decision_reason": "year_pnl_concentration",
            "year_split_reason": "year_pnl_concentration",
            "specificity_classification": "insufficient_trace_data",
            "t_stat_net": 2.3456,
        },
        {
            "run_id": "run_mech",
            "candidate_id": "BTCUSDT::cand_7d1d9583bddcf985",
            "methodology_epoch": "mechanism_backed",
            "mechanism_id": "forced_flow_reversal",
            "mechanism_version": "v1",
            "mechanism_preflight_status": "pass",
            "mechanism_classification": "mechanism_backed",
            "event_id": "PRICE_DOWN_OI_DOWN",
            "context": "VOL_REGIME=HIGH",
            "direction": "long",
            "horizon_bars": 24,
            "symbol": "BTCUSDT",
            "evidence_class": "validate_ready",
            "decision": "park",
            "decision_reason": "year_pnl_concentration",
            "year_split_reason": "year_pnl_concentration",
            "specificity_classification": "insufficient_trace_data",
            "t_stat_net": 2.3456,
        },
        {
            "run_id": "old_run",
            "candidate_id": "old_cand",
            "methodology_epoch": "pre_mechanism",
            "mechanism_id": "",
            "event_id": "PRICE_DOWN_OI_DOWN",
            "decision": "park",
        },
    ]
    (results_dir / "results_index.json").write_text(
        json.dumps({"schema_version": "results_index_v1", "rows": rows}),
        encoding="utf-8",
    )


def _write_year_split(root: Path) -> None:
    path = (
        root
        / "data"
        / "reports"
        / "regime"
        / "run_mech"
        / "BTCUSDT_cand_7d1d9583bddcf985_year_split.json"
    )
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "concentration": {
                    "max_pnl_year": 2022,
                    "max_pnl_share": 0.6393,
                }
            }
        ),
        encoding="utf-8",
    )


def test_mechanism_scorecard_summarizes_parked_forced_flow_candidate(tmp_path: Path):
    _write_results(tmp_path)
    _write_year_split(tmp_path)

    df = mechanism_scorecard.build_mechanism_scorecard(tmp_path)
    row = df[df["mechanism_id"] == "forced_flow_reversal"].iloc[0]

    assert row["status"] == "active"
    assert row["candidate_count"] == 1
    assert row["parked_count"] == 1
    assert row["killed_count"] == 0
    assert row["best_candidate_id"] == "BTCUSDT::cand_7d1d9583bddcf985"
    assert row["best_candidate_decision"] == "park"
    assert row["main_failure_reason"] == "year_pnl_concentration_2022"
    assert row["data_quality_blocker"] == "specificity_controls_missing"
    assert "build control traces" in row["next_research_action"]


def test_mechanism_scorecard_writers_emit_json_parquet_and_markdown(tmp_path: Path):
    _write_results(tmp_path)
    _write_year_split(tmp_path)

    df = mechanism_scorecard.build_mechanism_scorecard(tmp_path)
    json_path = tmp_path / "scorecard.json"
    parquet_path = tmp_path / "scorecard.parquet"
    md_path = tmp_path / "scorecard.md"

    mechanism_scorecard.write_scorecard_json(df, json_path)
    mechanism_scorecard.write_scorecard_parquet(df, parquet_path)
    mechanism_scorecard.write_scorecard_markdown(df, md_path)

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "mechanism_scorecard_v1"
    assert any(
        row["mechanism_id"] == "forced_flow_reversal" for row in payload["mechanisms"]
    )
    assert parquet_path.exists()
    assert "forced_flow_reversal" in md_path.read_text(encoding="utf-8")
