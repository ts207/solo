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
            "decision_reason": "context_proxy_and_year_pnl_concentration_2022",
            "year_split_reason": "year_pnl_concentration",
            "year_split_classification": "year_conditional",
            "specificity_classification": "context_proxy",
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
            "decision_reason": "context_proxy_and_year_pnl_concentration_2022",
            "year_split_reason": "year_pnl_concentration",
            "year_split_classification": "year_conditional",
            "specificity_classification": "context_proxy",
            "t_stat_net": 2.3456,
        },
        {
            "run_id": "run_oi_flush",
            "candidate_id": "hyp_oi_flush",
            "methodology_epoch": "mechanism_backed",
            "mechanism_id": "forced_flow_reversal",
            "mechanism_version": "v1",
            "mechanism_preflight_status": "pass",
            "mechanism_classification": "mechanism_backed",
            "event_id": "OI_FLUSH",
            "context": "VOL_REGIME=HIGH",
            "direction": "long",
            "horizon_bars": 24,
            "symbol": "BTCUSDT",
            "evidence_class": "killed_candidate",
            "decision": "kill",
            "decision_reason": "governed_reproduction_negative_t_stat",
            "governed_reproduction_status": "fail",
            "governed_reproduction_decision": "kill",
            "governed_reproduction_reason": "governed_reproduction_negative_t_stat",
            "t_stat_net": -2.0964,
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


def _write_autopsies(root: Path) -> None:
    for run_id, candidate_id in (
        ("run_mech", "BTCUSDT::cand_7d1d9583bddcf985"),
        ("run_oi_flush", "hyp_oi_flush"),
    ):
        safe = candidate_id.replace("::", "_")
        path = root / "data" / "reports" / "autopsy" / run_id / f"{safe}_autopsy.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"candidate_id": candidate_id}), encoding="utf-8")


def test_mechanism_scorecard_summarizes_parked_forced_flow_candidate(tmp_path: Path):
    _write_results(tmp_path)
    _write_year_split(tmp_path)
    _write_autopsies(tmp_path)

    df = mechanism_scorecard.build_mechanism_scorecard(tmp_path)
    row = df[df["mechanism_id"] == "forced_flow_reversal"].iloc[0]

    assert row["status"] == "active"
    assert row["mechanism_state"] == "active_no_surviving_candidate"
    assert row["candidate_count"] == 2
    assert row["surviving_candidate_count"] == 0
    assert row["parked_count"] == 1
    assert row["killed_count"] == 1
    assert row["best_candidate_id"] == "BTCUSDT::cand_7d1d9583bddcf985"
    assert row["best_candidate_decision"] == "park"
    assert row["best_candidate_autopsy_path"].endswith(
        "run_mech/BTCUSDT_cand_7d1d9583bddcf985_autopsy.json"
    )
    assert len(row["failed_candidate_autopsy_paths"]) == 2
    assert row["main_failure_reason"] == "no_confirmed_event_specific_forced_flow_candidate"
    assert row["failure_reasons"] == [
        "context_proxy_and_year_pnl_concentration_2022",
        "governed_reproduction_negative_t_stat",
    ]
    assert row["failure_reason_counts"] == {
        "context_proxy_and_year_pnl_concentration_2022": 1,
        "governed_reproduction_negative_t_stat": 1,
    }
    assert row["data_quality_blocker"] == ""
    assert row["next_research_action"] == (
        "test only a stronger forced-flow observable or define crisis/high-vol regime thesis before retesting"
    )


def test_mechanism_scorecard_writers_emit_json_parquet_and_markdown(tmp_path: Path):
    _write_results(tmp_path)
    _write_year_split(tmp_path)
    _write_autopsies(tmp_path)

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
    rendered = md_path.read_text(encoding="utf-8")
    assert "forced_flow_reversal" in rendered
    assert "governed_reproduction_negative_t_stat" in rendered
