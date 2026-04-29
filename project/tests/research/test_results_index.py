from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.research import results_index


def _base_row(**overrides):
    row = {
        "source_file": "eval_results",
        "run_id": "run",
        "program_id": "program",
        "event_type": "TEST_EVENT",
        "direction": "long",
        "horizon": "24b",
        "template_id": "mean_reversion",
        "symbol": "BTCUSDT",
        "t_stat": None,
        "robustness_score": None,
        "n_events": None,
        "n": None,
        "q_value": None,
        "after_cost_expectancy_per_trade": 1.23,
    }
    row.update(overrides)
    return row


def test_normalize_result_row_hides_expectancy_without_evaluable_metrics():
    row = results_index.normalize_result_row(_base_row(program_id="summary_only"))

    assert row["event_id"] == "TEST_EVENT"
    assert row["horizon_bars"] == 24
    assert row["mean_return_net_bps"] == 12300.0
    assert row["evidence_class"] == "review_only"
    assert row["decision"] == "review"
    assert row["decision_reason"] == "not_evaluated"


def test_normalize_result_row_marks_local_bridge_as_validate_ready():
    row = results_index.normalize_result_row(
        _base_row(
            t_stat=2.28,
            robustness_score=0.84,
            n_events=53,
            q_value=0.01,
            after_cost_expectancy_per_trade=0.005,
        )
    )

    assert row["evidence_class"] == "validate_ready"
    assert row["decision"] == "validate"
    assert row["decision_reason"] == "local_bridge_gate_passed"
    assert row["mean_return_net_bps"] == 50.0


def test_normalize_result_row_uses_trigger_payload_event_id_not_trigger_type():
    row = results_index.normalize_result_row(
        _base_row(
            event_type=None,
            trigger_type="event",
            trigger_payload='{"trigger_type": "event", "event_id": "PRICE_DOWN_OI_DOWN"}',
        )
    )

    assert row["event_id"] == "PRICE_DOWN_OI_DOWN"


def test_attach_manual_decisions_overrides_matching_rows():
    row = results_index.normalize_result_row(
        _base_row(
            event_type="CLIMAX_VOLUME_BAR",
            direction="long",
            horizon="24b",
            template_id="exhaustion_reversal",
            t_stat=2.2495,
            robustness_score=0.7041,
            n_events=309,
        )
    )
    decisions = [
        {
            "match": {
                "event_id": "CLIMAX_VOLUME_BAR",
                "direction": "long",
                "horizon_bars": 24,
                "template_id": "exhaustion_reversal",
            },
            "decision": "park",
            "evidence_class": "parked_candidate",
            "decision_reason": "forward_confirmation_failed",
            "next_safe_command": "write autopsy",
            "forbidden_rescue_actions": ["change_horizon"],
        }
    ]

    out = results_index.attach_manual_decisions([row], decisions)

    assert len(out) == 1
    assert out[0]["decision"] == "park"
    assert out[0]["evidence_class"] == "parked_candidate"
    assert out[0]["decision_reason"] == "forward_confirmation_failed"
    assert out[0]["forbidden_rescue_actions"] == ["change_horizon"]


def test_attach_manual_decisions_adds_unmatched_manual_rows():
    out = results_index.attach_manual_decisions(
        [],
        [
            {
                "match": {
                    "event_id": "BAND_BREAK",
                    "symbol": "ETHUSDT",
                    "context": "vol_regime=low",
                    "direction": "long",
                    "horizon_bars": 24,
                    "template_id": "mean_reversion",
                },
                "run_id": "repro_run",
                "decision": "kill",
                "evidence_class": "killed_candidate",
                "decision_reason": "governed_reproduction_failed",
            }
        ],
    )

    assert len(out) == 1
    assert out[0]["event_id"] == "BAND_BREAK"
    assert out[0]["run_id"] == "repro_run"
    assert out[0]["decision"] == "kill"


def test_attach_doctor_status_uses_bridge_candidate_boundary(monkeypatch, tmp_path: Path):
    row = results_index.normalize_result_row(
        _base_row(run_id="bridge_run", t_stat=2.2, robustness_score=0.8, n_events=40)
    )

    def fake_report(**_kwargs):
        return {
            "status": "review_candidate",
            "classification": "candidates_present_but_no_final_bridge_survivors",
            "next_safe_command": "Review top_candidates",
            "forbidden_rescue_actions": ["loosen_gates"],
        }

    monkeypatch.setattr(results_index, "build_discover_doctor_report", fake_report)

    out = results_index.attach_doctor_status([row], root=tmp_path)

    assert out[0]["evidence_class"] == "review_only"
    assert out[0]["decision"] == "review"
    assert out[0]["decision_reason"] == "candidates_present_but_no_final_bridge_survivors"
    assert out[0]["forbidden_rescue_actions"] == ["loosen_gates"]


def test_attach_doctor_status_does_not_upgrade_nonbridge_rows(monkeypatch, tmp_path: Path):
    row = results_index.normalize_result_row(
        _base_row(run_id="bridge_run", t_stat=1.2, robustness_score=0.8, n_events=40)
    )

    def fake_report(**_kwargs):
        return {
            "status": "validate_ready",
            "classification": "bridge_candidates_present",
            "next_safe_command": "make validate RUN_ID=bridge_run",
            "forbidden_rescue_actions": ["loosen_gates"],
        }

    monkeypatch.setattr(results_index, "build_discover_doctor_report", fake_report)

    out = results_index.attach_doctor_status([row], root=tmp_path)

    assert out[0]["evidence_class"] == "review_only"
    assert out[0]["decision"] == "review"
    assert out[0]["decision_reason"] == "below_bridge_gate"


def test_writers_emit_json_parquet_and_markdown(tmp_path: Path):
    df = pd.DataFrame(
        [
            {
                "run_id": "run",
                "program_id": "program",
                "candidate_id": "cand",
                "event_id": "TEST_EVENT",
                "template_id": "mean_reversion",
                "context": "",
                "direction": "long",
                "horizon_bars": 24,
                "symbol": "BTCUSDT",
                "n_obs": 20,
                "event_count": 20,
                "t_stat_net": 2.1,
                "mean_return_net_bps": 12.5,
                "q_value": 0.01,
                "robustness_score": 0.75,
                "evidence_class": "validate_ready",
                "decision": "validate",
                "decision_reason": "bridge_candidates_present",
                "next_safe_command": "make validate RUN_ID=run",
                "forbidden_rescue_actions": ["loosen_gates"],
            }
        ],
        columns=results_index.RESULT_COLUMNS,
    )

    json_path = tmp_path / "results_index.json"
    parquet_path = tmp_path / "results_index.parquet"
    md_path = tmp_path / "results.md"
    results_index.write_results_index_json(df, json_path)
    results_index.write_results_index_parquet(df, parquet_path)
    results_index.write_results_markdown(df, md_path)

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "results_index_v1"
    assert payload["rows"][0]["forbidden_rescue_actions"] == ["loosen_gates"]
    assert pd.read_parquet(parquet_path).iloc[0]["event_id"] == "TEST_EVENT"
    assert "TEST_EVENT" in md_path.read_text(encoding="utf-8")
