from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.research import search_ledger


def test_parse_command_window_extracts_start_end() -> None:
    command = json.dumps(
        [
            "python",
            "-m",
            "project.pipelines.run_all",
            "--start",
            "2022-01-01",
            "--end",
            "2024-12-31",
        ]
    )

    assert search_ledger._parse_command_window(command) == ("2022-01-01", "2024-12-31")


def test_attach_nearby_attempt_counts_uses_same_surface_with_horizon_ratio() -> None:
    df = pd.DataFrame(
        [
            {
                "event_id": "PRICE_DOWN_OI_DOWN",
                "template_id": "mean_reversion",
                "direction": "long",
                "symbol": "BTCUSDT",
                "horizon_bars": 24,
            },
            {
                "event_id": "PRICE_DOWN_OI_DOWN",
                "template_id": "mean_reversion",
                "direction": "long",
                "symbol": "BTCUSDT",
                "horizon_bars": 48,
            },
            {
                "event_id": "PRICE_DOWN_OI_DOWN",
                "template_id": "mean_reversion",
                "direction": "short",
                "symbol": "BTCUSDT",
                "horizon_bars": 24,
            },
        ]
    )

    out = search_ledger.attach_nearby_attempt_counts(df)

    assert out["nearby_attempt_count"].tolist() == [1, 1, 0]


def test_build_search_ledger_from_existing_rows(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        search_ledger.results_index,
        "build_results_index",
        lambda root: pd.DataFrame(
            [
                {
                    "run_id": "run_a",
                    "program_id": "program_a",
                    "event_id": "PRICE_DOWN_OI_DOWN",
                    "template_id": "mean_reversion",
                    "context": "vol_regime=high",
                    "direction": "long",
                    "horizon_bars": 24,
                    "symbol": "BTCUSDT",
                    "event_count": 79,
                    "n_obs": 79,
                    "t_stat_net": 2.3,
                    "q_value": 0.01,
                    "robustness_score": 0.83,
                    "evidence_class": "review_only",
                    "decision": "review",
                    "decision_reason": "year_split_pending",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        search_ledger,
        "collect_validated_plan_metadata",
        lambda root: {"run_a": {"program_id": "program_a", "estimated_hypothesis_count": 1}},
    )
    monkeypatch.setattr(
        search_ledger,
        "collect_proposal_metadata",
        lambda root: {
            "run_a": {
                "proposal_hash": "abc",
                "start": "2022-01-01",
                "end": "2024-12-31",
                "program_id": "program_a",
            }
        },
    )
    monkeypatch.setattr(search_ledger, "collect_memory_rows", lambda root: [])

    df = search_ledger.build_search_ledger(tmp_path)

    assert len(df) == 1
    row = df.iloc[0]
    assert row["proposal_hash"] == "abc"
    assert row["estimated_hypothesis_count"] == 1
    assert row["event_id"] == "PRICE_DOWN_OI_DOWN"
    assert row["methodology_epoch"] == "pre_mechanism"
    assert bool(row["active_research_candidate"]) is False
    assert row["archive_reason"] == "pre_mechanism_methodology"
    assert row["nearby_attempt_count"] == 0


def test_build_search_ledger_attaches_mechanism_metadata(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        search_ledger.results_index,
        "build_results_index",
        lambda root: pd.DataFrame(
            [
                {
                    "run_id": "run_mech",
                    "program_id": "program_mech",
                    "event_id": "PRICE_DOWN_OI_DOWN",
                    "template_id": "mean_reversion",
                    "context": "vol_regime=high",
                    "direction": "long",
                    "horizon_bars": 24,
                    "symbol": "BTCUSDT",
                    "event_count": 79,
                    "n_obs": 79,
                    "t_stat_net": 2.3,
                    "q_value": 0.01,
                    "robustness_score": 0.83,
                    "evidence_class": "validate_ready",
                    "decision": "validate",
                    "decision_reason": "bridge_candidates_present",
                    "methodology_epoch": "mechanism_backed",
                    "mechanism_id": "forced_flow_reversal",
                    "mechanism_version": "v1",
                    "mechanism_preflight_status": "pass",
                    "mechanism_classification": "mechanism_backed",
                    "active_research_candidate": True,
                    "archive_reason": "",
                    "required_falsification": ["governed_reproduction"],
                    "forbidden_rescue_actions": ["change_horizon_after_failure"],
                }
            ]
        ),
    )
    monkeypatch.setattr(
        search_ledger,
        "collect_validated_plan_metadata",
        lambda root: {"run_mech": {"program_id": "program_mech", "estimated_hypothesis_count": 1}},
    )
    monkeypatch.setattr(
        search_ledger,
        "collect_proposal_metadata",
        lambda root: {
            "run_mech": {
                "proposal_hash": "abc",
                "start": "2022-01-01",
                "end": "2024-12-31",
                "program_id": "program_mech",
            }
        },
    )
    monkeypatch.setattr(
        search_ledger.results_index,
        "collect_mechanism_metadata",
        lambda root: {
            "run_mech": {
                "methodology_epoch": "mechanism_backed",
                "mechanism_id": "forced_flow_reversal",
                "mechanism_version": "v1",
                "mechanism_preflight_status": "pass",
                "mechanism_classification": "mechanism_backed",
                "required_falsification": ["governed_reproduction"],
                "forbidden_rescue_actions": ["change_horizon_after_failure"],
            }
        },
    )
    monkeypatch.setattr(search_ledger, "collect_memory_rows", lambda root: [])

    df = search_ledger.build_search_ledger(tmp_path)

    row = df.iloc[0]
    assert row["methodology_epoch"] == "mechanism_backed"
    assert row["mechanism_id"] == "forced_flow_reversal"
    assert row["mechanism_preflight_status"] == "pass"
    assert bool(row["active_research_candidate"]) is True
    assert row["archive_reason"] == ""
    assert row["required_falsification"] == ["governed_reproduction"]


def test_search_ledger_writers_emit_json_and_parquet(tmp_path: Path) -> None:
    df = pd.DataFrame(
        [{column: "" for column in search_ledger.SEARCH_LEDGER_COLUMNS}],
        columns=search_ledger.SEARCH_LEDGER_COLUMNS,
    )
    df.loc[0, "run_id"] = "run"
    df.loc[0, "nearby_attempt_count"] = 0
    json_path = tmp_path / "search_burden.json"
    parquet_path = tmp_path / "search_burden.parquet"

    search_ledger.write_search_ledger_json(df, json_path)
    search_ledger.write_search_ledger_parquet(df, parquet_path)

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "search_burden_v1"
    assert payload["rows"][0]["run_id"] == "run"
    assert pd.read_parquet(parquet_path).iloc[0]["run_id"] == "run"
