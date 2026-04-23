from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from project.research.knowledge.memory import ensure_memory_store, read_memory_table, write_memory_table
from project.research.reports.operator_reporting import write_operator_outputs_for_run


def test_write_operator_outputs_for_run_writes_summary_and_ledger(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    run_id = "demo_run"
    program_id = "btc_campaign"

    proposal_dir = data_root / "artifacts" / "experiments" / program_id / "memory" / "proposals" / run_id
    proposal_dir.mkdir(parents=True, exist_ok=True)
    proposal_path = proposal_dir / "proposal.yaml"
    proposal_path.write_text(
        yaml.safe_dump(
            {
                "program_id": program_id,
                "start": "2021-01-01",
                "end": "2021-12-31",
                "symbols": ["BTCUSDT"],
                "trigger_space": {"allowed_trigger_types": ["EVENT"], "events": {"VOL_SHOCK": {}}},
                "templates": ["mean_reversion"],
                "timeframe": "5m",
                "horizons_bars": [12],
                "directions": ["short"],
                "entry_lags": [1],
            }
        ),
        encoding="utf-8",
    )

    ensure_memory_store(program_id, data_root=data_root)
    proposals = read_memory_table(program_id, "proposals", data_root=data_root)
    proposals = pd.concat(
        [
            proposals,
            pd.DataFrame(
                [
                    {
                        "proposal_id": f"proposal::{run_id}",
                        "program_id": program_id,
                        "run_id": run_id,
                        "proposal_path": str(proposal_path),
                        "status": "executed",
                        "symbols": "BTCUSDT",
                        "bounded_json": json.dumps({"frozen_fields": ["symbols"]}),
                        "baseline_run_id": "baseline_1",
                        "experiment_type": "confirmation",
                        "allowed_change_field": "end",
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    write_memory_table(program_id, "proposals", proposals, data_root=data_root)

    run_dir = data_root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "program_id": program_id,
                "status": "success",
                "terminal_status": "completed",
            }
        ),
        encoding="utf-8",
    )

    phase2_dir = data_root / "reports" / "phase2" / run_id
    phase2_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "event_type": "VOL_SHOCK",
                "template_id": "mean_reversion",
                "direction": "short",
                "horizon": "12b",
                "t_stat": -4.2,
                "primary_fail_gate": "min_t_stat",
            }
        ]
    ).to_parquet(phase2_dir / "phase2_candidates.parquet", index=False)

    monkeypatch.setattr(
        "project.research.reports.operator_reporting.build_run_reflection",
        lambda run_id, data_root=None: {
            "candidate_count": 1,
            "promoted_count": 0,
            "mechanical_outcome": "success",
            "statistical_outcome": "weak_signal",
            "primary_fail_gate": "min_t_stat",
            "recommended_next_action": "run_bounded_confirmation",
            "recommended_next_experiment": "2022_only",
        },
    )

    summary = write_operator_outputs_for_run(run_id=run_id, program_id=program_id, data_root=data_root)
    summary_md = data_root / "reports" / "operator" / run_id / "operator_summary.md"
    summary_json = data_root / "reports" / "operator" / run_id / "operator_summary.json"
    ledger = read_memory_table(program_id, "evidence_ledger", data_root=data_root)

    assert summary_md.exists()
    assert summary_json.exists()
    assert summary["top_candidate"]["label"].startswith("VOL_SHOCK / mean_reversion")
    assert "historical_trust_status" in summary["historical_trust"]
    assert len(ledger) == 1
    assert ledger.iloc[0]["run_id"] == run_id
    assert ledger.iloc[0]["verdict"] == "KEEP_RESEARCH"


def test_operator_summary_labels_rule_template_without_unknown(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    run_id = "rule_template_run"
    run_dir = data_root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run_manifest.json").write_text(
        json.dumps({"run_id": run_id, "status": "success", "terminal_status": "completed"}),
        encoding="utf-8",
    )

    phase2_dir = data_root / "reports" / "phase2" / run_id
    phase2_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "event_type": "VOL_SPIKE",
                "rule_template": "mean_reversion",
                "direction": "long",
                "horizon": "12b",
                "t_stat": 3.1,
            }
        ]
    ).to_parquet(phase2_dir / "phase2_candidates.parquet", index=False)

    import project.research.reports.operator_reporting as reporting

    monkeypatch.setattr(
        reporting,
        "build_run_reflection",
        lambda run_id, data_root=None: {
            "candidate_count": 1,
            "promoted_count": 0,
            "mechanical_outcome": "success",
            "statistical_outcome": "weak_signal",
        },
    )

    summary = write_operator_outputs_for_run(run_id=run_id, data_root=data_root)

    assert summary["top_candidate"]["label"].startswith("VOL_SPIKE / mean_reversion")
