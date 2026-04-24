from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

from project.core.exceptions import DataIntegrityError
from project.operator.stability import (
    write_negative_result_diagnostics,
    write_regime_split_report,
    write_time_slice_report,
)
from project.research.knowledge.memory import (
    ensure_memory_store,
    read_memory_table,
    write_memory_table,
)


def _write_summary_seed(*, data_root: Path, program_id: str, run_id: str, start: str, end: str, metric: float) -> None:
    proposal_dir = data_root / "artifacts" / "experiments" / program_id / "memory" / "proposals" / run_id
    proposal_dir.mkdir(parents=True, exist_ok=True)
    proposal_path = proposal_dir / "proposal.yaml"
    proposal_path.write_text(
        yaml.safe_dump(
            {
                "program_id": program_id,
                "start": start,
                "end": end,
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
                        "allowed_change_field": "date_range",
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
        json.dumps({"run_id": run_id, "program_id": program_id, "status": "success", "terminal_status": "completed"}),
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
                "t_stat": metric,
                "train_n_obs": 100,
            }
        ]
    ).to_parquet(phase2_dir / "phase2_candidates.parquet", index=False)


def test_write_time_slice_report_classifies_concentrated(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    program_id = "btc_campaign"
    _write_summary_seed(data_root=data_root, program_id=program_id, run_id="run_2021", start="2021-01-01", end="2021-12-31", metric=-3.8)
    _write_summary_seed(data_root=data_root, program_id=program_id, run_id="run_2022", start="2022-01-01", end="2022-12-31", metric=-0.4)

    monkeypatch.setattr(
        "project.research.reports.operator_reporting.build_run_reflection",
        lambda run_id, data_root=None: {
            "candidate_count": 1,
            "promoted_count": 0,
            "mechanical_outcome": "success",
            "statistical_outcome": "weak_signal",
            "recommended_next_action": "run_bounded_confirmation",
        },
    )

    report = write_time_slice_report(run_ids=["run_2021", "run_2022"], program_id=program_id, data_root=data_root)
    assert report["classification"] == "concentrated"
    assert Path(report["report_json_path"]).exists()


def test_negative_result_diagnostics_detect_regime_instability(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    program_id = "btc_campaign"
    run_id = "run_regime"
    _write_summary_seed(data_root=data_root, program_id=program_id, run_id=run_id, start="2021-01-01", end="2021-12-31", metric=-2.6)
    phase2 = data_root / "reports" / "phase2" / run_id / "phase2_candidates.parquet"
    pd.DataFrame(
        [
            {
                "event_type": "VOL_SHOCK",
                "template_id": "mean_reversion",
                "direction": "short",
                "horizon": "12b",
                "t_stat": -2.6,
                "train_n_obs": 120,
                "expectancy_by_regime_bps": json.dumps({"high_vol": -4.0, "low_vol": 2.0}),
            }
        ]
    ).to_parquet(phase2, index=False)

    import project.research.reports.operator_reporting as reporting

    monkeypatch.setattr(
        reporting,
        "build_run_reflection",
        lambda run_id, data_root=None: {
            "candidate_count": 1,
            "promoted_count": 0,
            "mechanical_outcome": "success",
            "statistical_outcome": "weak_signal",
            "primary_fail_gate": "gate_promo_stability_gate",
        },
    )
    diagnostics = write_negative_result_diagnostics(run_id=run_id, program_id=program_id, data_root=data_root)
    regime = write_regime_split_report(run_id=run_id, data_root=data_root)

    assert diagnostics["diagnosis"] == "regime_instability"
    assert regime["classification"] == "regime_instability"
    assert Path(diagnostics["report_json_path"]).exists()


def test_regime_split_report_labels_rule_template_without_unknown(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    program_id = "btc_campaign"
    run_id = "run_rule_template"
    _write_summary_seed(data_root=data_root, program_id=program_id, run_id=run_id, start="2021-01-01", end="2021-12-31", metric=2.4)
    phase2 = data_root / "reports" / "phase2" / run_id / "phase2_candidates.parquet"
    pd.DataFrame(
        [
            {
                "event_type": "VOL_SPIKE",
                "rule_template": "mean_reversion",
                "direction": "long",
                "horizon": "12b",
                "t_stat": 2.4,
                "train_n_obs": 120,
            }
        ]
    ).to_parquet(phase2, index=False)

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

    regime = write_regime_split_report(run_id=run_id, data_root=data_root)

    assert regime["candidate_regime_diagnostics"][0]["label"].startswith("VOL_SPIKE / mean_reversion")


def test_negative_result_diagnostics_warning_only_run_is_not_mechanical_gap(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    program_id = "btc_campaign"
    run_id = "run_warning_only"
    run_dir = data_root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "program_id": program_id,
                "status": "success",
                "terminal_status": "completed_with_contract_warnings",
            }
        ),
        encoding="utf-8",
    )
    phase2_dir = data_root / "reports" / "phase2" / run_id
    phase2_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"run_id": run_id}]).iloc[0:0].to_parquet(phase2_dir / "phase2_candidates.parquet", index=False)

    import project.research.reports.operator_reporting as reporting

    monkeypatch.setattr(
        reporting,
        "build_run_reflection",
        lambda run_id, data_root=None: {
            "candidate_count": 0,
            "promoted_count": 0,
            "mechanical_outcome": "warning_only",
            "statistical_outcome": "no_signal",
            "recommended_next_action": "hold",
        },
    )

    diagnostics = write_negative_result_diagnostics(run_id=run_id, program_id=program_id, data_root=data_root)

    assert diagnostics["diagnosis"] == "no_effect"
    assert diagnostics["recommended_next_action"] == "kill_or_reframe_hypothesis"


def test_negative_result_diagnostics_raises_on_malformed_regime_mapping(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    program_id = "btc_campaign"
    run_id = "run_bad_regime_map"
    _write_summary_seed(
        data_root=data_root,
        program_id=program_id,
        run_id=run_id,
        start="2021-01-01",
        end="2021-12-31",
        metric=-2.6,
    )
    phase2 = data_root / "reports" / "phase2" / run_id / "phase2_candidates.parquet"
    pd.DataFrame(
        [
            {
                "event_type": "VOL_SHOCK",
                "template_id": "mean_reversion",
                "direction": "short",
                "horizon": "12b",
                "t_stat": -2.6,
                "train_n_obs": 120,
                "expectancy_by_regime_bps": "{not valid json",
            }
        ]
    ).to_parquet(phase2, index=False)

    import project.research.reports.operator_reporting as reporting

    monkeypatch.setattr(
        reporting,
        "build_run_reflection",
        lambda run_id, data_root=None: {
            "candidate_count": 1,
            "promoted_count": 0,
            "mechanical_outcome": "success",
            "statistical_outcome": "weak_signal",
            "primary_fail_gate": "gate_promo_stability_gate",
        },
    )

    with pytest.raises(DataIntegrityError, match="Failed to parse stability mapping JSON"):
        write_negative_result_diagnostics(run_id=run_id, program_id=program_id, data_root=data_root)
