from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

from project.research.services.run_comparison_service import (
    compare_run_ids,
    research_diagnostics_paths,
)
from project.tests.conftest import PROJECT_ROOT


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _seed_research_diagnostics(
    data_root: Path,
    run_id: str,
    *,
    phase2: dict,
    promotion: dict,
    regime_effectiveness: dict | None = None,
    edge_candidates: list[dict] | None = None,
) -> None:
    paths = research_diagnostics_paths(data_root=data_root, run_id=run_id)
    _write_json(paths["phase2"], phase2)
    _write_json(paths["promotion"], promotion)
    _write_json(
        paths["regime_effectiveness"],
        regime_effectiveness
        or {
            "status": "ok",
            "regimes_total": 2,
            "episodes_total": 5,
            "scorecard_rows": 3,
            "recommended_bucket_counts": {"trade_generating": 1, "trade_filtering": 1},
            "top_regimes_by_incidence": [{"canonical_regime": "LIQUIDITY_STRESS", "episode_count": 3}],
        },
    )
    import pandas as pd

    paths["edge_candidates"].parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(edge_candidates or []).to_parquet(paths["edge_candidates"])


def _load_script_module():
    script_path = PROJECT_ROOT / "scripts" / "compare_research_runs.py"
    spec = importlib.util.spec_from_file_location("compare_research_runs", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_compare_run_ids_uses_canonical_research_report_paths(tmp_path):
    _seed_research_diagnostics(
        tmp_path,
        "baseline_run",
        phase2={
            "false_discovery_diagnostics": {
                "global": {
                    "candidates_total": 10,
                    "survivors_total": 2,
                    "symbols_total": 1,
                    "families_total": 1,
                },
                "sample_quality": {
                    "zero_eval_rows": 1,
                    "median_validation_n_obs": 25,
                    "median_test_n_obs": 18,
                },
                "survivor_quality": {"median_q_value": 0.04, "median_estimate_bps": 8.0},
            }
        },
        promotion={
            "decision_summary": {
                "candidates_total": 2,
                "promoted_count": 1,
                "rejected_count": 1,
                "mean_failed_gate_count_rejected": 1.0,
                "primary_fail_gate_counts": {"stability": 1},
                "primary_reject_reason_counts": {"stability_score": 1},
            }
        },
        edge_candidates=[
            {"gate_bridge_tradable": "fail", "resolved_cost_bps": 0.5, "expectancy_bps": -0.5}
        ],
    )
    _seed_research_diagnostics(
        tmp_path,
        "candidate_run",
        phase2={
            "false_discovery_diagnostics": {
                "global": {
                    "candidates_total": 14,
                    "survivors_total": 3,
                    "symbols_total": 2,
                    "families_total": 2,
                },
                "sample_quality": {
                    "zero_eval_rows": 0,
                    "median_validation_n_obs": 31,
                    "median_test_n_obs": 21,
                },
                "survivor_quality": {"median_q_value": 0.03, "median_estimate_bps": 10.0},
            }
        },
        promotion={
            "decision_summary": {
                "candidates_total": 3,
                "promoted_count": 2,
                "rejected_count": 1,
                "mean_failed_gate_count_rejected": 2.0,
                "primary_fail_gate_counts": {"stability": 1, "negative_control": 1},
                "primary_reject_reason_counts": {"stability_score": 1, "negative_control_fail": 1},
            }
        },
        edge_candidates=[
            {"gate_bridge_tradable": "pass", "resolved_cost_bps": 0.1, "expectancy_bps": -0.1}
        ],
    )

    out = compare_run_ids(
        data_root=tmp_path,
        baseline_run_id="baseline_run",
        candidate_run_id="candidate_run",
    )

    assert out["phase2"]["delta"]["candidate_count"] == 4
    assert out["phase2"]["delta"]["zero_eval_rows"] == -1
    assert out["promotion"]["delta"]["promoted_count"] == 1
    assert out["promotion"]["reject_reason_shift"]["negative_control_fail"] == 1
    assert out["edge_candidates"]["delta"]["tradable_count"] == 1
    assert out["regime_effectiveness"]["delta"]["episodes_total"] == 0


def test_compare_research_runs_script_writes_report(tmp_path, monkeypatch):
    module = _load_script_module()
    _seed_research_diagnostics(
        tmp_path / "data",
        "base1",
        phase2={
            "false_discovery_diagnostics": {
                "global": {
                    "candidates_total": 6,
                    "survivors_total": 1,
                    "symbols_total": 1,
                    "families_total": 1,
                },
                "sample_quality": {
                    "zero_eval_rows": 2,
                    "median_validation_n_obs": 20,
                    "median_test_n_obs": 15,
                },
                "survivor_quality": {"median_q_value": 0.05, "median_estimate_bps": 6.0},
            }
        },
        promotion={
            "decision_summary": {
                "candidates_total": 1,
                "promoted_count": 0,
                "rejected_count": 1,
                "mean_failed_gate_count_rejected": 1.0,
                "primary_fail_gate_counts": {"stability": 1},
                "primary_reject_reason_counts": {"stability_score": 1},
            }
        },
        edge_candidates=[
            {"gate_bridge_tradable": "fail", "resolved_cost_bps": 0.5, "expectancy_bps": -0.5}
        ],
    )
    _seed_research_diagnostics(
        tmp_path / "data",
        "cand1",
        phase2={
            "false_discovery_diagnostics": {
                "global": {
                    "candidates_total": 9,
                    "survivors_total": 2,
                    "symbols_total": 2,
                    "families_total": 1,
                },
                "sample_quality": {
                    "zero_eval_rows": 1,
                    "median_validation_n_obs": 26,
                    "median_test_n_obs": 19,
                },
                "survivor_quality": {"median_q_value": 0.03, "median_estimate_bps": 9.0},
            }
        },
        promotion={
            "decision_summary": {
                "candidates_total": 2,
                "promoted_count": 1,
                "rejected_count": 1,
                "mean_failed_gate_count_rejected": 1.0,
                "primary_fail_gate_counts": {"stability": 1},
                "primary_reject_reason_counts": {"stability_score": 1},
            }
        },
        edge_candidates=[
            {"gate_bridge_tradable": "pass", "resolved_cost_bps": 0.1, "expectancy_bps": -0.1}
        ],
    )

    out_dir = tmp_path / "data" / "reports" / "research_comparison" / "cand1" / "vs_base1"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compare_research_runs.py",
            "--baseline_run_id",
            "base1",
            "--candidate_run_id",
            "cand1",
            "--data_root",
            str(tmp_path / "data"),
            "--out_dir",
            str(out_dir),
            "--drift_mode",
            "warn",
            "--max_promotion_promoted_count_delta_abs",
            "0",
        ],
    )

    rc = module.main()

    assert rc == 0
    report_path = out_dir / "research_run_comparison.json"
    assert report_path.exists()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["baseline_run_id"] == "base1"
    assert report["candidate_run_id"] == "cand1"
    assert report["comparison"]["phase2"]["delta"]["candidate_count"] == 3
    assert report["comparison"]["promotion"]["delta"]["promoted_count"] == 1
    assert report["comparison"]["regime_effectiveness"]["delta"]["regimes_total"] == 0
    assert report["assessment"]["status"] == "warn"
    summary_path = out_dir / "research_run_comparison_summary.md"
    assert summary_path.exists()
    assert "Research Run Comparison" in summary_path.read_text(encoding="utf-8")
