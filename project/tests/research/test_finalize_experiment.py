from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.research import finalize_experiment as module


def test_finalize_experiment_returns_nonzero_when_experiment_dir_missing(tmp_path: Path) -> None:
    rc = module.finalize_experiment(tmp_path, "btc_campaign", "missing_run")
    assert rc == 1


def test_finalize_experiment_returns_nonzero_when_expanded_hypotheses_missing(
    tmp_path: Path,
) -> None:
    exp_dir = tmp_path / "artifacts" / "experiments" / "btc_campaign" / "run_1"
    exp_dir.mkdir(parents=True, exist_ok=True)

    rc = module.finalize_experiment(tmp_path, "btc_campaign", "run_1")
    assert rc == 1


def test_finalize_experiment_main_returns_nonzero_for_missing_inputs(tmp_path: Path) -> None:
    rc = module.main(
        [
            "--run_id",
            "missing_run",
            "--program_id",
            "btc_campaign",
            "--data_root",
            str(tmp_path),
        ]
    )
    assert rc == 1


def test_finalize_experiment_returns_zero_on_happy_path(tmp_path: Path) -> None:
    exp_dir = tmp_path / "artifacts" / "experiments" / "btc_campaign" / "run_ok"
    exp_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"hypothesis_id": "hyp_1"}]).to_parquet(
        exp_dir / "expanded_hypotheses.parquet",
        index=False,
    )

    rc = module.finalize_experiment(tmp_path, "btc_campaign", "run_ok")
    assert rc == 0
    assert (exp_dir / "summary.json").exists()


def test_finalize_experiment_keeps_evaluated_hypotheses_when_no_bridge_candidates(
    tmp_path: Path,
) -> None:
    run_id = "run_no_bridge"
    exp_dir = tmp_path / "artifacts" / "experiments" / "btc_campaign" / run_id
    exp_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"hypothesis_id": "hyp_1"}]).to_parquet(
        exp_dir / "expanded_hypotheses.parquet",
        index=False,
    )

    phase2_dir = tmp_path / "reports" / "phase2" / run_id
    phase2_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(columns=["candidate_id", "hypothesis_id"]).to_parquet(
        phase2_dir / "phase2_candidates.parquet",
        index=False,
    )
    evaluated_dir = phase2_dir / "hypotheses" / "BTCUSDT"
    evaluated_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "hypothesis_id": "hyp_1",
                "mean_return_bps": 4.2,
                "after_cost_expectancy_bps": 2.2,
                "n": 685,
                "status": "evaluated",
            }
        ]
    ).to_parquet(evaluated_dir / "evaluated_hypotheses.parquet", index=False)

    rc = module.finalize_experiment(tmp_path, "btc_campaign", run_id)

    assert rc == 0
    eval_df = pd.read_parquet(exp_dir / "evaluation_results.parquet")
    assert eval_df["hypothesis_id"].tolist() == ["hyp_1"]
    assert eval_df["eval_status"].tolist() == ["evaluated"]
    assert eval_df["after_cost_expectancy_bps"].tolist() == [2.2]


def test_finalize_experiment_main_writes_manifest(tmp_path: Path, monkeypatch) -> None:
    run_id = "run_manifest_ok"
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))
    exp_dir = tmp_path / "artifacts" / "experiments" / "btc_campaign" / run_id
    exp_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"hypothesis_id": "hyp_1"}]).to_parquet(
        exp_dir / "expanded_hypotheses.parquet",
        index=False,
    )

    rc = module.main(
        [
            "--run_id",
            run_id,
            "--program_id",
            "btc_campaign",
            "--data_root",
            str(tmp_path),
        ]
    )

    assert rc == 0
    manifest = json.loads((tmp_path / "runs" / run_id / "finalize_experiment.json").read_text())
    assert manifest["status"] == "success"
    assert manifest["stats"]["total_hypotheses"] == 1
