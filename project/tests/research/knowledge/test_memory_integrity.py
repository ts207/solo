from __future__ import annotations

import pandas as pd
import pytest

from project.core.exceptions import DataIntegrityError
from project.research.knowledge.memory import build_failures_snapshot, build_tested_regions_snapshot


def test_build_tested_regions_snapshot_raises_on_corrupted_expanded_hypotheses(tmp_path) -> None:
    data_root = tmp_path / "data"
    run_id = "run_1"
    program_id = "btc_campaign"

    phase2_dir = data_root / "reports" / "phase2" / run_id / "search_engine"
    phase2_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "hypothesis_id": "hyp_1",
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "trigger_type": "EVENT",
                "template_id": "mean_reversion",
                "direction": "long",
                "horizon": "12b",
            }
        ]
    ).to_parquet(phase2_dir / "phase2_candidates.parquet", index=False)

    exp_dir = data_root / "artifacts" / "experiments" / program_id / run_id
    exp_dir.mkdir(parents=True, exist_ok=True)
    (exp_dir / "expanded_hypotheses.parquet").write_bytes(b"NOTPARQUET")

    with pytest.raises(DataIntegrityError):
        build_tested_regions_snapshot(run_id=run_id, program_id=program_id, data_root=data_root)


def test_build_tested_regions_snapshot_falls_back_to_evaluated_hypotheses_when_phase2_candidates_empty(
    tmp_path,
) -> None:
    data_root = tmp_path / "data"
    run_id = "run_eval_only"
    program_id = "btc_campaign"

    run_dir = data_root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run_manifest.json").write_text(
        '{"run_id":"run_eval_only","config_resolution":{}}',
        encoding="utf-8",
    )

    phase2_dir = data_root / "reports" / "phase2" / run_id
    phase2_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(columns=["candidate_id"]).to_parquet(phase2_dir / "phase2_candidates.parquet", index=False)

    hypotheses_dir = phase2_dir / "hypotheses" / "BTCUSDT"
    hypotheses_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "hypothesis_id": "hyp_1",
                "trigger_type": "event",
                "trigger_key": "event:VOL_SHOCK",
                "template_id": "continuation",
                "direction": "short",
                "horizon": "24b",
                "entry_lag_bars": 2,
                "n": 1868,
                "t_stat": 0.92,
                "p_value_for_fdr": 0.081,
                "cost_adjusted_return_bps": 1.25,
                "status": "evaluated",
                "valid": True,
            }
        ]
    ).to_parquet(hypotheses_dir / "evaluated_hypotheses.parquet", index=False)

    tested = build_tested_regions_snapshot(run_id=run_id, program_id=program_id, data_root=data_root)

    assert len(tested) == 1
    assert tested.iloc[0]["event_type"] == "VOL_SHOCK"
    assert tested.iloc[0]["template_id"] == "continuation"
    assert tested.iloc[0]["entry_lag"] == 2
    assert tested.iloc[0]["after_cost_expectancy"] == pytest.approx(1.25)
    assert tested.iloc[0]["q_value"] == pytest.approx(0.081)


def test_build_failures_snapshot_ignores_null_failed_stage(tmp_path) -> None:
    data_root = tmp_path / "data"
    run_id = "run_ok"
    program_id = "btc_campaign"

    run_dir = data_root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run_manifest.json").write_text(
        '{"run_id":"run_ok","failed_stage":null,"status":"success"}',
        encoding="utf-8",
    )

    failures = build_failures_snapshot(run_id=run_id, program_id=program_id, data_root=data_root)

    assert failures.empty


def test_build_tested_regions_snapshot_prefers_flat_phase2_candidates_over_legacy_nested(
    tmp_path,
) -> None:
    data_root = tmp_path / "data"
    run_id = "run_flat"
    program_id = "btc_campaign"

    run_dir = data_root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run_manifest.json").write_text(
        '{"run_id":"run_flat","config_resolution":{}}',
        encoding="utf-8",
    )

    nested_path = data_root / "reports" / "phase2" / run_id / "search_engine"
    flat_path = data_root / "reports" / "phase2" / run_id
    nested_path.mkdir(parents=True, exist_ok=True)
    flat_path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "nested",
                "event_type": "OLD_EVENT",
                "trigger_type": "event",
                "template_id": "continuation",
                "direction": "long",
                "horizon": "12b",
            }
        ]
    ).to_parquet(nested_path / "phase2_candidates.parquet", index=False)
    pd.DataFrame(
        [
            {
                "candidate_id": "flat",
                "event_type": "NEW_EVENT",
                "trigger_type": "event",
                "template_id": "continuation",
                "direction": "short",
                "horizon": "24b",
            }
        ]
    ).to_parquet(flat_path / "phase2_candidates.parquet", index=False)

    tested = build_tested_regions_snapshot(run_id=run_id, program_id=program_id, data_root=data_root)

    assert len(tested) == 1
    assert tested.iloc[0]["candidate_id"] == "flat"
    assert tested.iloc[0]["event_type"] == "NEW_EVENT"
