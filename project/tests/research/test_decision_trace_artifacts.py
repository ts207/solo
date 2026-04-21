from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.research.decision_trace_artifacts import (
    build_discovery_trace_frame,
    build_promotion_trace_frame,
    build_validation_trace_frame,
    merge_research_decision_trace,
    write_merged_research_trace,
)
from project.research.validation.contracts import (
    ValidationBundle,
    ValidationDecision,
    ValidationMetrics,
    ValidatedCandidateRecord,
)


def test_build_discovery_trace_frame_preserves_candidate_identity():
    df = pd.DataFrame([
        {
            "candidate_id": "cand_1",
            "hypothesis_id": "hyp_1",
            "event_type": "VOL_SPIKE",
            "symbol": "BTCUSDT",
            "rule_template": "mean_reversion",
            "direction": "long",
            "horizon_bars": 24,
            "t_stat": 2.5,
            "n_obs": 120,
            "is_discovery": True,
        }
    ])
    out = build_discovery_trace_frame(df, run_id="run_1")
    assert len(out) == 1
    assert out.iloc[0]["candidate_id"] == "cand_1"
    assert out.iloc[0]["hypothesis_id"] == "hyp_1"


def test_build_validation_and_promotion_trace_merge_to_final_decision(tmp_path: Path):
    bundle = ValidationBundle(
        run_id="run_1",
        created_at="2026-01-01T00:00:00Z",
        validated_candidates=[
            ValidatedCandidateRecord(
                candidate_id="cand_1",
                decision=ValidationDecision(status="validated", candidate_id="cand_1", run_id="run_1"),
                metrics=ValidationMetrics(sample_count=100, q_value=0.01),
                template_id="mean_reversion",
                direction="long",
                horizon_bars=24,
            )
        ],
    )
    validation = build_validation_trace_frame(bundle)
    promotion = build_promotion_trace_frame(
        audit_df=pd.DataFrame([
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SPIKE",
                "symbol": "BTCUSDT",
                "promotion_decision": "promoted",
                "promotion_track": "paper_promoted",
                "policy_version": "v1",
                "bundle_version": "v1",
            }
        ]),
        promoted_df=pd.DataFrame([{"candidate_id": "cand_1"}]),
        run_id="run_1",
    )
    merged = merge_research_decision_trace(validation_trace=validation, promotion_trace=promotion)
    assert len(merged) == 1
    assert merged.iloc[0]["final_decision"] == "promoted"
    out = write_merged_research_trace(
        out_dir=tmp_path,
        data_root=tmp_path,
        run_id="run_1",
        validation_trace=validation,
        promotion_trace=promotion,
    )
    assert out is not None and out.exists()
