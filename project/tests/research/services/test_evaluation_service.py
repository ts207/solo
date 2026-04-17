from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.research.services.evaluation_service import EvaluationSummaryService
from project.research.validation.contracts import (
    ValidationBundle,
    ValidatedCandidateRecord,
    ValidationDecision,
    ValidationMetrics,
)
from project.research.validation.result_writer import (
    write_validation_bundle,
    write_validated_candidate_tables,
)


def test_evaluation_summary_uses_stage_candidates_and_validation_outputs(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    run_id = "summary_run"
    edge_dir = data_root / "reports" / "edge_candidates" / run_id
    edge_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "family": "VOL",
                "q_value": 0.01,
                "stability_score": 0.9,
                "n_events": 100,
            },
            {
                "candidate_id": "cand_2",
                "event_type": "VOL_SHOCK",
                "family": "VOL",
                "q_value": 0.02,
                "stability_score": 0.7,
                "n_events": 80,
            },
        ]
    ).to_parquet(edge_dir / "edge_candidates_normalized.parquet")

    validation_dir = data_root / "reports" / "validation" / run_id
    bundle = ValidationBundle(
        run_id=run_id,
        created_at="2026-01-01T00:00:00",
        validated_candidates=[
            ValidatedCandidateRecord(
                candidate_id="cand_1",
                decision=ValidationDecision(
                    status="validated",
                    candidate_id="cand_1",
                    run_id=run_id,
                    reason_codes=[],
                ),
                metrics=ValidationMetrics(
                    sample_count=100,
                    q_value=0.01,
                    stability_score=0.9,
                ),
                template_id="tpl",
                direction="long",
                horizon_bars=12,
            )
        ],
        rejected_candidates=[
            ValidatedCandidateRecord(
                candidate_id="cand_2",
                decision=ValidationDecision(
                    status="rejected",
                    candidate_id="cand_2",
                    run_id=run_id,
                    reason_codes=["failed_oos_validation", "failed_stability"],
                ),
                metrics=ValidationMetrics(
                    sample_count=80,
                    q_value=0.02,
                    stability_score=0.7,
                ),
                template_id="tpl",
                direction="short",
                horizon_bars=12,
            )
        ],
        summary_stats={"total": 2, "validated": 1, "rejected": 1, "inconclusive": 0},
    )
    write_validation_bundle(bundle, base_dir=validation_dir)
    write_validated_candidate_tables(bundle, base_dir=validation_dir)

    service = EvaluationSummaryService(data_root=data_root)
    summary = service.summarize_run(run_id)

    assert summary.run_id == run_id
    assert summary.total_candidates == 2
    assert summary.gate_pass_count == 1
    assert summary.gate_pass_rate == 0.5
    assert summary.primary_event_ids == ["VOL_SHOCK"]
    assert summary.event_families == ["VOL"]
    assert summary.top_fail_reasons[0]["reason"] == "failed_oos_validation"
    assert summary.by_primary_event_id["VOL_SHOCK"]["candidate_count"] == 2
    assert summary.by_event_family["VOL"]["gate_pass_count"] == 1
    assert summary.funnel_payload["validated"] == 1
    assert summary.source_files["edge_candidates"].endswith("edge_candidates_normalized.parquet")
