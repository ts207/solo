from __future__ import annotations

import pandas as pd

from project.research.services.reporting_service import (
    write_candidate_reports,
    write_promotion_reports,
)


def test_write_candidate_reports_writes_bundle(tmp_path):
    combined = pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "hypothesis_id": "hyp_1",
                "event_type": "VOL_SHOCK",
                "symbol": "BTCUSDT",
                "run_id": "r1",
            }
        ]
    )
    symbols = {"BTCUSDT": combined.copy()}
    result = write_candidate_reports(
        out_dir=tmp_path,
        combined_candidates=combined,
        symbol_candidates=symbols,
        diagnostics={"run_id": "r1"},
    )
    assert (tmp_path / "phase2_candidates.parquet").exists() or (
        tmp_path / "phase2_candidates.csv"
    ).exists()
    assert (tmp_path / "symbols" / "BTCUSDT").exists()
    assert (tmp_path / "phase2_diagnostics.json").exists()
    assert "combined_candidates" in result.written_frames


def test_write_promotion_reports_writes_bundle(tmp_path):
    audit = pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "hypothesis_id": "hyp1",
                "event_type": "VOL_SHOCK",
                "promotion_decision": "promoted",
                "promotion_track": "standard",
            }
        ]
    )
    promoted = pd.DataFrame(
        [{"candidate_id": "c1", "event_type": "VOL_SHOCK", "status": "PROMOTED"}]
    )
    summary = pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "event_type": "VOL_SHOCK",
                "stage": "statistical",
                "statistic": "{}",
                "threshold": "{}",
                "pass_fail": True,
            }
        ]
    )
    result = write_promotion_reports(
        out_dir=tmp_path,
        audit_df=audit,
        promoted_df=promoted,
        evidence_bundle_summary=pd.DataFrame(
            [
                {
                    "candidate_id": "c1",
                    "hypothesis_id": "hyp1",
                    "event_type": "VOL_SHOCK",
                    "promotion_decision": "promoted",
                    "promotion_track": "standard",
                }
            ]
        ),
        promotion_decisions=pd.DataFrame(
            [
                {
                    "candidate_id": "c1",
                    "hypothesis_id": "hyp1",
                    "event_type": "VOL_SHOCK",
                    "promotion_decision": "promoted",
                    "promotion_track": "standard",
                }
            ]
        ),
        diagnostics={"promoted": 1},
        promotion_summary=summary,
    )
    assert (tmp_path / "promotion_summary.csv").exists()
    assert (tmp_path / "promotion_diagnostics.json").exists()
    assert "promotion_audit" in result.written_frames
