import json
from pathlib import Path

import pandas as pd

from project.research.live_export import export_promoted_theses_for_run
from project.research.validation.contracts import (
    ValidatedCandidateRecord,
    ValidationBundle,
    ValidationDecision,
    ValidationMetrics,
)
from project.research.validation.result_writer import (
    write_promotion_ready_candidates,
    write_validation_bundle,
)


def _bundle() -> dict:
    return {
        "candidate_id": "cand_1",
        "event_type": "VOL_SHOCK",
        "event_family": "VOL_SHOCK",
        "promotion_decision": {"promotion_status": "promoted", "promotion_track": "standard"},
        "sample_definition": {
            "n_events": 120,
            "validation_samples": 60,
            "test_samples": 60,
            "symbol": "BTCUSDT",
        },
        "split_definition": {"split_scheme_id": "confirmatory", "bar_duration_minutes": 5},
        "effect_estimates": {"estimate_bps": 7.0},
        "uncertainty_estimates": {"q_value": 0.01},
        "stability_tests": {"stability_score": 0.8},
        "falsification_results": {},
        "cost_robustness": {"net_expectancy_bps": 5.0, "cost_survival_ratio": 0.9, "tob_coverage": 0.95},
        "multiplicity_adjustment": {},
        "metadata": {"program_id": "prog_1", "campaign_id": "camp_1", "source_run_mode": "confirmatory"},
    }


def test_export_promoted_theses_includes_governance_and_source(tmp_path: Path) -> None:
    bundle = ValidationBundle(
        run_id="run_1",
        created_at="2026-01-01T00:00:00Z",
        validated_candidates=[
            ValidatedCandidateRecord(
                candidate_id="cand_1",
                decision=ValidationDecision(
                    status="validated",
                    candidate_id="cand_1",
                    run_id="run_1",
                ),
                metrics=ValidationMetrics(sample_count=120, stability_score=0.8),
            )
        ],
        rejected_candidates=[],
        inconclusive_candidates=[],
        summary_stats={"total": 1, "validated": 1},
        effect_stability_report={},
    )
    validation_dir = tmp_path / "reports" / "validation" / "run_1"
    write_validation_bundle(bundle, base_dir=validation_dir)
    write_promotion_ready_candidates(bundle, base_dir=validation_dir)
    promoted_df = pd.DataFrame([
        {"candidate_id": "cand_1", "event_type": "VOL_SHOCK", "status": "PROMOTED"}
    ])
    blueprints = [
        {
            "id": "bp_1",
            "candidate_id": "cand_1",
            "direction": "long",
            "symbol_scope": {"mode": "single_symbol", "symbols": ["BTCUSDT"], "candidate_symbol": "BTCUSDT"},
            "exit": {"invalidation": {"metric": "adverse_proxy", "operator": ">", "value": 0.02}},
            "lineage": {"proposal_id": "proposal_1"},
        }
    ]

    result = export_promoted_theses_for_run(
        "run_1",
        data_root=tmp_path,
        bundles=[_bundle()],
        promoted_df=promoted_df,
        blueprints=blueprints,
    )

    payload = result.output_path.read_text(encoding="utf-8")
    assert '"governance"' in payload
    assert '"source"' in payload
    assert '"requirements"' in payload
    assert '"source_campaign_id": "camp_1"' in payload
    assert result.contract_json_path is not None
    assert result.contract_md_path is not None
    assert result.contract_json_path.exists()
    assert result.contract_md_path.exists()
    contract_payload = json.loads(result.contract_json_path.read_text(encoding="utf-8"))
    assert contract_payload["contracts"][0]["primary_event_id"] == "VOL_SHOCK"
    assert contract_payload["contracts"][0]["compat_event_family"] == "VOL_SHOCK"
    assert contract_payload["contracts"][0]["source_campaign_id"] == "camp_1"
    assert contract_payload["contracts"][0]["trade_trigger_eligible"] is True
