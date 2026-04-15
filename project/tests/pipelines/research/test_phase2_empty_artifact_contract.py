from __future__ import annotations

import json

from project.research.services.phase2_support import write_empty_phase2_outputs_with_diagnostics


def test_write_empty_phase2_outputs_emits_report_and_core_artifacts(tmp_path):
    reports_root = tmp_path / "phase2" / "run_x" / "EVENT_X"
    diagnostics = {
        "run_id": "run_x",
        "event_type": "EVENT_X",
        "mode": "skipped_not_centroid",
        "results_count": 0,
        "skip_reason_counts": {"not_centroid": 1},
    }
    write_empty_phase2_outputs_with_diagnostics(
        reports_root=reports_root,
        generation_diagnostics=diagnostics,
        spec_hashes={"gates.yaml": "abc"},
        template_config_hash="sha256:cfg",
        run_manifest_ontology_hash="sha256:run",
        current_ontology_hash="sha256:current",
        current_ontology_components={
            "taxonomy_hash": "sha256:tax",
            "canonical_event_registry_hash": "sha256:cer",
            "state_registry_hash": "sha256:state",
            "verb_lexicon_hash": "sha256:verb",
        },
        operator_registry_version="2",
        cost_coordinate={
            "config_digest": "digest",
            "cost_bps": 6.0,
            "fee_bps_per_side": 4.0,
            "slippage_bps_per_fill": 2.0,
        },
        gate_profile="discovery",
        entry_lag_bars=1,
        summary_overrides={"skipped_not_centroid": True},
        budget_diagnostics=None,
    )

    assert (reports_root / "phase2_generation_diagnostics.json").exists()
    assert (reports_root / "phase2_report.json").exists()
    assert (reports_root / "phase2_candidates_raw.parquet").exists()
    assert (reports_root / "phase2_candidates.parquet").exists()
    assert (reports_root / "phase2_candidates.csv").exists()
    assert (reports_root / "phase2_pvals.parquet").exists()
    assert (reports_root / "phase2_fdr.parquet").exists()

    report = json.loads((reports_root / "phase2_report.json").read_text(encoding="utf-8"))
    assert report["summary"]["total_tested"] == 0
    assert report["summary"]["discoveries_statistical"] == 0
    assert report["summary"]["survivors_phase2"] == 0
    assert report["summary"]["skipped_not_centroid"] is True
    assert report["cost_coordinate"]["cost_bps"] == 6.0
