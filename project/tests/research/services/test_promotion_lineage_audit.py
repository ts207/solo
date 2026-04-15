from pathlib import Path

import pandas as pd

from project.research.services.promotion_service import _write_promotion_lineage_audit


def test_write_promotion_lineage_audit(tmp_path: Path) -> None:
    out = _write_promotion_lineage_audit(
        out_dir=tmp_path,
        run_id="run_1",
        evidence_bundles=[
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "promotion_decision": {"promotion_status": "promoted", "promotion_track": "standard"},
                "bundle_version": "v1",
                "policy_version": "p1",
                "metadata": {"program_id": "prog_1", "campaign_id": "camp_1"},
            }
        ],
        promoted_df=pd.DataFrame([{"candidate_id": "cand_1"}]),
        live_export_diagnostics={
            "thesis_count": 1,
            "output_path": "/tmp/live/promoted_theses.json",
            "contract_json_path": "/tmp/live/promoted_thesis_contracts.json",
            "contract_md_path": "/tmp/live/promoted_thesis_contracts.md",
        },
        historical_trust={
            "historical_trust_status": "trusted_under_current_rules",
            "canonical_reuse_allowed": True,
            "compat_reuse_allowed": True,
        },
    )
    assert Path(out["json_path"]).exists()
    assert Path(out["md_path"]).exists()
    markdown = Path(out["md_path"]).read_text(encoding="utf-8")
    assert "camp_1" in markdown
    assert "promoted_thesis_contracts.json" in markdown
    assert "promoted_thesis_contracts.md" in markdown
    assert "trusted_under_current_rules" in markdown
