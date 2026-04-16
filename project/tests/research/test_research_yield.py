from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from project import discover, promote, validate

log = logging.getLogger(__name__)

@pytest.mark.integration
class TestResearchYield:
    """
    Guards against pipeline drift by ensuring that at least one highly productive 
    historical proposal (the Golden Path) retains the ability to navigate 
    the full 4-stage lifecycle (Discover -> Validate -> Promote -> Deploy) 
    and output a candidate that completes validation successfully.
    """

    def test_productive_golden_path_yields_deployable_thesis(self, tmp_path: Path):
        """
        Executes the canonical golden path proposal end-to-end.
        """
        golden_path_proposal = Path("spec/proposals/productive_golden_path.yaml")
        assert golden_path_proposal.exists(), "Productive Golden Path proposal missing from spec directory."

        # Stage 1: Discover
        # Executes the event search, metric evaluation, and bridge gates.
        discover_result = discover.run(
            proposal_path=golden_path_proposal,
            registry_root=Path("project/configs/registries"),
            check=True,
            plan_only=False,
            dry_run=False,
        )
        
        run_id = discover_result.get("run_id")
        assert run_id is not None, "Discovery failed to produce a run_id."
        
        diagnostics = discover_result.get("execution", {}).get("phase2_diagnostics", {})
        if not diagnostics:
            # We can also load from disk if the orchestrator result structure shifts.
            diagnostics_path = Path(f"data/reports/phase2/{run_id}/phase2_diagnostics.json")
            if diagnostics_path.exists():
                diagnostics = json.loads(diagnostics_path.read_text())

        candidates_written = diagnostics.get("gate_funnel", {}).get("phase2_candidates_written", 0)
        assert candidates_written > 0, "Regression: Golden path failed to yield any feasible candidates from phase 2."

        # Stage 2: Validate
        # Evaluates time-slicing stability and cross-regime consistency.
        validate_result = validate.run(run_id=run_id)
        assert validate_result.exit_code == 0, f"Validation stage failed for {run_id}"
        
        val_bundle_path = Path(f"data/reports/validation/{run_id}/validation_bundle.json")
        assert val_bundle_path.exists(), "Validation bundle missing."
        bundle = json.loads(val_bundle_path.read_text())
        
        # Verify that we actually validated at least one candidate
        stats = bundle.get("summary_stats", {})
        assert stats.get("validated", 0) > 0, "Regression: No golden path candidates survived OOS and validation checks."
        
        # Stage 3: Promote
        # Formats the validated candidate and confirms final statistics prior to export.
        promote_result = promote.run(
            run_id=run_id,
            symbols="BTCUSDT",
            retail_profile="capital_constrained",
        )
        assert promote_result.exit_code == 0, "Promotion evaluation failed."
        
        # Stage 4: Export (Packaging)
        # Packages into production-bound theses.
        export_result = promote.export(
            run_id=run_id,
            allow_bundle_only_export=True,
        )
        assert export_result.thesis_count > 0, "Regression: Export yield is 0, no theses could be exported."
