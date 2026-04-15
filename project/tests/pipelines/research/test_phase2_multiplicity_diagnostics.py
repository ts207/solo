from __future__ import annotations

import pandas as pd

from project.research.multiplicity import (
    apply_multiplicity_controls,
    build_multiplicity_diagnostics,
)


def test_build_multiplicity_diagnostics_exports_global_and_family_views():
    raw_df = pd.DataFrame(
        [
            {"candidate_id": "a1", "family_id": "fam_a", "p_value": 1e-4, "sample_size": 200},
            {"candidate_id": "a2", "family_id": "fam_a", "p_value": 0.20, "sample_size": 210},
            {"candidate_id": "b1", "family_id": "fam_b", "p_value": 0.02, "sample_size": 220},
            {"candidate_id": "c1", "family_id": "fam_c", "p_value": 0.90, "sample_size": 230},
        ]
    )
    scored = apply_multiplicity_controls(
        raw_df=raw_df,
        max_q=0.05,
        mode="production",
        min_sample_size=0,
    )

    diagnostics = build_multiplicity_diagnostics(
        scored,
        max_q=0.05,
        mode="production",
        min_sample_size=0,
    )

    assert "global" in diagnostics
    assert "by_family" in diagnostics
    assert "by_cluster" in diagnostics
    assert diagnostics["global"]["candidates_total"] == 4
    assert diagnostics["global"]["families_total"] == 3
    assert diagnostics["global"]["families_pool_eligible"] == 3
    assert diagnostics["global"]["discoveries_total"] == int(scored["is_discovery"].sum())
    assert "discoveries_by_total" in diagnostics["global"]
    assert "q_value_by" in scored.columns
    assert "family_cluster_id" in scored.columns
    assert len(diagnostics["by_family"]) == 3
