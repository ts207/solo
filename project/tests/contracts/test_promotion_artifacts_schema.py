from __future__ import annotations

import pandas as pd

from project.reliability.contracts import (
    validate_evidence_bundle_summary,
    validate_promotion_audit,
    validate_promotion_decisions,
)


def test_validate_promotion_artifacts_schemas():
    audit = pd.DataFrame(
        {
            "candidate_id": ["c1"],
            "hypothesis_id": ["hyp1"],
            "event_type": ["VOL_SHOCK"],
            "promotion_decision": ["promoted"],
            "promotion_track": ["standard"],
            "policy_version": ["p1"],
            "bundle_version": ["b1"],
            "is_reduced_evidence": [False],
            "gate_promo_statistical": ["pass"],
            "gate_promo_stability": ["pass"],
            "gate_promo_cost_survival": ["pass"],
            "gate_promo_negative_control": ["pass"],
        }
    )
    summary = pd.DataFrame(
        {
            "candidate_id": ["c1"],
            "hypothesis_id": ["hyp1"],
            "event_type": ["VOL_SHOCK"],
            "promotion_decision": ["promoted"],
            "promotion_track": ["standard"],
            "policy_version": ["p1"],
            "bundle_version": ["b1"],
            "is_reduced_evidence": [False],
        }
    )
    decisions = summary.copy()
    assert len(validate_promotion_audit(audit)) == 1
    assert len(validate_evidence_bundle_summary(summary)) == 1
    assert len(validate_promotion_decisions(decisions)) == 1
