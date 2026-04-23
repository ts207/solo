import json

import pandas as pd

from project.research.audit_schema import (
    PROMOTION_AUDIT_SCHEMA_VERSION,
    normalize_promotion_trace_rows,
)


def test_normalize_promotion_trace_rows_preserves_core_audit_fields() -> None:
    df = pd.DataFrame(
        [
            {
                "candidate_id": "cand-1",
                "event_type": "VOL_SPIKE",
                "promotion_decision": "rejected",
                "promotion_track": "fallback_only",
                "fallback_used": True,
                "fallback_reason": "gate_promo_oos_validation",
                "reject_reason": "oos_direction_flip",
                "promotion_fail_gate_primary": "gate_promo_oos_validation",
                "promotion_metrics_trace": json.dumps(
                    {
                        "oos_validation": {
                            "observed": {"validation_samples": 4},
                            "thresholds": {"min_oos_event_count": 20},
                            "passed": False,
                        }
                    }
                ),
            }
        ]
    )
    out = normalize_promotion_trace_rows(df)
    assert not out.empty
    row = out.iloc[0].to_dict()
    assert row["candidate_id"] == "cand-1"
    assert row["event_type"] == "VOL_SPIKE"
    assert row["promotion_track"] == "fallback_only"
    assert row["fallback_used"] is True
    assert row["promotion_fail_gate_primary"] == "gate_promo_oos_validation"
    assert row["pass_fail"] is False
    assert PROMOTION_AUDIT_SCHEMA_VERSION == "v1"
