from __future__ import annotations

import json

import pandas as pd

from project.research.services import candidate_discovery_service as discovery_svc
from project.research.services import promotion_service as promotion_svc


def test_promotion_decision_characterization_captures_reason_gate_and_outcomes():
    audit_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "promotion_decision": "rejected",
                "promotion_fail_gate_primary": "gate_promo_negative_control",
                "promotion_fail_reason_primary": "",
                "reject_reason": "negative_control_fail|failed_placebo_controls",
                "promotion_metrics_trace": json.dumps(
                    {
                        "statistical": {
                            "passed": True,
                            "observed": {"n_events": 140, "q_value": 0.03},
                            "thresholds": {"max_q_value": 0.1, "min_events": 100},
                        },
                        "negative_control": {
                            "passed": False,
                            "observed": {"control_pass_rate": 0.2},
                            "thresholds": {"max_negative_control_pass_rate": 0.01},
                        },
                        "stability": {
                            "passed": True,
                            "observed": {"stability_score": 0.4},
                            "thresholds": {"min_stability_score": 0.05},
                        },
                    }
                ),
            }
        ]
    )

    out = promotion_svc._annotate_promotion_audit_decisions(audit_df)
    row = out.iloc[0]

    assert row["primary_reject_reason"] == "negative_control_fail"
    assert row["weakest_fail_stage"] == "negative_control"
    assert row["failed_gate_count"] == 1
    assert row["failed_gate_list"] == "negative_control"


def test_candidate_discovery_characterization_captures_split_counts_and_cost_sensitivity():
    combined = pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "symbol": "BTCUSDT",
                "family_id": "fam_a",
                "validation_n_obs": 12,
                "test_n_obs": 10,
                "n_obs": 22,
                "q_value": 0.02,
                "q_value_by": 0.03,
                "estimate_bps": 9.0,
                "resolved_cost_bps": 5.0,
                "is_discovery": True,
            },
            {
                "candidate_id": "cand_2",
                "symbol": "BTCUSDT",
                "family_id": "fam_b",
                "validation_n_obs": 0,
                "test_n_obs": 0,
                "n_obs": 0,
                "q_value": 0.8,
                "q_value_by": 0.9,
                "estimate_bps": 1.0,
                "resolved_cost_bps": 12.0,
                "is_discovery": False,
            },
        ]
    )

    diagnostics = discovery_svc._build_false_discovery_diagnostics(combined)

    assert diagnostics["sample_quality"]["zero_validation_rows"] == 1
    assert diagnostics["sample_quality"]["zero_test_rows"] == 1
    assert diagnostics["survivor_quality"]["median_cost_bps"] == 5.0
    assert diagnostics["survivor_quality"]["median_q_value"] == 0.02
