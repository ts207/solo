from __future__ import annotations

import json

import pandas as pd

from project.research.services import shadow_playbook_service as svc


def test_build_shadow_playbook_groups_candidates_and_applies_confirmatory_blockers(tmp_path):
    data_root = tmp_path / "data"
    edge_dir = data_root / "reports" / "edge_candidates" / "r1"
    phase2_dir = data_root / "reports" / "phase2" / "r1"
    confirm_dir = data_root / "reports" / "confirm"
    edge_dir.mkdir(parents=True, exist_ok=True)
    phase2_dir.mkdir(parents=True, exist_ok=True)
    confirm_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "symbol": "BTCUSDT",
                "event_type": "STATE_CHOP_STATE",
                "direction": "long",
                "horizon": "60m",
                "rule_template": "continuation",
                "q_value": 0.01,
                "after_cost_expectancy_per_trade": 0.002,
                "stressed_after_cost_expectancy_per_trade": 0.0015,
                "gate_bridge_tradable": "pass",
                "gate_multiplicity_strict": True,
            },
            {
                "candidate_id": "c2",
                "symbol": "BTCUSDT",
                "event_type": "STATE_CHOP_STATE",
                "direction": "long",
                "horizon": "60m",
                "rule_template": "mean_reversion",
                "q_value": 0.02,
                "after_cost_expectancy_per_trade": 0.001,
                "stressed_after_cost_expectancy_per_trade": 0.0008,
                "gate_bridge_tradable": "pass",
                "gate_multiplicity_strict": True,
            },
        ]
    ).to_parquet(edge_dir / "edge_candidates_normalized.parquet", index=False)

    pd.DataFrame(
        [
            {"candidate_id": "c1", "naive_expectancy": 0.0002, "event_count": 500},
            {"candidate_id": "c2", "naive_expectancy": 0.0001, "event_count": 500},
        ]
    ).to_parquet(phase2_dir / "naive_evaluation.parquet", index=False)

    confirm_path = confirm_dir / "confirm.json"
    confirm_path.write_text(
        json.dumps(
            {
                "matched_candidates": [
                    {
                        "symbol": "BTCUSDT",
                        "event_type": "STATE_CHOP_STATE",
                        "direction": "long",
                        "horizon": "60m",
                        "candidate_id_target": "t1",
                        "target_gate_pass": False,
                        "target_bridge_pass": False,
                        "target_q_value": 0.3,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    payload = svc.build_shadow_playbook_payload(
        data_root=data_root,
        run_id="r1",
        confirmatory_report_path=confirm_path,
    )

    assert payload["candidate_count"] == 2
    assert len(payload["playbook_groups"]) == 1
    group = payload["playbook_groups"][0]
    assert group["template_count"] == 2
    assert group["representative"]["candidate_id"] == "c1"
    assert group["status"] == "research_only"
    assert "confirmatory_gate_fail" in group["deploy_blockers"]


def test_build_shadow_playbook_applies_adjacent_survivorship_blocker(tmp_path):
    data_root = tmp_path / "data"
    edge_dir = data_root / "reports" / "edge_candidates" / "r1"
    phase2_dir = data_root / "reports" / "phase2" / "r1"
    adjacent_dir = data_root / "reports" / "adjacent"
    edge_dir.mkdir(parents=True, exist_ok=True)
    phase2_dir.mkdir(parents=True, exist_ok=True)
    adjacent_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "symbol": "BTCUSDT",
                "event_type": "STATE_CHOP_STATE",
                "direction": "long",
                "horizon": "60m",
                "rule_template": "continuation",
                "q_value": 0.01,
                "after_cost_expectancy_per_trade": 0.002,
                "stressed_after_cost_expectancy_per_trade": 0.0015,
                "gate_bridge_tradable": "pass",
                "gate_multiplicity_strict": True,
            }
        ]
    ).to_parquet(edge_dir / "edge_candidates_normalized.parquet", index=False)

    pd.DataFrame(
        [
            {"candidate_id": "c1", "naive_expectancy": 0.0002, "event_count": 500},
        ]
    ).to_parquet(phase2_dir / "naive_evaluation.parquet", index=False)

    adjacent_path = adjacent_dir / "adjacent.json"
    adjacent_path.write_text(
        json.dumps(
            {
                "candidate_rows": [
                    {
                        "symbol": "BTCUSDT",
                        "event_type": "STATE_CHOP_STATE",
                        "direction": "long",
                        "horizon": "60m",
                        "target_candidate_id": "t1",
                        "survived_adjacent_window": False,
                        "failure_reasons": ["after_cost_negative", "bridge_fail"],
                        "target_after_cost_expectancy_per_trade": -0.001,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    payload = svc.build_shadow_playbook_payload(
        data_root=data_root,
        run_id="r1",
        adjacent_survivorship_report_path=adjacent_path,
    )

    group = payload["playbook_groups"][0]
    assert group["status"] == "research_only"
    assert "adjacent_window_fail" in group["deploy_blockers"]
    assert group["adjacent_survivorship"]["matched"] is True
    assert group["adjacent_survivorship"]["failure_reasons"] == [
        "after_cost_negative",
        "bridge_fail",
    ]
