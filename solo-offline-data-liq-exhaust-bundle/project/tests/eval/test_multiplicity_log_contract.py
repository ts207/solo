from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.eval import multiplicity


def test_update_program_hypothesis_log_normalizes_mixed_direction_types(tmp_path: Path):
    data_root = tmp_path
    program_id = "prog_1"
    existing_path = multiplicity.get_program_hypothesis_log_path(program_id, data_root)
    existing_path.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {"hypothesis_id": "old_long", "p_value": 0.01, "direction": 1.0},
            {"hypothesis_id": "old_short", "p_value": 0.02, "direction": -1.0},
        ]
    ).to_parquet(existing_path, index=False)

    new_hypotheses = pd.DataFrame(
        [
            {"hypothesis_id": "new_long", "p_value": 0.03, "direction": "long"},
            {"hypothesis_id": "new_short", "p_value": 0.04, "direction": "short"},
        ]
    )

    combined = multiplicity.update_program_hypothesis_log(
        program_id=program_id,
        data_root=data_root,
        new_hypotheses=new_hypotheses,
    )

    assert list(combined["direction"]) == ["long", "short", "long", "short"]

    persisted = pd.read_parquet(existing_path)
    assert list(persisted["direction"]) == ["long", "short", "long", "short"]


def test_update_program_hypothesis_log_normalizes_mixed_gate_bool_types(tmp_path: Path):
    data_root = tmp_path
    program_id = "prog_bool"
    existing_path = multiplicity.get_program_hypothesis_log_path(program_id, data_root)
    existing_path.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "hypothesis_id": "old_pass",
                "p_value": 0.01,
                "gate_bridge_tradable": "True",
            },
            {
                "hypothesis_id": "old_fail",
                "p_value": 0.02,
                "gate_bridge_tradable": "False",
            },
        ]
    ).to_parquet(existing_path, index=False)

    new_hypotheses = pd.DataFrame(
        [
            {
                "hypothesis_id": "new_pass",
                "p_value": 0.03,
                "gate_bridge_tradable": True,
            },
            {
                "hypothesis_id": "new_missing",
                "p_value": 0.04,
                "gate_bridge_tradable": None,
            },
        ]
    )

    combined = multiplicity.update_program_hypothesis_log(
        program_id=program_id,
        data_root=data_root,
        new_hypotheses=new_hypotheses,
    )

    assert str(combined["gate_bridge_tradable"].dtype) == "boolean"
    assert combined["gate_bridge_tradable"].tolist() == [True, False, True, pd.NA]

    persisted = pd.read_parquet(existing_path)
    assert str(persisted["gate_bridge_tradable"].dtype) == "boolean"
    assert persisted["gate_bridge_tradable"].tolist() == [True, False, True, pd.NA]


def test_apply_program_multiplicity_control_preserves_candidate_level_q_metrics(tmp_path: Path):
    data_root = tmp_path
    program_id = "prog_2"
    existing_path = multiplicity.get_program_hypothesis_log_path(program_id, data_root)
    existing_path.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {"hypothesis_id": "hyp_a", "p_value": 0.08},
            {"hypothesis_id": "hyp_b", "p_value": 0.12},
            {"hypothesis_id": "hyp_c", "p_value": 0.20},
        ]
    ).to_parquet(existing_path, index=False)

    candidates = pd.DataFrame(
        [
            {
                "hypothesis_id": "hyp_a",
                "p_value": 0.08,
                "q_value": 0.04,
                "q_value_family": 0.04,
                "q_value_cluster": 0.03,
                "q_value_by": 0.06,
            },
            {
                "hypothesis_id": "hyp_b",
                "p_value": 0.12,
                "q_value": 0.07,
                "q_value_family": 0.07,
                "q_value_cluster": 0.05,
                "q_value_by": 0.09,
            },
        ]
    )

    out = multiplicity.apply_program_multiplicity_control(
        candidates=candidates,
        program_id=program_id,
        data_root=data_root,
        alpha=0.10,
    )

    assert "q_value_program" in out.columns
    assert list(out["q_value_by"]) == [0.06, 0.09]
    assert list(out["q_value_cluster"]) == [0.03, 0.05]
    assert list(out["q_value"]) == [0.04, 0.07]
