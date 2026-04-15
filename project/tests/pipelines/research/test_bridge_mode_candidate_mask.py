from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.research.bridge_evaluate_phase2 import (
    _load_candidates,
    _select_bridge_candidates,
)


def test_bridge_select_candidates_research_mode_uses_gate_phase2_research():
    df = pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "gate_phase2_research": True,
                "gate_phase2_final": False,
                "is_discovery": False,
            },
            {
                "candidate_id": "c2",
                "gate_phase2_research": False,
                "gate_phase2_final": True,
                "is_discovery": True,
            },
        ]
    )
    out = _select_bridge_candidates(full_candidates=df, mode="research")
    assert out["candidate_id"].tolist() == ["c1"]


def test_bridge_select_candidates_production_mode_preserves_strict_discovery_gate():
    df = pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "gate_phase2_research": True,
                "gate_phase2_final": True,
                "is_discovery": False,
            },
            {
                "candidate_id": "c2",
                "gate_phase2_research": False,
                "gate_phase2_final": True,
                "is_discovery": True,
            },
        ]
    )
    out = _select_bridge_candidates(full_candidates=df, mode="production")
    assert out["candidate_id"].tolist() == ["c2"]


def test_bridge_select_candidates_candidate_mask_all_bypasses_mode_filters():
    df = pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "gate_phase2_research": False,
                "gate_phase2_final": False,
                "is_discovery": False,
            },
            {
                "candidate_id": "c2",
                "gate_phase2_research": True,
                "gate_phase2_final": False,
                "is_discovery": False,
            },
        ]
    )
    out = _select_bridge_candidates(full_candidates=df, mode="production", candidate_mask="all")
    assert out["candidate_id"].tolist() == ["c1", "c2"]


def test_bridge_select_candidates_candidate_mask_final_forces_strict_final_gate():
    df = pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "gate_phase2_research": True,
                "gate_phase2_final": True,
                "is_discovery": False,
            },
            {
                "candidate_id": "c2",
                "gate_phase2_research": False,
                "gate_phase2_final": True,
                "is_discovery": True,
            },
            {
                "candidate_id": "c3",
                "gate_phase2_research": True,
                "gate_phase2_final": False,
                "is_discovery": True,
            },
        ]
    )
    out = _select_bridge_candidates(full_candidates=df, mode="research", candidate_mask="final")
    assert out["candidate_id"].tolist() == ["c2"]


def test_load_candidates_drops_stale_bridge_columns_for_reruns(tmp_path: Path):
    path = tmp_path / "phase2_candidates.csv"
    pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "event_type": "E1",
                "effective_lag_bars": 1,
                "horizon": "15m",
                "gate_phase2_final": True,
                "gate_bridge_tradable": True,
                "bridge_validation_after_cost_bps": 12.3,
                "bridge_validation_after_cost_bps_bridge": 7.7,
                "custom_bridge": 1,
            }
        ]
    ).to_csv(path, index=False)

    out = _load_candidates(path)
    assert out["gate_bridge_tradable"].tolist() == [False]
    assert "bridge_validation_after_cost_bps" not in out.columns
    assert "bridge_validation_after_cost_bps_bridge" not in out.columns
    assert "custom_bridge" not in out.columns
