from __future__ import annotations

import pandas as pd

from project.research.condition_key_contract import CANONICAL_EVENT_JOIN_KEYS
from project.research.discovery_kpis import compute_discovery_quality_kpis
from project.research.gating_statistics import apply_statistical_gates
from project.research.multiplicity import apply_multiplicity_controls, make_family_id


def test_compute_discovery_quality_kpis_prefers_research_family() -> None:
    candidates = pd.DataFrame(
        [
            {"candidate_id": "a", "q_value": 0.01, "research_family": "FLOW", "canonical_event_type": "E1"},
            {"candidate_id": "b", "q_value": 0.02, "research_family": "FLOW", "canonical_event_type": "E2"},
            {"candidate_id": "c", "q_value": 0.03, "research_family": "VOL", "canonical_event_type": "E3"},
        ]
    )

    out = compute_discovery_quality_kpis(candidates)

    assert out["diversity"]["num_families_discovered"] == 2
    assert out["diversity"]["family_concentration"] == 2 / 3


def test_canonical_event_join_keys_include_research_family() -> None:
    assert "research_family" in CANONICAL_EVENT_JOIN_KEYS


def test_apply_statistical_gates_builds_group_key_from_research_family() -> None:
    candidates = pd.DataFrame(
        [
            {
                "candidate_id": "a",
                "p_value": 0.01,
                "research_family": "FLOW_EXHAUSTION",
                "event_type": "ABSORPTION_PROXY",
                "template_verb": "mean_reversion",
                "horizon": "60m",
            }
        ]
    )

    out = apply_statistical_gates(
        candidates,
        {"group_columns": ("canonical_family", "event_type", "template_verb", "horizon")},
    )

    assert out.loc[0, "group_key"] == "FLOW_EXHAUSTION::ABSORPTION_PROXY::mean_reversion::60m"


def test_make_family_id_accepts_research_family_alias() -> None:
    family_id = make_family_id(
        "BTCUSDT",
        "VOL_SHOCK",
        "continuation",
        "60m",
        "",
        research_family="VOLATILITY_TRANSITION",
    )

    assert family_id.startswith("BTCUSDT_VOLATILITY_TRANSITION::VOL_SHOCK")


def test_apply_multiplicity_controls_cluster_key_prefers_research_family() -> None:
    frame = pd.DataFrame(
        [
            {
                "candidate_id": "a",
                "family_id": "fam_a",
                "p_value_for_fdr": 0.01,
                "symbol": "BTCUSDT",
                "research_family": "LIQUIDITY_DISLOCATION",
                "event_type": "DEPTH_STRESS_PROXY",
                "horizon": "60m",
                "multiplicity_pool_eligible": True,
            }
        ]
    )

    out = apply_multiplicity_controls(frame, max_q=0.05)

    assert out.loc[0, "family_cluster_id"] == "BTCUSDT_LIQUIDITY_DISLOCATION_60m_"
