from __future__ import annotations

from project.research.compile_strategy_blueprints import (
    _build_blueprint,
    _load_operator_registry,
)


def test_build_blueprint_records_bridge_embargo_in_lineage():
    row = {
        "candidate_id": "cand_1",
        "source_path": "data/reports/phase2/r1/VOL_SHOCK/phase2_candidates.csv",
        "promotion_track": "standard",
        "bridge_embargo_days_used": 3,
        "n_events": 150,
        "condition": "all",
        "action": "long",
        "event": "VOL_SHOCK",
    }

    bp, _ = _build_blueprint(
        run_id="r1",
        run_symbols=["BTCUSDT"],
        event_type="VOL_SHOCK",
        row=row,
        phase2_lookup={},
        stats={},
        fees_bps=2.0,
        slippage_bps=4.0,
        min_events=100,
        cost_config_digest="digest",
    )

    assert bp.lineage.bridge_embargo_days_used == 3


def test_blueprint_contains_ontology_contract():
    row = {
        "candidate_id": "cand_2",
        "source_path": "data/reports/phase2/r1/CROSS_VENUE_DESYNC/phase2_candidates.csv",
        "promotion_track": "standard",
        "bridge_embargo_days_used": 3,
        "n_events": 150,
        "condition": "all",
        "action": "long",
        "event": "CROSS_VENUE_DESYNC",
        "canonical_event_type": "CROSS_VENUE_DESYNC",
        "research_family": "INFORMATION_DESYNC",
        "canonical_family": "INFORMATION_DESYNC",
        "template_verb": "desync_repair",
        "state_id": "DESYNC_PERSISTENCE_STATE",
    }

    bp, _ = _build_blueprint(
        run_id="r1",
        run_symbols=["BTCUSDT"],
        event_type="CROSS_VENUE_DESYNC",
        row=row,
        phase2_lookup={},
        stats={},
        fees_bps=2.0,
        slippage_bps=4.0,
        min_events=100,
        cost_config_digest="digest",
        ontology_spec_hash_value="sha256:test",
        operator_registry=_load_operator_registry(),
    )

    assert bp.lineage.ontology_spec_hash == "sha256:test"
    assert bp.lineage.canonical_event_type == "CROSS_VENUE_DESYNC"
    assert bp.lineage.research_family == "INFORMATION_DESYNC"
    assert bp.lineage.canonical_family == "INFORMATION_DESYNC"
    assert bp.lineage.template_verb == "desync_repair"
    assert bp.lineage.operator_version


def test_build_blueprint_preserves_gate_audit_trail_tri_state():
    row = {
        "candidate_id": "cand_3",
        "source_path": "data/reports/phase2/r1/VOL_SHOCK/phase2_candidates.csv",
        "promotion_track": "fallback_only",
        "n_events": 150,
        "condition": "all",
        "action": "long",
        "event": "VOL_SHOCK",
        "gate_bridge_tradable": "fail",
        "gate_promo_statistical": "missing_evidence",
        "gate_after_cost_positive": True,
    }

    bp, _ = _build_blueprint(
        run_id="r1",
        run_symbols=["BTCUSDT"],
        event_type="VOL_SHOCK",
        row=row,
        phase2_lookup={},
        stats={},
        fees_bps=2.0,
        slippage_bps=4.0,
        min_events=100,
        cost_config_digest="digest",
    )

    trail = bp.lineage.constraints["gate_audit_trail"]
    assert trail["gate_bridge_tradable"] == "fail"
    assert trail["gate_promo_statistical"] == "missing_evidence"
    assert trail["gate_after_cost_positive"] == "pass"
