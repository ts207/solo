from __future__ import annotations

from pathlib import Path

import yaml


def test_proxy_canonical_events_have_evidence_tier_in_registry():
    """TICKET-014: proxy canonical events must have evidence_tier in canonical_event_registry.yaml."""
    from project.spec_registry import load_yaml_relative

    registry = load_yaml_relative("spec/events/canonical_event_registry.yaml")
    proxy_events = {
        "ABSORPTION_EVENT",
        "DEPTH_COLLAPSE",
        "ORDERFLOW_IMBALANCE_SHOCK",
        "SWEEP_STOPRUN",
        "FORCED_FLOW_EXHAUSTION",
    }
    event_metadata = registry.get("event_metadata", {})
    for event_type in proxy_events:
        assert event_type in event_metadata, f"{event_type} not in registry event_metadata"
        tier = event_metadata[event_type].get("evidence_tier")
        assert tier == "proxy", f"{event_type} expected evidence_tier=proxy, got {tier!r}"


def test_proposal_validation_warns_on_proxy_tier_events():
    """TICKET-014: validating a proposal with proxy-tier events must return proxy warnings."""
    from project.research.agent_io.proposal_schema import validate_proposal_with_warnings

    payload = {
        "program_id": "test_proxy",
        "objective_name": "test",
        "symbols": ["BTCUSDT"],
        "timeframe": "5m",
        "start": "2024-01-01",
        "end": "2024-06-01",
        "hypothesis": {
            "anchor": {"type": "event", "event_id": "ABSORPTION_EVENT"},
            "filters": {},
            "sampling_policy": {"entry_lag_bars": 1},
            "template": {"id": "continuation"},
            "direction": "long",
            "horizon_bars": 12,
        },
    }
    warnings = validate_proposal_with_warnings(payload)
    proxy_warnings = [w for w in warnings if "proxy" in w.lower() and "ABSORPTION_EVENT" in w]
    assert proxy_warnings, f"Expected proxy-tier warning for ABSORPTION_EVENT; got: {warnings}"


def test_proposal_schema_accepts_canonical_regimes_without_explicit_events():
    from project.research.agent_io.proposal_schema import load_agent_proposal

    payload = {
        "program_id": "regime_campaign",
        "objective": "test",
        "symbols": ["BTCUSDT"],
        "timeframe": "5m",
        "start": "2024-01-01",
        "end": "2024-06-01",
        "trigger_space": {
            "allowed_trigger_types": ["EVENT"],
            "canonical_regimes": ["LIQUIDITY_STRESS"],
            "subtypes": ["liquidity_stress"],
            "phases": ["shock"],
            "evidence_modes": ["direct"],
        },
        "templates": ["continuation"],
        "horizons_bars": [12],
        "directions": ["long"],
        "entry_lags": [1],
    }

    proposal = load_agent_proposal(payload)

    assert proposal.trigger_space["canonical_regimes"] == ["LIQUIDITY_STRESS"]
    assert proposal.trigger_space["events"] == {}


def test_proposal_schema_canonicalizes_carry_state_aliases():
    from project.research.agent_io.proposal_schema import load_agent_proposal

    payload = {
        "program_id": "carry_campaign",
        "objective": "test",
        "symbols": ["BTCUSDT"],
        "timeframe": "5m",
        "start": "2024-01-01",
        "end": "2024-06-01",
        "trigger_space": {
            "allowed_trigger_types": ["EVENT"],
            "events": {"include": ["VOL_SHOCK"]},
        },
        "templates": ["continuation"],
        "contexts": {"carry_state": ["positive", "negative", "neutral"]},
        "horizons_bars": [12],
        "directions": ["long"],
        "entry_lags": [1],
    }

    proposal = load_agent_proposal(payload)

    assert proposal.contexts["carry_state"] == ["funding_pos", "funding_neg", "neutral"]


def test_synthetic_proposal_carries_explicit_pipeline_overrides(tmp_path: Path):
    from project.research.agent_io.proposal_schema import load_agent_proposal

    proposal_path = tmp_path / "demo_synthetic_fast.yaml"
    proposal_path.write_text(
        yaml.safe_dump(
            {
                "program_id": "demo_synthetic_fast",
                "objective": "synthetic test",
                "symbols": ["BTCUSDT"],
                "timeframe": "5m",
                "start": "2024-01-01",
                "end": "2024-01-31",
                "trigger_space": {
                    "allowed_trigger_types": ["EVENT"],
                    "events": {"include": ["VOL_SHOCK"]},
                },
                "templates": ["continuation"],
                "horizons_bars": [12],
                "directions": ["long"],
                "entry_lags": [1],
                "discovery_profile": "synthetic",
                "phase2_gate_profile": "synthetic",
                "search_spec": "synthetic_truth",
                "config_overlays": ["project/configs/golden_synthetic_discovery_fast.yaml"],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    proposal = load_agent_proposal(proposal_path)

    assert proposal.discovery_profile == "synthetic"
    assert proposal.phase2_gate_profile == "synthetic"
    assert proposal.search_spec == "synthetic_truth"
    assert proposal.config_overlays == ["project/configs/golden_synthetic_discovery_fast.yaml"]
