from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

import pytest

from project.domain.compiled_registry import get_domain_registry, refresh_domain_registry
from project.domain.hypotheses import HypothesisSpec, TriggerSpec, TriggerType
from project.domain.models import (
    DomainRegistry,
    EventDefinition,
    RegimeDefinition,
    StateDefinition,
    TemplateOperatorDefinition,
)
from project.events.event_aliases import EVENT_ALIASES, resolve_event_alias
from project.spec_registry.loaders import clear_caches


@dataclass
class FakeRegistry:
    event_ids: tuple[str, ...]
    state_ids: tuple[str, ...]
    valid_state_ids: tuple[str, ...]
    def has_event(self, event_id: str) -> bool:
        return str(event_id).strip().upper() in self.event_ids


def test_event_alias_resolution_handles_known_and_unknown_values():
    assert resolve_event_alias("basis_dislocation") == "BASIS_DISLOC"
    assert resolve_event_alias("depth_collapse") == "DEPTH_COLLAPSE"
    assert resolve_event_alias("custom_event") == "CUSTOM_EVENT"
    assert EVENT_ALIASES["ABSORPTION_EVENT"] == "ABSORPTION_PROXY"


def test_trigger_spec_validation_label_and_dict_roundtrip(monkeypatch):
    fake = FakeRegistry(event_ids=("E1", "E2"), state_ids=("S1", "S2"), valid_state_ids=("S1", "S2"))

    monkeypatch.setattr("project.domain.hypotheses.get_domain_registry", lambda: fake)

    ev = TriggerSpec.event("e1")
    assert ev.trigger_type == TriggerType.EVENT
    assert ev.label() == "event:E1"
    assert ev.to_dict()["event_id"] == "E1"

    st = TriggerSpec.state("s1", active=False)
    assert st.label() == "state:S1:inactive"
    tr = TriggerSpec.transition("s2", "s1")
    assert tr.to_dict()["to_state"] == "S1"

    fp = TriggerSpec.feature_predicate("rv_pct_5", ">=", 1.5)
    assert fp.label() == "pred:RV_PCT_5>=1.5"

    seq = TriggerSpec.sequence("seq1", ["e1", "e2"], max_gap=[4])
    assert seq.to_dict()["sequence_id"] == "SEQ1"

    inter = TriggerSpec.interaction("motif1", "e2", "e1", "AND", lag=3)
    assert inter.left == "E1" and inter.right == "E2"

    round_trip = TriggerSpec.from_dict(inter.to_dict())
    assert round_trip.label() == inter.label()

    directed = TriggerSpec.interaction(
        "motif2",
        "e1",
        "e2",
        "confirm",
        lag=6,
        left_direction="up",
    )
    assert directed.to_dict()["left_direction"] == "up"
    assert TriggerSpec.from_dict(directed.to_dict()).left_direction == "up"

    with pytest.raises(ValueError):
        TriggerSpec.feature_predicate("rv", "!!", 1.0)
    with pytest.raises(ValueError):
        TriggerSpec.sequence("seqx", ["e1"], max_gap=[1])
    with pytest.raises(ValueError):
        TriggerSpec.interaction("m", "unknown", "e1", "or")
    with pytest.raises(ValueError):
        TriggerSpec.interaction("m2", "e1", "e2", "confirm", left_direction="sideways")


def test_hypothesis_spec_hash_and_roundtrip(monkeypatch):
    fake = FakeRegistry(event_ids=("E1",), state_ids=("S1",), valid_state_ids=("S1",))
    monkeypatch.setattr("project.domain.hypotheses.get_domain_registry", lambda: fake)

    trigger = TriggerSpec.event("e1")
    feature_condition = TriggerSpec.feature_predicate("rv", ">", 0.5)
    hyp = HypothesisSpec(
        trigger=trigger,
        direction="Long",
        horizon="1h",
        template_id="templ_a",
        context={"b": "2", "a": "1"},
        feature_condition=feature_condition,
        entry_lag=2,
        cost_profile="premium",
        objective_profile="sharpe",
    )
    assert hyp.label().startswith("event:E1|long|1h|templ_a")
    assert hyp.context == {"a": "1", "b": "2"}
    assert hyp.hypothesis_id().startswith("hyp_")
    assert hyp.hypothesis_id() == HypothesisSpec.from_dict(hyp.to_dict()).hypothesis_id()

    expected_prefix = "hyp_" + hashlib.sha256(json.dumps(hyp.to_dict(), sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:20]
    assert hyp.hypothesis_id() == expected_prefix


def test_domain_registry_helpers_work_on_small_synthetic_registry():
    registry = DomainRegistry(
        unified_payload={
            "defaults": {"templates": ["t1", "t2"]},
            "families": {"FAM": {"templates": ["t2", "t3"], "x": 1}},
        },
        event_definitions={
            "E1": EventDefinition(
                event_type="E1",
                canonical_family="FAM",
                canonical_regime="FAM",
                event_kind="market_event",
                reports_dir="reports",
                events_file="e1.parquet",
                signal_column="sig1",
                parameters={},
                raw={"k": 1},
                spec_path="spec/e1.yaml",
            ),
            "E2": EventDefinition(
                event_type="E2",
                canonical_family="FAM",
                canonical_regime="FAM",
                event_kind="market_event",
                reports_dir="reports",
                events_file="e2.parquet",
                signal_column="sig2",
                parameters={},
                raw={"k": 2},
                spec_path="spec/e2.yaml",
            ),
        },
        state_definitions={
            "S1": StateDefinition("S1", "SFAM", "SRC", {"a": 1}),
            "S2": StateDefinition("S2", "SFAM", "SRC", {"b": 2}),
        },
        template_operator_definitions={
            "templ": TemplateOperatorDefinition("templ", ("FAM",), "expression_template", {"a": 1}),
            "t2": TemplateOperatorDefinition(
                "t2",
                ("FAM",),
                "filter_template",
                {"feature": "x", "operator": ">=", "threshold": 1.0},
            ),
            "t3": TemplateOperatorDefinition("t3", ("FAM",), "execution_template", {"a": 2}),
        },
        regime_definitions={
            "FAM": RegimeDefinition(canonical_regime="FAM", bucket="trade_generating"),
        },
        gates_spec={"gate": 1},
        unified_registry_path="spec/events/event_registry_unified.yaml",
        template_registry_payload={
            "defaults": {"templates": ["t1", "t2"]},
            "families": {"FAM": {"templates": ["t2", "t3"]}},
            "filter_templates": {
                "t2": {"feature": "x", "operator": ">=", "threshold": 1.0},
            },
        },
        family_registry_payload={"event_families": {"FAM": {"searchable": True}}, "state_families": {"SFAM": {"searchable": False}}},
        context_state_map={("fam", "low"): "S1", ("fam", "high"): "S2"},
        searchable_event_families=("FAM",),
        searchable_state_families=(),
        state_aliases=("ALIAS1",),
        stress_scenarios=({"name": "stress", "feature": "rv", "operator": ">="},),
        kill_switch_candidate_features=("a", "b"),
        sequence_definitions=({"name": "seq", "events": ["E1", "E2"]},),
        interaction_definitions=({"name": "motif", "left": "E1", "right": "S1", "op": "and"},),
    )
    assert registry.has_event("e1")
    assert registry.get_event("E2").signal_column == "sig2"
    assert registry.has_state("s1")
    assert registry.get_operator("templ").compatible_families == ("FAM",)
    assert registry.operator_rows()["templ"]["a"] == 1
    assert registry.family_templates("fam") == ("t2", "t3")
    assert registry.family_defaults("fam")["x"] == 1
    assert registry.defaults()["templates"] == ["t1", "t2"]
    assert registry.event_row("e1")["k"] == 1
    assert registry.event_spec_path("e2").endswith("e2.yaml")
    assert registry.get_event_ids_for_family("fam") == ("E1", "E2")
    assert registry.get_state_ids_for_family("sfam") == ("S1", "S2")
    assert registry.resolve_context_state("fam", "low") == "S1"
    assert registry.context_labels_for_family("fam") == ("high", "low")
    assert registry.state_ids == ("S1", "S2")
    assert registry.event_ids == ("E1", "E2")
    assert registry.valid_state_ids == ("ALIAS1", "S1", "S2")
    assert registry.default_templates() == ("t1", "t2")
    assert registry.default_hypothesis_templates() == ("t1",)
    assert registry.family_filter_templates("fam")[0]["feature"] == "x"
    assert registry.family_execution_templates("fam") == ("t3",)
    assert registry.family_hypothesis_templates("fam") == ()
    assert registry.template_kind("t2") == "filter_template"
    assert registry.is_filter_template("t2") is True
    assert registry.is_hypothesis_template("t2") is False
    assert registry.is_expression_template("templ") is True
    assert registry.default_entry_lags() == (1, 2)
    assert registry.stress_scenario_rows()[0]["name"] == "stress"
    assert registry.kill_switch_candidates() == ["a", "b"]
    assert registry.sequence_rows()[0]["name"] == "seq"
    assert registry.interaction_rows()[0]["name"] == "motif"


def test_refresh_domain_registry_clears_caches(monkeypatch):
    clear_caches()
    called = {"n": 0}

    def fake_load():
        called["n"] += 1
        return DomainRegistry(
            unified_payload={"defaults": {}, "families": {}},
            event_definitions={},
            state_definitions={},
            template_operator_definitions={},
            regime_definitions={},
            gates_spec={},
            unified_registry_path="x",
        )

    monkeypatch.setattr("project.domain.compiled_registry.load_domain_registry_from_graph", fake_load)
    monkeypatch.setattr("project.domain.compiled_registry.clear_caches", lambda: None)
    refresh_domain_registry()
    assert called["n"] == 1
    get_domain_registry.cache_clear()
