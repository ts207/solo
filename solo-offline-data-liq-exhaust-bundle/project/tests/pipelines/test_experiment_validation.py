import pytest
from pathlib import Path
from types import SimpleNamespace
import yaml
import pandas as pd
from project.research.experiment_engine import build_experiment_plan
from project.research.experiment_engine_schema import (
    AgentExperimentRequest,
    ContextSelection,
    EvaluationConfig,
    InstrumentScope,
    PromotionConfig,
    RegistryBundle,
    SearchControl,
    TemplateSelection,
    TriggerSpace,
)
from project.research.experiment_engine_validators import _ordered_run_ids, _resolve_requested_event_ids


@pytest.fixture
def registry_root(tmp_path):
    reg_dir = tmp_path / "registries"
    reg_dir.mkdir()

    (reg_dir / "events.yaml").write_text(
        yaml.dump(
            {
                "events": {
                    "VOL_SHOCK": {
                        "enabled": False,
                        "instrument_classes": ["equities"],
                        "sequence_eligible": True,
                    },
                    "DEPTH_COLLAPSE": {
                        "enabled": False,
                        "instrument_classes": ["equities"],
                        "sequence_eligible": True,
                    },
                    "SEQ_VOL_COMP_THEN_BREAKOUT": {
                        "enabled": True,
                        "instrument_classes": ["equities"],
                        "sequence_eligible": False,
                    },
                }
            }
        )
    )

    (reg_dir / "states.yaml").write_text(
        yaml.dump({"states": {"HIGH_VOL_REGIME": {"enabled": False, "instrument_classes": ["crypto"]}}})
    )

    (reg_dir / "features.yaml").write_text(
        yaml.dump(
            {
                "features": {
                    "rsi_14": {"allowed_operators": [">", "<"], "instrument_classes": ["crypto"]}
                }
            }
        )
    )

    (reg_dir / "templates.yaml").write_text(
        yaml.dump(
            {
                "templates": {
                    "continuation": {
                        "enabled": False,
                        "supports_trigger_types": [
                            "FEATURE_PREDICATE",
                        ],
                    },
                    "breakout_followthrough": {
                        "enabled": False,
                        "supports_trigger_types": ["STATE"],
                    },
                }
            }
        )
    )

    (reg_dir / "contexts.yaml").write_text(yaml.dump({"context_dimensions": {}}))

    (reg_dir / "search_limits.yaml").write_text(
        yaml.dump(
            {
                "limits": {
                    "max_events_per_run": 10,
                    "max_templates_per_run": 10,
                    "max_horizons_per_run": 10,
                    "max_directions_per_run": 10,
                    "max_sequence_length": 3,
                }
            }
        )
    )

    (reg_dir / "detectors.yaml").write_text(yaml.dump({"detector_ownership": {}}))

    return reg_dir


def _make_config(tmp_path, **overrides):
    config = {
        "program_id": "test",
        "run_mode": "research",
        "instrument_scope": {
            "instrument_classes": ["crypto"],
            "symbols": ["BTCUSDT"],
            "timeframe": "1m",
            "start": "2024-01-01",
            "end": "2024-01-02",
        },
        "trigger_space": {"allowed_trigger_types": ["EVENT"], "events": {"include": ["VOL_SHOCK"]}},
        "templates": {"include": ["continuation"]},
        "evaluation": {"horizons_bars": [10], "directions": ["long"], "entry_lags": [1]},
        "contexts": {"include": {}},
        "search_control": {
            "max_hypotheses_total": 100,
            "max_hypotheses_per_template": 100,
            "max_hypotheses_per_event_family": 100,
        },
        "promotion": {"enabled": False},
    }
    # Deep merge overrides
    for k, v in overrides.items():
        if isinstance(v, dict) and k in config:
            config[k].update(v)
        else:
            config[k] = v

    p = tmp_path / "config.yaml"
    p.write_text(yaml.dump(config))
    return p


def test_validate_template_compatibility(registry_root, tmp_path):
    # breakout_followthrough supports EVENT/SEQUENCE canonically, not STATE.
    conf = _make_config(
        tmp_path,
        trigger_space={"allowed_trigger_types": ["STATE"], "states": {"include": ["HIGH_VOL_REGIME"]}},
        templates={"include": ["breakout_followthrough"]},
    )
    with pytest.raises(
        ValueError, match="Template 'breakout_followthrough' does not support trigger type 'STATE'"
    ):
        build_experiment_plan(conf, registry_root)


def test_validate_sequence_eligibility(registry_root, tmp_path):
    conf = _make_config(
        tmp_path,
        trigger_space={
            "allowed_trigger_types": ["SEQUENCE"],
            "sequences": {"include": [["VOL_SHOCK", "SEQ_VOL_COMP_THEN_BREAKOUT"]]},
        },
    )
    with pytest.raises(ValueError, match="Event 'SEQ_VOL_COMP_THEN_BREAKOUT' is not sequence-eligible"):
        build_experiment_plan(conf, registry_root)


def test_validate_feature_predicate(registry_root, tmp_path, monkeypatch):
    from project.research.semantic_registry_views import build_canonical_semantic_registry_views

    canonical = build_canonical_semantic_registry_views()
    canonical["templates"]["templates"]["continuation"]["supports_trigger_types"].append(
        "FEATURE_PREDICATE"
    )
    monkeypatch.setattr(
        "project.research.experiment_engine_schema.build_canonical_semantic_registry_views",
        lambda: canonical,
    )

    conf = _make_config(
        tmp_path,
        trigger_space={
            "allowed_trigger_types": ["FEATURE_PREDICATE"],
            "feature_predicates": {"include": [{"feature": "rsi_14", "operator": "=="}]},
        },
    )
    with pytest.raises(ValueError, match="Operator '==' not allowed for feature 'rsi_14'"):
        build_experiment_plan(conf, registry_root)


def test_validate_instrument_mismatch(registry_root, tmp_path):
    conf = _make_config(
        tmp_path,
        instrument_scope={"instrument_classes": ["equities"]},
        trigger_space={"allowed_trigger_types": ["EVENT"], "events": {"include": ["VOL_SHOCK"]}},
    )
    with pytest.raises(
        ValueError, match="Event 'VOL_SHOCK' is not allowed for instrument class 'equities'"
    ):
        build_experiment_plan(conf, registry_root)


def test_validate_entry_lag_zero_is_rejected(registry_root, tmp_path):
    conf = _make_config(
        tmp_path,
        evaluation={"horizons_bars": [10], "directions": ["long"], "entry_lags": [0]},
    )
    with pytest.raises(ValueError, match="entry_lags must be >= 1"):
        build_experiment_plan(conf, registry_root)


def test_registry_bundle_ignores_conflicting_runtime_semantic_mirrors(registry_root):
    registries = RegistryBundle(registry_root)

    assert registries.events["events"]["VOL_SHOCK"]["enabled"] is True
    assert registries.events["events"]["VOL_SHOCK"]["instrument_classes"] == ["crypto", "futures"]
    assert registries.templates["templates"]["continuation"]["enabled"] is True
    assert "EVENT" in registries.templates["templates"]["continuation"]["supports_trigger_types"]
    assert registries.states["states"]["HIGH_VOL_REGIME"]["enabled"] is True


def test_build_experiment_plan_canonicalizes_carry_state_aliases(registry_root, tmp_path):
    (registry_root / "contexts.yaml").write_text(
        yaml.dump(
            {
                "context_dimensions": {
                    "carry_state": {"allowed_values": ["funding_pos", "funding_neg"]},
                }
            }
        )
    )
    conf = _make_config(
        tmp_path,
        contexts={"include": {"carry_state": ["positive"]}},
    )

    plan = build_experiment_plan(conf, registry_root)

    assert plan.hypotheses
    assert {hyp.context["carry_state"] for hyp in plan.hypotheses if hyp.context} == {"funding_pos"}


def test_build_experiment_plan_uses_explicit_data_root(registry_root, tmp_path, monkeypatch):
    from project.core import config as config_mod

    wrong_root = tmp_path / "wrong_root"
    actual_root = tmp_path / "actual_root"
    halted_dir = actual_root / "artifacts" / "experiments" / "test"
    halted_dir.mkdir(parents=True)
    (halted_dir / "campaign_state.json").write_text('{"state": "halted_unsupported"}')
    monkeypatch.setattr(config_mod, "get_data_root", lambda: wrong_root)

    conf = _make_config(tmp_path)
    with pytest.raises(ValueError, match="cannot accept new proposals"):
        build_experiment_plan(conf, registry_root, data_root=actual_root)


def test_ordered_run_ids_prefers_created_at_over_row_encounter_order() -> None:
    df = pd.DataFrame(
        {
            "run_id": ["run_new", "run_old"],
            "created_at": ["2024-02-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00"],
        }
    )

    assert _ordered_run_ids(df) == ["run_old", "run_new"]


def test_resolve_requested_event_ids_expands_regime_only_requests_and_drops_non_authoritative_events(
    registry_root, monkeypatch
):
    request = AgentExperimentRequest(
        program_id="test",
        run_mode="research",
        description="",
        instrument_scope=InstrumentScope(
            instrument_classes=["crypto"],
            symbols=["BTCUSDT"],
            timeframe="1m",
            start="2024-01-01",
            end="2024-01-02",
        ),
        trigger_space=TriggerSpace(
            allowed_trigger_types=["EVENT"],
            events={"include": []},
            canonical_regimes=["LIQUIDITY_STRESS"],
        ),
        templates=TemplateSelection(include=["continuation"]),
        evaluation=EvaluationConfig(horizons_bars=[10], directions=["long"], entry_lags=[1]),
        contexts=ContextSelection(include={}),
        search_control=SearchControl(
            max_hypotheses_total=10,
            max_hypotheses_per_template=10,
            max_hypotheses_per_event_family=10,
        ),
        promotion=PromotionConfig(enabled=False),
    )
    registries = RegistryBundle(registry_root)

    class _RegistryStub:
        def get_event_ids_for_regime(self, regime: str, executable_only: bool = True):
            assert regime == "LIQUIDITY_STRESS"
            assert executable_only is True
            return ["VOL_SHOCK", "NOT_IN_AUTHORITY"]

        def get_event(self, event_id: str):
            return SimpleNamespace(
                canonical_regime="LIQUIDITY_STRESS",
                subtype="",
                phase="",
                evidence_mode="",
            )

    monkeypatch.setattr(
        "project.research.experiment_engine_validators.get_domain_registry",
        lambda: _RegistryStub(),
    )

    assert _resolve_requested_event_ids(request, registries) == ["VOL_SHOCK"]


def test_resolve_requested_event_ids_keeps_explicit_events_authoritative_when_regime_matches(
    registry_root, monkeypatch
):
    request = AgentExperimentRequest(
        program_id="test",
        run_mode="research",
        description="",
        instrument_scope=InstrumentScope(
            instrument_classes=["crypto"],
            symbols=["BTCUSDT"],
            timeframe="1m",
            start="2024-01-01",
            end="2024-01-02",
        ),
        trigger_space=TriggerSpace(
            allowed_trigger_types=["EVENT"],
            events={"include": ["VOL_SHOCK"]},
            canonical_regimes=["LIQUIDITY_STRESS"],
        ),
        templates=TemplateSelection(include=["continuation"]),
        evaluation=EvaluationConfig(horizons_bars=[10], directions=["long"], entry_lags=[1]),
        contexts=ContextSelection(include={}),
        search_control=SearchControl(
            max_hypotheses_total=10,
            max_hypotheses_per_template=10,
            max_hypotheses_per_event_family=10,
        ),
        promotion=PromotionConfig(enabled=False),
    )
    registries = RegistryBundle(registry_root)

    class _RegistryStub:
        def get_event_ids_for_regime(self, regime: str, executable_only: bool = True):
            assert regime == "LIQUIDITY_STRESS"
            assert executable_only is True
            return ["VOL_SHOCK", "DEPTH_COLLAPSE"]

        def get_event(self, event_id: str):
            return SimpleNamespace(
                canonical_regime="LIQUIDITY_STRESS",
                subtype="",
                phase="",
                evidence_mode="",
            )

    monkeypatch.setattr(
        "project.research.experiment_engine_validators.get_domain_registry",
        lambda: _RegistryStub(),
    )

    assert _resolve_requested_event_ids(request, registries) == ["VOL_SHOCK"]


def test_resolve_requested_event_ids_rejects_explicit_event_regime_mismatch(
    registry_root, monkeypatch
):
    request = AgentExperimentRequest(
        program_id="test",
        run_mode="research",
        description="",
        instrument_scope=InstrumentScope(
            instrument_classes=["crypto"],
            symbols=["BTCUSDT"],
            timeframe="1m",
            start="2024-01-01",
            end="2024-01-02",
        ),
        trigger_space=TriggerSpace(
            allowed_trigger_types=["EVENT"],
            events={"include": ["VOL_SHOCK"]},
            canonical_regimes=["VOLATILITY_EXPANSION"],
        ),
        templates=TemplateSelection(include=["continuation"]),
        evaluation=EvaluationConfig(horizons_bars=[10], directions=["long"], entry_lags=[1]),
        contexts=ContextSelection(include={}),
        search_control=SearchControl(
            max_hypotheses_total=10,
            max_hypotheses_per_template=10,
            max_hypotheses_per_event_family=10,
        ),
        promotion=PromotionConfig(enabled=False),
    )
    registries = RegistryBundle(registry_root)

    class _RegistryStub:
        def get_event_ids_for_regime(self, regime: str, executable_only: bool = True):
            assert regime == "VOLATILITY_EXPANSION"
            assert executable_only is True
            return ["DEPTH_COLLAPSE"]

        def get_event(self, event_id: str):
            return SimpleNamespace(
                canonical_regime="LIQUIDITY_STRESS",
                subtype="",
                phase="",
                evidence_mode="",
            )

    monkeypatch.setattr(
        "project.research.experiment_engine_validators.get_domain_registry",
        lambda: _RegistryStub(),
    )

    with pytest.raises(ValueError, match="does not belong to requested canonical_regimes"):
        _resolve_requested_event_ids(request, registries)
