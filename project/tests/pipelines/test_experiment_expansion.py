
import pytest
import yaml

from project.research.experiment_engine import build_experiment_plan


@pytest.fixture
def registry_root(tmp_path):
    reg_dir = tmp_path / "registries"
    reg_dir.mkdir()

    (reg_dir / "events.yaml").write_text(
        yaml.dump(
            {
                "events": {
                    "E1": {
                        "enabled": True,
                        "instrument_classes": ["crypto"],
                        "sequence_eligible": True,
                        "requires_features": ["f1"],
                    },
                    "E2": {
                        "enabled": True,
                        "instrument_classes": ["crypto"],
                        "sequence_eligible": True,
                        "requires_features": ["f2"],
                    },
                }
            }
        )
    )

    (reg_dir / "states.yaml").write_text(
        yaml.dump({"states": {"S1": {"enabled": True, "instrument_classes": ["crypto"]}}})
    )

    (reg_dir / "features.yaml").write_text(
        yaml.dump(
            {
                "features": {
                    "f1": {"allowed_operators": [">"], "instrument_classes": ["crypto"]},
                    "f2": {"allowed_operators": [">"], "instrument_classes": ["crypto"]},
                    "p1": {"allowed_operators": [">"], "instrument_classes": ["crypto"]},
                }
            }
        )
    )

    (reg_dir / "templates.yaml").write_text(
        yaml.dump(
            {
                "templates": {
                    "tpl": {
                        "enabled": True,
                        "supports_trigger_types": [
                            "EVENT",
                            "STATE",
                            "SEQUENCE",
                            "FEATURE_PREDICATE",
                            "INTERACTION",
                            "TRANSITION",
                        ],
                    }
                }
            }
        )
    )

    (reg_dir / "contexts.yaml").write_text(yaml.dump({"context_dimensions": {}}))
    (reg_dir / "search_limits.yaml").write_text(
        yaml.dump(
            {
                "limits": {
                    "max_events_per_run": 100,
                    "max_templates_per_run": 100,
                    "max_horizons_per_run": 10,
                    "max_directions_per_run": 10,
                    "max_sequence_length": 5,
                }
            }
        )
    )
    (reg_dir / "detectors.yaml").write_text(
        yaml.dump({"detector_ownership": {"E1": "D1", "E2": "D2"}})
    )

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
        "trigger_space": {"allowed_trigger_types": ["EVENT"], "events": {"include": ["E1"]}},
        "templates": {"include": ["tpl"]},
        "evaluation": {"horizons_bars": [10], "directions": ["long"], "entry_lags": [1]},
        "contexts": {"include": {}},
        "search_control": {
            "max_hypotheses_total": 1000,
            "max_hypotheses_per_template": 1000,
            "max_hypotheses_per_event_family": 1000,
        },
        "promotion": {"enabled": False},
    }
    for k, v in overrides.items():
        if isinstance(v, dict) and k in config:
            config[k].update(v)
        else:
            config[k] = v
    p = tmp_path / "config.yaml"
    p.write_text(yaml.dump(config))
    return p


from unittest.mock import MagicMock, patch


@pytest.fixture(autouse=True)
def mock_domain_registry():
    with patch("project.domain.hypotheses.get_domain_registry") as mock_get:
        mock_reg = MagicMock()
        # Make has_event return True for anything we use in tests
        mock_reg.has_event.return_value = True
        # Make valid_state_ids return what we need
        mock_reg.valid_state_ids = ["S1", "HIGH_VOL"]
        mock_get.return_value = mock_reg
        yield mock_reg


def test_expand_and_resolve_sequence(registry_root, tmp_path):
    conf = _make_config(
        tmp_path,
        trigger_space={
            "allowed_trigger_types": ["SEQUENCE"],
            "sequences": {"include": [["E1", "E2"]], "max_gaps_bars": [3]},
        },
    )
    plan = build_experiment_plan(conf, registry_root)
    assert len(plan.hypotheses) == 1
    assert plan.hypotheses[0].trigger.trigger_type == "sequence"
    assert plan.hypotheses[0].trigger.events == ["E1", "E2"]
    assert plan.hypotheses[0].trigger.max_gap == [3]

    assert "EventSequenceDetector" in plan.required_detectors
    assert "D1" in plan.required_detectors
    assert "D2" in plan.required_detectors
    assert "f1" in plan.required_features
    assert "f2" in plan.required_features


def test_expand_and_resolve_state_and_transition(registry_root, tmp_path):
    conf = _make_config(
        tmp_path,
        trigger_space={
            "allowed_trigger_types": ["STATE", "TRANSITION"],
            "states": {"include": ["S1"]},
            "transitions": {
                "include": [{"from_state": "S1", "to_state": "S1"}]
            },  # Simplified self-transition for test
        },
    )
    plan = build_experiment_plan(conf, registry_root)
    # 1 state + 1 transition = 2 hypotheses
    assert len(plan.hypotheses) == 2
    assert plan.required_states == ["S1"]


def test_expand_and_resolve_feature_predicate(registry_root, tmp_path):
    conf = _make_config(
        tmp_path,
        trigger_space={
            "allowed_trigger_types": ["FEATURE_PREDICATE"],
            "feature_predicates": {
                "include": [{"feature": "p1", "operator": ">", "threshold": 0.5}]
            },
        },
    )
    plan = build_experiment_plan(conf, registry_root)
    assert len(plan.hypotheses) == 1
    assert plan.required_features == ["P1"]


def test_expand_and_resolve_interaction(registry_root, tmp_path):
    conf = _make_config(
        tmp_path,
        trigger_space={
            "allowed_trigger_types": ["INTERACTION"],
            "interactions": {"include": [{"left": "E1", "right": "S1", "op": "AND"}]},
        },
    )
    plan = build_experiment_plan(conf, registry_root)
    assert len(plan.hypotheses) == 1
    assert "EventInteractionDetector" in plan.required_detectors
    assert "D1" in plan.required_detectors
    assert "f1" in plan.required_features
    assert "S1" in plan.required_states


def test_expand_and_resolve_interaction_with_left_direction(registry_root, tmp_path):
    conf = _make_config(
        tmp_path,
        trigger_space={
            "allowed_trigger_types": ["INTERACTION"],
            "interactions": {
                "include": [
                    {
                        "left": "E1",
                        "right": "E2",
                        "op": "CONFIRM",
                        "left_direction": "up",
                    }
                ]
            },
        },
    )
    plan = build_experiment_plan(conf, registry_root)
    assert len(plan.hypotheses) == 1
    trigger = plan.hypotheses[0].trigger
    assert trigger.left_direction == "up"
    assert trigger.right_direction is None


def test_expand_event_trigger_with_event_direction(registry_root, tmp_path):
    conf = _make_config(
        tmp_path,
        trigger_space={
            "allowed_trigger_types": ["EVENT"],
            "events": {"include": [{"event_id": "E1", "event_direction": "up"}]},
        },
    )
    plan = build_experiment_plan(conf, registry_root)
    assert len(plan.hypotheses) == 1
    trigger = plan.hypotheses[0].trigger
    assert trigger.event_id == "E1"
    assert trigger.event_direction == "up"
