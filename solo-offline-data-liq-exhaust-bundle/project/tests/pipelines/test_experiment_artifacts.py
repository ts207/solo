import pytest
import pandas as pd
from pathlib import Path
import yaml
import json
import hashlib
from project.research.experiment_engine import (
    build_experiment_plan,
    export_experiment_artifacts,
)


@pytest.fixture
def test_env(tmp_path):
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
                        "requires_features": ["poison_feature"],
                    },
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
                        "supports_trigger_types": ["FEATURE_PREDICATE"],
                    }
                }
            }
        )
    )
    (reg_dir / "states.yaml").write_text(yaml.dump({"states": {}}))
    (reg_dir / "features.yaml").write_text(yaml.dump({"features": {}}))
    (reg_dir / "contexts.yaml").write_text(
        yaml.dump({"context_dimensions": {"session": {"allowed_values": ["open", "close"]}}})
    )
    (reg_dir / "search_limits.yaml").write_text(
        yaml.dump(
            {
                "limits": {
                    "max_events_per_run": 10,
                    "max_templates_per_run": 10,
                    "max_horizons_per_run": 10,
                    "max_directions_per_run": 10,
                    "max_sequence_length": 5,
                }
            }
        )
    )
    (reg_dir / "detectors.yaml").write_text(
        yaml.dump({"detector_ownership": {"VOL_SHOCK": "VolShockDetector"}})
    )

    config = {
        "program_id": "test_prog",
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
        "contexts": {"include": {"session": ["open"]}},
        "search_control": {
            "max_hypotheses_total": 1000,
            "max_hypotheses_per_template": 1000,
            "max_hypotheses_per_event_family": 1000,
        },
        "promotion": {"enabled": False},
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config))

    out_dir = tmp_path / "artifacts"
    out_dir.mkdir()

    return config_path, reg_dir, out_dir


def test_artifacts_contract(test_env):
    config_path, registry_root, out_dir = test_env
    plan = build_experiment_plan(config_path, registry_root, out_dir=out_dir)

    assert (out_dir / "request.yaml").exists()
    assert (out_dir / "request_hash.txt").exists()
    assert (out_dir / "registry_hash.txt").exists()
    assert (out_dir / "registry_sources.json").exists()
    assert (out_dir / "validated_plan.json").exists()
    assert (out_dir / "execution_requirements.json").exists()
    assert (out_dir / "expanded_hypotheses.parquet").exists()

    # Check execution reqs
    reqs = json.loads((out_dir / "execution_requirements.json").read_text())
    registry_sources = json.loads((out_dir / "registry_sources.json").read_text())
    assert "detectors" in reqs
    assert "features" in reqs
    assert "state_engines" in reqs
    assert reqs["detectors"] == ["VolShockDetector"]
    assert "spec/events/VOL_SHOCK.yaml" in registry_sources["events"]
    assert registry_sources["templates"] == ["spec/templates/registry.yaml"]
    assert "project/configs/registries/events.yaml" not in json.dumps(registry_sources)

    # Check hypotheses parquet schema
    df = pd.read_parquet(out_dir / "expanded_hypotheses.parquet")
    assert len(df) == 1
    expected_cols = {
        "hypothesis_id",
        "trigger_type",
        "trigger_payload",
        "template_id",
        "horizon",
        "direction",
        "entry_lag",
        "context_slice",
    }
    assert expected_cols.issubset(df.columns)

    assert df.iloc[0]["trigger_type"] == "event"
    assert "session" in df.iloc[0]["context_slice"]


def test_determinism(test_env):
    config_path, registry_root, out_dir = test_env
    plan1 = build_experiment_plan(config_path, registry_root, out_dir=out_dir)
    id1 = plan1.hypotheses[0].hypothesis_id()

    # Run again, verify identical ID
    plan2 = build_experiment_plan(config_path, registry_root, out_dir=out_dir)
    id2 = plan2.hypotheses[0].hypothesis_id()

    assert id1 == id2


from unittest.mock import patch, MagicMock


@pytest.fixture(autouse=True)
def mock_domain_registry():
    with patch("project.domain.hypotheses.get_domain_registry") as mock_get:
        mock_reg = MagicMock()
        mock_reg.has_event.return_value = True
        mock_reg.valid_state_ids = ["S1", "HIGH_VOL", "A_EVENT", "B_EVENT"]
        mock_get.return_value = mock_reg
        yield mock_reg


def test_interaction_commutativity(mock_domain_registry):
    from project.domain.hypotheses import TriggerSpec

    # AND(A, B) should canonicalize to same ID as AND(B, A)
    t1 = TriggerSpec.interaction("I1", "A_EVENT", "B_EVENT", "AND")
    t2 = TriggerSpec.interaction("I1", "B_EVENT", "A_EVENT", "AND")

    assert t1.to_dict() == t2.to_dict()


def test_feature_predicate_typing():
    from project.domain.hypotheses import TriggerSpec

    t1 = TriggerSpec.feature_predicate("F1", ">", 2)
    t2 = TriggerSpec.feature_predicate("F1", ">", 2.0)
    t3 = TriggerSpec.feature_predicate("F1", ">", "2.0")

    assert t1.to_dict() == t2.to_dict()
    assert t2.to_dict() == t3.to_dict()
