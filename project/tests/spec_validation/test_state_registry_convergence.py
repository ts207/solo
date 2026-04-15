from __future__ import annotations

from pathlib import Path

import yaml

from project.domain.registry_loader import build_domain_graph_payload
from project.spec_registry import load_state_family_registry, load_state_registry, resolve_relative_spec_path
from project.spec_validation import loaders as spec_validation_loaders


def _load_yaml(path: Path) -> dict:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def test_generated_state_read_models_declare_canonical_authored_source() -> None:
    state_registry = load_state_registry()
    state_families = load_state_family_registry()
    runtime_states = _load_yaml(Path("project/configs/registries/states.yaml"))
    grammar_registry = _load_yaml(resolve_relative_spec_path("spec/grammar/state_registry.yaml"))

    assert state_registry["metadata"]["authored_sources"] == ["spec/states/*.yaml"]
    assert state_families["metadata"]["authored_sources"] == ["spec/states/*.yaml"]
    assert runtime_states["metadata"]["authored_source"] == "spec/states/*.yaml"
    assert grammar_registry["metadata"]["authored_source"] == "spec/states/*.yaml"


def test_every_materialized_state_has_exactly_one_canonical_authored_spec_file() -> None:
    payload = load_state_registry()

    for row in payload["states"]:
        state_id = row["state_id"]
        path = resolve_relative_spec_path(f"spec/states/{state_id}.yaml")
        assert path.exists(), f"Missing authored state spec for {state_id}"
        authored = _load_yaml(path)
        assert authored.get("state_id") == state_id


def test_every_context_dimension_is_authored_under_spec_states() -> None:
    payload = load_state_registry()

    for family_name, cfg in payload["context_dimensions"].items():
        path = resolve_relative_spec_path(f"spec/states/{family_name}.yaml")
        assert path.exists(), f"Missing authored context spec for {family_name}"
        authored = _load_yaml(path)
        assert authored.get("state_name") == family_name
        assert authored.get("mapping") == cfg["mapping"]


def test_domain_graph_state_payload_traces_to_canonical_state_specs() -> None:
    payload = build_domain_graph_payload()

    for state_id, row in payload["states"].items():
        assert row["spec_path"].endswith(f"spec/states/{state_id}.yaml")


def test_ontology_state_loader_ignores_generated_ontology_shadow_files(
    tmp_path,
    monkeypatch,
) -> None:
    shadow_root = tmp_path / "ontology" / "states"
    shadow_root.mkdir(parents=True)
    (shadow_root / "HIGH_VOL_REGIME.yaml").write_text(
        yaml.safe_dump(
            {
                "state_id": "HIGH_VOL_REGIME",
                "family": "SHADOW_FAMILY",
                "enabled": False,
                "description": "shadow copy should not be loaded",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(spec_validation_loaders, "ONTOLOGY_DIR", tmp_path / "ontology")

    payload = spec_validation_loaders.load_ontology_states()

    assert payload["HIGH_VOL_REGIME"]["family"] == "VOLATILITY_TRANSITION"
    assert payload["HIGH_VOL_REGIME"]["source_event_type"] == "VOL_CLUSTER_SHIFT"
