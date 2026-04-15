from __future__ import annotations

from project.spec_registry import (
    ontology_spec_paths,
    runtime_spec_paths,
    feature_schema_registry_path,
)


def test_registry_exposes_ontology_and_runtime_paths():
    ontology = ontology_spec_paths()
    runtime = runtime_spec_paths()
    assert {
        "taxonomy",
        "canonical_event_registry",
        "state_registry",
        "template_verb_lexicon",
    }.issubset(ontology)
    assert {"lanes", "firewall", "hashing"}.issubset(runtime)
    assert feature_schema_registry_path().name.startswith("feature_schema_")
