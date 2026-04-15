from __future__ import annotations

from project.spec_registry import load_concept_spec, load_event_spec, load_global_defaults


def test_extended_registry_loaders_return_mappings() -> None:
    assert isinstance(load_global_defaults(), dict)
    assert isinstance(load_event_spec("BAND_BREAK"), dict)
    assert isinstance(load_concept_spec("C_MICROSTRUCTURE_METRICS"), dict)
