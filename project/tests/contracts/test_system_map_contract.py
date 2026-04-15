from __future__ import annotations

from project.contracts.stage_dag import build_stage_specs
from project.contracts.system_map import (
    build_system_map_payload,
    render_system_map_json,
    render_system_map_markdown,
    validate_system_map_surfaces,
)


def test_system_map_surfaces_are_valid() -> None:
    assert validate_system_map_surfaces() == []


def test_system_map_includes_all_stage_families() -> None:
    payload = build_system_map_payload()
    families = {item["family"] for item in payload["stage_families"]}
    expected = {item.family for item in build_stage_specs()}
    assert families == expected


def test_system_map_markdown_labels_canonical_and_removed_compatibility() -> None:
    payload = build_system_map_payload()
    markdown = render_system_map_markdown(payload)
    assert "## Canonical Entrypoints" in markdown
    assert "## Compatibility Surfaces" in markdown
    assert "project.pipelines.run_all" in markdown
    assert "Legacy wrapper surfaces have been removed." in markdown


def test_system_map_has_no_compatibility_surfaces() -> None:
    payload = build_system_map_payload()
    assert payload["compatibility_surfaces"] == []


def test_system_map_artifact_contract_fields_render_as_lists_of_strings() -> None:
    payload = build_system_map_payload()
    for contract in payload["artifact_contracts"]:
        for field_name in (
            "stage_patterns",
            "inputs",
            "optional_inputs",
            "outputs",
            "external_inputs",
        ):
            value = contract[field_name]
            assert isinstance(value, list)
            assert all(isinstance(item, str) for item in value)


def test_system_map_canonicalize_event_contract_renders_without_character_splitting() -> None:
    payload = build_system_map_payload()
    contract = next(
        item
        for item in payload["artifact_contracts"]
        if item["stage_patterns"] == ["canonicalize_event_episodes*"]
    )
    assert contract["external_inputs"] == ["phase2.event_registry.{event_type}"]

    markdown = render_system_map_markdown(payload)
    assert "`phase2.event_registry.{event_type}`" in markdown
    assert "`p`, `h`, `a`, `s`, `e`" not in markdown

    json_text = render_system_map_json(payload)
    assert '"external_inputs": [' in json_text
    assert '"phase2.event_registry.{event_type}"' in json_text
