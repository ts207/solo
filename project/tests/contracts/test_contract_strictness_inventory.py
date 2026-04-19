from __future__ import annotations

from project.contracts.artifacts import list_artifact_contracts
from project.contracts.pipeline_registry import STAGE_ARTIFACT_REGISTRY
from project.contracts.schemas import list_payload_schema_contracts, list_schema_contracts
from project.contracts.strictness_inventory import (
    build_contract_strictness_inventory_payload,
    render_contract_strictness_inventory_json,
    render_contract_strictness_inventory_markdown,
)


def test_contract_strictness_inventory_covers_all_contract_registries() -> None:
    payload = build_contract_strictness_inventory_payload()
    sections = payload["sections"]

    assert len(sections["stage_artifact_contracts"]) == len(STAGE_ARTIFACT_REGISTRY)
    assert len(sections["lifecycle_artifact_contracts"]) == len(list_artifact_contracts())
    assert len(sections["dataframe_schema_contracts"]) == len(list_schema_contracts())
    assert len(sections["payload_schema_contracts"]) == len(list_payload_schema_contracts())

    total = sum(len(rows) for rows in sections.values())
    assert payload["summary"]["total_contracts"] == total
    assert payload["summary"]["strictness_counts"]["strict"] >= len(list_artifact_contracts())


def test_contract_strictness_inventory_renders_markdown_and_json() -> None:
    payload = build_contract_strictness_inventory_payload()

    markdown = render_contract_strictness_inventory_markdown(payload)
    assert "# Contract Strictness Inventory" in markdown
    assert "## Stage Artifact Contracts" in markdown
    assert "## Payload Schema Contracts" in markdown

    json_text = render_contract_strictness_inventory_json(payload)
    assert '"schema_version": "contract_strictness_inventory_v1"' in json_text
    assert '"strictness_counts"' in json_text
