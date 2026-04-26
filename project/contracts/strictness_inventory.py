from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable
from typing import Any

from project.contracts.artifacts import list_artifact_contracts
from project.contracts.pipeline_registry import STAGE_ARTIFACT_REGISTRY
from project.contracts.schemas import list_payload_schema_contracts, list_schema_contracts

SCHEMA_VERSION = "contract_strictness_inventory_v1"


def _strictness_counts(rows: Iterable[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(str(row.get("strictness", "")).strip() or "unknown" for row in rows)
    return {key: int(counts[key]) for key in sorted(counts)}


def _stage_artifact_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx, contract in enumerate(STAGE_ARTIFACT_REGISTRY, start=1):
        rows.append(
            {
                "id": f"stage_artifact::{idx:03d}",
                "kind": "stage_artifact_contract",
                "strictness": contract.strictness,
                "stage_patterns": list(contract.stage_patterns),
                "inputs": list(contract.inputs),
                "optional_inputs": list(contract.optional_inputs),
                "outputs": list(contract.outputs),
                "external_inputs": list(contract.external_inputs),
            }
        )
    return rows


def _lifecycle_artifact_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for contract in list_artifact_contracts():
        rows.append(
            {
                "id": contract.contract_id,
                "kind": "lifecycle_artifact_contract",
                "strictness": contract.strictness,
                "producer_stage_family": contract.producer_stage_family,
                "consumer_stage_families": list(contract.consumer_stage_families),
                "required": bool(contract.required),
                "schema_id": contract.schema_id,
                "schema_version": contract.schema_version,
                "path_pattern": contract.path_pattern,
                "legacy_aliases": list(contract.legacy_aliases),
            }
        )
    return rows


def _dataframe_schema_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for contract in list_schema_contracts():
        rows.append(
            {
                "id": contract.name,
                "kind": "dataframe_schema_contract",
                "strictness": contract.strictness,
                "schema_version": contract.schema_version,
                "required_columns": list(contract.required_columns),
                "optional_columns": list(contract.optional_columns),
            }
        )
    return rows


def _payload_schema_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for contract in list_payload_schema_contracts():
        rows.append(
            {
                "id": contract.name,
                "kind": "payload_schema_contract",
                "strictness": contract.strictness,
                "schema_version": contract.schema_version,
                "required_fields": [name for name, _ in contract.required_fields],
                "optional_fields": [name for name, _ in contract.optional_fields],
                "version_field": contract.version_field,
                "version_value": contract.version_value,
            }
        )
    return rows


def build_contract_strictness_inventory_payload() -> dict[str, Any]:
    sections = {
        "stage_artifact_contracts": _stage_artifact_rows(),
        "lifecycle_artifact_contracts": _lifecycle_artifact_rows(),
        "dataframe_schema_contracts": _dataframe_schema_rows(),
        "payload_schema_contracts": _payload_schema_rows(),
    }
    all_rows = [row for rows in sections.values() for row in rows]
    return {
        "schema_version": SCHEMA_VERSION,
        "summary": {
            "total_contracts": len(all_rows),
            "strictness_counts": _strictness_counts(all_rows),
            "section_counts": {key: len(value) for key, value in sections.items()},
        },
        "sections": sections,
    }


def render_contract_strictness_inventory_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _compact(value: Any) -> str:
    if value in (None, "", [], ()):
        return ""
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return str(value)


def _render_rows(title: str, rows: list[dict[str, Any]], columns: tuple[str, ...]) -> list[str]:
    lines = [f"## {title}", ""]
    if not rows:
        lines.extend(["No contracts.", ""])
        return lines
    lines.append("| " + " | ".join(columns) + " |")
    lines.append("| " + " | ".join("---" for _ in columns) + " |")
    for row in rows:
        lines.append("| " + " | ".join(_compact(row.get(column)) for column in columns) + " |")
    lines.append("")
    return lines


def render_contract_strictness_inventory_markdown(payload: dict[str, Any]) -> str:
    summary = dict(payload.get("summary", {}))
    strictness_counts = dict(summary.get("strictness_counts", {}))
    sections = dict(payload.get("sections", {}))
    lines = [
        "# Contract Strictness Inventory",
        "",
        "Generated from `project.contracts` registries. Do not edit by hand.",
        "",
        "## Summary",
        "",
        f"- total_contracts: `{int(summary.get('total_contracts', 0))}`",
    ]
    for strictness, count in strictness_counts.items():
        lines.append(f"- {strictness}: `{count}`")
    lines.append("")
    lines.extend(
        _render_rows(
            "Stage Artifact Contracts",
            list(sections.get("stage_artifact_contracts", [])),
            ("id", "strictness", "stage_patterns", "inputs", "optional_inputs", "outputs"),
        )
    )
    lines.extend(
        _render_rows(
            "Lifecycle Artifact Contracts",
            list(sections.get("lifecycle_artifact_contracts", [])),
            ("id", "strictness", "producer_stage_family", "consumer_stage_families", "schema_id", "path_pattern"),
        )
    )
    lines.extend(
        _render_rows(
            "DataFrame Schema Contracts",
            list(sections.get("dataframe_schema_contracts", [])),
            ("id", "strictness", "schema_version", "required_columns"),
        )
    )
    lines.extend(
        _render_rows(
            "Payload Schema Contracts",
            list(sections.get("payload_schema_contracts", [])),
            ("id", "strictness", "schema_version", "required_fields", "version_field"),
        )
    )
    return "\n".join(lines).rstrip() + "\n"
