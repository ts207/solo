from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from project.spec_validation.loaders import load_template_registry, load_yaml


def load_yaml_relative(relative_path: str, root: Path = Path(".")) -> dict[str, Any]:
    return load_yaml(root / relative_path)

def _has_path(row: Mapping[str, Any], path: str) -> bool:
    current: Any = row
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return False
        current = current[part]
    return current not in (None, "", [], {})

def validate_template_contracts(root: Path = Path(".")) -> list[tuple[str, str]]:
    registry = load_template_registry(root=root)
    operators = registry.get("operators", {}) if isinstance(registry, dict) else {}
    if not isinstance(operators, dict):
        return [("spec/templates/registry.yaml", "operators must be a mapping")]

    contract_path = "spec/templates/template_contract.yaml"
    contract = load_yaml_relative(contract_path, root=root)
    if not isinstance(contract, dict):
        return [(contract_path, "Root must be a mapping")]

    required_fields = (
        contract.get("required_expression_fields", [])
    )
    required_paths = [str(item).strip() for item in required_fields if str(item).strip()]

    label_contracts = load_yaml_relative("spec/templates/template_label_contracts.yaml", root=root)
    concrete_templates = set()
    if isinstance(label_contracts, dict) and isinstance(label_contracts.get("template_labels"), dict):
        concrete_templates = {
            str(item).strip()
            for item in label_contracts["template_labels"]
            if str(item).strip()
        }

    abstract_templates = set()
    replacements = contract.get("generic_template_replacements", {})
    if isinstance(replacements, dict):
        abstract_templates = {str(item).strip() for item in replacements if str(item).strip()}

    errors: list[tuple[str, str]] = []
    for template_id, row in sorted(operators.items()):
        if not isinstance(row, Mapping):
            continue
        kind = str(row.get("template_kind", "")).strip().lower()
        if kind != "expression_template":
            continue

        template = str(template_id).strip()
        if template in abstract_templates:
            status = str(row.get("contract_status", "")).strip().lower()
            if status != "abstract_template_family":
                errors.append(
                    (
                        "spec/templates/registry.yaml",
                        f"{template}: abstract template families must declare "
                        "contract_status=abstract_template_family",
                    )
                )
            continue

        if template not in concrete_templates:
            continue

        missing = [path for path in required_paths if not _has_path(row, path)]
        if missing:
            errors.append(
                (
                    "spec/templates/registry.yaml",
                    f"{template}: concrete expression template missing contract fields: "
                    + ", ".join(missing),
                )
            )

    return errors

def validate_event_template_matrix(root: Path = Path(".")) -> list[tuple[str, str]]:
    errors: list[tuple[str, str]] = []

    matrix_path = "spec/compatibility/event_template_matrix.yaml"
    matrix_doc = load_yaml_relative(matrix_path, root=root)
    if not isinstance(matrix_doc, dict):
        return [(matrix_path, "Root must be a mapping")]

    matrix = matrix_doc.get("event_template_matrix", {})
    if not isinstance(matrix, dict):
        return [(matrix_path, "event_template_matrix must be a mapping")]

    eligibility_path = root / "docs/generated/detector_eligibility_matrix.json"
    if not eligibility_path.exists():
        return [("docs/generated/detector_eligibility_matrix.json", "Missing eligibility matrix")]

    try:
        with open(eligibility_path) as f:
            eligibility_rows = json.load(f)
    except Exception as e:
        return [(str(eligibility_path), f"Failed to parse: {e}")]

    for row in eligibility_rows:
        if row.get("runtime") is True:
            event_name = row.get("event_name", "")
            if event_name not in matrix:
                errors.append((matrix_path, f"Missing matrix coverage for runtime event: {event_name}"))

    # Enforce that every template name in event_template_matrix exists in the template registry
    registry = load_template_registry(root=root)
    known_templates = set()
    if isinstance(registry, dict):
        known_templates.update(registry.get("operators", {}).keys())
        known_templates.update(registry.get("filter_templates", {}).keys())
        known_templates.update(registry.get("expression_templates", {}).keys())

    contract = load_yaml_relative("spec/templates/template_contract.yaml", root=root)
    if isinstance(contract, dict):
        replacements = contract.get("generic_template_replacements", {})
        if isinstance(replacements, dict):
            known_templates.update(replacements.keys())

    # Generic abstract templates are only allowed under status: forbidden in the matrix
    abstract_templates = {
        "mean_reversion", "continuation", "exhaustion_reversal",
        "reversal_or_squeeze", "convexity_capture", "trend_continuation",
        "generic_continuation", "generic_mean_reversion", "unconditioned_mean_reversion"
    }

    for event_name, templates in matrix.items():
        if not isinstance(templates, dict):
            continue
        for template_name, cfg in templates.items():
            status = cfg.get("status") if isinstance(cfg, dict) else ""

            if template_name in abstract_templates:
                if status != "forbidden":
                     errors.append((matrix_path, f"Generic abstract template '{template_name}' must be marked as 'forbidden' in matrix under event '{event_name}'"))
                continue

            if template_name not in known_templates:
                errors.append((matrix_path, f"Unknown template '{template_name}' referenced under event '{event_name}'"))

    return errors
