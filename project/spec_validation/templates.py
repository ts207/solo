from __future__ import annotations

from typing import Any, List, Mapping, Tuple

from project.spec_registry.loaders import load_template_registry, load_yaml_relative


def _has_path(row: Mapping[str, Any], path: str) -> bool:
    current: Any = row
    for part in path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return False
        current = current[part]
    return current not in (None, "", [], {})


def validate_template_contracts() -> List[Tuple[str, str]]:
    registry = load_template_registry()
    operators = registry.get("operators", {}) if isinstance(registry, dict) else {}
    if not isinstance(operators, dict):
        return [("spec/templates/registry.yaml", "operators must be a mapping")]

    contract = load_yaml_relative("spec/templates/template_contract.yaml")
    required_fields = (
        contract.get("required_expression_fields", []) if isinstance(contract, dict) else []
    )
    required_paths = [str(item).strip() for item in required_fields if str(item).strip()]

    label_contracts = load_yaml_relative("spec/templates/template_label_contracts.yaml")
    concrete_templates = set()
    if isinstance(label_contracts, dict) and isinstance(label_contracts.get("template_labels"), dict):
        concrete_templates = {
            str(item).strip()
            for item in label_contracts["template_labels"]
            if str(item).strip()
        }

    abstract_templates = set()
    replacements = contract.get("generic_template_replacements", {}) if isinstance(contract, dict) else {}
    if isinstance(replacements, dict):
        abstract_templates = {str(item).strip() for item in replacements if str(item).strip()}

    errors: List[Tuple[str, str]] = []
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
