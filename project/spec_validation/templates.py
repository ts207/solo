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


def validate_event_template_matrix() -> List[Tuple[str, str]]:
    errors: List[Tuple[str, str]] = []
    
    matrix_path = "spec/compatibility/event_template_matrix.yaml"
    matrix_doc = load_yaml_relative(matrix_path)
    if not isinstance(matrix_doc, dict):
        return [(matrix_path, "Root must be a mapping")]
        
    matrix = matrix_doc.get("event_template_matrix", {})
    if not isinstance(matrix, dict):
        return [(matrix_path, "event_template_matrix must be a mapping")]
        
    # We enforce that all runtime_default=True events (from generated eligibility matrix)
    # MUST be defined in the event_template_matrix.
    import json
    from pathlib import Path
    eligibility_path = Path("docs/generated/detector_eligibility_matrix.json")
    if not eligibility_path.exists():
        return [("docs/generated/detector_eligibility_matrix.json", "Missing eligibility matrix")]
        
    try:
        with open(eligibility_path, "r") as f:
            eligibility_rows = json.load(f)
    except Exception as e:
        return [(str(eligibility_path), f"Failed to parse: {e}")]
        
    for row in eligibility_rows:
        if row.get("runtime") is True:
            event_name = row.get("event_name", "")
            if event_name not in matrix:
                errors.append((matrix_path, f"Missing matrix coverage for runtime event: {event_name}"))
                
    return errors
