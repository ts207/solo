from __future__ import annotations

from project.spec_validation import templates as template_lint
from project.spec_validation.templates import validate_template_contracts


def test_authored_template_contracts_are_complete() -> None:
    assert validate_template_contracts() == []


def test_template_contract_lint_detects_missing_concrete_fields(monkeypatch) -> None:
    monkeypatch.setattr(
        template_lint,
        "load_template_registry",
        lambda: {
            "operators": {
                "forced_flow_rebound": {
                    "template_kind": "expression_template",
                    "side_policy": "contrarian",
                    "label_target": "contrarian_signed_return_h",
                }
            }
        },
    )
    monkeypatch.setattr(
        template_lint,
        "load_yaml_relative",
        lambda path: {
            "spec/templates/template_contract.yaml": {
                "required_expression_fields": ["side_policy", "entry", "exit", "labels.primary"],
                "generic_template_replacements": {},
            },
            "spec/templates/template_label_contracts.yaml": {
                "template_labels": {"forced_flow_rebound": {"primary": "contrarian_signed_return_h"}}
            },
        }[path],
    )

    errors = validate_template_contracts()

    assert errors
    assert "forced_flow_rebound" in errors[0][1]
    assert "entry" in errors[0][1]
