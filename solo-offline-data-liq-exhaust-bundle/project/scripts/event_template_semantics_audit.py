#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Mapping

import yaml

from project.domain.compiled_registry import get_domain_registry
from project.events.config import compose_event_config


def _load_yaml(path: str) -> Dict[str, Any]:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def run_audit() -> Dict[str, Any]:
    registry = get_domain_registry()
    template_registry = _load_yaml("spec/templates/registry.yaml")
    ontology_template_registry = _load_yaml("spec/ontology/templates/template_registry.yaml")
    runtime_templates_payload = _load_yaml("project/configs/registries/templates.yaml")
    lexicon_payload = _load_yaml("spec/hypotheses/template_verb_lexicon.yaml")
    state_registry_payload = _load_yaml("spec/states/state_registry.yaml")

    default_templates = tuple(str(item) for item in template_registry.get("defaults", {}).get("templates", []) or [])
    event_template_rows = template_registry.get("events", {})
    if not isinstance(event_template_rows, Mapping):
        event_template_rows = {}
    runtime_templates = set((runtime_templates_payload.get("templates", {}) or {}).keys())
    family_templates = {
        str(template_id)
        for row in (ontology_template_registry.get("families", {}) or {}).values()
        if isinstance(row, Mapping)
        for template_id in row.get("allowed_templates", []) or []
    }
    family_templates.update(str(template_id) for template_id in (ontology_template_registry.get("filter_templates", {}) or {}).keys())
    lexicon_templates = {
        str(template_id)
        for group in (lexicon_payload.get("verbs", {}) or {}).values()
        if isinstance(group, list)
        for template_id in group
    }
    cross_asset_templates = {
        "basis_convergence",
        "cross_asset_mean_reversion",
        "lead_lag_continuation",
        "relative_value_basket",
    }

    events_using_default_template_set: list[str] = []
    events_missing_event_template_row: list[str] = []
    events_with_unregistered_templates: Dict[str, list[str]] = {}
    events_with_runtime_template_drops: Dict[str, Dict[str, Any]] = {}
    intentional_runtime_template_suppression: Dict[str, Dict[str, Any]] = {}
    events_with_templates_outside_operator_compatibility: Dict[str, Dict[str, Any]] = {}
    single_asset_events_with_cross_asset_templates: Dict[str, list[str]] = {}

    for event_id in registry.event_ids:
        event = registry.event_definitions[event_id]
        raw_templates = tuple(str(item) for item in (event.raw.get("templates", []) or []))
        runtime_cfg = compose_event_config(event_id)
        if raw_templates == default_templates:
            events_using_default_template_set.append(event_id)
        if event_id not in event_template_rows:
            events_missing_event_template_row.append(event_id)
        unregistered_templates = sorted(template_id for template_id in raw_templates if template_id not in runtime_templates)
        if unregistered_templates:
            events_with_unregistered_templates[event_id] = unregistered_templates
        runtime_templates_for_event = tuple(str(item) for item in runtime_cfg.templates)
        if raw_templates != runtime_templates_for_event:
            payload = {
                "raw_templates": list(raw_templates),
                "runtime_templates": list(runtime_templates_for_event),
                "family": runtime_cfg.family,
            }
            if event.is_composite or event.research_only or event_id.startswith("POST_DELEVERAGING"):
                intentional_runtime_template_suppression[event_id] = payload
            else:
                events_with_runtime_template_drops[event_id] = payload

        family_candidates = {
            token
            for token in (
                event.canonical_regime,
                event.canonical_family,
                runtime_cfg.family,
                str((event.parameters or {}).get("canonical_family", "")).strip().upper(),
            )
            if isinstance(token, str) and token.strip()
        }
        incompatible_templates: list[str] = []
        for template_id in raw_templates:
            operator = registry.get_operator(template_id)
            compatible_families = set(operator.compatible_families) if operator is not None else set()
            if operator is not None and compatible_families and family_candidates.isdisjoint(compatible_families):
                incompatible_templates.append(template_id)
        if incompatible_templates:
            events_with_templates_outside_operator_compatibility[event_id] = {
                "canonical_regime": event.canonical_regime,
                "family_candidates": sorted(family_candidates),
                "incompatible_templates": incompatible_templates,
                "raw_templates": list(raw_templates),
            }

        cross_asset_overlap = sorted(set(raw_templates) & cross_asset_templates)
        if cross_asset_overlap and event.asset_scope == "single_asset":
            single_asset_events_with_cross_asset_templates[event_id] = cross_asset_overlap

    family_templates_missing_runtime_registration = sorted(family_templates - runtime_templates)
    lexicon_templates_missing_runtime_registration = sorted(lexicon_templates - runtime_templates)
    runtime_templates_missing_lexicon = sorted(runtime_templates - lexicon_templates)
    used_template_ids = {
        template_id
        for event_id in registry.event_ids
        for template_id in (registry.event_definitions[event_id].raw.get("templates", []) or [])
    }
    state_rows = state_registry_payload.get("states", state_registry_payload)
    if isinstance(state_rows, list):
        for row in state_rows:
            if isinstance(row, Mapping):
                used_template_ids.update(str(template_id) for template_id in row.get("allowed_templates", []) or [])
    unused_runtime_templates = sorted(
        template_id
        for template_id in runtime_templates
        if template_id not in used_template_ids
    )

    issue_count = (
        len(events_using_default_template_set)
        + len(events_with_unregistered_templates)
        + len(events_with_runtime_template_drops)
        + len(events_with_templates_outside_operator_compatibility)
        + len(single_asset_events_with_cross_asset_templates)
        + len(family_templates_missing_runtime_registration)
        + len(lexicon_templates_missing_runtime_registration)
        + len(runtime_templates_missing_lexicon)
        + len(unused_runtime_templates)
    )

    return {
        "summary": {
            "status": "attention" if issue_count else "passed",
            "active_event_count": len(registry.event_ids),
            "default_template_event_count": len(events_using_default_template_set),
            "missing_event_template_row_count": len(events_missing_event_template_row),
            "unregistered_template_event_count": len(events_with_unregistered_templates),
            "runtime_template_drop_event_count": len(events_with_runtime_template_drops),
            "intentional_runtime_template_suppression_count": len(intentional_runtime_template_suppression),
            "operator_compatibility_override_count": len(events_with_templates_outside_operator_compatibility),
            "single_asset_cross_asset_template_count": len(single_asset_events_with_cross_asset_templates),
            "family_templates_missing_runtime_count": len(family_templates_missing_runtime_registration),
            "lexicon_templates_missing_runtime_count": len(lexicon_templates_missing_runtime_registration),
            "runtime_templates_missing_lexicon_count": len(runtime_templates_missing_lexicon),
            "unused_runtime_template_count": len(unused_runtime_templates),
            "issue_count": issue_count,
        },
        "default_templates": list(default_templates),
        "events_using_default_template_set": sorted(events_using_default_template_set),
        "events_missing_event_template_row": sorted(events_missing_event_template_row),
        "events_with_unregistered_templates": events_with_unregistered_templates,
        "events_with_runtime_template_drops": events_with_runtime_template_drops,
        "intentional_runtime_template_suppression": intentional_runtime_template_suppression,
        "events_with_templates_outside_operator_compatibility": events_with_templates_outside_operator_compatibility,
        "single_asset_events_with_cross_asset_templates": single_asset_events_with_cross_asset_templates,
        "family_templates_missing_runtime_registration": family_templates_missing_runtime_registration,
        "lexicon_templates_missing_runtime_registration": lexicon_templates_missing_runtime_registration,
        "runtime_templates_missing_lexicon": runtime_templates_missing_lexicon,
        "unused_runtime_templates": unused_runtime_templates,
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# Event Template Semantics Audit",
        "",
        f"- Status: `{summary['status']}`",
        f"- Active events: `{summary['active_event_count']}`",
        f"- Events using default template set: `{summary['default_template_event_count']}`",
        f"- Events missing event-specific template row: `{summary['missing_event_template_row_count']}`",
        f"- Events with unregistered templates: `{summary['unregistered_template_event_count']}`",
        f"- Events with runtime template drops: `{summary['runtime_template_drop_event_count']}`",
        f"- Events with intentional runtime suppression: `{summary['intentional_runtime_template_suppression_count']}`",
        f"- Events with operator-compatibility overrides: `{summary['operator_compatibility_override_count']}`",
        f"- Single-asset events exposing cross-asset templates: `{summary['single_asset_cross_asset_template_count']}`",
        f"- Family templates missing runtime registration: `{summary['family_templates_missing_runtime_count']}`",
        f"- Lexicon templates missing runtime registration: `{summary['lexicon_templates_missing_runtime_count']}`",
        f"- Runtime templates missing lexicon: `{summary['runtime_templates_missing_lexicon_count']}`",
        f"- Unused runtime templates: `{summary['unused_runtime_template_count']}`",
        "",
        "## Default Template Inheritance",
        "",
        f"- Default template set: `{report.get('default_templates', [])}`",
        f"- Events: `{report.get('events_using_default_template_set', [])}`",
        "",
        "## Missing Event-Specific Template Rows",
        "",
        f"- Events: `{report.get('events_missing_event_template_row', [])}`",
        "",
        "## Events With Unregistered Templates",
        "",
    ]
    unregistered = report.get("events_with_unregistered_templates", {})
    if unregistered:
        for event_id, templates in unregistered.items():
            lines.append(f"- `{event_id}`: `{templates}`")
    else:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Runtime Template Drops",
            "",
        ]
    )
    runtime_drops = report.get("events_with_runtime_template_drops", {})
    if runtime_drops:
        for event_id, payload in runtime_drops.items():
            lines.append(
                f"- `{event_id}`: raw={payload['raw_templates']}, runtime={payload['runtime_templates']}, family=`{payload['family']}`, "
            )
    else:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Intentional Runtime Suppression",
            "",
        ]
    )
    suppressed = report.get("intentional_runtime_template_suppression", {})
    if suppressed:
        for event_id, payload in suppressed.items():
            lines.append(
                f"- `{event_id}`: raw={payload['raw_templates']}, runtime={payload['runtime_templates']}, family=`{payload['family']}`, "
            )
    else:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Operator Compatibility Overrides",
            "",
        ]
    )
    compatibility = report.get("events_with_templates_outside_operator_compatibility", {})
    if compatibility:
        for event_id, payload in compatibility.items():
            lines.append(
                f"- `{event_id}`: incompatible={payload['incompatible_templates']}, families={payload['family_candidates']}"
            )
    else:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Single-Asset Events With Cross-Asset Templates",
            "",
        ]
    )
    cross_asset = report.get("single_asset_events_with_cross_asset_templates", {})
    if cross_asset:
        for event_id, templates in cross_asset.items():
            lines.append(f"- `{event_id}`: `{templates}`")
    else:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Template Vocabulary Drift",
            "",
            f"- family_templates_missing_runtime_registration: `{report.get('family_templates_missing_runtime_registration', [])}`",
            f"- lexicon_templates_missing_runtime_registration: `{report.get('lexicon_templates_missing_runtime_registration', [])}`",
            f"- runtime_templates_missing_lexicon: `{report.get('runtime_templates_missing_lexicon', [])}`",
            f"- unused_runtime_templates: `{report.get('unused_runtime_templates', [])}`",
            "",
        ]
    )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit event-template semantics across registry surfaces.")
    parser.add_argument(
        "--json-out",
        default="docs/generated/event_template_semantics_audit.json",
    )
    parser.add_argument(
        "--md-out",
        default="docs/generated/event_template_semantics_audit.md",
    )
    parser.add_argument("--check", action="store_true", help="Fail if outputs drift from disk.")
    args = parser.parse_args(argv)

    report = run_audit()
    expected_json = json.dumps(report, indent=2, sort_keys=True) + "\n"
    expected_md = render_markdown(report) + "\n"

    json_path = Path(args.json_out)
    md_path = Path(args.md_out)

    if args.check:
        drift: list[str] = []
        for path, content in ((json_path, expected_json), (md_path, expected_md)):
            current = path.read_text(encoding="utf-8") if path.exists() else None
            if current != content:
                drift.append(str(path))
        if drift:
            for path in drift:
                print(f"event template audit drift: {path}", file=sys.stderr)
            return 1
        return 0

    json_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(expected_json, encoding="utf-8")
    md_path.write_text(expected_md, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
