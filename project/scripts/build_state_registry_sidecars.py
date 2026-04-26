#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from project import PROJECT_ROOT
from project.spec_registry import (
    load_state_family_registry,
    load_state_registry,
    resolve_relative_spec_path,
)


def _state_registry_payload() -> dict[str, Any]:
    payload = load_state_registry()
    return payload if isinstance(payload, dict) else {}


def _state_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("states", [])
    return [dict(row) for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _runtime_defaults(payload: dict[str, Any]) -> dict[str, Any]:
    defaults = payload.get("defaults", {})
    if not isinstance(defaults, dict):
        return {}
    runtime = defaults.get("runtime", {})
    return dict(runtime) if isinstance(runtime, dict) else {}


def build_runtime_state_registry_payload() -> dict[str, Any]:
    payload = _state_registry_payload()
    runtime_defaults = _runtime_defaults(payload)
    states: dict[str, dict[str, Any]] = {}
    for row in _state_rows(payload):
        state_id = str(row.get("state_id", "")).strip().upper()
        if not state_id:
            continue
        runtime = row.get("runtime", {})
        if not isinstance(runtime, dict):
            runtime = {}
        merged = dict(runtime_defaults)
        merged.update(runtime)
        record: dict[str, Any] = {
            "enabled": bool(merged.get("enabled", True)),
            "instrument_classes": [
                str(item).strip()
                for item in merged.get("instrument_classes", [])
                if str(item).strip()
            ],
            "tags": [
                str(item).strip()
                for item in merged.get("tags", merged.get("runtime_tags", []))
                if str(item).strip()
            ],
            "family": str(row.get("family", "")).strip().upper(),
            "source_event_type": str(row.get("source_event_type", "")).strip().upper(),
        }
        state_engine = str(merged.get("state_engine", "")).strip()
        if state_engine:
            record["state_engine"] = state_engine
        description = str(row.get("description", merged.get("description", ""))).strip()
        if description:
            record["description"] = description
        states[state_id] = record
    return {
        "version": 1,
        "kind": "runtime_state_registry",
        "metadata": {
            "status": "generated",
            "authored_source": "spec/states/*.yaml",
            "generated_notice": "GENERATED FILE - DO NOT EDIT",
            "generated_from": "spec/states/*.yaml",
        },
        "states": states,
    }


def build_state_grammar_payload() -> dict[str, Any]:
    payload = _state_registry_payload()
    context_dimensions = payload.get("context_dimensions", {})
    if not isinstance(context_dimensions, dict):
        context_dimensions = {}
    regimes: dict[str, list[str]] = {}
    context_state_map: dict[str, dict[str, str]] = {}
    for family, cfg in sorted(context_dimensions.items()):
        if not isinstance(cfg, dict):
            continue
        allowed_values = [
            str(item).strip()
            for item in cfg.get("allowed_values", [])
            if str(item).strip()
        ]
        if allowed_values:
            regimes[str(family).strip()] = allowed_values
        mapping = cfg.get("mapping", {})
        if isinstance(mapping, dict) and mapping:
            context_state_map[str(family).strip()] = {
                str(label).strip(): str(state_id).strip().lower()
                for label, state_id in mapping.items()
                if str(label).strip() and str(state_id).strip()
            }
    return {
        "version": 1,
        "kind": "state_grammar_registry",
        "metadata": {
            "status": "generated",
            "authored_source": "spec/states/*.yaml",
            "generated_notice": "GENERATED FILE - DO NOT EDIT",
            "generated_from": "spec/states/*.yaml",
        },
        "regimes": regimes,
        "context_state_map": context_state_map,
    }


def build_context_registry_payload() -> dict[str, Any]:
    payload = _state_registry_payload()
    context_dimensions = payload.get("context_dimensions", {})
    if not isinstance(context_dimensions, dict):
        context_dimensions = {}
    normalized: dict[str, dict[str, Any]] = {}
    for family, cfg in sorted(context_dimensions.items()):
        if not isinstance(cfg, dict):
            continue
        allowed_values = [
            str(item).strip()
            for item in cfg.get("allowed_values", [])
            if str(item).strip()
        ]
        if not allowed_values:
            continue
        row: dict[str, Any] = {"allowed_values": allowed_values}
        mapping = cfg.get("mapping", {})
        if isinstance(mapping, dict) and mapping:
            row["mapping"] = {
                str(label).strip(): str(state_id).strip().upper()
                for label, state_id in mapping.items()
                if str(label).strip() and str(state_id).strip()
            }
        normalized[str(family).strip()] = row

    # Load legacy aliases from authored source
    dim_registry_path = PROJECT_ROOT.parent / "spec/contexts/context_dimension_registry.yaml"
    legacy_aliases = {}
    if dim_registry_path.exists():
        try:
            with open(dim_registry_path) as f:
                dim_doc = yaml.safe_load(f)
            legacy_aliases = dim_doc.get("legacy_context_aliases", {})
        except Exception:
            pass

    return {
        "version": 1,
        "kind": "context_registry",
        "metadata": {
            "status": "generated",
            "authored_sources": [
                "spec/states/*.yaml",
                "spec/contexts/context_dimension_registry.yaml",
            ],
            "generated_notice": "GENERATED FILE - DO NOT EDIT",
        },
        "context_dimensions": normalized,
        "legacy_context_aliases": legacy_aliases,
    }


def build_state_ontology_specs() -> dict[str, dict[str, Any]]:
    payload = _state_registry_payload()
    defaults = payload.get("defaults", {})
    if not isinstance(defaults, dict):
        defaults = {}
    default_scope = str(defaults.get("state_scope", "source_only")).strip() or "source_only"
    default_min_events = int(defaults.get("min_events", 200) or 200)
    out: dict[str, dict[str, Any]] = {}
    for row in _state_rows(payload):
        state_id = str(row.get("state_id", "")).strip().upper()
        if not state_id:
            continue
        ontology_row: dict[str, Any] = {
            "version": 1,
            "kind": "state_ontology_spec",
            "metadata": {
                "status": "generated",
                "authored_source": f"spec/states/{state_id}.yaml",
                "generated_notice": "GENERATED FILE - DO NOT EDIT",
                "generated_from": "spec/states/*.yaml",
            },
            "state_id": state_id,
            "family": str(row.get("family", "")).strip().upper(),
            "source_event_type": str(row.get("source_event_type", "")).strip().upper(),
            "activation_rule": str(row.get("activation_rule", "")).strip(),
            "decay_rule": str(row.get("decay_rule", "")).strip(),
            "allowed_templates": [
                str(item).strip()
                for item in row.get("allowed_templates", [])
                if str(item).strip()
            ],
        }
        state_scope = str(row.get("state_scope", default_scope)).strip()
        if state_scope:
            ontology_row["state_scope"] = state_scope
        min_events = int(row.get("min_events", default_min_events) or default_min_events)
        ontology_row["min_events"] = min_events
        features_required = [
            str(item).strip()
            for item in row.get("features_required", [])
            if str(item).strip()
        ]
        if features_required:
            ontology_row["features_required"] = features_required
        description = str(row.get("description", "")).strip()
        if description:
            ontology_row["description"] = description
        max_duration = row.get("max_duration")
        if max_duration not in (None, ""):
            ontology_row["max_duration"] = max_duration
        out[state_id] = ontology_row
    return out


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _write_ontology_specs(ontology_specs: dict[str, dict[str, Any]]) -> None:
    out_dir = resolve_relative_spec_path("spec/ontology/states", repo_root=PROJECT_ROOT.parent)
    out_dir.mkdir(parents=True, exist_ok=True)
    for state_id, payload in sorted(ontology_specs.items()):
        path = out_dir / f"{state_id}.yaml"
        _write_yaml(path, payload)


def main() -> int:
    registry_path = resolve_relative_spec_path("spec/states/state_registry.yaml", repo_root=PROJECT_ROOT.parent)
    family_registry_path = resolve_relative_spec_path("spec/states/state_families.yaml", repo_root=PROJECT_ROOT.parent)
    runtime_path = PROJECT_ROOT / "configs" / "registries" / "states.yaml"
    context_path = PROJECT_ROOT / "configs" / "registries" / "contexts.yaml"
    grammar_path = resolve_relative_spec_path("spec/grammar/state_registry.yaml", repo_root=PROJECT_ROOT.parent)
    _write_yaml(registry_path, _state_registry_payload())
    _write_yaml(family_registry_path, load_state_family_registry())
    _write_yaml(runtime_path, build_runtime_state_registry_payload())
    _write_yaml(context_path, build_context_registry_payload())
    _write_yaml(grammar_path, build_state_grammar_payload())
    _write_ontology_specs(build_state_ontology_specs())
    print(f"Wrote {registry_path}")
    print(f"Wrote {family_registry_path}")
    print(f"Wrote {runtime_path}")
    print(f"Wrote {context_path}")
    print(f"Wrote {grammar_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
