#!/usr/bin/env python3
from __future__ import annotations

import argparse
import difflib
from pathlib import Path
from typing import Any, Dict

import yaml

from project import PROJECT_ROOT
from project.spec_registry import load_template_registry, resolve_relative_spec_path


def _canonical_template_registry() -> Dict[str, Any]:
    payload = load_template_registry()
    return payload if isinstance(payload, dict) else {}


def build_template_registry_compat_payload() -> Dict[str, Any]:
    canonical = _canonical_template_registry()
    payload = dict(canonical)
    events = canonical.get("events", {})
    if isinstance(events, dict):
        compat_events: Dict[str, Dict[str, Any]] = {}
        for event_id, row in events.items():
            if not isinstance(row, dict):
                continue
            compat_row = dict(row)
            research_family = str(
                row.get("research_family", row.get("canonical_family", ""))
            ).strip().upper()
            if research_family:
                compat_row.setdefault("canonical_family", research_family)
            compat_events[str(event_id).strip().upper()] = compat_row
        payload["events"] = compat_events
    metadata = canonical.get("metadata", {})
    payload["metadata"] = {
        **(dict(metadata) if isinstance(metadata, dict) else {}),
        "status": "generated",
        "authored_source": "spec/templates/registry.yaml",
    }
    return payload


def build_runtime_template_registry_payload() -> Dict[str, Any]:
    canonical = _canonical_template_registry()
    operators = canonical.get("operators", {})
    if not isinstance(operators, dict):
        operators = {}
    templates: Dict[str, Dict[str, Any]] = {}
    for template_id, row in sorted(operators.items()):
        if not isinstance(row, dict):
            continue
        templates[str(template_id)] = {
            "enabled": bool(row.get("enabled", True)),
            "template_kind": str(row.get("template_kind", "")).strip().lower(),
            "supports_contexts": bool(row.get("supports_contexts", True)),
            "supports_directions": [
                str(item).strip()
                for item in row.get("supports_directions", [])
                if str(item).strip()
            ],
            "supports_trigger_types": [
                str(item).strip().upper()
                for item in row.get("supports_trigger_types", [])
                if str(item).strip()
            ],
        }
    return {
        "version": 1,
        "kind": "runtime_template_registry",
        "metadata": {
            "status": "generated",
            "authored_source": "spec/templates/registry.yaml",
        },
        "templates": templates,
    }


def build_ontology_template_registry_payload() -> Dict[str, Any]:
    canonical = _canonical_template_registry()
    defaults = canonical.get("defaults", {})
    if not isinstance(defaults, dict):
        defaults = {}
    families = canonical.get("families", {})
    if not isinstance(families, dict):
        families = {}
    filter_templates = canonical.get("filter_templates", {})
    if not isinstance(filter_templates, dict):
        filter_templates = {}

    out_families: Dict[str, Dict[str, Any]] = {}
    for family, row in sorted(families.items()):
        if not isinstance(row, dict):
            continue
        out_row: Dict[str, Any] = {
            "allowed_templates": [
                str(item).strip()
                for item in row.get("templates", row.get("allowed_templates", []))
                if str(item).strip()
            ]
        }
        if "default_horizon" in row:
            out_row["default_horizon"] = row.get("default_horizon")
        out_families[str(family).strip().upper()] = out_row

    return {
        "version": 1,
        "kind": "template_registry",
        "metadata": {
            "status": "generated",
            "authored_source": "spec/templates/registry.yaml",
        },
        "defaults": {
            "templates": [
                str(item).strip()
                for item in defaults.get("templates", [])
                if str(item).strip()
            ],
            "horizons": [
                str(item).strip()
                for item in defaults.get("horizons", [])
                if str(item).strip()
            ],
            "conditioning_cols": [
                str(item).strip()
                for item in defaults.get("conditioning_cols", [])
                if str(item).strip()
            ],
            "param_grids": dict(defaults.get("template_param_grid_defaults", {}))
            if isinstance(defaults.get("template_param_grid_defaults"), dict)
            else {},
        },
        "families": out_families,
        "filter_templates": dict(filter_templates),
    }


def _write_yaml(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _render_yaml(payload: Dict[str, Any]) -> str:
    return yaml.safe_dump(payload, sort_keys=False)


def _check_or_write(path: Path, payload: Dict[str, Any], *, check: bool) -> bool:
    rendered = _render_yaml(payload)
    if check:
        current = path.read_text(encoding="utf-8") if path.exists() else ""
        if current != rendered:
            diff = "\n".join(
                difflib.unified_diff(
                    current.splitlines(),
                    rendered.splitlines(),
                    fromfile=str(path),
                    tofile=f"{path} (regenerated)",
                    lineterm="",
                )
            )
            print(f"Template registry sidecar is stale: {path}")
            if diff:
                print(diff)
            return False
        print(f"Template registry sidecar is fresh: {path}")
        return True
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(rendered, encoding="utf-8")
    print(f"Wrote {path}")
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build or check generated template registry sidecars.")
    parser.add_argument("--check", action="store_true", help="fail if generated sidecars are stale")
    args = parser.parse_args(argv)

    outputs = [
        (
            resolve_relative_spec_path(
                "spec/templates/event_template_registry.yaml",
                repo_root=PROJECT_ROOT.parent,
            ),
            build_template_registry_compat_payload(),
        ),
        (PROJECT_ROOT / "configs" / "registries" / "templates.yaml", build_runtime_template_registry_payload()),
        (
            resolve_relative_spec_path(
                "spec/ontology/templates/template_registry.yaml",
                repo_root=PROJECT_ROOT.parent,
            ),
            build_ontology_template_registry_payload(),
        ),
    ]
    ok = True
    for path, payload in outputs:
        ok = _check_or_write(path, payload, check=args.check) and ok
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
