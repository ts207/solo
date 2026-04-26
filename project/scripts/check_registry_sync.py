#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_REGISTRY = ROOT / "spec" / "templates" / "registry.yaml"
EVENT_TEMPLATE_REGISTRY = ROOT / "spec" / "templates" / "event_template_registry.yaml"
EVENT_REGISTRY = ROOT / "spec" / "events" / "event_registry_unified.yaml"


def _load(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} did not parse to a mapping")
    return payload


def _keys(payload: dict[str, Any], key: str) -> set[str]:
    value = payload.get(key, {})
    return set(value) if isinstance(value, dict) else set()


def check_registry_sync() -> list[str]:
    authored = _load(TEMPLATE_REGISTRY)
    generated = _load(EVENT_TEMPLATE_REGISTRY)
    events = _load(EVENT_REGISTRY)

    errors: list[str] = []

    authored_events = _keys(authored, "events")
    generated_events = _keys(generated, "events")
    unified_events = _keys(events, "events")

    if authored_events != generated_events:
        errors.append(
            "template registry event keys drift from generated event-template registry: "
            f"missing_generated={sorted(authored_events - generated_events)[:10]} "
            f"extra_generated={sorted(generated_events - authored_events)[:10]}"
        )

    generated_only = generated_events - unified_events
    active_generated_only = []
    generated_rows = generated.get("events", {})
    if isinstance(generated_rows, dict):
        for event_id in sorted(generated_only):
            row = generated_rows.get(event_id, {})
            if isinstance(row, dict) and row.get("active") is False:
                continue
            active_generated_only.append(event_id)
    if active_generated_only:
        errors.append(
            "generated event-template registry references active events absent from unified registry: "
            f"{active_generated_only[:10]}"
        )

    for key in ("families", "filter_templates", "operators", "expression_templates"):
        if _keys(authored, key) != _keys(generated, key):
            errors.append(f"{key} keys drift between {TEMPLATE_REGISTRY} and {EVENT_TEMPLATE_REGISTRY}")

    authored_defaults = authored.get("defaults", {})
    generated_defaults = generated.get("defaults", {})
    if authored_defaults != generated_defaults:
        errors.append("defaults drift between template registry and generated event-template registry")

    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check registry sidecars for obvious drift.")
    parser.parse_args(argv)
    errors = check_registry_sync()
    if errors:
        print("Registry sync check failed:")
        for error in errors:
            print(f"- {error}")
        return 1
    print("Registry sync check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
