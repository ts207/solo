#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

REQUIRED_TOP = {
    "event_side": "unknown",
    "event_direction": 0,
    "polarity_semantics": "unknown",
    "polarity_source": "migration_default",
    "magnitude_source": "migration_default",
    "anchor_role": "research_only",
    "compatibility_status": "research_only",
    "compatibility_reason_codes": "legacy_fixture_migrated",
    "mechanism_label": "unavailable",
    "mechanism_valid": False,
    "mechanism_success_rate": 0.0,
}


def _manifest(thesis_id: str, state: str) -> dict[str, Any]:
    return {
        "thesis_id": thesis_id,
        "thesis_version": "legacy_fixture_migration_v1",
        "promotion_state": state,
        "event_contract_hash": "fixture:event",
        "template_contract_hash": "fixture:template",
        "domain_graph_hash": "fixture:domain",
        "evidence_bundle_hash": "fixture:evidence",
        "risk_contract_hash": "fixture:risk",
        "allowed_runtime_modes": ["monitor_only", "simulation"],
    }


def _migrate_obj(obj: Any) -> tuple[Any, bool]:
    changed = False
    if isinstance(obj, dict):
        for key, value in REQUIRED_TOP.items():
            if key not in obj:
                obj[key] = value
                changed = True
        if "thesis_id" in obj and "runtime_manifest" not in obj:
            state = str(obj.get("deployment_state", obj.get("promotion_state", "paper_only")))
            obj["runtime_manifest"] = _manifest(str(obj.get("thesis_id", "fixture_thesis")), state)
            changed = True
        for key, value in list(obj.items()):
            new_value, child_changed = _migrate_obj(value)
            obj[key] = new_value
            changed = changed or child_changed
    elif isinstance(obj, list):
        for i, value in enumerate(obj):
            new_value, child_changed = _migrate_obj(value)
            obj[i] = new_value
            changed = changed or child_changed
    return obj, changed


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate JSON contract fixtures to v10 semantic/runtime fields.")
    parser.add_argument("paths", nargs="+", help="JSON files or directories")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    files: list[Path] = []
    for raw in args.paths:
        p = Path(raw)
        if p.is_dir():
            files.extend(p.glob("**/*.json"))
        elif p.suffix == ".json":
            files.append(p)
    changed_files = 0
    for path in sorted(set(files)):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        migrated, changed = _migrate_obj(data)
        if changed:
            changed_files += 1
            print(f"migrated_needed: {path}")
            if args.write:
                path.write_text(json.dumps(migrated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"fixture migration scan complete: files={len(files)} changed={changed_files} write={args.write}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
