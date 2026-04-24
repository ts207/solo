#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

from project import PROJECT_ROOT
from project.events.canonical_registry_sidecars import (
    canonical_event_registry_payload,
    event_contract_overrides_payload,
    event_ontology_mapping_payload,
)

REPO_ROOT = PROJECT_ROOT.parent


def _write_yaml(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def main() -> int:
    outputs = {
        REPO_ROOT / "spec" / "events" / "event_ontology_mapping.yaml": event_ontology_mapping_payload(),
        REPO_ROOT / "spec" / "events" / "event_contract_overrides.yaml": event_contract_overrides_payload(),
        REPO_ROOT / "spec" / "events" / "canonical_event_registry.yaml": canonical_event_registry_payload(),
    }
    for path, payload in outputs.items():
        _write_yaml(path, payload)
        print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
