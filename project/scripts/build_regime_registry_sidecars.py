#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from project import PROJECT_ROOT
from project.spec_registry import load_regime_registry, resolve_relative_spec_path


def _canonical_regime_registry() -> dict[str, Any]:
    payload = load_regime_registry()
    return payload if isinstance(payload, dict) else {}


def build_regime_routing_payload() -> dict[str, Any]:
    canonical = _canonical_regime_registry()
    payload = dict(canonical)
    metadata = canonical.get("metadata", {})
    payload["metadata"] = {
        **(dict(metadata) if isinstance(metadata, dict) else {}),
        "status": "generated",
        "authored_source": "spec/regimes/registry.yaml",
        "routing_profile_id": str((metadata or {}).get("routing_profile_id", "regime_routing_v1")).strip()
        or "regime_routing_v1",
        "scorecard_version": str((metadata or {}).get("scorecard_version", "regime_effectiveness_v1")).strip()
        or "regime_effectiveness_v1",
        "scorecard_source_run": str((metadata or {}).get("scorecard_source_run", "authored_baseline")).strip()
        or "authored_baseline",
    }
    payload["kind"] = "regime_routing"
    return payload


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def main() -> int:
    routing_path = resolve_relative_spec_path(
        "spec/events/regime_routing.yaml",
        repo_root=PROJECT_ROOT.parent,
    )
    _write_yaml(routing_path, build_regime_routing_payload())
    print(f"Wrote {routing_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
