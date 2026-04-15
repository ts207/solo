#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from project.domain.compiled_registry import get_domain_registry


def _runtime_payload() -> dict[str, object]:
    registry = get_domain_registry()
    events: dict[str, dict[str, object]] = {}
    for event_type in registry.event_ids:
        event = registry.event_definitions[event_type]
        events[event_type] = {
            "detector": event.detector_name,
            "enabled": event.enabled,
            "family": event.canonical_family or event.canonical_regime,
            "instrument_classes": list(event.instrument_classes),
            "requires_features": list(event.requires_features),
            "sequence_eligible": event.sequence_eligible,
            "tags": list(event.runtime_tags),
        }
    return {"events": events}


def _write_or_check(path: Path, content: str, *, check: bool) -> bool:
    if check:
        current = path.read_text(encoding="utf-8") if path.exists() else None
        return current == content
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate the runtime event registry from event specs.")
    parser.add_argument(
        "--output",
        default="project/configs/registries/events.yaml",
        help="Path to the generated runtime event registry.",
    )
    parser.add_argument("--check", action="store_true", help="Fail if the generated registry drifts.")
    args = parser.parse_args(argv)

    path = Path(args.output)
    content = yaml.safe_dump(_runtime_payload(), sort_keys=False)
    if not _write_or_check(path, content, check=args.check):
        print(f"runtime event registry drift: {path}", file=sys.stderr)
        return 1
    if not args.check:
        print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
