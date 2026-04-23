from __future__ import annotations

import argparse
import sys
from typing import Any

import yaml

from project.domain.registry_loader import domain_graph_path, spec_sources_digest


def _load_graph_metadata() -> dict[str, Any]:
    path = domain_graph_path()
    if not path.exists():
        raise FileNotFoundError(f"compiled domain graph missing: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"compiled domain graph must be a mapping: {path}")
    metadata = payload.get("metadata", {})
    if not isinstance(metadata, dict):
        return {}
    return dict(metadata)


def check_domain_graph_freshness() -> tuple[bool, dict[str, Any]]:
    metadata = _load_graph_metadata()
    recorded = str(metadata.get("spec_sources_digest", "") or "").strip()
    current = spec_sources_digest()
    return recorded == current and bool(recorded), {
        "recorded_spec_sources_digest": recorded,
        "current_spec_sources_digest": current,
        "generated_at_utc": str(metadata.get("generated_at_utc", "") or ""),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check compiled domain graph freshness.")
    parser.parse_args(argv)

    try:
        fresh, details = check_domain_graph_freshness()
    except Exception as exc:
        print(f"domain graph freshness check failed: {exc}", file=sys.stderr)
        return 1
    if fresh:
        print("domain graph freshness: OK")
        return 0
    print(
        "domain graph freshness: STALE. Rebuild with "
        "`PYTHONPATH=. python3 project/scripts/build_domain_graph.py`.",
        file=sys.stderr,
    )
    print(
        "recorded_spec_sources_digest="
        f"{details['recorded_spec_sources_digest'] or '<missing>'}",
        file=sys.stderr,
    )
    print(f"current_spec_sources_digest={details['current_spec_sources_digest']}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
