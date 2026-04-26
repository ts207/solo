#!/usr/bin/env python3
from __future__ import annotations

import argparse
import difflib

import yaml

from project.domain.registry_loader import build_domain_graph_payload, domain_graph_path


def _render(payload: object) -> str:
    return yaml.safe_dump(payload, sort_keys=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build or check the generated domain graph.")
    parser.add_argument("--check", action="store_true", help="fail if the checked-in graph is stale")
    args = parser.parse_args(argv)

    payload = build_domain_graph_payload()
    path = domain_graph_path()
    rendered = _render(payload)

    if args.check:
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
            print(f"Domain graph is stale: {path}")
            if diff:
                print(diff)
            return 1
        print(f"Domain graph is fresh: {path}")
        return 0

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(rendered, encoding="utf-8")
    print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
