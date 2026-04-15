#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

import yaml

from project import PROJECT_ROOT
from project.domain.registry_loader import build_domain_graph_payload, domain_graph_path


def main() -> int:
    payload = build_domain_graph_payload()
    path = domain_graph_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
