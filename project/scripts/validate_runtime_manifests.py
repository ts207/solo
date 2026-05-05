#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from project.live.contracts.promoted_thesis import PromotedThesis
from project.live.runtime_admission import validate_runtime_mode_against_theses


def _iter_files(root: Path) -> list[Path]:
    if root.is_file():
        return [root]
    patterns = ["**/promoted_theses.json", "**/*promoted_theses*.json"]
    out: list[Path] = []
    for pat in patterns:
        out.extend(root.glob(pat))
    return sorted(set(out))


def _load_theses(path: Path) -> list[PromotedThesis]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    items: Any = payload.get("theses", payload if isinstance(payload, list) else [])
    if not isinstance(items, list):
        return []
    return [PromotedThesis.model_validate(item) for item in items if isinstance(item, dict)]


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate promoted-thesis runtime manifests.")
    parser.add_argument("root", nargs="?", default="data", help="File or directory to scan")
    parser.add_argument("--runtime-mode", default="simulation", choices=["monitor_only", "simulation", "shadow", "trading"])
    parser.add_argument("--require-manifest", action="store_true", help="Require non-empty RuntimeThesisManifest")
    args = parser.parse_args()

    root = Path(args.root)
    files = _iter_files(root)
    failures: list[str] = []
    checked = 0
    for path in files:
        try:
            theses = _load_theses(path)
            if not theses:
                continue
            validate_runtime_mode_against_theses(args.runtime_mode, theses, require_manifest=bool(args.require_manifest))
            checked += len(theses)
        except Exception as exc:
            failures.append(f"{path}: {exc}")
    if failures:
        for item in failures:
            print(item)
        return 1
    print(f"runtime manifest validation OK: files={len(files)} theses={checked}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
