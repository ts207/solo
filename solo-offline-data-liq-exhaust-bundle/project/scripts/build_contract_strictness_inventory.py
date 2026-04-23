from __future__ import annotations

import argparse
import sys
from pathlib import Path

from project import PROJECT_ROOT
from project.contracts.strictness_inventory import (
    build_contract_strictness_inventory_payload,
    render_contract_strictness_inventory_json,
    render_contract_strictness_inventory_markdown,
)


def _target_paths() -> tuple[Path, Path]:
    generated = PROJECT_ROOT.parent / "docs" / "generated"
    return generated / "contract_strictness_inventory.md", generated / "contract_strictness_inventory.json"


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate contract strictness inventory artifacts.")
    parser.add_argument("--format", choices=("markdown", "json", "both"), default="both")
    parser.add_argument("--check", action="store_true", help="Fail if generated files drift from disk.")
    args = parser.parse_args(argv)

    payload = build_contract_strictness_inventory_payload()
    markdown = render_contract_strictness_inventory_markdown(payload)
    json_text = render_contract_strictness_inventory_json(payload)
    markdown_path, json_path = _target_paths()

    expected: list[tuple[Path, str]] = []
    if args.format in {"markdown", "both"}:
        expected.append((markdown_path, markdown))
    if args.format in {"json", "both"}:
        expected.append((json_path, json_text))

    if args.check:
        drift: list[str] = []
        for path, content in expected:
            current = path.read_text(encoding="utf-8") if path.exists() else None
            if current != content:
                drift.append(str(path))
        if drift:
            for path in drift:
                print(f"contract strictness inventory drift: {path}", file=sys.stderr)
            return 1
        return 0

    for path, content in expected:
        _write(path, content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
