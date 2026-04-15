#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from pathlib import Path
from typing import Any

from project.events.contract_registry import (
    REQUIRED_CONTRACT_FIELDS,
    load_active_event_contracts,
    validate_contract_completeness,
)


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_or_check(path: Path, content: str, *, check: bool) -> bool:
    if check:
        current = path.read_text(encoding="utf-8") if path.exists() else None
        return current == content
    _write(path, content)
    return True


def build_artifacts(base_dir: str = "docs/generated") -> dict[str, Any]:
    out_dir = Path(base_dir)
    contracts = load_active_event_contracts()
    missing = validate_contract_completeness(contracts)

    score_columns = list(next(iter(contracts.values()))["maturity_scores"].keys()) if contracts else []
    matrix_buffer = io.StringIO()
    fieldnames = [
        "event_type",
        "tier",
        "operational_role",
        "deployment_disposition",
        "runtime_category",
        *score_columns,
    ]
    writer = csv.DictWriter(matrix_buffer, fieldnames=fieldnames, lineterminator='\n')
    writer.writeheader()
    for event_type, contract in sorted(contracts.items()):
        row = {
            "event_type": event_type,
            "tier": contract["tier"],
            "operational_role": contract["operational_role"],
            "deployment_disposition": contract["deployment_disposition"],
            "runtime_category": contract["runtime_category"],
        }
        row.update(contract["maturity_scores"])
        writer.writerow(row)
    matrix_csv = matrix_buffer.getvalue()

    tiers_md = ["# Event tiers", ""]
    grouped: dict[str, list[str]] = {}
    for event_type, contract in contracts.items():
        grouped.setdefault(contract["tier"], []).append(event_type)
    for tier in sorted(grouped):
        tiers_md.append(f"## Tier {tier}")
        tiers_md.append("")
        for event_type in sorted(grouped[tier]):
            contract = contracts[event_type]
            tiers_md.append(
                f"- `{event_type}` — role `{contract['operational_role']}`, disposition `{contract['deployment_disposition']}`"
            )
        tiers_md.append("")
    event_tiers_text = "\n".join(tiers_md).rstrip() + "\n"

    completeness_payload = {
        "summary": {
            "active_event_count": len(contracts),
            "complete_event_count": len(contracts) - len(missing),
            "missing_event_count": len(missing),
            "required_fields": list(REQUIRED_CONTRACT_FIELDS),
        },
        "missing": missing,
    }
    completeness_json = json.dumps(completeness_payload, indent=2, sort_keys=True) + "\n"

    md_lines = [
        "# Event contract completeness",
        "",
        f"- Active events: `{len(contracts)}`",
        f"- Complete contracts: `{len(contracts) - len(missing)}`",
        f"- Missing contracts: `{len(missing)}`",
        "",
        "## Missing fields by event",
        "",
    ]
    if not missing:
        md_lines.append("- None")
    else:
        for event_type, fields in sorted(missing.items()):
            md_lines.append(f"- `{event_type}`: {', '.join(fields)}")
    completeness_md = "\n".join(md_lines) + "\n"

    return {
        "payload": completeness_payload,
        "outputs": {
            out_dir / "event_maturity_matrix.csv": matrix_csv,
            out_dir / "event_tiers.md": event_tiers_text,
            out_dir / "event_contract_completeness.json": completeness_json,
            out_dir / "event_contract_completeness.md": completeness_md,
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate event contract governance artifacts.")
    parser.add_argument("--base-dir", default="docs/generated")
    parser.add_argument("--check", action="store_true", help="Fail if generated artifacts drift.")
    args = parser.parse_args(argv)

    built = build_artifacts(args.base_dir)
    drift: list[str] = []
    for path, content in built["outputs"].items():
        if not _write_or_check(path, content, check=args.check):
            drift.append(str(path))
    if drift:
        for path in drift:
            print(f"event contract artifact drift: {path}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
