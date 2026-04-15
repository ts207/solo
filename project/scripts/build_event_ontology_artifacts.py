#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

from project.domain.compiled_registry import get_domain_registry


def _json_text(payload: object) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _write_or_check(path: Path, content: str, *, check: bool) -> bool:
    if check:
        current = path.read_text(encoding="utf-8") if path.exists() else None
        return current == content
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return True


def _rows() -> list[Dict[str, Any]]:
    registry = get_domain_registry()
    rows: list[Dict[str, Any]] = []
    for event_type in registry.event_ids:
        spec = registry.get_event(event_type)
        if spec is None:
            continue
        rows.append(
            {
                "event_type": spec.event_type,
                "canonical_regime": spec.canonical_regime,
                "subtype": spec.subtype,
                "phase": spec.phase,
                "evidence_mode": spec.evidence_mode,
                "layer": spec.layer,
                "disposition": spec.disposition,
                "asset_scope": spec.asset_scope,
                "venue_scope": spec.venue_scope,
                "deconflict_priority": spec.deconflict_priority,
                "research_only": spec.research_only,
                "strategy_only": spec.strategy_only,
                "is_composite": spec.is_composite,
                "is_context_tag": spec.is_context_tag,
                "is_strategy_construct": spec.is_strategy_construct,
                "notes": spec.notes,
            }
        )
    return rows


def _catalog(rows: Iterable[Mapping[str, Any]], *, flag: str) -> list[Dict[str, Any]]:
    return [dict(row) for row in rows if bool(row.get(flag, False))]


def _canonical_map(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        regime = str(row.get("canonical_regime", "")).strip()
        if not regime:
            continue
        bucket = grouped.setdefault(
            regime,
            {"event_types": [], "default_executable_event_types": []},
        )
        event_type = str(row.get("event_type", "")).strip()
        if event_type:
            bucket["event_types"].append(event_type)
            if not any(
                bool(row.get(flag, False))
                for flag in ("is_composite", "is_context_tag", "is_strategy_construct")
            ):
                bucket["default_executable_event_types"].append(event_type)
    return {
        regime: {
            "event_types": sorted(values["event_types"]),
            "default_executable_event_types": sorted(values["default_executable_event_types"]),
        }
        for regime, values in sorted(grouped.items())
    }


def _render_mapping_markdown(rows: list[Mapping[str, Any]]) -> str:
    lines = [
        "# Event Ontology Mapping",
        "",
        "| event_type | canonical_regime | subtype | phase | evidence_mode | layer | disposition | research_only | strategy_only |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| {event_type} | {canonical_regime} | {subtype} | {phase} | {evidence_mode} | {layer} | {disposition} | {research_only} | {strategy_only} |".format(
                **row
            )
        )
    return "\n".join(lines) + "\n"


def _render_catalog_markdown(title: str, rows: list[Mapping[str, Any]]) -> str:
    lines = [f"# {title}", ""]
    if not rows:
        lines.append("- None")
        return "\n".join(lines) + "\n"
    lines.extend(
        [
            "| event_type | canonical_regime | subtype | phase | evidence_mode | notes |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in rows:
        lines.append(
            "| {event_type} | {canonical_regime} | {subtype} | {phase} | {evidence_mode} | {notes} |".format(
                **row
            )
        )
    return "\n".join(lines) + "\n"


def _render_canonical_map_markdown(payload: Mapping[str, Mapping[str, Any]]) -> str:
    lines = ["# Canonical To Raw Event Map", ""]
    for regime, row in payload.items():
        lines.append(f"## {regime}")
        lines.append("")
        lines.append(f"- Raw event types: `{', '.join(row['event_types'])}`")
        lines.append(
            f"- Default executable event types: `{', '.join(row['default_executable_event_types'])}`"
        )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def build_outputs(base_dir: str = "docs/generated") -> Dict[Path, str]:
    rows = _rows()
    mapping_payload = {"rows": rows}
    canonical_payload = _canonical_map(rows)
    composite_rows = _catalog(rows, flag="is_composite")
    context_rows = _catalog(rows, flag="is_context_tag")
    strategy_rows = _catalog(rows, flag="is_strategy_construct")
    out_root = Path(base_dir)
    return {
        out_root / "event_ontology_mapping.json": _json_text(mapping_payload),
        out_root / "event_ontology_mapping.md": _render_mapping_markdown(rows),
        out_root / "canonical_to_raw_event_map.json": _json_text(canonical_payload),
        out_root / "canonical_to_raw_event_map.md": _render_canonical_map_markdown(canonical_payload),
        out_root / "composite_event_catalog.json": _json_text({"rows": composite_rows}),
        out_root / "composite_event_catalog.md": _render_catalog_markdown(
            "Composite Event Catalog", composite_rows
        ),
        out_root / "context_tag_catalog.json": _json_text({"rows": context_rows}),
        out_root / "context_tag_catalog.md": _render_catalog_markdown(
            "Context Tag Catalog", context_rows
        ),
        out_root / "strategy_construct_catalog.json": _json_text({"rows": strategy_rows}),
        out_root / "strategy_construct_catalog.md": _render_catalog_markdown(
            "Strategy Construct Catalog", strategy_rows
        ),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate event ontology mapping artifacts.")
    parser.add_argument("--base-dir", default="docs/generated")
    parser.add_argument("--check", action="store_true", help="Fail if generated artifacts drift.")
    args = parser.parse_args(argv)

    drift: list[str] = []
    for path, content in build_outputs(args.base_dir).items():
        if not _write_or_check(path, content, check=args.check):
            drift.append(str(path))
    if drift:
        for path in drift:
            print(f"event ontology artifact drift: {path}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
