#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

from project.domain.compiled_registry import get_domain_registry
from project.events.ontology_mapping import (
    ontology_rows_by_event,
    validate_mapping_rows,
)


def _issue(check_id: str, severity: str, message: str) -> Dict[str, str]:
    return {"check_id": check_id, "severity": severity, "message": message}


def _proxy_direct_groups(rows: Mapping[str, Mapping[str, Any]]) -> Dict[str, list[str]]:
    out: Dict[str, list[str]] = {}
    for event_type in rows:
        token = str(event_type).strip().upper()
        stem = token.replace("_DIRECT", "").replace("_PROXY", "")
        out.setdefault(stem, []).append(token)
    return out


def run_audit() -> Dict[str, Any]:
    registry = get_domain_registry()
    mapped_rows = ontology_rows_by_event()
    issues: list[Dict[str, str]] = []

    active_event_ids = set(registry.event_ids)
    mapped_event_ids = set(mapped_rows.keys())

    missing = sorted(active_event_ids - mapped_event_ids)
    extra = sorted(mapped_event_ids - active_event_ids)
    for event_type in missing:
        issues.append(_issue("mapping_missing", "error", f"Active event missing ontology row: {event_type}"))
    for event_type in extra:
        issues.append(_issue("mapping_extra", "warning", f"Ontology row has no active event: {event_type}"))

    for issue in validate_mapping_rows(mapped_rows):
        issues.append(_issue("mapping_invalid", "error", issue))

    for event_type in sorted(active_event_ids):
        spec = registry.get_event(event_type)
        if spec is None:
            continue
        if not spec.canonical_regime or not spec.subtype or not spec.phase or not spec.evidence_mode:
            issues.append(
                _issue(
                    "runtime_missing_fields",
                    "error",
                    f"{event_type}: compiled registry missing canonical ontology fields",
                )
            )
        if sum([spec.is_composite, spec.is_context_tag, spec.is_strategy_construct]) > 1:
            issues.append(
                _issue(
                    "runtime_layer_conflict",
                    "error",
                    f"{event_type}: multiple non-canonical layer flags enabled",
                )
            )
        if event_type.startswith("SEQ_") and not spec.is_composite:
            issues.append(
                _issue(
                    "sequence_not_composite",
                    "error",
                    f"{event_type}: SEQ_* event must be marked composite",
                )
            )

    for stem, event_types in sorted(_proxy_direct_groups(mapped_rows).items()):
        if len(event_types) < 2:
            continue
        regimes = {
            str(mapped_rows[event_type].get("canonical_regime", "")).strip().upper()
            for event_type in event_types
        }
        if len(regimes) > 1:
            issues.append(
                _issue(
                    "proxy_direct_regime_drift",
                    "error",
                    f"{stem}: direct/proxy variants resolve to multiple canonical regimes {sorted(regimes)}",
                )
            )

    default_executable = set(registry.default_executable_event_ids())
    leaked = sorted(
        event_type
        for event_type in default_executable
        if (spec := registry.get_event(event_type)) is not None and spec.is_strategy_construct
    )
    for event_type in leaked:
        issues.append(
            _issue(
                "strategy_leak",
                "error",
                f"{event_type}: strategy construct leaked into default executable event set",
            )
        )

    summary = {
        "status": "failed" if any(issue["severity"] == "error" for issue in issues) else "passed",
        "active_event_count": len(active_event_ids),
        "mapped_event_count": len(mapped_event_ids),
        "default_executable_event_count": len(default_executable),
        "issue_count": len(issues),
        "error_count": sum(1 for issue in issues if issue["severity"] == "error"),
        "warning_count": sum(1 for issue in issues if issue["severity"] == "warning"),
    }
    return {
        "summary": summary,
        "default_executable_event_ids": sorted(default_executable),
        "canonical_regime_map": registry.canonical_regime_rows(),
        "issues": issues,
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# Event Ontology Audit",
        "",
        f"- Status: `{summary['status']}`",
        f"- Active events: `{summary['active_event_count']}`",
        f"- Mapped events: `{summary['mapped_event_count']}`",
        f"- Default executable events: `{summary['default_executable_event_count']}`",
        f"- Issues: `{summary['issue_count']}`",
        "",
        "## Issues",
        "",
    ]
    issues = report.get("issues", [])
    if not issues:
        lines.append("- None")
    else:
        for issue in issues:
            lines.append(f"- [{issue['severity']}] {issue['message']}")
    return "\n".join(lines) + "\n"


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit event ontology mapping and runtime wiring.")
    parser.add_argument("--json-out", default="docs/generated/event_ontology_audit.json")
    parser.add_argument("--md-out", default="docs/generated/event_ontology_audit.md")
    parser.add_argument("--check", action="store_true", help="Fail if outputs drift from disk.")
    args = parser.parse_args(argv)

    report = run_audit()
    expected_json = json.dumps(report, indent=2, sort_keys=True) + "\n"
    expected_md = render_markdown(report)

    if args.check:
        drift: list[str] = []
        for path, content in ((Path(args.json_out), expected_json), (Path(args.md_out), expected_md)):
            current = path.read_text(encoding="utf-8") if path.exists() else None
            if current != content:
                drift.append(str(path))
        if drift:
            for path in drift:
                print(f"event ontology audit drift: {path}", file=sys.stderr)
            return 1
        return 0

    _write(Path(args.json_out), expected_json)
    _write(Path(args.md_out), expected_md)
    return 0 if report["summary"]["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
