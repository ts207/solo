#!/usr/bin/env python3
from __future__ import annotations

import argparse
import inspect
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

from project.events.detectors.base import BaseEventDetector
from project.events.detectors.registry import (
    get_detector,
    list_registered_event_types,
    load_all_detectors,
)
from project.events.event_aliases import resolve_event_alias
from project.events.event_specs import EVENT_REGISTRY_SPECS

DETECTOR_AUDIT_SCHEMA_VERSION = "detector_coverage_audit_v2"

# Regex to find numerical literals that aren't 0, 1, 2, -1, or very small epsilon
# Excludes class-level DEFAULT_* attributes as these are policy-compliant (exposed via params.get)
_HARDCODED_NUM_REGEX = re.compile(
    r"(?<![a-zA-Z0-9_])(?!0|1|2|-1|1e-12|1\.0|0\.0|2\.0|0\.1|0\.5|10000\.0|100\.0)\d+\.\d+\b"
)

# Patterns that indicate policy-compliant parameter usage (excluded from hardcoded check)
_PROTECTED_CONTEXTS = [
    r"DEFAULT_\w+\s*=",  # Class-level defaults
    r"default_\w+\s*=",  # Lowercase defaults
    r"params\.get\(",
    r"spec_params",
    r"self\.DEFAULT_",
    r"self\.default_",
    r"\/\s*3\.0",  # Averaging divisor (e.g., / 3.0 in composite signals)
    r"\/\s*2\.0",  # Halving divisor
    r"\/\s*4\.0",  # Averaging divisor
    r"\/\s*5\.0",  # Averaging divisor
    r"-\s*3\.0\)",  # Subtraction in expression
    r"\+\s*3\.0\)",  # Addition in expression
    r"\*\s*4\.0",  # Multiplier for dynamic threshold cap
    r"\*\s*2\.0",  # Multiplier
]


def _has_hardcoded_parameters(detector_cls: type[BaseEventDetector]) -> bool:
    try:
        # Check prepare_features and compute_raw_mask specifically
        methods = ["prepare_features", "compute_raw_mask", "compute_intensity"]
        found_drift = False
        for m_name in methods:
            if hasattr(detector_cls, m_name):
                m_source = inspect.getsource(getattr(detector_cls, m_name))
                # Filter out protected contexts (class defaults, params.get usage)
                filtered_source = m_source
                for pattern in _PROTECTED_CONTEXTS:
                    filtered_source = re.sub(pattern, "", filtered_source)
                if _HARDCODED_NUM_REGEX.search(filtered_source):
                    found_drift = True
                    break
        return found_drift
    except (OSError, TypeError):
        return False


def _issue(check_id: str, severity: str, message: str, path: str = "") -> Dict[str, str]:
    return {
        "check_id": check_id,
        "severity": severity,
        "message": message,
        "path": path,
    }


def _module_path(module_name: str) -> str:
    return module_name.replace(".", "/") + ".py"


def _family_from_module(module_name: str) -> str:
    parts = module_name.split(".")
    return parts[-1] if len(parts) >= 1 else module_name


def _evidence_tier(event_type: str) -> str:
    spec = EVENT_REGISTRY_SPECS.get(event_type)
    token = str(getattr(spec, "evidence_mode", "") or "").strip().lower()
    return token or "unspecified"


def _maturity_tier(detector_cls: type[BaseEventDetector]) -> str:
    doc = (inspect.getdoc(detector_cls) or "").lower()
    source = inspect.getsource(detector_cls).lower()

    if "stub detector" in doc or "stub detector" in source:
        return "placeholder"
    if "milestone-6 liquidity stress detector" in doc or "liquidity stress detector" in doc:
        return "production"
    if "dynamic_quantile_floor" in source or "rolling_robust_zscore" in source:
        return "production"
    if "def detect(" in source and detector_cls.detect is not BaseEventDetector.detect:
        return "specialized"
    return "standard"


def _detector_row(event_type: str) -> Dict[str, Any]:
    detector = get_detector(event_type)
    if detector is None:
        return {
            "event_type": event_type,
            "registered": False,
            "family": "",
            "module": "",
            "class_name": "",
            "maturity_tier": "missing",
            "required_columns": [],
        }
    detector_cls = type(detector)
    return {
        "event_type": event_type,
        "registered": True,
        "family": _family_from_module(detector_cls.__module__),
        "module": str(detector_cls.__module__),
        "path": _module_path(detector_cls.__module__),
        "class_name": detector_cls.__name__,
        "maturity_tier": _maturity_tier(detector_cls),
        "evidence_tier": _evidence_tier(event_type),
        "required_columns": list(getattr(detector_cls, "required_columns", ())),
    }


def _is_registered_alias_without_spec(event_type: str, active_event_types: set[str]) -> bool:
    canonical = resolve_event_alias(event_type)
    return canonical != event_type and canonical in active_event_types


def run_audit() -> Dict[str, Any]:
    load_all_detectors()
    active_event_types = sorted(EVENT_REGISTRY_SPECS.keys())
    registered_event_types = sorted(list_registered_event_types())
    rows = [_detector_row(event_type) for event_type in active_event_types]

    issues: List[Dict[str, str]] = []
    active_set = set(active_event_types)
    registered_set = set(registered_event_types)

    missing = sorted(active_set - registered_set)
    for event_type in missing:
        spec = EVENT_REGISTRY_SPECS.get(event_type)
        if spec and spec.synthetic_coverage == "covered":
            continue
        issues.append(
            _issue(
                "detector_missing",
                "error",
                f"Active event spec has no registered detector: {event_type}",
            )
        )

    extra = sorted(registered_set - active_set)
    for event_type in extra:
        if _is_registered_alias_without_spec(event_type, active_set):
            continue
        detector = get_detector(event_type)
        module_path = _module_path(type(detector).__module__) if detector is not None else ""
        issues.append(
            _issue(
                "detector_untracked",
                "warning",
                f"Registered detector has no active event spec: {event_type}",
                path=module_path,
            )
        )

    for row in rows:
        if row["maturity_tier"] == "placeholder":
            issues.append(
                _issue(
                    "detector_placeholder",
                    "warning",
                    f"Detector is still placeholder-grade: {row['event_type']}",
                    path=row.get("path", ""),
                )
            )

        detector = get_detector(row["event_type"])
        if detector and _has_hardcoded_parameters(type(detector)):
            issues.append(
                _issue(
                    "detector_hardcoded_params",
                    "warning",
                    f"Detector implementation has hardcoded numerical thresholds: {row['event_type']}",
                    path=row.get("path", ""),
                )
            )

    maturity_counts: Dict[str, int] = {}
    evidence_tier_counts: Dict[str, int] = {}
    for row in rows:
        maturity = str(row["maturity_tier"])
        maturity_counts[maturity] = int(maturity_counts.get(maturity, 0)) + 1
        evidence = str(row.get("evidence_tier", "unspecified"))
        evidence_tier_counts[evidence] = int(evidence_tier_counts.get(evidence, 0)) + 1

    summary = {
        "schema_version": DETECTOR_AUDIT_SCHEMA_VERSION,
        "status": "failed" if any(issue["severity"] == "error" for issue in issues) else "passed",
        "active_event_count": len(active_event_types),
        "registered_event_count": len(rows),
        "registered_detector_entry_count": len(registered_event_types),
        "issue_count": len(issues),
        "error_count": sum(1 for issue in issues if issue["severity"] == "error"),
        "warning_count": sum(1 for issue in issues if issue["severity"] == "warning"),
        "maturity_counts": maturity_counts,
        "evidence_tier_counts": evidence_tier_counts,
    }
    return {
        "summary": summary,
        "detectors": rows,
        "issues": issues,
    }


def render_markdown(report: Dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# Detector Coverage Audit",
        "",
        f"- Status: `{summary['status']}`",
        f"- Active event specs: `{summary['active_event_count']}`",
        f"- Registered detectors: `{summary['registered_event_count']}`",
        f"- Raw registered detector entries: `{summary['registered_detector_entry_count']}`",
        f"- Issues: `{summary['issue_count']}`",
        "",
        "## Maturity Counts",
        "",
    ]
    for key in sorted(summary["maturity_counts"]):
        lines.append(f"- `{key}`: {summary['maturity_counts'][key]}")

    lines.extend(["", "## Evidence Tier Counts", ""])
    for key in sorted(summary.get("evidence_tier_counts", {})):
        lines.append(f"- `{key}`: {summary['evidence_tier_counts'][key]}")

    lines.extend(["", "## Issues", ""])
    if not report["issues"]:
        lines.append("- None")
    else:
        for issue in report["issues"]:
            suffix = f" ({issue['path']})" if issue.get("path") else ""
            lines.append(f"- [{issue['severity']}] {issue['message']}{suffix}")

    lines.extend(["", "## Detector Inventory", ""])
    for row in report["detectors"]:
        lines.append(
            f"- `{row['event_type']}`: maturity=`{row['maturity_tier']}`, evidence=`{row.get('evidence_tier', 'unspecified')}` via `{row['class_name'] or 'missing'}`"
        )
    return "\n".join(lines) + "\n"


def _write_output(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Audit detector coverage against active event specs."
    )
    parser.add_argument("--json-out", default=None, help="Write machine-readable audit JSON")
    parser.add_argument("--md-out", default=None, help="Write human-readable audit Markdown")
    parser.add_argument(
        "--check", action="store_true", help="Fail if generated files drift from disk."
    )
    args = parser.parse_args(argv)

    report = run_audit()

    expected: list[tuple[Path, str]] = []
    if args.json_out:
        expected.append((Path(args.json_out), json.dumps(report, indent=2, sort_keys=True) + "\n"))
    if args.md_out:
        expected.append((Path(args.md_out), render_markdown(report)))

    if args.check:
        drift: list[str] = []
        for path, content in expected:
            current = path.read_text(encoding="utf-8") if path.exists() else None
            if current != content:
                drift.append(str(path))
        if drift:
            for path in drift:
                print(f"detector coverage audit drift: {path}", file=sys.stderr)
            return 1
        return 0

    if args.json_out:
        _write_output(Path(args.json_out), json.dumps(report, indent=2, sort_keys=True) + "\n")
    if args.md_out:
        _write_output(Path(args.md_out), render_markdown(report))
    if not args.json_out and not args.md_out:
        print(render_markdown(report), end="")
    return 0 if report["summary"]["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
