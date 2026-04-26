#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from project.domain.compiled_registry import get_domain_registry
from project.events.contract_registry import (
    load_active_event_contracts,
    validate_contract_completeness,
)
from project.events.detectors.registry import (
    get_detector,
    load_all_detectors,
)
from project.events.event_aliases import EVENT_ALIASES, EXECUTABLE_EVENT_ALIASES
from project.events.event_specs import EVENT_REGISTRY_SPECS
from project.events.governance import (
    get_event_governance_metadata,
    governed_default_planning_event_ids,
)
from project.events.ontology_mapping import ontology_rows_by_event
from project.scripts.detector_coverage_audit import run_audit as run_detector_audit
from project.scripts.event_ontology_audit import run_audit as run_ontology_audit
from project.scripts.regime_routing_audit import validate_regime_routing_spec

_GENERIC_CALIBRATION_METHOD = (
    "Documented by detector-specific calibration policy and stability checks."
)


def _status(*, passed: bool) -> str:
    return "passed" if passed else "attention"


def _as_int_counter(values: Iterable[str]) -> dict[str, int]:
    counter = Counter(str(value) for value in values)
    return {key: int(counter[key]) for key in sorted(counter)}


def _required_columns_by_event() -> dict[str, list[str]]:
    load_all_detectors()
    rows: dict[str, list[str]] = {}
    for event_type in sorted(EVENT_REGISTRY_SPECS):
        detector = get_detector(event_type)
        if detector is None:
            rows[event_type] = []
            continue
        rows[event_type] = list(getattr(type(detector), "required_columns", ()))
    return rows


def _build_tasks() -> list[dict[str, Any]]:
    registry = get_domain_registry()
    contracts = load_active_event_contracts()
    completeness_missing = validate_contract_completeness(contracts)
    ontology_report = run_ontology_audit()
    detector_report = run_detector_audit()
    regime_report = validate_regime_routing_spec()
    required_columns_map = _required_columns_by_event()
    ontology_rows = ontology_rows_by_event()

    active_event_ids = sorted(contracts)
    default_executable_event_ids = sorted(registry.default_executable_event_ids())
    planning_event_ids = list(governed_default_planning_event_ids())
    runtime_event_ids = sorted(registry.runtime_eligible_event_ids())
    promotion_event_ids = sorted(registry.promotion_eligible_event_ids())
    detector_rows = list(detector_report["detectors"])
    proxy_evidence_event_ids = sorted(
        row["event_type"]
        for row in detector_rows
        if str(row.get("evidence_tier", "")).strip().lower() == "proxy"
    )
    proxy_planning_event_ids = sorted(set(proxy_evidence_event_ids) & set(planning_event_ids))
    descriptive_non_planning_active = sorted(
        event_id
        for event_id in set(active_event_ids) - set(planning_event_ids)
        if str(get_event_governance_metadata(event_id).get("operational_role", "")).lower()
        in {"context", "filter", "research_only", "sequence_component"}
        or str(get_event_governance_metadata(event_id).get("deployment_disposition", "")).lower()
        in {
            "context_only",
            "research_only",
            "repair_before_promotion",
            "inactive",
            "deprecated",
            "alias_only",
        }
    )

    tier_counts = _as_int_counter(contract["tier"] for contract in contracts.values())
    role_counts = _as_int_counter(contract["operational_role"] for contract in contracts.values())
    disposition_counts = _as_int_counter(
        contract["deployment_disposition"] for contract in contracts.values()
    )
    threshold_method_counts = _as_int_counter(
        contract["threshold_method"] for contract in contracts.values()
    )
    calibration_method_counts = _as_int_counter(
        contract["calibration_method"] for contract in contracts.values()
    )

    generic_calibration_events = sorted(
        event_type
        for event_type, contract in contracts.items()
        if str(contract.get("calibration_method", "")).strip() == _GENERIC_CALIBRATION_METHOD
    )
    declared_threshold_events = sorted(
        event_type
        for event_type, contract in contracts.items()
        if str(contract.get("threshold_method", "")).strip() == "declared_detector_threshold"
    )

    required_feature_counter: Counter[str] = Counter()
    required_feature_lengths: list[int] = []
    disabled_regime_lengths: list[int] = []
    overlap_lengths: list[int] = []
    for contract in contracts.values():
        features = [str(value) for value in contract.get("required_features", [])]
        required_feature_counter.update(features)
        required_feature_lengths.append(len(features))
        disabled_regime_lengths.append(len(contract.get("disabled_regimes", [])))
        overlap_lengths.append(len(contract.get("expected_overlap", [])))

    required_column_counter: Counter[str] = Counter()
    empty_required_columns: list[str] = []
    for event_type, columns in required_columns_map.items():
        if not columns:
            empty_required_columns.append(event_type)
        required_column_counter.update(columns)

    sibling_groups: dict[str, list[str]] = defaultdict(list)
    ontology_key_groups: dict[tuple[str, str, str, str], list[str]] = defaultdict(list)
    for event_type, row in sorted(ontology_rows.items()):
        sibling_groups[event_type.replace("_DIRECT", "").replace("_PROXY", "")].append(event_type)
        ontology_key_groups[
            (
                str(row.get("canonical_regime", "")).strip().upper(),
                str(row.get("subtype", "")).strip().upper(),
                str(row.get("phase", "")).strip().upper(),
                str(row.get("evidence_mode", "")).strip().upper(),
            )
        ].append(event_type)

    direct_proxy_groups = {
        key: value for key, value in sorted(sibling_groups.items()) if len(value) > 1
    }
    ontology_collisions = {
        "|".join(key): sorted(value)
        for key, value in sorted(ontology_key_groups.items())
        if len(value) > 1
    }

    workflow_paths = [
        ".github/workflows/tier1.yml",
        ".github/workflows/tier2.yml",
        ".github/workflows/tier3.yml",
    ]
    guard_paths = [
        "project/scripts/build_event_contract_artifacts.py",
        "project/scripts/build_event_ontology_artifacts.py",
        "project/scripts/build_event_deep_analysis_suite.py",
        "project/scripts/detector_coverage_audit.py",
        "project/scripts/event_ontology_audit.py",
        "project/scripts/regime_routing_audit.py",
        "project/tests/docs/test_event_governance_artifacts.py",
        "project/tests/docs/test_event_deep_analysis_suite.py",
        "project/tests/events/test_event_governance_integration.py",
        "project/tests/scripts/test_detector_coverage_audit.py",
        "project/tests/scripts/test_event_deep_analysis_suite.py",
    ]

    repo_root = Path(__file__).resolve().parents[2]
    existing_guard_paths = [path for path in guard_paths if (repo_root / path).exists()]
    existing_workflow_paths = [path for path in workflow_paths if (repo_root / path).exists()]

    tasks: list[dict[str, Any]] = [
        {
            "id": "01_event_universe",
            "title": "Audit event universe",
            "status": _status(
                passed=ontology_report["summary"]["error_count"] == 0
                and detector_report["summary"]["error_count"] == 0
            ),
            "summary": {
                "active_event_count": len(active_event_ids),
                "default_executable_event_count": len(default_executable_event_ids),
                "planning_event_count": len(planning_event_ids),
                "runtime_eligible_event_count": len(runtime_event_ids),
                "promotion_eligible_event_count": len(promotion_event_ids),
                "registered_detector_entry_count": int(
                    detector_report["summary"]["registered_detector_entry_count"]
                ),
                "alias_count": int(len(EVENT_ALIASES) + len(EXECUTABLE_EVENT_ALIASES)),
            },
            "details": {
                "runtime_aliases": sorted(
                    {*EVENT_ALIASES.keys(), *EXECUTABLE_EVENT_ALIASES.keys()}
                ),
                "default_executable_but_not_planning": sorted(
                    set(default_executable_event_ids) - set(planning_event_ids)
                ),
                "planning_not_default_executable": sorted(
                    set(planning_event_ids) - set(default_executable_event_ids)
                ),
                "source_audits": [
                    "docs/generated/event_ontology_audit.json",
                    "docs/generated/detector_coverage.json",
                ],
            },
            "verification_commands": [
                "python -m project.scripts.event_ontology_audit --check",
                "python -m project.scripts.detector_coverage_audit --check --json-out docs/generated/detector_coverage.json --md-out docs/generated/detector_coverage.md",
            ],
        },
        {
            "id": "02_event_contracts",
            "title": "Review event contracts",
            "status": _status(passed=not completeness_missing),
            "summary": {
                "active_event_count": len(contracts),
                "complete_event_count": int(len(contracts) - len(completeness_missing)),
                "missing_event_count": len(completeness_missing),
            },
            "details": {
                "missing_fields": completeness_missing,
                "source_artifacts": [
                    "docs/generated/event_contract_completeness.json",
                    "docs/generated/event_contract_completeness.md",
                ],
            },
            "verification_commands": [
                "python -m project.scripts.build_event_contract_artifacts --check",
            ],
        },
        {
            "id": "03_detector_fidelity",
            "title": "Inspect detector fidelity",
            "status": _status(passed=detector_report["summary"]["issue_count"] == 0),
            "summary": {
                "active_event_count": int(detector_report["summary"]["active_event_count"]),
                "issue_count": int(detector_report["summary"]["issue_count"]),
                "warning_count": int(detector_report["summary"]["warning_count"]),
                "error_count": int(detector_report["summary"]["error_count"]),
            },
            "details": {
                "maturity_counts": detector_report["summary"]["maturity_counts"],
                "evidence_tier_counts": detector_report["summary"].get("evidence_tier_counts", {}),
                "issues": detector_report["issues"],
                "source_artifacts": [
                    "docs/generated/detector_coverage.json",
                    "docs/generated/detector_coverage.md",
                ],
            },
            "verification_commands": [
                "python -m project.scripts.detector_coverage_audit --check --json-out docs/generated/detector_coverage.json --md-out docs/generated/detector_coverage.md",
            ],
        },
        {
            "id": "04_maturity_tiers",
            "title": "Check maturity tiers",
            "status": _status(
                passed=(tier_counts.get("X", 0) == 0 and tier_counts.get("D", 0) <= 5)
            ),
            "summary": {
                "tier_counts": tier_counts,
                "role_counts": role_counts,
                "deployment_disposition_counts": disposition_counts,
            },
            "details": {
                "planning_default_tiers": ["A", "B"],
                "planning_event_count": len(planning_event_ids),
                "non_planning_active_events": sorted(
                    set(active_event_ids) - set(planning_event_ids)
                ),
                "source_artifacts": [
                    "docs/generated/event_maturity_matrix.csv",
                    "docs/generated/event_tiers.md",
                ],
            },
            "verification_commands": [
                "python -m project.scripts.build_event_contract_artifacts --check",
                "python -m pytest project/tests/events/test_event_governance_integration.py -q",
            ],
        },
        {
            "id": "05_threshold_calibration",
            "title": "Audit thresholds and calibration",
            "status": _status(
                passed=(
                    len(generic_calibration_events) == 0 and len(declared_threshold_events) == 0
                )
            ),
            "summary": {
                "threshold_method_counts": threshold_method_counts,
                "calibration_method_counts": calibration_method_counts,
                "generic_calibration_event_count": len(generic_calibration_events),
                "declared_threshold_event_count": len(declared_threshold_events),
            },
            "details": {
                "generic_calibration_events": generic_calibration_events,
                "declared_threshold_events": declared_threshold_events,
            },
            "verification_commands": [
                "python -m project.scripts.detector_coverage_audit --check --json-out docs/generated/detector_coverage.json --md-out docs/generated/detector_coverage.md",
                "python -m pytest project/tests/events/test_detector_hardening.py -q",
            ],
        },
        {
            "id": "06_overlap_collisions",
            "title": "Map overlap and collisions",
            "status": _status(passed=not ontology_collisions),
            "summary": {
                "contracts_with_overlap_notes": int(
                    sum(1 for count in overlap_lengths if count > 0)
                ),
                "avg_expected_overlap_entries": round(
                    sum(overlap_lengths) / max(1, len(overlap_lengths)), 3
                ),
                "direct_proxy_group_count": len(direct_proxy_groups),
                "ontology_collision_group_count": len(ontology_collisions),
            },
            "details": {
                "direct_proxy_groups": direct_proxy_groups,
                "ontology_collisions": ontology_collisions,
            },
            "verification_commands": [
                "python -m project.scripts.event_ontology_audit --check",
                "python -m pytest project/tests/events/test_ontology_deconfliction.py -q",
            ],
        },
        {
            "id": "07_regime_restrictions",
            "title": "Review regime restrictions",
            "status": _status(passed=bool(regime_report.get("is_valid", False))),
            "summary": {
                "routed_regime_count": len(regime_report.get("routed_regimes", [])),
                "missing_regime_count": len(regime_report.get("missing_regimes", [])),
                "contracts_with_disabled_regimes": int(
                    sum(1 for count in disabled_regime_lengths if count > 0)
                ),
                "avg_disabled_regime_entries": round(
                    sum(disabled_regime_lengths) / max(1, len(disabled_regime_lengths)), 3
                ),
            },
            "details": {
                "missing_regimes": regime_report.get("missing_regimes", []),
                "unexpected_regimes": regime_report.get("unexpected_regimes", []),
                "non_routable_entries": regime_report.get("non_routable_entries", []),
                "invalid_templates": regime_report.get("invalid_templates", {}),
            },
            "verification_commands": [
                "python -m project.scripts.regime_routing_audit --check",
            ],
        },
        {
            "id": "08_data_dependencies",
            "title": "Validate data dependencies",
            "status": _status(passed=not empty_required_columns),
            "summary": {
                "events_with_required_features": int(
                    sum(1 for count in required_feature_lengths if count > 0)
                ),
                "unique_required_feature_count": len(required_feature_counter),
                "unique_required_column_count": len(required_column_counter),
                "events_missing_required_columns": len(empty_required_columns),
            },
            "details": {
                "top_required_features": dict(required_feature_counter.most_common(15)),
                "top_required_columns": dict(required_column_counter.most_common(15)),
                "events_missing_required_columns": empty_required_columns,
            },
            "verification_commands": [
                "python -m pytest project/tests/events/test_detector_contract.py -q",
                "python -m pytest project/tests/events/test_registry_loader.py -q",
            ],
        },
        {
            "id": "09_ci_event_guards",
            "title": "Test CI event guards",
            "status": _status(
                passed=len(existing_guard_paths) == len(guard_paths)
                and len(existing_workflow_paths) == len(workflow_paths)
            ),
            "summary": {
                "configured_guard_path_count": len(existing_guard_paths),
                "expected_guard_path_count": len(guard_paths),
                "workflow_file_count": len(existing_workflow_paths),
            },
            "details": {
                "guard_paths": existing_guard_paths,
                "workflow_paths": existing_workflow_paths,
            },
            "verification_commands": [
                "bash project/scripts/pre_commit.sh",
                "python -m project.scripts.run_researcher_verification --mode contracts",
            ],
        },
    ]

    residual_priorities: list[dict[str, Any]] = []
    if generic_calibration_events:
        residual_priorities.append(
            {
                "label": "replace_generic_calibration_text",
                "reason": "Most active events still rely on the generic calibration-method fallback, which weakens spec-to-code fidelity for future promotion review.",
                "event_count": len(generic_calibration_events),
            }
        )
    if declared_threshold_events:
        residual_priorities.append(
            {
                "label": "parameterize_thresholds_more_explicitly",
                "reason": "Many active events still resolve to declared_detector_threshold rather than an explicit calibrated search-range or detector-specific threshold policy.",
                "event_count": len(declared_threshold_events),
            }
        )
    proxy_count = len(proxy_evidence_event_ids)
    proxy_planning_count = len(proxy_planning_event_ids)
    if proxy_planning_count:
        residual_priorities.append(
            {
                "label": "quarantine_proxy_evidence_from_default_planning",
                "reason": "Proxy-evidence events should remain outside the default planning set until they are upgraded or explicitly requested for exploratory work.",
                "event_count": proxy_planning_count,
            }
        )
    if descriptive_non_planning_active:
        residual_priorities.append(
            {
                "label": "keep_context_and_research_events_out_of_default_planning",
                "reason": "Active events that are context-only, research-only, or sequence-only must stay outside the default planning set to preserve promotion safety.",
                "event_count": len(descriptive_non_planning_active),
            }
        )
    for idx, priority in enumerate(residual_priorities, start=1):
        priority["priority"] = idx

    recommended_next_actions = []
    if generic_calibration_events or declared_threshold_events:
        recommended_next_actions.append(
            "Keep contract-level threshold and calibration policies explicit so future event additions do not fall back to generic placeholders."
        )
    if proxy_planning_count:
        recommended_next_actions.append(
            "Keep proxy-evidence events out of the default planning set unless they are explicitly requested for exploratory research."
        )
    elif proxy_count:
        recommended_next_actions.append(
            "Proxy-evidence events are quarantined from default planning; upgrade them to stronger evidence before reintroducing them to default trigger selection."
        )
    if descriptive_non_planning_active:
        recommended_next_actions.append(
            "Keep context-only, research-only, and sequence-only events out of the default planning set to preserve promotion safety."
        )
    recommended_next_actions.append(
        "Keep the new deep-analysis suite in artifact regeneration and contract verification so drift is caught automatically."
    )

    overall_passed = all(task["status"] == "passed" for task in tasks[:4]) and bool(
        regime_report.get("is_valid", False)
    )
    tasks.append(
        {
            "id": "10_synthesis",
            "title": "Synthesize event findings",
            "status": _status(passed=overall_passed),
            "summary": {
                "overall_status": "passed" if overall_passed else "attention",
                "critical_issue_count": int(
                    sum(
                        1
                        for task in tasks
                        if task["id"] != "10_synthesis" and task["status"] != "passed"
                    )
                ),
                "residual_priority_count": len(residual_priorities),
            },
            "details": {
                "proxy_evidence_event_count": proxy_count,
                "proxy_evidence_planning_event_count": proxy_planning_count,
                "proxy_evidence_events": proxy_evidence_event_ids,
                "proxy_evidence_planning_events": proxy_planning_event_ids,
                "descriptive_non_planning_event_count": len(descriptive_non_planning_active),
                "descriptive_non_planning_events": descriptive_non_planning_active,
                "residual_priorities": residual_priorities,
                "recommended_next_actions": recommended_next_actions,
            },
            "verification_commands": [
                "python -m project.scripts.build_event_deep_analysis_suite --check",
                "bash project/scripts/regenerate_artifacts.sh",
            ],
        }
    )
    return tasks


def build_report() -> dict[str, Any]:
    tasks = _build_tasks()
    overall_status = (
        "passed" if all(task["status"] == "passed" for task in tasks[:-1]) else "attention"
    )
    return {
        "summary": {
            "task_count": len(tasks),
            "passed_task_count": int(sum(1 for task in tasks if task["status"] == "passed")),
            "attention_task_count": int(sum(1 for task in tasks if task["status"] != "passed")),
            "overall_status": overall_status,
        },
        "tasks": tasks,
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# Event Deep Analysis Suite",
        "",
        f"- Overall status: `{summary['overall_status']}`",
        f"- Task count: `{summary['task_count']}`",
        f"- Passed tasks: `{summary['passed_task_count']}`",
        f"- Tasks needing attention: `{summary['attention_task_count']}`",
        "",
    ]
    for index, task in enumerate(report["tasks"], start=1):
        lines.extend(
            [
                f"## {index:02d}. {task['title']}",
                "",
                f"- Task id: `{task['id']}`",
                f"- Status: `{task['status']}`",
                "- Summary:",
            ]
        )
        for key, value in task["summary"].items():
            lines.append(f"  - `{key}`: `{value}`")
        lines.extend(["", "- Details:"])
        for key, value in task["details"].items():
            lines.append(f"  - `{key}`: `{value}`")
        lines.extend(["", "- Verification commands:"])
        for command in task["verification_commands"]:
            lines.append(f"  - `{command}`")
        lines.append("")
    return "\n".join(lines)


def build_outputs(base_dir: str = "docs/generated") -> dict[Path, str]:
    out_dir = Path(base_dir)
    report = build_report()
    return {
        out_dir / "event_deep_analysis_suite.json": json.dumps(report, indent=2, sort_keys=True)
        + "\n",
        out_dir / "event_deep_analysis_suite.md": render_markdown(report),
    }


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate the 10-pass deep-analysis artifact suite for the event system."
    )
    parser.add_argument("--base-dir", default="docs/generated")
    parser.add_argument("--check", action="store_true", help="Fail if outputs drift from disk.")
    args = parser.parse_args(argv)

    outputs = build_outputs(args.base_dir)
    drift: list[str] = []
    for path, content in outputs.items():
        if args.check:
            current = path.read_text(encoding="utf-8") if path.exists() else None
            if current != content:
                drift.append(str(path))
            continue
        _write(path, content)

    if drift:
        for path in drift:
            print(f"event deep analysis artifact drift: {path}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
