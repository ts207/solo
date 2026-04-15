#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import yaml

from project import PROJECT_ROOT
from project.contracts.artifacts import validate_artifact_registry_definitions
from project.contracts.stage_dag import validate_stage_registry_definitions
from project.contracts.system_map import validate_system_map_surfaces

REPO_ROOT = PROJECT_ROOT.parent
SPEC_ROOT = REPO_ROOT / "spec"
PROJECT_DIR = REPO_ROOT / "project"
GOVERNANCE_SCHEMA_VERSION = "pipeline_governance_audit_v1"
CATALOG_ONLY_FEATURE_FAMILIES = {
    "copula_mispricing",
    "derivatives",
    "evaluation",
    "execution",
    "market_quality",
    "momentum",
    "performance",
    "risk",
    "volatility",
}
FEATURE_CALLABLE_ALIASES = {
    "carry_state": {"calculate_funding_rate_bps"},
    "vol_regime": {"calculate_rv_percentile_24h"},
    "amihud": {"calculate_amihud_illiquidity"},
    "roll": {"calculate_roll", "calculate_roll_spread_bps"},
    "kyle": {"calculate_kyle_lambda"},
    "vpin": {"calculate_vpin_score"},
    "order_book": {"calculate_effective_spread_bps"},
    "correlation": set(),
    "microstructure": set(),
}


def get_yaml_specs(subdir: str) -> Dict[str, dict]:
    specs: Dict[str, dict] = {}
    spec_path = SPEC_ROOT / subdir
    if not spec_path.exists():
        return specs
    for yaml_file in sorted(spec_path.glob("*.yaml")):
        try:
            data = yaml.safe_load(yaml_file.read_text(encoding="utf-8"))
        except Exception as exc:
            specs[yaml_file.stem] = {"__load_error__": str(exc)}
            continue
        if data:
            specs[yaml_file.stem] = data
    return specs


def _issue(check_id: str, severity: str, message: str, path: str = "") -> Dict[str, str]:
    return {
        "check_id": check_id,
        "severity": severity,
        "message": message,
        "path": path,
    }


def audit_features() -> Dict[str, Any]:
    specs = get_yaml_specs("features")
    issues: List[Dict[str, str]] = []
    families = sorted(
        {
            str(spec.get("feature_family", "")).strip()
            for spec in specs.values()
            if isinstance(spec, dict)
        }
    )
    impl_dir = PROJECT_DIR / "features"
    missing_family_impls: set[str] = set()
    missing_function_checks: set[tuple[str, str]] = set()

    for name, spec in specs.items():
        if "__load_error__" in spec:
            issues.append(
                _issue(
                    "feature_spec_load",
                    "error",
                    f"Failed to load feature spec: {spec['__load_error__']}",
                    path=f"spec/features/{name}.yaml",
                )
            )
            continue
        family = str(spec.get("feature_family", "")).strip()
        if not family:
            issues.append(
                _issue(
                    "feature_family_missing",
                    "error",
                    "Feature spec missing feature_family",
                    path=f"spec/features/{name}.yaml",
                )
            )
            continue
        impl_file = impl_dir / f"{family}.py"
        if not impl_file.exists():
            if family in CATALOG_ONLY_FEATURE_FAMILIES:
                continue
            if family not in missing_family_impls:
                issues.append(
                    _issue(
                        "feature_impl_missing",
                        "error",
                        f"Missing implementation file for feature family '{family}'",
                        path=str(impl_file.relative_to(REPO_ROOT)),
                    )
                )
                missing_family_impls.add(family)
            continue
        content = impl_file.read_text(encoding="utf-8")
        check_key = (family, name)
        allowed_aliases = FEATURE_CALLABLE_ALIASES.get(name, set())
        if allowed_aliases == set():
            # Explicitly catalog-only feature entry within an implemented family.
            continue
        if (
            check_key not in missing_function_checks
            and f"def calculate_{name}" not in content
            and f"def {name}" not in content
            and not any(alias in content for alias in allowed_aliases)
        ):
            issues.append(
                _issue(
                    "feature_function_missing",
                    "error",
                    f"Missing callable for feature '{name}' in family '{family}'",
                    path=str(impl_file.relative_to(REPO_ROOT)),
                )
            )
            missing_function_checks.add(check_key)

    return {
        "name": "features",
        "spec_count": len(specs),
        "family_count": len([fam for fam in families if fam]),
        "issues": issues,
    }


def audit_events() -> Dict[str, Any]:
    specs = get_yaml_specs("events")
    issues: List[Dict[str, str]] = []
    required = {"event_type", "reports_dir", "events_file", "signal_column"}
    skipped_files = {
        "registry",
        "event_registry_unified",
        "compatibility",
        "precedence",
        "_defaults",
        "_families",
        "DESIGN",
    }

    for name, spec in specs.items():
        if "__load_error__" in spec:
            issues.append(
                _issue(
                    "event_spec_load",
                    "error",
                    f"Failed to load event spec: {spec['__load_error__']}",
                    path=f"spec/events/{name}.yaml",
                )
            )
            continue
        if spec.get("kind") == "canonical_event_registry" or name in skipped_files:
            continue
        active = bool(spec.get("active", True))
        status = str(spec.get("status", "")).strip().lower()
        required_fields = set(required)
        if not active or status == "planned":
            required_fields.discard("events_file")
        missing = sorted(required_fields - set(spec.keys()))
        if missing:
            issues.append(
                _issue(
                    "event_required_fields",
                    "error",
                    f"Event spec missing required fields: {', '.join(missing)}",
                    path=f"spec/events/{name}.yaml",
                )
            )

    return {
        "name": "events",
        "spec_count": len(specs),
        "issues": issues,
    }


def audit_contracts() -> Dict[str, Any]:
    stage_issues = [
        _issue("stage_registry", "error", issue, path="project/contracts/stage_dag.py")
        for issue in validate_stage_registry_definitions(PROJECT_ROOT)
    ]
    artifact_issues = [
        _issue("artifact_registry", "error", issue, path="project/contracts/artifacts.py")
        for issue in validate_artifact_registry_definitions()
    ]
    system_map_issues = [
        _issue("system_map_surface", "error", issue, path="project/contracts/system_map.py")
        for issue in validate_system_map_surfaces()
    ]
    return {
        "name": "contracts",
        "issues": stage_issues + artifact_issues + system_map_issues,
    }


def run_audit(repo_root: Path | None = None) -> Dict[str, Any]:
    del repo_root  # Reserved for future override support; audit currently uses repository globals.
    checks = [audit_features(), audit_events(), audit_contracts()]
    issues = [issue for check in checks for issue in check["issues"]]
    summary = {
        "schema_version": GOVERNANCE_SCHEMA_VERSION,
        "status": "failed" if issues else "passed",
        "check_count": len(checks),
        "issue_count": len(issues),
        "error_count": sum(1 for issue in issues if issue["severity"] == "error"),
        "warning_count": sum(1 for issue in issues if issue["severity"] == "warning"),
    }
    return {
        "summary": summary,
        "checks": checks,
        "issues": issues,
    }


def render_markdown(report: Dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# Pipeline Governance Audit",
        "",
        f"- Status: `{summary['status']}`",
        f"- Checks: `{summary['check_count']}`",
        f"- Issues: `{summary['issue_count']}`",
        "",
        "## Checks",
        "",
    ]
    for check in report["checks"]:
        lines.append(f"### {check['name']}")
        lines.append("")
        meta = [f"issues={len(check['issues'])}"]
        if "spec_count" in check:
            meta.append(f"spec_count={check['spec_count']}")
        if "family_count" in check:
            meta.append(f"family_count={check['family_count']}")
        lines.append(f"- Summary: {', '.join(meta)}")
        if check["issues"]:
            for issue in check["issues"]:
                path = f" [{issue['path']}]" if issue.get("path") else ""
                lines.append(f"- `{issue['check_id']}`: {issue['message']}{path}")
        else:
            lines.append("- No issues.")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def sync_schemas() -> Path:
    specs = get_yaml_specs("features")
    schema_path = PROJECT_DIR / "schemas" / "feature_catalog.json"
    catalog = {
        "version": "1.0.0",
        "features": {
            name: {"family": spec.get("feature_family"), "params": spec.get("params")}
            for name, spec in specs.items()
            if "__load_error__" not in spec
        },
    }
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    schema_path.write_text(json.dumps(catalog, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return schema_path


def _write_output(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pipeline Governance Tool")
    parser.add_argument("--audit", action="store_true", help="Run audit on specs and contracts")
    parser.add_argument("--sync", action="store_true", help="Sync schemas and registries")
    parser.add_argument("--json-out", default=None, help="Write machine-readable audit JSON")
    parser.add_argument("--md-out", default=None, help="Write human-readable audit Markdown")
    args = parser.parse_args(argv)

    rc = 0
    if args.audit:
        report = run_audit(REPO_ROOT)
        if args.json_out:
            _write_output(Path(args.json_out), json.dumps(report, indent=2, sort_keys=True) + "\n")
        if args.md_out:
            _write_output(Path(args.md_out), render_markdown(report))
        if not args.json_out and not args.md_out:
            print(render_markdown(report), end="")
        if report["summary"]["issue_count"] > 0:
            rc = 1

    if args.sync:
        schema_path = sync_schemas()
        print(f"UPDATED: {schema_path}")

    if not args.audit and not args.sync:
        parser.print_help()
        return 2
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
