from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def build_promotion_readiness_report(
    *,
    benchmark_review: Dict[str, Any],
    benchmark_certification: Dict[str, Any],
    confirmatory_plan: Optional[Dict[str, Any]] = None,
    promotion_audit: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Combine multiple governance artifacts into one compact readiness report.
    """
    matrix_id = benchmark_review.get("matrix_id", "unknown")

    # 1. Per-family health status
    family_health = {}
    slices = benchmark_review.get("slices", [])
    cert_issues = benchmark_certification.get("issues", [])

    for s in slices:
        family = s.get("family", "UNKNOWN")
        bid = s.get("benchmark_id")
        status = s.get("benchmark_status")
        foundation = s.get("live_foundation_readiness")

        # Determine health
        issues = [
            i for i in cert_issues if i.get("benchmark_id") == bid and i.get("severity") == "fail"
        ]
        is_healthy = len(issues) == 0 and status not in {
            "blocked",
            "empty",
            "coverage_limited",
            "foundation_only",
        }

        family_health[family] = {
            "benchmark_id": bid,
            "status": status,
            "foundation": foundation,
            "healthy": is_healthy,
            "primary_issue": issues[0].get("message") if issues else None,
        }

    # 2. Confirmatory Readiness
    conf_readiness = "unknown"
    conf_message = "No confirmatory plan provided."
    if confirmatory_plan:
        conf_readiness = confirmatory_plan.get("readiness", "unknown")
        conf_message = f"Status: {conf_readiness}. Suggested month: {confirmatory_plan.get('suggested_forward_month')}"

    # 3. Promotion Blockers
    blockers = []
    if not benchmark_certification.get("passed", False):
        blockers.append("Benchmark certification FAILED.")

    if promotion_audit:
        # Check for benchmark-related rejections
        bench_rejects = [
            r for r in promotion_audit if "benchmark" in str(r.get("reject_reason", "")).lower()
        ]
        if bench_rejects:
            blockers.append(f"{len(bench_rejects)} candidates blocked by benchmark health.")

    # 4. Priorities
    # Slices that are failed or degraded should be rerun first
    rerun_priority = [
        health["benchmark_id"] for bid, health in family_health.items() if not health["healthy"]
    ]

    return {
        "schema_version": "promotion_readiness_v1",
        "matrix_id": matrix_id,
        "overall_passed": benchmark_certification.get("passed", False)
        and conf_readiness != "blocked",
        "family_health": family_health,
        "confirmatory": {"readiness": conf_readiness, "message": conf_message},
        "blockers": blockers,
        "rerun_priority": rerun_priority,
    }


def render_promotion_readiness_terminal(report: Dict[str, Any]) -> str:
    lines = []
    lines.append("=" * 80)
    lines.append(f"PROMOTION READINESS: {report.get('matrix_id')}")
    status_str = "READY" if report.get("overall_passed") else "BLOCKED"
    lines.append(f"OVERALL STATUS: {status_str}")
    lines.append("=" * 80)

    lines.append("\nFAMILY HEALTH:")
    header = f"{'Family':<25} | {'Healthy':<8} | {'Status':<15} | {'Issue'}"
    lines.append(header)
    lines.append("-" * 80)
    for fam, health in sorted(report.get("family_health", {}).items()):
        healthy_str = "YES" if health["healthy"] else "NO"
        issue_str = health["primary_issue"] or "None"
        lines.append(f"{fam:<25} | {healthy_str:<8} | {health['status']:<15} | {issue_str}")

    lines.append("\nCONFIRMATORY:")
    lines.append(f"  {report.get('confirmatory', {}).get('message')}")

    if report.get("blockers"):
        lines.append("\nPROMOTION BLOCKERS:")
        for b in report["blockers"]:
            lines.append(f"  - {b}")

    if report.get("rerun_priority"):
        lines.append("\nRERUN PRIORITY:")
        for p in report["rerun_priority"]:
            lines.append(f"  - {p}")

    lines.append("\n" + "=" * 80)
    return "\n".join(lines)


def render_promotion_readiness_markdown(report: Dict[str, Any]) -> str:
    lines = [
        "# Promotion Readiness Report",
        "",
        f"- **Matrix ID**: `{report.get('matrix_id')}`",
        f"- **Overall Status**: `{'READY' if report.get('overall_passed') else 'BLOCKED'}`",
        "",
        "## Family Health",
        "",
        "| Family | Healthy | Status | Issue |",
        "| --- | --- | --- | --- |",
    ]
    for fam, health in sorted(report.get("family_health", {}).items()):
        healthy_str = "✅" if health["healthy"] else "❌"
        issue_str = health["primary_issue"] or "None"
        lines.append(f"| {fam} | {healthy_str} | `{health['status']}` | {issue_str} |")

    lines.extend(
        ["", "## Confirmatory", "", f"{report.get('confirmatory', {}).get('message')}", ""]
    )

    if report.get("blockers"):
        lines.extend(["## Promotion Blockers", ""])
        for b in report["blockers"]:
            lines.append(f"- {b}")
        lines.append("")

    if report.get("rerun_priority"):
        lines.extend(["## Rerun Priority", ""])
        for p in report["rerun_priority"]:
            lines.append(f"- {p}")
        lines.append("")

    return "\n".join(lines)


def write_promotion_readiness_report(*, out_dir: Path, report: Dict[str, Any]) -> Dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "promotion_readiness.json"
    md_path = out_dir / "promotion_readiness.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(render_promotion_readiness_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
