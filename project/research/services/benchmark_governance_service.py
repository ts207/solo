from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_acceptance_thresholds(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        return payload.get("thresholds", {}) if isinstance(payload, dict) else {}
    except Exception:
        return {}


def certify_benchmark_review(
    *,
    current_review: Dict[str, Any],
    prior_review: Optional[Dict[str, Any] | List[Dict[str, Any]]] = None,
    acceptance_thresholds: Optional[Dict[str, Any]] = None,
    execution_manifest: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Certify a new benchmark review against absolute thresholds and one or more prior baselines.
    """
    issues = []
    deltas = []
    historical_drift = []

    current_slices = {
        s["benchmark_id"]: s for s in current_review.get("slices", []) if s.get("benchmark_id")
    }

    priors: List[Dict[str, Any]] = []
    if isinstance(prior_review, list):
        priors = prior_review
    elif isinstance(prior_review, dict):
        priors = [prior_review]

    latest_prior = priors[0] if priors else {}
    latest_prior_slices = {
        s["benchmark_id"]: s for s in latest_prior.get("slices", []) if s.get("benchmark_id")
    }

    status_order = [
        "coverage_limited",
        "foundation_only",
        "informative",
        "quality_boundary",
        "authoritative",
    ]
    foundation_order = ["blocked", "warn", "ready"]

    # 0. Execution Certification
    manifest = execution_manifest if isinstance(execution_manifest, dict) else {}
    manifest_failures = int(manifest.get("failures", 0) or 0)
    manifest_results = manifest.get("results", [])
    if manifest_failures > 0:
        failed_runs: List[str] = []
        if isinstance(manifest_results, list):
            for row in manifest_results:
                if not isinstance(row, dict):
                    continue
                if str(row.get("status", "")).strip() != "failed":
                    continue
                run_id = str(row.get("run_id", "")).strip()
                if run_id:
                    failed_runs.append(run_id)
        issues.append(
            {
                "benchmark_id": "__matrix__",
                "severity": "fail",
                "type": "execution_failures",
                "message": (
                    f"Benchmark matrix recorded {manifest_failures} failed run(s): "
                    f"{', '.join(failed_runs) if failed_runs else 'unknown runs'}"
                ),
                "failed_run_ids": failed_runs,
            }
        )

    # 1. Absolute Certification
    for bid, s in current_slices.items():
        status = s.get("benchmark_status", "unknown")
        readiness = s.get("live_foundation_readiness", "unknown")
        hard_rows = s.get("hard_evaluated_rows", 0)

        thr = acceptance_thresholds.get(bid, {}) if acceptance_thresholds else {}

        # Status floor
        req_status = thr.get("required_status")
        if req_status and req_status in status_order:
            curr_rank = status_order.index(status) if status in status_order else -1
            req_rank = status_order.index(req_status)
            if curr_rank < req_rank:
                issues.append(
                    {
                        "benchmark_id": bid,
                        "severity": "fail",
                        "type": "low_status",
                        "message": f"Slice {bid} status '{status}' < required '{req_status}'",
                    }
                )

        # Foundation floor
        req_found = thr.get("required_foundation")
        if req_found and req_found in foundation_order:
            curr_f_rank = foundation_order.index(readiness) if readiness in foundation_order else -1
            req_f_rank = foundation_order.index(req_found)
            if curr_f_rank < req_f_rank:
                issues.append(
                    {
                        "benchmark_id": bid,
                        "severity": "fail",
                        "type": "low_foundation",
                        "message": f"Slice {bid} foundation '{readiness}' < required '{req_found}'",
                    }
                )

        # Row floor
        min_rows = thr.get("min_evaluated_rows")
        if min_rows is not None and hard_rows < min_rows:
            issues.append(
                {
                    "benchmark_id": bid,
                    "severity": "fail",
                    "type": "low_sample",
                    "message": f"Slice {bid} hard_evaluated_rows={hard_rows} < threshold={min_rows}",
                }
            )

    # 2. Baseline Delta Certification (against latest prior)
    if latest_prior_slices:
        for bid, curr in current_slices.items():
            if bid not in latest_prior_slices:
                deltas.append(
                    {
                        "benchmark_id": bid,
                        "type": "new_slice",
                        "message": f"New slice {bid} added since prior review.",
                    }
                )
                continue

            prev = latest_prior_slices[bid]

            # Status regression
            curr_rank = (
                status_order.index(curr.get("benchmark_status", "coverage_limited"))
                if curr.get("benchmark_status") in status_order
                else -1
            )
            prev_rank = (
                status_order.index(prev.get("benchmark_status", "coverage_limited"))
                if prev.get("benchmark_status") in status_order
                else -1
            )

            if curr_rank < prev_rank:
                issues.append(
                    {
                        "benchmark_id": bid,
                        "severity": "fail",
                        "type": "status_regression",
                        "message": f"Slice {bid} status regressed from {prev.get('benchmark_status')} to {curr.get('benchmark_status')}",
                    }
                )

            # Sample collapse
            prev_hard = prev.get("hard_evaluated_rows", 0)
            curr_hard = curr.get("hard_evaluated_rows", 0)
            if prev_hard > 0 and curr_hard < (prev_hard * 0.8):
                issues.append(
                    {
                        "benchmark_id": bid,
                        "severity": "warn",
                        "type": "sample_collapse",
                        "message": f"Slice {bid} hard_evaluated_rows dropped by >20% ({prev_hard} -> {curr_hard})",
                    }
                )

    # 3. Multi-Baseline Historical Drift
    if len(priors) > 1:
        for bid, curr in current_slices.items():
            counts = [curr.get("hard_evaluated_rows", 0)]
            for p in priors:
                pslices = {
                    s["benchmark_id"]: s for s in p.get("slices", []) if s.get("benchmark_id")
                }
                counts.append(pslices.get(bid, {}).get("hard_evaluated_rows", 0))

            historical_drift.append(
                {
                    "benchmark_id": bid,
                    "hard_row_counts": counts,  # [current, prior1, prior2, ...]
                    "mean": float(sum(counts) / len(counts)),
                    "std": float(pd.Series(counts).std()) if len(counts) > 1 else 0.0,
                }
            )

    passed = not any(i["severity"] == "fail" for i in issues)

    return {
        "schema_version": "benchmark_certification_v1",
        "matrix_id": current_review.get("matrix_id"),
        "passed": passed,
        "issue_count": len(issues),
        "issues": issues,
        "deltas": deltas,
        "historical_drift": historical_drift,
    }


def render_certification_report_markdown(cert: Dict[str, Any]) -> str:
    lines = [
        "# Benchmark Certification Report",
        "",
        f"- **Matrix ID**: `{cert.get('matrix_id', '')}`",
        f"- **Status**: `{'PASS' if cert.get('passed') else 'FAIL'}`",
        f"- **Issue Count**: `{cert.get('issue_count', 0)}`",
        "",
    ]

    if cert.get("issues"):
        lines.extend(["## Issues", ""])
        lines.append("| Benchmark ID | Severity | Type | Message |")
        lines.append("| --- | --- | --- | --- |")
        for i in cert["issues"]:
            lines.append(
                f"| `{i.get('benchmark_id')}` | `{i.get('severity')}` | `{i.get('type')}` | {i.get('message')} |"
            )
        lines.append("")

    if cert.get("historical_drift"):
        lines.extend(["## Historical Drift", ""])
        lines.append("| Benchmark ID | Counts (curr -> past) | Mean | Std |")
        lines.append("| --- | --- | --- | --- |")
        for d in cert["historical_drift"]:
            counts_str = " -> ".join(map(str, d.get("hard_row_counts", [])))
            lines.append(
                f"| `{d.get('benchmark_id')}` | {counts_str} | {d.get('mean'):.2f} | {d.get('std'):.2f} |"
            )
        lines.append("")

    if cert.get("deltas"):
        lines.extend(["## Deltas", ""])
        for d in cert["deltas"]:
            lines.append(f"- **{d.get('type')}**: {d.get('message')}")
        lines.append("")

    return "\n".join(lines)


def write_certification_report(*, out_dir: Path, cert: Dict[str, Any]) -> Dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "benchmark_certification.json"
    md_path = out_dir / "benchmark_certification.md"
    json_path.write_text(json.dumps(cert, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(render_certification_report_markdown(cert), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}


def get_benchmark_certification_for_family(
    *,
    family: str,
    certification_path: Optional[Path] = None,
    review_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Resolve the benchmark certification status for a specific family.
    """
    data_root = get_data_root()

    CERT_DEFAULT = data_root / "reports" / "benchmarks" / "latest" / "benchmark_certification.json"
    REVIEW_DEFAULT = data_root / "reports" / "benchmarks" / "latest" / "benchmark_review.json"

    cp = certification_path or CERT_DEFAULT
    rp = review_path or REVIEW_DEFAULT

    if not rp.exists():
        return {
            "passed": False,
            "status": "missing",
            "message": f"Benchmark review not found at {rp}",
        }

    review = _load_json(rp)
    cert = _load_json(cp) if cp.exists() else None

    target_slice = None
    for s in review.get("slices", []):
        if str(s.get("family", "")).upper() == family.upper():
            target_slice = s
            break

    if not target_slice:
        return {
            "passed": False,
            "status": "no_coverage",
            "message": f"No benchmark slice defined for family {family}",
        }

    bid = target_slice.get("benchmark_id")

    if cert and not cert.get("passed", False):
        slice_issues = [
            i
            for i in cert.get("issues", [])
            if i.get("benchmark_id") == bid and i.get("severity") == "fail"
        ]
        if slice_issues:
            return {
                "passed": False,
                "status": "failed",
                "message": f"Benchmark certification failed for {family} ({bid}): {slice_issues[0].get('message')}",
            }

    status = target_slice.get("benchmark_status")
    if status in {"foundation_only", "coverage_limited", "blocked", "empty"}:
        return {
            "passed": False,
            "status": "degraded",
            "message": f"Benchmark for {family} is degraded: status={status}",
        }

    return {
        "passed": True,
        "status": status,
        "message": f"Benchmark for {family} is healthy ({status})",
    }


def get_data_root() -> Path:
    from project.core.config import get_data_root as _get

    return _get()
