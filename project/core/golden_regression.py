from __future__ import annotations

from project.core.coercion import safe_float, safe_int, as_bool

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

from project.artifacts import (
    blueprint_summary_path,
    checklist_path,
    kpi_scorecard_path,
    load_json_dict,
    promotion_summary_path,
    release_signoff_path,
    run_manifest_path,
)


def collect_core_artifact_snapshot(*, data_root: Path, run_id: str) -> Dict[str, Any]:
    run_manifest = load_json_dict(run_manifest_path(run_id, data_root))
    promotion_summary = load_json_dict(promotion_summary_path(run_id, data_root))
    blueprint_summary = load_json_dict(blueprint_summary_path(run_id, data_root))
    checklist = load_json_dict(checklist_path(run_id, data_root))
    release_signoff = load_json_dict(release_signoff_path(run_id, data_root))
    kpi_scorecard = load_json_dict(kpi_scorecard_path(run_id, data_root))
    kpi_metrics = (
        kpi_scorecard.get("metrics", {})
        if isinstance(kpi_scorecard.get("metrics", {}), dict)
        else {}
    )

    tier_counts = promotion_summary.get("promotion_tier_counts", {})
    if not isinstance(tier_counts, dict):
        tier_counts = {}

    release_override = (
        release_signoff.get("override_audit", {})
        if isinstance(release_signoff.get("override_audit", {}), dict)
        else {}
    )

    snapshot = {
        "run_id": str(run_id),
        "run_manifest": {
            "status": str(run_manifest.get("status", "")),
            "run_mode": str(run_manifest.get("run_mode", run_manifest.get("mode", ""))),
            "objective_name": str(run_manifest.get("objective_name", "")),
            "retail_profile_name": str(run_manifest.get("retail_profile_name", "")),
            "objective_spec_hash": str(run_manifest.get("objective_spec_hash", "")),
            "retail_profile_spec_hash": str(run_manifest.get("retail_profile_spec_hash", "")),
        },
        "promotion_summary": {
            "candidates_promoted_final": safe_int(
                promotion_summary.get("candidates_promoted_final"), 0
            ),
            "rejected_total": safe_int(promotion_summary.get("rejected_total"), 0),
            "tier_counts": {
                "deployable": safe_int(tier_counts.get("deployable"), 0),
                "shadow": safe_int(tier_counts.get("shadow"), 0),
                "research": safe_int(tier_counts.get("research"), 0),
            },
        },
        "blueprint_summary": {
            "blueprint_count": safe_int(blueprint_summary.get("blueprint_count"), 0),
            "fallback_event_count": safe_int(blueprint_summary.get("fallback_event_count"), 0),
            "candidates_compiled": safe_int(blueprint_summary.get("candidates_compiled"), 0),
        },
        "checklist": {
            "decision": str(checklist.get("decision", "")),
        },
        "release_signoff": {
            "decision": str(release_signoff.get("decision", "")),
            "override_count": safe_int(release_override.get("non_production_override_count"), 0),
        },
        "kpi_scorecard": {
            "net_expectancy_bps": safe_float(
                (kpi_metrics.get("net_expectancy_bps", {}) or {}).get("value")
                if isinstance(kpi_metrics.get("net_expectancy_bps", {}), dict)
                else None
            ),
            "oos_sign_consistency": safe_float(
                (kpi_metrics.get("oos_sign_consistency", {}) or {}).get("value")
                if isinstance(kpi_metrics.get("oos_sign_consistency", {}), dict)
                else None
            ),
            "max_drawdown_pct": safe_float(
                (kpi_metrics.get("max_drawdown_pct", {}) or {}).get("value")
                if isinstance(kpi_metrics.get("max_drawdown_pct", {}), dict)
                else None
            ),
            "trade_count": safe_float(
                (kpi_metrics.get("trade_count", {}) or {}).get("value")
                if isinstance(kpi_metrics.get("trade_count", {}), dict)
                else None
            ),
            "turnover_proxy_mean": safe_float(
                (kpi_metrics.get("turnover_proxy_mean", {}) or {}).get("value")
                if isinstance(kpi_metrics.get("turnover_proxy_mean", {}), dict)
                else None
            ),
        },
    }
    return snapshot


@dataclass(frozen=True)
class GoldenToleranceConfig:
    default_numeric_abs_tolerance: float
    per_metric_abs_tolerance: Dict[str, float]

    def tolerance_for(self, key: str) -> float:
        if key in self.per_metric_abs_tolerance:
            return float(self.per_metric_abs_tolerance[key])
        return float(self.default_numeric_abs_tolerance)


def load_tolerance_config(path: Path) -> GoldenToleranceConfig:
    if not path.exists():
        return GoldenToleranceConfig(
            default_numeric_abs_tolerance=0.0,
            per_metric_abs_tolerance={},
        )
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return GoldenToleranceConfig(
            default_numeric_abs_tolerance=0.0,
            per_metric_abs_tolerance={},
        )
    defaults = payload.get("defaults", {})
    if not isinstance(defaults, dict):
        defaults = {}
    default_tol = safe_float(defaults.get("numeric_abs_tolerance"))
    if default_tol is None:
        default_tol = 0.0
    per_metric = payload.get("metric_tolerances", {})
    out: Dict[str, float] = {}
    if isinstance(per_metric, dict):
        for key, value in per_metric.items():
            key_text = str(key).strip()
            tol = safe_float(value)
            if key_text and tol is not None:
                out[key_text] = float(max(tol, 0.0))
    return GoldenToleranceConfig(
        default_numeric_abs_tolerance=float(max(default_tol, 0.0)),
        per_metric_abs_tolerance=out,
    )


def _flatten_snapshot(payload: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in payload.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict):
            out.update(_flatten_snapshot(value, prefix=path))
        else:
            out[path] = value
    return out


def compare_golden_snapshots(
    *,
    baseline: Dict[str, Any],
    candidate: Dict[str, Any],
    tolerance: GoldenToleranceConfig,
) -> Dict[str, Any]:
    baseline_flat = _flatten_snapshot(baseline)
    candidate_flat = _flatten_snapshot(candidate)

    diffs: List[Dict[str, Any]] = []
    checked = 0

    for key, baseline_value in sorted(baseline_flat.items()):
        checked += 1
        if key not in candidate_flat:
            diffs.append(
                {
                    "metric": key,
                    "reason": "missing_in_candidate",
                    "baseline": baseline_value,
                    "candidate": None,
                }
            )
            continue

        candidate_value = candidate_flat[key]
        if isinstance(baseline_value, bool) or isinstance(candidate_value, bool):
            if bool(baseline_value) != bool(candidate_value):
                diffs.append(
                    {
                        "metric": key,
                        "reason": "bool_mismatch",
                        "baseline": bool(baseline_value),
                        "candidate": bool(candidate_value),
                    }
                )
            continue

        base_num = safe_float(baseline_value)
        cand_num = safe_float(candidate_value)
        if base_num is not None and cand_num is not None:
            tol = float(tolerance.tolerance_for(key))
            delta = abs(base_num - cand_num)
            if delta > tol:
                diffs.append(
                    {
                        "metric": key,
                        "reason": "numeric_delta_exceeds_tolerance",
                        "baseline": base_num,
                        "candidate": cand_num,
                        "delta": delta,
                        "tolerance": tol,
                    }
                )
            continue

        if baseline_value != candidate_value:
            diffs.append(
                {
                    "metric": key,
                    "reason": "value_mismatch",
                    "baseline": baseline_value,
                    "candidate": candidate_value,
                }
            )

    return {
        "passed": len(diffs) == 0,
        "checked_metric_count": int(checked),
        "diff_count": int(len(diffs)),
        "diffs": diffs,
    }
