from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from project.core.config import get_data_root
from project import PROJECT_ROOT


THRESHOLDS_PATH = PROJECT_ROOT / "configs" / "benchmarks" / "discovery" / "thresholds_v1.yaml"


def load_thresholds(path: Optional[Path] = None) -> Dict[str, Any]:
    p = path or THRESHOLDS_PATH
    if not p.exists():
        return {}
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
        return data.get("thresholds", {}) if isinstance(data, dict) else {}
    except Exception:
        return {}


def evaluate_thresholds(
    *,
    mode_results: Dict[str, Dict[str, Any]],
    thresholds: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Evaluate a suite of mode results against acceptance thresholds.

    Returns a scorecard with per-component deltas vs baseline (mode A)
    and a suite-level recommendation.
    """
    if thresholds is None:
        thresholds = load_thresholds()

    baseline = mode_results.get("A", {})
    scorecard: Dict[str, Any] = {}

    for mode_id, result in mode_results.items():
        if mode_id == "A":
            scorecard[mode_id] = {"is_baseline": True}
            continue

        top10 = result.get("top10", {})
        baseline_top10 = baseline.get("top10", {})

        delta_quality = _safe_delta(
            top10.get("promotion_density"),
            baseline_top10.get("promotion_density"),
        )
        delta_integrity = _safe_delta(
            top10.get("placebo_fail_rate"),
            baseline_top10.get("placebo_fail_rate"),
            invert=True,
        )
        delta_diversity = _safe_delta(
            top10.get("rank_diversity_score"),
            baseline_top10.get("rank_diversity_score"),
        )
        delta_runtime = _safe_delta(
            top10.get("median_after_cost_expectancy_bps"),
            baseline_top10.get("median_after_cost_expectancy_bps"),
        )
        delta_efficiency = _safe_delta(
            top10.get("median_cost_survival_ratio"),
            baseline_top10.get("median_cost_survival_ratio"),
        )
        delta_discovery_quality = _safe_delta(
            result.get("median_discovery_quality_score"),
            baseline.get("median_discovery_quality_score"),
        )
        delta_falsification = _safe_delta(
            result.get("median_falsification_component"),
            baseline.get("median_falsification_component"),
            invert=True,
        )

        emergence_delta = None
        baseline_emergence = baseline.get("emergence", False)
        current_emergence = result.get("emergence", False)
        if baseline_emergence and current_emergence:
            emergence_delta = 0.0
        elif not baseline_emergence and current_emergence:
            emergence_delta = 1.0
        elif baseline_emergence and not current_emergence:
            emergence_delta = -1.0
        else:
            emergence_delta = 0.0

        scorecard[mode_id] = {
            "delta_quality_vs_A": delta_quality,
            "delta_integrity_vs_A": delta_integrity,
            "delta_diversity_vs_A": delta_diversity,
            "delta_runtime_compat_vs_A": delta_runtime,
            "delta_efficiency_vs_A": delta_efficiency,
            "delta_discovery_quality_vs_A": delta_discovery_quality,
            "delta_falsification_vs_A": delta_falsification,
            "emergence_delta_vs_A": emergence_delta,
        }

    components = _evaluate_components(mode_results, thresholds)
    recommendation = _suite_recommendation(components, thresholds)

    return {
        "scorecard": scorecard,
        "components": components,
        "recommendation": recommendation,
    }


def _safe_delta(curr: Optional[float], baseline: Optional[float], invert: bool = False) -> Optional[float]:
    if curr is None or baseline is None or baseline == 0:
        return None
    val = (curr - baseline) / abs(baseline) if baseline != 0 else None
    if val is not None and invert:
        val = -val
    return round(val, 4) if val is not None else None


def _evaluate_components(mode_results: Dict[str, Dict[str, Any]], thresholds: Dict[str, Any]) -> Dict[str, Any]:
    """Evaluate individual components (scoring, folds, hierarchical, ledger, diversification)."""
    components: Dict[str, Any] = {}

    components["scoring"] = _component_recommendation(
        compare=["A", "B"],
        mode_results=mode_results,
        thresholds=thresholds,
        metric="promotion_density",
    )

    components["folds"] = _component_recommendation(
        compare=["B", "C"],
        mode_results=mode_results,
        thresholds=thresholds,
        metric="placebo_fail_rate",
        invert=True,
    )

    components["hierarchical"] = _component_recommendation(
        compare=["C", "D"],
        mode_results=mode_results,
        thresholds=thresholds,
        metric="rank_diversity_score",
    )

    components["ledger"] = _component_recommendation(
        compare=["D", "E"],
        mode_results=mode_results,
        thresholds=thresholds,
        metric="promotion_density",
    )

    components["diversification"] = _component_recommendation(
        compare=["E", "F"],
        mode_results=mode_results,
        thresholds=thresholds,
        metric="rank_diversity_score",
    )

    return components


_COMPONENT_METRIC_MAP = {
    "scoring": "promotion_density",
    "folds": "placebo_fail_rate",
    "hierarchical": "rank_diversity_score",
    "ledger": "promotion_density",
    "diversification": "rank_diversity_score",
}

_COMPONENT_INVERT_MAP = {
    "folds": True,
}


def _component_recommendation(
    *,
    compare: List[str],
    mode_results: Dict[str, Dict[str, Any]],
    thresholds: Dict[str, Any],
    metric: str,
    invert: bool = False,
) -> str:
    if len(compare) != 2:
        return "inconclusive"

    base_id, enhanced_id = compare
    base_result = mode_results.get(base_id, {})
    enhanced_result = mode_results.get(enhanced_id, {})

    base_val = base_result.get("top10", {}).get(metric)
    enhanced_val = enhanced_result.get("top10", {}).get(metric)

    base_emergence = base_result.get("emergence", False)
    enhanced_emergence = enhanced_result.get("emergence", False)

    component_name = None
    for name, pairs in [("scoring", ["A", "B"]), ("folds", ["B", "C"]),
                         ("hierarchical", ["C", "D"]), ("ledger", ["D", "E"]),
                         ("diversification", ["E", "F"])]:
        if compare == pairs:
            component_name = name
            break

    if component_name == "hierarchical":
        base_count = base_result.get("candidate_count", 0)
        enhanced_count = enhanced_result.get("candidate_count", 0)
        base_quality = base_result.get("median_discovery_quality_score")
        enhanced_quality = enhanced_result.get("median_discovery_quality_score")

        if base_count == 0 and enhanced_count > 0:
            if enhanced_quality is not None and enhanced_quality > 0:
                return "promote"
            elif enhanced_quality is not None and enhanced_quality >= -0.5:
                return "hold"
            else:
                return "inconclusive"
        elif base_count == 0 and enhanced_count == 0:
            return "inconclusive"
        elif enhanced_count > base_count and base_count > 0:
            if enhanced_quality is not None and base_quality is not None:
                if enhanced_quality > base_quality:
                    return "promote"
                elif enhanced_quality >= base_quality - 0.5:
                    return "hold"
            return "hold"

    if base_val is None or enhanced_val is None:
        if component_name != "hierarchical":
            return "inconclusive"

        if enhanced_emergence and not base_emergence:
            return "promote"
        return "inconclusive"

    improvement = enhanced_val - base_val
    if invert:
        improvement = -improvement

    comp_thresholds = thresholds.get(component_name, {}) if component_name else {}
    min_improvement = comp_thresholds.get("min_quality_improvement",
                          comp_thresholds.get("min_integrity_improvement",
                          comp_thresholds.get("min_diversity_improvement",
                          comp_thresholds.get("min_quality_non_regression", 0.05))))
    min_floor = comp_thresholds.get("min_promotion_density", 0.20)

    if improvement > min_improvement and enhanced_val > min_floor:
        return "promote"
    elif improvement > 0:
        return "hold"
    elif improvement < -0.10:
        return "reject"
    else:
        return "inconclusive"


def _suite_recommendation(components: Dict[str, Any], thresholds: Dict[str, Any]) -> str:
    recs = list(components.values())
    if not recs:
        return "inconclusive"

    promote_count = sum(1 for r in recs if r == "promote")
    reject_count = sum(1 for r in recs if r == "reject")
    inconclusive_count = sum(1 for r in recs if r == "inconclusive")

    if reject_count > promote_count:
        return "reject"
    elif promote_count >= 2 and reject_count == 0:
        return "promote"
    elif inconclusive_count == len(recs):
        return "inconclusive"
    else:
        return "hold"


def find_historical_reviews(matrix_id: str, history_limit: int = 5) -> List[Dict[str, Any]]:
    """Return the N latest review+certification bundles for a specific matrix_id."""
    root = get_data_root()
    search_paths = [
        root / "reports" / "benchmarks" / "history",
        root / "reports" / "perf_benchmarks" / "history",
    ]

    matches: List[Dict[str, Any]] = []
    for p in search_paths:
        if p.exists():
            for d in p.iterdir():
                if d.is_dir() and d.name.startswith(f"{matrix_id}_"):
                    review_file = d / "benchmark_review.json"
                    cert_file = d / "benchmark_certification.json"
                    if review_file.exists():
                        entry: Dict[str, Any] = {
                            "path": str(d),
                            "review": None,
                            "certification": None,
                        }
                        try:
                            entry["review"] = json.loads(review_file.read_text(encoding="utf-8"))
                        except Exception:
                            pass
                        if cert_file.exists():
                            try:
                                entry["certification"] = json.loads(
                                    cert_file.read_text(encoding="utf-8")
                                )
                            except Exception:
                                pass
                        matches.append({"_mtime": d.stat().st_mtime, **entry})

    matches.sort(key=lambda x: x["_mtime"], reverse=True)
    return [m for m in matches[:history_limit]]
