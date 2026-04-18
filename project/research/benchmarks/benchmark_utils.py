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
    """Evaluate benchmark results for the single canonical D path."""
    if thresholds is None:
        thresholds = load_thresholds()

    scorecard: Dict[str, Any] = {}

    for mode_id, result in mode_results.items():
        scorecard[mode_id] = {
            "is_canonical": mode_id == "D",
            "emergence": bool(result.get("emergence", False)),
            "candidate_count": int(result.get("candidate_count", 0) or 0),
            "promotion_density": result.get("top10", {}).get("promotion_density"),
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
    """Evaluate the canonical D path without alternate-mode comparisons."""
    d_result = mode_results.get("D", {})
    min_candidates = int(thresholds.get("min_final_candidates", 0) or 0)
    candidate_count = int(d_result.get("candidate_count", 0) or 0)
    if not d_result:
        status = "missing"
    elif candidate_count < min_candidates:
        status = "hold"
    else:
        status = "promote"
    return {"canonical_d": status}


def _suite_recommendation(components: Dict[str, Any], thresholds: Dict[str, Any]) -> str:
    status = components.get("canonical_d")
    if status == "promote":
        return "promote"
    if status == "hold":
        return "hold"
    return "inconclusive"


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
