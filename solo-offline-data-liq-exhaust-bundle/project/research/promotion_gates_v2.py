from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PromotionDecision:
    status: str
    passed: bool
    reasons: list[str]
    metrics: dict[str, Any]


def evaluate_family_promotion(
    analyzer_results: dict[str, Any],
    *,
    min_events: int = 20,
    max_cluster_rate: float = 0.35,
    min_best_net_bps: float = 0.0,
    min_sign_consistency: float = 0.45,
    max_asset_dispersion: float = 15.0,
) -> PromotionDecision:
    integrity = (
        getattr(analyzer_results.get("integrity"), "summary", {}) if analyzer_results else {}
    )
    edge = getattr(analyzer_results.get("edge"), "summary", {}) if analyzer_results else {}
    stability = (
        getattr(analyzer_results.get("stability"), "summary", {}) if analyzer_results else {}
    )

    metrics = {
        "n_events": int(integrity.get("n_events", 0) or 0),
        "cluster_rate": float(integrity.get("cluster_rate", 0.0) or 0.0),
        "pit_ok": bool(integrity.get("pit_ok", True)),
        "best_net_mean_bps": float(edge.get("best_net_mean_bps", 0.0) or 0.0),
        "sign_consistency": float(stability.get("sign_consistency", 0.0) or 0.0),
        "asset_mean_dispersion": float(stability.get("asset_mean_dispersion", 0.0) or 0.0),
    }
    reasons: list[str] = []
    if metrics["n_events"] < min_events:
        reasons.append(f"insufficient events: {metrics['n_events']} < {min_events}")
    if metrics["cluster_rate"] > max_cluster_rate:
        reasons.append(
            f"cluster rate too high: {metrics['cluster_rate']:.3f} > {max_cluster_rate:.3f}"
        )
    if not metrics["pit_ok"]:
        reasons.append("pit validation failed")
    if metrics["best_net_mean_bps"] < min_best_net_bps:
        reasons.append(
            f"edge too weak: {metrics['best_net_mean_bps']:.3f} < {min_best_net_bps:.3f}"
        )
    if metrics["sign_consistency"] < min_sign_consistency:
        reasons.append(
            f"sign consistency too low: {metrics['sign_consistency']:.3f} < {min_sign_consistency:.3f}"
        )
    if metrics["asset_mean_dispersion"] > max_asset_dispersion:
        reasons.append(
            f"asset dispersion too high: {metrics['asset_mean_dispersion']:.3f} > {max_asset_dispersion:.3f}"
        )
    passed = not reasons
    return PromotionDecision(
        status="approved" if passed else "prototype",
        passed=passed,
        reasons=reasons,
        metrics=metrics,
    )
