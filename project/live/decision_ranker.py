from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from project.live.contradiction_model import assess_contradictions
from project.live.regime_reliability import evaluate_regime_reliability
from project.live.trade_valuator import estimate_fill_probability

if TYPE_CHECKING:
    from project.live.edge_model import EdgeModel


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


@dataclass(frozen=True)
class RankedDecision:
    match: Any
    probability_positive_post_cost: float
    expected_net_pnl_bps: float
    expected_downside_bps: float
    fill_probability: float
    regime_reliability: float
    contradiction_penalty_bps: float
    utility_score: float
    reasons: tuple[str, ...] = ()
    edge_model_used: bool = False
    edge_model_confidence: float = 0.0


def rank_match_by_expected_value(
    *,
    match: Any,
    context: Any,
    edge_model: EdgeModel | None = None,
) -> RankedDecision:
    thesis = match.thesis
    evidence = thesis.evidence
    live_features = getattr(context, "live_features", {}) or {}
    gross = _finite(evidence.estimate_bps, 0.0)
    net = _finite(
        evidence.net_expectancy_bps,
        gross - _finite(live_features.get("expected_cost_bps"), 0.0),
    )
    stop_value = (getattr(thesis, "expected_response", {}) or {}).get("stop_value")
    downside = abs(_finite(stop_value, 0.0) * 10_000.0)
    if downside <= 0.0:
        downside = max(5.0, gross)

    # Optionally enrich with learned edge model predictions when confidence is sufficient
    edge_model_used = False
    edge_model_confidence = 0.0
    if edge_model is not None:
        model_features = {
            "estimate_bps": gross,
            "net_expectancy_bps": net,
            "q_value": _finite(getattr(evidence, "q_value", None), 1.0),
            "stability_score": _finite(getattr(evidence, "stability_score", None)),
            "cost_survival_ratio": _finite(getattr(evidence, "cost_survival_ratio", None), 1.0),
            "sample_size": _finite(getattr(evidence, "sample_size", None)),
            "spread_bps": _finite(live_features.get("spread_bps"), 5.0),
            "depth_usd": _finite(live_features.get("top_of_book_depth_usd")),
            "fill_probability": _finite(live_features.get("fill_probability"), 0.85),
            "contradiction_penalty": _finite(
                getattr(match, "contradiction_penalty", None)
            ),
            "support_score": _finite(getattr(match, "support_score", None)),
            # Fallback values passed through to fallback path
            "downside_bps_static": downside,
        }
        prediction = edge_model.predict(model_features)
        if prediction.used_model and prediction.model_confidence >= 0.5:
            net = prediction.predicted_net_edge_bps
            downside = prediction.predicted_downside_bps
            edge_model_used = True
            edge_model_confidence = prediction.model_confidence
    fill_probability = estimate_fill_probability(market_state=live_features)
    regime = evaluate_regime_reliability(thesis=thesis, context=context)
    contradiction = assess_contradictions(match=match, context=context)
    probability = max(
        0.01,
        min(
            0.99,
            0.50
            + (net / max(1.0, downside * 2.0))
            + (0.20 * regime.reliability)
            - contradiction.probability_penalty,
        ),
    )
    expected_net_pnl = net * max(0.0, min(1.0, fill_probability)) * regime.reliability
    utility = expected_net_pnl - (downside * (1.0 - probability)) - contradiction.penalty_bps
    reasons = [regime.reason]
    reasons.extend(contradiction.reasons)
    if net <= 0.0:
        reasons.append("expected_net_edge_non_positive")
    return RankedDecision(
        match=match,
        probability_positive_post_cost=float(probability),
        expected_net_pnl_bps=float(expected_net_pnl),
        expected_downside_bps=float(downside),
        fill_probability=float(max(0.0, min(1.0, fill_probability))),
        regime_reliability=float(regime.reliability),
        contradiction_penalty_bps=float(contradiction.penalty_bps),
        utility_score=float(utility),
        reasons=tuple(reasons),
        edge_model_used=edge_model_used,
        edge_model_confidence=edge_model_confidence,
    )


def rank_decisions_by_expected_value(
    *,
    matches: Iterable[Any],
    context: Any,
    edge_model: EdgeModel | None = None,
) -> list[RankedDecision]:
    ranked = [
        rank_match_by_expected_value(match=match, context=context, edge_model=edge_model)
        for match in matches
        if bool(getattr(match, "eligibility_passed", False))
    ]
    ranked.sort(key=lambda item: item.utility_score, reverse=True)
    return ranked
