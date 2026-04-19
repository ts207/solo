from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Iterable

from project.live.contradiction_model import assess_contradictions
from project.live.regime_reliability import evaluate_regime_reliability


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


def rank_match_by_expected_value(*, match: Any, context: Any) -> RankedDecision:
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
    spread = _finite(live_features.get("spread_bps"), 999.0)
    depth = _finite(
        live_features.get("top_of_book_depth_usd")
        or live_features.get("depth_usd")
        or live_features.get("liquidity_available"),
        0.0,
    )
    fill_probability = 0.75
    if spread <= 3.0:
        fill_probability += 0.10
    elif spread >= 10.0:
        fill_probability -= 0.20
    if depth >= 100_000.0:
        fill_probability += 0.10
    elif depth < 25_000.0:
        fill_probability -= 0.20
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
    )


def rank_decisions_by_expected_value(
    *,
    matches: Iterable[Any],
    context: Any,
) -> list[RankedDecision]:
    ranked = [
        rank_match_by_expected_value(match=match, context=context)
        for match in matches
        if bool(getattr(match, "eligibility_passed", False))
    ]
    ranked.sort(key=lambda item: item.utility_score, reverse=True)
    return ranked
