from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List

from project.live.contracts.live_trade_context import LiveTradeContext
from project.live.retriever import ThesisMatch


@dataclass(frozen=True)
class DecisionScore:
    total_score: float
    setup_match_score: float
    execution_quality_score: float
    thesis_strength_score: float
    regime_alignment_score: float
    contradiction_penalty: float
    reasons_for: List[str]
    reasons_against: List[str]


def _finite_metric(value: object, default: float) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return float(default)
    if not math.isfinite(numeric):
        return float(default)
    return float(numeric)


def _execution_quality_score(context: LiveTradeContext) -> tuple[float, list[str], list[str]]:
    features = context.live_features or {}
    reasons_for: list[str] = []
    reasons_against: list[str] = []
    score = 0.0
    spread_bps = float(features.get("spread_bps", 999.0) or 999.0)
    depth_usd = float(features.get("depth_usd", 0.0) or 0.0)
    tob_coverage = float(features.get("tob_coverage", 0.0) or 0.0)
    if spread_bps <= 5.0:
        score += 0.10
        reasons_for.append("spread_ok")
    else:
        reasons_against.append("spread_wide")
    if depth_usd >= 25_000.0:
        score += 0.10
        reasons_for.append("depth_ok")
    else:
        reasons_against.append("depth_thin")
    if tob_coverage >= 0.80:
        score += 0.10
        reasons_for.append("tob_coverage_ok")
    else:
        reasons_against.append("tob_coverage_low")
    return score, reasons_for, reasons_against


def build_decision_score(match: ThesisMatch, context: LiveTradeContext) -> DecisionScore:
    setup_match_score = max(0.0, min(1.0, match.support_score))
    execution_quality_score, exec_for, exec_against = _execution_quality_score(context)

    evidence = match.thesis.evidence
    q_value = _finite_metric(evidence.q_value, 1.0)
    net_expectancy_bps = _finite_metric(evidence.net_expectancy_bps, 0.0)
    thesis_strength_score = 0.0
    reasons_for = list(match.reasons_for)
    reasons_against = list(match.reasons_against)
    if evidence.sample_size >= 100:
        thesis_strength_score += 0.15
        reasons_for.append("sample_size_100_plus")
    elif evidence.sample_size >= 30:
        thesis_strength_score += 0.10
        reasons_for.append("sample_size_30_plus")
    else:
        reasons_against.append("sample_size_low")
    if q_value <= 0.05:
        thesis_strength_score += 0.10
        reasons_for.append("q_value_ok")
    else:
        reasons_against.append("q_value_weak")
    if net_expectancy_bps > 0.0:
        thesis_strength_score += 0.10
        reasons_for.append("net_expectancy_positive")
    else:
        reasons_against.append("net_expectancy_non_positive")
    if any(item.startswith("episode_match:") for item in reasons_for):
        thesis_strength_score += 0.10
    if any(item.startswith("confirmation_match:") for item in reasons_for):
        thesis_strength_score += 0.05

    regime_alignment_score = 0.10 if any(item.startswith("regime_match:") for item in reasons_for) else 0.0
    contradiction_penalty = max(0.0, min(1.0, match.contradiction_penalty))
    if any(item.startswith("confirmation_missing:") for item in reasons_against):
        contradiction_penalty = min(1.0, contradiction_penalty + 0.05)

    reasons_for.extend(exec_for)
    reasons_against.extend(exec_against)

    # Hard gate: setup match is the triggering condition. Non-zero execution quality
    # and thesis strength must not compensate for a missing event match.
    # Without this floor, a thesis with zero setup match can reach 0.90 total score
    # (0 + 0.30 + 0.50 + 0.10) and cross the trade_normal threshold of 0.75.
    MIN_SETUP_MATCH = 0.20
    if setup_match_score < MIN_SETUP_MATCH:
        reasons_against.append("setup_match_below_floor")
        return DecisionScore(
            total_score=0.0,
            setup_match_score=setup_match_score,
            execution_quality_score=execution_quality_score,
            thesis_strength_score=thesis_strength_score,
            regime_alignment_score=regime_alignment_score,
            contradiction_penalty=contradiction_penalty,
            reasons_for=reasons_for,
            reasons_against=reasons_against,
        )

    total_score = (
        (0.45 * setup_match_score)
        + execution_quality_score
        + thesis_strength_score
        + regime_alignment_score
        - contradiction_penalty
    )
    return DecisionScore(
        total_score=max(0.0, min(1.0, total_score)),
        setup_match_score=setup_match_score,
        execution_quality_score=execution_quality_score,
        thesis_strength_score=thesis_strength_score,
        regime_alignment_score=regime_alignment_score,
        contradiction_penalty=contradiction_penalty,
        reasons_for=reasons_for,
        reasons_against=reasons_against,
    )
