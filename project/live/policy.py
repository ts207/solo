from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from project.live.contracts.trade_intent import TradeIntent
from project.live.scoring import DecisionScore


@dataclass(frozen=True)
class PolicyThresholds:
    watch_min: float = 0.20
    probe_min: float = 0.35
    small_min: float = 0.55
    normal_min: float = 0.75
    max_contradiction_penalty: float = 0.45


def thresholds_from_config(config: Dict[str, Any] | None) -> PolicyThresholds:
    payload = dict(config or {})
    return PolicyThresholds(
        watch_min=float(payload.get("watch_min", 0.20) or 0.20),
        probe_min=float(payload.get("probe_min", 0.35) or 0.35),
        small_min=float(payload.get("small_min", 0.55) or 0.55),
        normal_min=float(payload.get("normal_min", 0.75) or 0.75),
        max_contradiction_penalty=float(payload.get("max_contradiction_penalty", 0.45) or 0.45),
    )


def score_to_action(
    *,
    score: DecisionScore,
    symbol: str,
    side: str,
    thesis_id: str,
    invalidation: Dict[str, Any],
    thresholds: PolicyThresholds | None = None,
) -> TradeIntent:
    ladder = thresholds or PolicyThresholds()
    if score.contradiction_penalty >= ladder.max_contradiction_penalty:
        action = "reject"
        confidence_band = "none"
        size_fraction = 0.0
    elif score.total_score >= ladder.normal_min:
        action = "trade_normal"
        confidence_band = "high"
        size_fraction = 1.0
    elif score.total_score >= ladder.small_min:
        action = "trade_small"
        confidence_band = "medium"
        size_fraction = 0.50
    elif score.total_score >= ladder.probe_min:
        action = "probe"
        confidence_band = "medium"
        size_fraction = 0.20
    elif score.total_score >= ladder.watch_min:
        action = "watch"
        confidence_band = "low"
        size_fraction = 0.0
    else:
        action = "reject"
        confidence_band = "none"
        size_fraction = 0.0
    return TradeIntent(
        action=action,
        symbol=symbol,
        side=side if action != "reject" else "flat",
        thesis_id=thesis_id,
        support_score=score.total_score,
        contradiction_penalty=score.contradiction_penalty,
        confidence_band=confidence_band,
        size_fraction=size_fraction,
        invalidation=dict(invalidation or {}),
        reasons_for=list(score.reasons_for),
        reasons_against=list(score.reasons_against),
        metadata={
            "setup_match_score": score.setup_match_score,
            "execution_quality_score": score.execution_quality_score,
            "thesis_strength_score": score.thesis_strength_score,
            "regime_alignment_score": score.regime_alignment_score,
        },
    )
