from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterable, Mapping

from project.live.contracts.trade_intent import TradeIntent
from project.live.scoring import DecisionScore

if TYPE_CHECKING:
    from project.live.retriever import ThesisMatch


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


def normalize_live_event_detector_config(config: Mapping[str, Any] | None) -> Dict[str, Any]:
    payload = dict(config or {})
    adapter = str(
        payload.get("adapter")
        or payload.get("mode")
        or payload.get("detector_adapter")
        or "governed_runtime_core"
    ).strip().lower()
    legacy_enabled = bool(
        payload.get("legacy_heuristic_enabled", False)
        or payload.get("legacy_mode", False)
        or payload.get("use_legacy_heuristic", False)
    )
    if adapter in {"governed", "runtime_core", "runtime-core", "governed_runtime_core"}:
        payload["adapter"] = "governed_runtime_core"
        payload["legacy_heuristic_enabled"] = False
        return payload
    if adapter in {"heuristic", "legacy", "legacy_heuristic", "legacy-heuristic"}:
        if not legacy_enabled:
            raise ValueError(
                "strategy_runtime.event_detector heuristic mode requires "
                "legacy_heuristic_enabled=true"
            )
        payload["adapter"] = "heuristic"
        payload["legacy_heuristic_enabled"] = True
        return payload
    raise ValueError(f"unsupported live event detector adapter '{adapter}'")


def build_live_decision_trace(
    *,
    context: Any,
    ranked_matches: Iterable["ThesisMatch"],
    trade_intent: TradeIntent,
    top_score: DecisionScore | None,
) -> Dict[str, Any]:
    matches = list(ranked_matches)
    blocked: list[Dict[str, Any]] = []
    for match in matches:
        if match.eligibility_passed:
            continue
        blocked.append(
            {
                "thesis_id": str(match.thesis.thesis_id),
                "reasons": list(match.reasons_against),
            }
        )
    return {
        "detected_event": {
            "event_id": str(context.primary_event_id or context.event_family),
            "event_family": str(context.event_family),
            "canonical_regime": str(context.canonical_regime),
            "event_side": str(context.event_side),
            "confidence": float(context.event_confidence)
            if context.event_confidence is not None
            else None,
            "severity": float(context.event_severity)
            if context.event_severity is not None
            else None,
            "data_quality_flag": str(context.data_quality_flag),
            "threshold_version": str(context.threshold_version),
        },
        "matched_thesis_ids": [
            str(match.thesis.thesis_id) for match in matches if match.eligibility_passed
        ],
        "blocked_theses": blocked,
        "trade_intent": {
            "action": str(trade_intent.action),
            "thesis_id": str(trade_intent.thesis_id),
            "side": str(trade_intent.side),
            "confidence_band": str(trade_intent.confidence_band),
            "size_fraction": float(trade_intent.size_fraction),
            "support_score": float(trade_intent.support_score),
            "contradiction_penalty": float(trade_intent.contradiction_penalty),
        },
        "score_components": (
            {
                "total_score": float(top_score.total_score),
                "setup_match_score": float(top_score.setup_match_score),
                "execution_quality_score": float(top_score.execution_quality_score),
                "thesis_strength_score": float(top_score.thesis_strength_score),
                "regime_alignment_score": float(top_score.regime_alignment_score),
                "contradiction_penalty": float(top_score.contradiction_penalty),
                "probability_positive_post_cost": float(
                    top_score.probability_positive_post_cost
                ),
                "expected_net_pnl_bps": float(top_score.expected_net_pnl_bps),
                "expected_downside_bps": float(top_score.expected_downside_bps),
                "fill_probability": float(top_score.fill_probability),
                "regime_reliability": float(top_score.regime_reliability),
                "utility_score": float(top_score.utility_score),
            }
            if top_score is not None
            else None
        ),
    }


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
    probability = float(score.probability_positive_post_cost)
    expected_net_pnl = float(score.expected_net_pnl_bps)
    utility = float(score.utility_score)
    if score.contradiction_penalty >= ladder.max_contradiction_penalty:
        action = "reject"
        confidence_band = "none"
        size_fraction = 0.0
    elif utility <= 0.0 or expected_net_pnl <= 0.0:
        action = "reject"
        confidence_band = "none"
        size_fraction = 0.0
    elif probability >= 0.65 and expected_net_pnl >= 8.0 and utility >= 5.0:
        action = "trade_normal"
        confidence_band = "high"
        size_fraction = max(0.25, min(1.0, utility / max(1.0, score.expected_downside_bps)))
    elif probability >= 0.55 and expected_net_pnl >= 3.0:
        action = "trade_small"
        confidence_band = "medium"
        size_fraction = max(0.10, min(0.50, utility / max(1.0, score.expected_downside_bps)))
    elif probability >= 0.45 and expected_net_pnl > 0.0:
        action = "probe"
        confidence_band = "medium"
        size_fraction = max(0.05, min(0.20, utility / max(1.0, score.expected_downside_bps)))
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
        probability_positive_post_cost=score.probability_positive_post_cost,
        expected_net_edge_bps=score.expected_net_pnl_bps / max(score.fill_probability, 1e-9),
        expected_downside_bps=score.expected_downside_bps,
        expected_net_pnl_bps=score.expected_net_pnl_bps,
        fill_probability=score.fill_probability,
        edge_confidence=score.regime_reliability,
        utility_score=score.utility_score,
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
            "probability_positive_post_cost": score.probability_positive_post_cost,
            "expected_net_pnl_bps": score.expected_net_pnl_bps,
            "expected_downside_bps": score.expected_downside_bps,
            "fill_probability": score.fill_probability,
            "regime_reliability": score.regime_reliability,
            "utility_score": score.utility_score,
        },
    )
