from __future__ import annotations

from typing import Any, Dict, Mapping

from project.episodes import infer_live_episode_matches
from project.live.contracts.live_trade_context import LiveTradeContext
from project.live.event_detector import DetectedEvent


def _unique_tokens(*groups: object) -> list[str]:
    out: list[str] = []
    for group in groups:
        if group is None:
            continue
        if isinstance(group, (str, bytes)):
            items = [group]
        else:
            try:
                items = list(group)
            except TypeError:
                items = [group]
        for raw in items:
            token = str(raw or "").strip().upper()
            if token and token not in out:
                out.append(token)
    return out


def _canonical_regime_from_move(move_bps: float) -> str:
    if abs(move_bps) >= 80.0:
        return "VOLATILITY"
    if abs(move_bps) >= 35.0:
        return "TRANSITION"
    return "CALM"


def build_live_trade_context(
    *,
    timestamp: str,
    symbol: str,
    timeframe: str,
    detected_event: DetectedEvent,
    market_features: Mapping[str, Any],
    portfolio_state: Mapping[str, Any],
    execution_env: Mapping[str, Any],
    active_groups: set[str] | None = None,
    family_budgets: Dict[str, float] | None = None,
) -> LiveTradeContext:
    move_bps = float(detected_event.features.get("move_bps", 0.0) or 0.0)
    detected_regime = str(detected_event.canonical_regime or "").strip().upper()
    regime_snapshot: Dict[str, Any] = {
        "canonical_regime": detected_regime or _canonical_regime_from_move(move_bps),
        "move_bps": move_bps,
    }
    if "spread_bps" in market_features and float(market_features.get("spread_bps", 0.0) or 0.0) <= 5.0:
        regime_snapshot["microstructure_regime"] = "healthy"
    else:
        regime_snapshot["microstructure_regime"] = "degraded"

    primary_event_id = str(detected_event.event_id or detected_event.event_family).strip().upper()
    compat_event_family = str(detected_event.event_family or detected_event.event_id).strip().upper()

    active_event_ids = _unique_tokens(
        market_features.get("active_event_ids", []),
        [primary_event_id],
    )
    raw_active_families = list(market_features.get("active_event_families", []))
    if compat_event_family and compat_event_family != primary_event_id:
        raw_active_families.append(compat_event_family)
    active_event_families = _unique_tokens(raw_active_families)

    contradiction_event_ids = _unique_tokens(market_features.get("contradiction_event_ids", []))
    raw_contradiction_families = list(market_features.get("contradiction_event_families", []))
    contradiction_event_families = _unique_tokens(raw_contradiction_families)

    episode_matches = infer_live_episode_matches(
        active_event_ids,
        regime_snapshot=regime_snapshot,
        live_features=dict(market_features) | dict(detected_event.features),
    )
    provided_episode_ids = [str(item or "").strip().upper() for item in market_features.get("active_episode_ids", []) if str(item or "").strip()]
    active_episode_ids: list[str] = []
    for token in provided_episode_ids + [match.episode_id for match in episode_matches]:
        if token and token not in active_episode_ids:
            active_episode_ids.append(token)

    episode_snapshot = {
        "episode_ids": active_episode_ids,
        "matches": [
            {
                "episode_id": match.episode_id,
                "observed_events": list(match.observed_events),
                "matched_required_events": int(match.matched_required_events),
                "runtime_hint": match.runtime_hint,
            }
            for match in episode_matches
        ],
        "activation_mode": "heuristic_live_runtime",
    }

    return LiveTradeContext(
        timestamp=str(timestamp),
        symbol=str(symbol).upper(),
        timeframe=str(timeframe),
        primary_event_id=primary_event_id,
        event_family=compat_event_family,
        canonical_regime=str(regime_snapshot.get("canonical_regime", "")).strip().upper(),
        event_side=str(detected_event.event_side).lower(),
        event_confidence=detected_event.event_confidence,
        event_severity=detected_event.event_severity,
        data_quality_flag=detected_event.data_quality_flag,
        event_version=detected_event.event_version,
        threshold_version=detected_event.threshold_version,
        live_features=dict(market_features),
        regime_snapshot=regime_snapshot,
        execution_env=dict(execution_env),
        portfolio_state=dict(portfolio_state),
        active_event_families=active_event_families,
        active_event_ids=active_event_ids,
        active_episode_ids=active_episode_ids,
        contradiction_event_families=contradiction_event_families,
        contradiction_event_ids=contradiction_event_ids,
        episode_snapshot=episode_snapshot,
        active_groups=active_groups or set(),
        family_budgets=family_budgets or {},
    )
