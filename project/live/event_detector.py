from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping

from project.core.coercion import safe_float
from project.domain.compiled_registry import get_domain_registry


@dataclass(frozen=True)
class DetectedEvent:
    event_id: str
    event_family: str
    canonical_regime: str
    event_side: str
    features: Dict[str, Any]
    event_confidence: float | None = None
    event_severity: float | None = None
    data_quality_flag: str = "ok"
    event_version: str = "v1"
    threshold_version: str = "1.0"


def _build_detected_event(
    *,
    event_id: str,
    event_side: str,
    features: Mapping[str, Any],
) -> DetectedEvent:
    normalized_event_id = str(event_id).strip().upper()
    spec = get_domain_registry().get_event(normalized_event_id)
    canonical_regime = ""
    if spec is not None:
        canonical_regime = str(spec.canonical_regime or spec.canonical_family).strip().upper()
    return DetectedEvent(
        event_id=normalized_event_id,
        event_family=normalized_event_id,
        canonical_regime=canonical_regime,
        event_side=str(event_side).strip().lower(),
        features=dict(features),
    )


def _move_bps(current_close: float, previous_close: float | None) -> float | None:
    if previous_close is None or previous_close <= 0.0:
        return None
    return ((float(current_close) / float(previous_close)) - 1.0) * 10_000.0


def _detect_vol_shock(*, symbol: str, timeframe: str, move_bps: float | None, volume: float, config: Mapping[str, Any]) -> DetectedEvent | None:
    if move_bps is None:
        return None
    min_abs_move_bps = float(config.get("vol_shock_min_abs_move_bps", 35.0) or 35.0)
    if abs(move_bps) < min_abs_move_bps:
        return None
    event_side = "long" if move_bps > 0.0 else "short"
    return _build_detected_event(
        event_id="VOL_SHOCK",
        event_side=event_side,
        features={
            "symbol": str(symbol).upper(),
            "timeframe": str(timeframe),
            "move_bps": float(move_bps),
            "volume": safe_float(volume, 0.0),
            "detector_strength": abs(float(move_bps)) / max(min_abs_move_bps, 1.0),
        },
    )


def _detect_vol_spike(*, symbol: str, timeframe: str, move_bps: float | None, volume: float, config: Mapping[str, Any]) -> DetectedEvent | None:
    if move_bps is None:
        return None
    min_abs_move_bps = float(config.get("vol_spike_min_abs_move_bps", 55.0) or 55.0)
    min_volume = float(config.get("vol_spike_min_volume", 50_000.0) or 50_000.0)
    if abs(move_bps) < min_abs_move_bps or volume < min_volume:
        return None
    event_side = "long" if move_bps > 0.0 else "short"
    return _build_detected_event(
        event_id="VOL_SPIKE",
        event_side=event_side,
        features={
            "symbol": str(symbol).upper(),
            "timeframe": str(timeframe),
            "move_bps": float(move_bps),
            "volume": safe_float(volume, 0.0),
            "detector_strength": min(3.0, abs(float(move_bps)) / max(min_abs_move_bps, 1.0) + (float(volume) / max(min_volume, 1.0))),
        },
    )


def _detect_liquidity_vacuum(*, symbol: str, timeframe: str, move_bps: float | None, market_features: Mapping[str, Any], config: Mapping[str, Any]) -> DetectedEvent | None:
    spread_bps = float(market_features.get("spread_bps", 0.0) or 0.0)
    depth_usd = float(market_features.get("depth_usd", 0.0) or 0.0)
    min_spread_bps = float(config.get("liquidity_vacuum_min_spread_bps", 5.0) or 5.0)
    max_depth_usd = float(config.get("liquidity_vacuum_max_depth_usd", 25_000.0) or 25_000.0)
    if spread_bps < min_spread_bps or depth_usd > max_depth_usd:
        return None
    signed_move = float(move_bps or 0.0)
    event_side = "long" if signed_move > 0.0 else "short" if signed_move < 0.0 else "conditional"
    return _build_detected_event(
        event_id="LIQUIDITY_VACUUM",
        event_side=event_side,
        features={
            "symbol": str(symbol).upper(),
            "timeframe": str(timeframe),
            "move_bps": float(signed_move),
            "spread_bps": spread_bps,
            "depth_usd": depth_usd,
            "detector_strength": (spread_bps / max(min_spread_bps, 1.0)) + (max_depth_usd / max(depth_usd, 1.0)),
        },
    )


def _detect_liquidation_cascade(*, symbol: str, timeframe: str, move_bps: float | None, market_features: Mapping[str, Any], config: Mapping[str, Any]) -> DetectedEvent | None:
    signed_move = float(move_bps or 0.0)
    oi_delta_fraction = float(
        market_features.get("open_interest_delta_fraction", market_features.get("oi_delta_fraction", 0.0)) or 0.0
    )
    funding_rate = float(market_features.get("funding_rate", 0.0) or 0.0)
    min_abs_move_bps = float(config.get("liquidation_cascade_min_abs_move_bps", 80.0) or 80.0)
    min_abs_oi_drop_fraction = float(config.get("liquidation_cascade_min_abs_oi_drop_fraction", 0.03) or 0.03)
    min_abs_funding_rate = float(config.get("liquidation_cascade_min_abs_funding_rate", 0.0005) or 0.0005)
    if abs(signed_move) < min_abs_move_bps:
        return None
    if oi_delta_fraction >= 0.0 or abs(oi_delta_fraction) < min_abs_oi_drop_fraction:
        return None
    if abs(funding_rate) < min_abs_funding_rate:
        return None
    event_side = "long" if signed_move > 0.0 else "short"
    detector_strength = (
        abs(signed_move) / max(min_abs_move_bps, 1.0)
        + abs(oi_delta_fraction) / max(min_abs_oi_drop_fraction, 1e-9)
        + abs(funding_rate) / max(min_abs_funding_rate, 1e-9)
    )
    return _build_detected_event(
        event_id="LIQUIDATION_CASCADE",
        event_side=event_side,
        features={
            "symbol": str(symbol).upper(),
            "timeframe": str(timeframe),
            "move_bps": float(signed_move),
            "open_interest_delta_fraction": float(oi_delta_fraction),
            "funding_rate": float(funding_rate),
            "detector_strength": float(detector_strength),
        },
    )


def detect_live_event(
    *,
    symbol: str,
    timeframe: str,
    current_close: float,
    previous_close: float | None,
    volume: float | None = None,
    market_features: Mapping[str, Any] | None = None,
    supported_event_ids: List[str] | None = None,
    supported_event_families: List[str] | None = None,
    detector_config: Mapping[str, Any] | None = None,
) -> DetectedEvent | None:
    configured_supported = supported_event_ids
    if configured_supported is None:
        configured_supported = supported_event_families
    supported = [
        str(item).strip().upper()
        for item in list(configured_supported or ["VOL_SHOCK"])
        if str(item).strip()
    ]
    if not supported:
        supported = ["VOL_SHOCK"]

    config = dict(detector_config or {})
    features = dict(market_features or {})
    move_bps = _move_bps(float(current_close), previous_close)
    vol = float(volume or 0.0)

    detectors = {
        "VOL_SHOCK": lambda: _detect_vol_shock(symbol=symbol, timeframe=timeframe, move_bps=move_bps, volume=vol, config=config),
        "VOL_SPIKE": lambda: _detect_vol_spike(symbol=symbol, timeframe=timeframe, move_bps=move_bps, volume=vol, config=config),
        "LIQUIDITY_VACUUM": lambda: _detect_liquidity_vacuum(symbol=symbol, timeframe=timeframe, move_bps=move_bps, market_features=features, config=config),
        "LIQUIDATION_CASCADE": lambda: _detect_liquidation_cascade(symbol=symbol, timeframe=timeframe, move_bps=move_bps, market_features=features, config=config),
    }

    for family in supported:
        detector = detectors.get(family)
        if detector is None:
            continue
        result = detector()
        if result is not None:
            return result
    return None
