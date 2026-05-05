from __future__ import annotations

from enum import Enum
from typing import Any, Mapping

CANONICAL_EVENT_SIDES = {"bullish", "bearish", "neutral", "bidirectional", "unknown"}

class PolaritySemantics(str, Enum):
    PRICE_DIRECTION = "price_direction"
    DEVIATION_DIRECTION = "deviation_direction"
    BASIS_SPREAD_DIRECTION = "basis_spread_direction"
    FUNDING_CROWDING_SIDE = "funding_crowding_side"
    PRICE_OI_QUADRANT = "price_oi_quadrant"
    LIQUIDATION_SIDE = "liquidation_side"
    LIQUIDITY_SWEEP_SIDE = "liquidity_sweep_side"
    REGIME_TRANSITION = "regime_transition"
    TEMPORAL_GUARD = "temporal_guard"
    EXECUTION_GUARD = "execution_guard"
    NEUTRAL_GUARD = "neutral_guard"
    UNKNOWN = "unknown"

_SIDE_ALIASES = {
    "up": "bullish", "bull": "bullish", "bullish": "bullish", "long": "bullish", "buy": "bullish", "positive": "bullish", "pos": "bullish", "+1": "bullish", "1": "bullish",
    "down": "bearish", "bear": "bearish", "bearish": "bearish", "short": "bearish", "sell": "bearish", "negative": "bearish", "neg": "bearish", "-1": "bearish",
    "longs_liquidated": "bearish", "long_liquidation": "bearish", "long_liquidated": "bearish",
    "shorts_liquidated": "bullish", "short_liquidation": "bullish", "short_liquidated": "bullish",
    "neutral": "neutral", "flat": "neutral", "none": "neutral", "0": "neutral", "non_directional": "neutral", "non-directional": "neutral",
    "both": "bidirectional", "conditional": "bidirectional", "bidirectional": "bidirectional",
    "unknown": "unknown", "": "unknown",
}

_DIRECTION_ALIASES = {
    "bullish": "up",
    "bearish": "down",
    "neutral": "non_directional",
    "bidirectional": "non_directional",
    "unknown": "non_directional",
}

LIVE_SIDE = {
    "bullish": "long",
    "bearish": "short",
    "neutral": "conditional",
    "bidirectional": "both",
    "unknown": "conditional",
}
ORDER_SIDE = {
    "bullish": "buy",
    "bearish": "sell",
    "neutral": "flat",
    "bidirectional": "flat",
    "unknown": "flat",
}


def normalize_event_side(value: Any) -> str:
    token = str(value or "unknown").strip().lower()
    return _SIDE_ALIASES.get(token, token if token in CANONICAL_EVENT_SIDES else "unknown")


def side_to_direction(side: Any, fallback: Any = 0) -> int:
    side_norm = normalize_event_side(side)
    if side_norm == "bullish":
        return 1
    if side_norm == "bearish":
        return -1
    try:
        value = float(fallback)
    except (TypeError, ValueError):
        return 0
    return 1 if value > 0 else -1 if value < 0 else 0


def side_to_legacy_direction(side: Any) -> str:
    return _DIRECTION_ALIASES.get(normalize_event_side(side), "non_directional")


def side_to_live_side(side: Any) -> str:
    return LIVE_SIDE.get(normalize_event_side(side), "conditional")


def side_to_order_side(side: Any) -> str:
    return ORDER_SIDE.get(normalize_event_side(side), "flat")


def normalize_polarity_semantics(value: Any) -> str:
    token = str(value or "unknown").strip().lower()
    aliases = {
        "basis": PolaritySemantics.BASIS_SPREAD_DIRECTION.value,
        "basis_direction": PolaritySemantics.BASIS_SPREAD_DIRECTION.value,
        "spread_direction": PolaritySemantics.BASIS_SPREAD_DIRECTION.value,
        "price_oi": PolaritySemantics.PRICE_OI_QUADRANT.value,
        "oi_quadrant": PolaritySemantics.PRICE_OI_QUADRANT.value,
        "liquidation": PolaritySemantics.LIQUIDATION_SIDE.value,
        "execution": PolaritySemantics.EXECUTION_GUARD.value,
        "temporal": PolaritySemantics.TEMPORAL_GUARD.value,
        "guard": PolaritySemantics.NEUTRAL_GUARD.value,
    }
    token = aliases.get(token, token)
    valid = {item.value for item in PolaritySemantics}
    return token if token in valid else PolaritySemantics.UNKNOWN.value


def infer_semantics_from_event(event_id: str = "", family: str = "", subtype: str = "", role: str = "", metadata: Mapping[str, Any] | None = None) -> str:
    event = str(event_id or "").strip().upper()
    fam = str(family or "").strip().upper()
    sub = str(subtype or "").strip().lower()
    role_l = str(role or "").strip().lower()
    meta = dict(metadata or {})
    explicit = meta.get("polarity_semantics") or meta.get("side_semantics")
    if explicit:
        return normalize_polarity_semantics(explicit)
    if event.startswith("SESSION_") or "NEWS_WINDOW" in event or "TIMESTAMP" in event:
        return PolaritySemantics.TEMPORAL_GUARD.value
    if "SLIPPAGE" in event or "SPREAD_REGIME" in event or "SPREAD_BLOWOUT" in event or "DEPTH_STRESS" in event or "LIQUIDITY_STRESS" in event:
        return PolaritySemantics.EXECUTION_GUARD.value
    if "BASIS" in event or "DESYNC" in event or "DIVERGENCE" in event or "LEAD_LAG" in event or event in {"FND_DISLOC", "COPULA_PAIRS_TRADING"}:
        return PolaritySemantics.BASIS_SPREAD_DIRECTION.value
    if "FUNDING" in event or "FND_" in event:
        return PolaritySemantics.FUNDING_CROWDING_SIDE.value
    if "PRICE_" in event and "OI_" in event:
        return PolaritySemantics.PRICE_OI_QUADRANT.value
    if "OI_FLUSH" in event or event.startswith("OI_SPIKE"):
        return PolaritySemantics.PRICE_OI_QUADRANT.value
    if "LIQUIDATION" in event or "DELEVERAGING" in event or "FORCED_FLOW" in event or "FLOW_EXHAUSTION" in event:
        return PolaritySemantics.LIQUIDATION_SIDE.value
    if "SWEEP" in event or "WICK" in event or "ABSORPTION" in event or "ORDERFLOW" in event:
        return PolaritySemantics.LIQUIDITY_SWEEP_SIDE.value
    if fam == "LIQUIDITY_DISLOCATION" and any(x in event for x in ("VACUUM", "SHOCK", "DEPTH", "GAP", "SPREAD")):
        return PolaritySemantics.NEUTRAL_GUARD.value
    if fam == "REGIME_TRANSITION" or "REGIME" in event or "CHOP_TO_TREND" in event or "TREND_TO_CHOP" in event or "BETA_SPIKE" in event:
        return PolaritySemantics.REGIME_TRANSITION.value
    if fam == "VOLATILITY_TRANSITION" and event in {"VOL_CLUSTER_SHIFT", "VOL_RELAXATION_START", "RANGE_COMPRESSION_END"}:
        return PolaritySemantics.REGIME_TRANSITION.value
    if fam == "STATISTICAL_DISLOCATION" and event not in {"BASIS_DISLOC", "FND_DISLOC"}:
        return PolaritySemantics.DEVIATION_DIRECTION.value
    if fam in {"TREND_STRUCTURE", "VOLATILITY_TRANSITION"} or sub in {"breakout", "breakout_trigger", "trend"} or any(x in event for x in ("TREND_", "CLIMAX", "FAILED_CONTINUATION", "PRICE_VOL_IMBALANCE")):
        return PolaritySemantics.PRICE_DIRECTION.value
    if role_l in {"context", "filter"}:
        return PolaritySemantics.NEUTRAL_GUARD.value
    return PolaritySemantics.UNKNOWN.value


def _series_value(features: Mapping[str, Any], key: str, idx: int) -> Any:
    value = features.get(key)
    if value is None:
        return None
    if hasattr(value, "iloc"):
        return value.iloc[idx]
    try:
        return value[idx]
    except Exception:
        return value


def infer_side_from_features(features: Mapping[str, Any], idx: int, *, semantics: str = "unknown", fallback_intensity: float | None = None) -> tuple[str, str]:
    semantics = normalize_polarity_semantics(semantics)
    side_keys = (
        "event_side", "side", "breakout_side", "cascade_side", "liquidation_side", "event_polarity", "sweep_side", "wick_side", "imbalance_side",
    )
    for key in side_keys:
        raw = _series_value(features, key, idx)
        if raw is None:
            continue
        side = normalize_event_side(raw)
        if side != "unknown":
            return side, key
    if semantics == PolaritySemantics.PRICE_OI_QUADRANT.value:
        price = _numeric_feature(features, idx, ("close_ret", "signed_return", "logret_1", "signed_move_bps"))
        oi = _numeric_feature(features, idx, ("oi_pct_change", "oi_delta", "oi_z"))
        if price is not None and price != 0:
            return ("bullish" if price > 0 else "bearish"), "price_oi_quadrant"
    if semantics in {PolaritySemantics.BASIS_SPREAD_DIRECTION.value, PolaritySemantics.FUNDING_CROWDING_SIDE.value}:
        keys = ("basis_zscore", "basis_z", "basis_bps", "funding_signed", "funding_rate", "funding_rate_bps", "funding_bps", "fr_sign")
    elif semantics == PolaritySemantics.DEVIATION_DIRECTION.value:
        keys = ("px_z", "zscore", "deviation_z", "close_ret", "signed_return", "logret_1")
    else:
        keys = ("signed_move_bps", "signed_return", "close_ret", "logret_1", "event_direction", "signed_direction")
    val = _numeric_feature(features, idx, keys)
    if val is not None and val != 0:
        return ("bullish" if val > 0 else "bearish"), next((k for k in keys if _series_value(features, k, idx) is not None), "numeric_feature")
    if semantics in {PolaritySemantics.EXECUTION_GUARD.value, PolaritySemantics.TEMPORAL_GUARD.value, PolaritySemantics.NEUTRAL_GUARD.value, PolaritySemantics.REGIME_TRANSITION.value}:
        return "neutral", semantics
    if fallback_intensity is not None and fallback_intensity != 0 and semantics == PolaritySemantics.PRICE_DIRECTION.value:
        return ("bullish" if fallback_intensity > 0 else "bearish"), "fallback_intensity"
    return "unknown", "unknown"


def _numeric_feature(features: Mapping[str, Any], idx: int, keys: tuple[str, ...]) -> float | None:
    for key in keys:
        raw = _series_value(features, key, idx)
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if value == value:
            return value
    return None


def infer_magnitude_from_features(features: Mapping[str, Any], idx: int, *, fallback: float | None = None) -> tuple[float | None, str]:
    keys = (
        "magnitude", "abs_move_bps", "signed_move_bps", "breakout_dist", "liquidation_notional", "funding_abs_pct", "oi_z", "zscore", "px_z", "basis_zscore", "basis_z", "trigger_value", "event_score",
    )
    for key in keys:
        raw = _series_value(features, key, idx)
        if raw is None:
            continue
        try:
            value = abs(float(raw))
        except (TypeError, ValueError):
            continue
        if value == value:
            return value, key
    if fallback is not None:
        return abs(float(fallback)), "fallback_intensity"
    return None, "unknown"


def severity_bucket_from_score(value: Any) -> str:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return "unknown"
    if score >= 0.90:
        return "extreme"
    if score >= 0.66:
        return "high"
    if score >= 0.33:
        return "medium"
    return "low"


def anchor_role_from_event(role: str = "", deployment_disposition: str = "", family: str = "", event_id: str = "", semantics: str = "") -> str:
    role_l = str(role or "").strip().lower()
    disp = str(deployment_disposition or "").strip().lower()
    fam = str(family or "").strip().upper()
    event = str(event_id or "").strip().upper()
    sem = normalize_polarity_semantics(semantics)
    if role_l in {"context", "filter"}:
        return "context_filter"
    if role_l in {"sequence_component", "composite"}:
        return "sequence_component"
    if role_l == "research_only":
        return "research_only"
    if sem == PolaritySemantics.EXECUTION_GUARD.value:
        return "execution_guard"
    if sem == PolaritySemantics.TEMPORAL_GUARD.value:
        return "temporal_guard"
    if sem in {PolaritySemantics.NEUTRAL_GUARD.value, PolaritySemantics.REGIME_TRANSITION.value}:
        return "risk_guard" if fam in {"REGIME_TRANSITION", "LIQUIDITY_DISLOCATION", "EXECUTION_FRICTION"} else "confirmation_anchor"
    if "primary" in disp:
        return "alpha_anchor"
    if "confirm" in disp or "secondary" in disp:
        return "confirmation_anchor"
    return "alpha_anchor" if role_l == "trigger" else "context_filter"
