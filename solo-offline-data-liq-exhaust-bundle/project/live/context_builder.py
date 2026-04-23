from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Mapping, Sequence

import numpy as np
import pandas as pd

from project.core.coercion import safe_float
from project.episodes import infer_live_episode_matches
from project.events.registry import get_detector_contract
from project.live.contracts.live_trade_context import LiveTradeContext

if TYPE_CHECKING:
    from project.live.event_detector import DetectedEvent


@dataclass(frozen=True)
class LiveRuntimeCoreDetectorInputSurface:
    row: Dict[str, Any]
    detector_input_status: Dict[str, Any]


_RUNTIME_CORE_EVENT_INPUT_BINDINGS: dict[str, dict[str, dict[str, Any]]] = {
    "BASIS_DISLOC": {
        "timestamp": {"mode": "direct"},
        "close_perp": {"mode": "direct"},
        "close_spot": {
            "mode": "direct",
            "accepted_sources": {"close_spot", "spot_close", "index_price", "spot_price"},
        },
    },
    "FND_DISLOC": {
        "timestamp": {"mode": "direct"},
        "close_perp": {"mode": "direct"},
        "close_spot": {
            "mode": "direct",
            "accepted_sources": {"close_spot", "spot_close", "index_price", "spot_price"},
        },
        "funding_rate_scaled": {
            "mode": "direct",
            "accepted_sources": {"funding_rate_scaled", "funding_rate"},
        },
    },
    "LIQUIDATION_CASCADE": {
        "timestamp": {"mode": "direct"},
        "liquidation_notional": {
            "mode": "direct",
            "accepted_sources": {
                "liquidation_notional",
                "liquidation_notional_usd",
                "data_manager",
            },
        },
        "oi_delta_1h": {"mode": "derived", "depends_on": ("oi_notional",)},
        "oi_notional": {
            "mode": "direct",
            "accepted_sources": {"oi_notional", "open_interest"},
        },
        "close": {"mode": "direct"},
        "high": {"mode": "direct"},
        "low": {"mode": "direct"},
    },
    "LIQUIDITY_SHOCK": {
        "timestamp": {"mode": "direct"},
        "close": {"mode": "direct"},
        "high": {"mode": "direct"},
        "low": {"mode": "direct"},
    },
    "LIQUIDITY_STRESS_DIRECT": {
        "timestamp": {"mode": "direct"},
        "close": {"mode": "direct"},
        "high": {"mode": "direct"},
        "low": {"mode": "direct"},
        "depth_usd": {
            "mode": "direct",
            "accepted_sources": {"depth_usd", "liquidity_available"},
        },
        "spread_bps": {
            "mode": "direct",
            "accepted_sources": {"spread_bps", "book_ticker"},
        },
    },
    "LIQUIDITY_VACUUM": {
        "timestamp": {"mode": "direct"},
        "close": {"mode": "direct"},
        "high": {"mode": "direct"},
        "low": {"mode": "direct"},
    },
    "OI_SPIKE_NEGATIVE": {
        "timestamp": {"mode": "direct"},
        "oi_notional": {
            "mode": "direct",
            "accepted_sources": {"oi_notional", "open_interest"},
        },
        "close": {"mode": "direct"},
        "ms_oi_state": {"mode": "derived", "depends_on": ("oi_notional",)},
        "ms_oi_confidence": {"mode": "derived", "depends_on": ("oi_notional",)},
        "ms_oi_entropy": {"mode": "derived", "depends_on": ("oi_notional",)},
    },
    "SPOT_PERP_BASIS_SHOCK": {
        "timestamp": {"mode": "direct"},
        "close_perp": {"mode": "direct"},
        "close_spot": {
            "mode": "direct",
            "accepted_sources": {"close_spot", "spot_close", "index_price", "spot_price"},
        },
    },
    "VOL_SHOCK": {
        "timestamp": {"mode": "direct"},
        "close": {"mode": "direct"},
        "rv_96": {"mode": "derived", "depends_on": ("close",)},
        "range_96": {"mode": "derived", "depends_on": ("high", "low", "close")},
        "range_med_2880": {"mode": "derived", "depends_on": ("high", "low", "close")},
    },
    "VOL_SPIKE": {
        "timestamp": {"mode": "direct"},
        "close": {"mode": "direct"},
        "rv_96": {"mode": "derived", "depends_on": ("close",)},
        "range_96": {"mode": "derived", "depends_on": ("high", "low", "close")},
        "range_med_2880": {"mode": "derived", "depends_on": ("high", "low", "close")},
    },
}


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


def _normalize_timestamp(raw: object) -> pd.Timestamp:
    parsed = pd.to_datetime(raw, utc=True, errors="coerce")
    if pd.isna(parsed):
        return pd.Timestamp.now(tz="UTC")
    return parsed


def _timeframe_minutes(timeframe: str) -> int:
    token = str(timeframe or "5m").strip().lower()
    if token.endswith("m"):
        try:
            return max(1, int(token[:-1]))
        except ValueError:
            return 5
    if token.endswith("h"):
        try:
            return max(1, int(token[:-1])) * 60
        except ValueError:
            return 60
    return 5


def _field_source_value(
    market_features: Mapping[str, Any],
    candidates: Sequence[str],
    *,
    default: float,
) -> tuple[float, str]:
    for key in candidates:
        if key in market_features:
            return safe_float(market_features.get(key), default), str(key)
    return float(default), "missing"


def _source_override(market_features: Mapping[str, Any], field: str, fallback: str) -> str:
    explicit = str(market_features.get(f"{field}_source", "") or "").strip()
    return explicit or fallback


def _bool_source_override(market_features: Mapping[str, Any], field: str) -> str:
    explicit = str(market_features.get(f"{field}_source", "") or "").strip()
    return explicit or "direct"


def build_runtime_core_detector_input_surface(
    *,
    symbol: str,
    timeframe: str,
    current_close: float,
    previous_close: float | None,
    volume: float | None,
    market_features: Mapping[str, Any],
    supported_event_ids: Sequence[str],
) -> LiveRuntimeCoreDetectorInputSurface:
    current_close_f = float(current_close)
    previous_close_f = float(previous_close) if previous_close is not None else None
    high, high_source = _field_source_value(
        market_features,
        ("high", "last_high"),
        default=max(current_close_f, previous_close_f or current_close_f),
    )
    low, low_source = _field_source_value(
        market_features,
        ("low", "last_low"),
        default=min(current_close_f, previous_close_f or current_close_f),
    )
    close_spot, close_spot_source = _field_source_value(
        market_features,
        ("close_spot", "spot_close", "index_price", "spot_price"),
        default=current_close_f,
    )
    if close_spot_source == "missing":
        close_spot_source = (
            "mark_price" if "mark_price" in market_features else "current_close_fallback"
        )
        close_spot = safe_float(
            market_features.get("mark_price", current_close_f),
            current_close_f,
        )

    spread_bps, spread_source = _field_source_value(
        market_features,
        ("spread_bps",),
        default=0.0,
    )
    depth_usd, depth_source = _field_source_value(
        market_features,
        ("depth_usd", "liquidity_available"),
        default=0.0,
    )
    open_interest, oi_source = _field_source_value(
        market_features,
        ("oi_notional", "open_interest"),
        default=0.0,
    )
    funding_rate_scaled, funding_source = _field_source_value(
        market_features,
        ("funding_rate_scaled", "funding_rate"),
        default=0.0,
    )
    liquidation_notional, liquidation_source = _field_source_value(
        market_features,
        ("liquidation_notional", "liquidation_notional_usd"),
        default=0.0,
    )
    if "liquidation_notional_source" in market_features:
        liquidation_source = (
            str(market_features.get("liquidation_notional_source") or "").strip()
            or liquidation_source
        )

    if "spread_bps_source" in market_features:
        spread_source = str(market_features.get("spread_bps_source") or "").strip() or spread_source
    if "depth_usd_source" in market_features:
        depth_source = str(market_features.get("depth_usd_source") or "").strip() or depth_source
    if "open_interest_source" in market_features:
        oi_source = str(market_features.get("open_interest_source") or "").strip() or oi_source
    if "funding_rate_source" in market_features:
        funding_source = (
            str(market_features.get("funding_rate_source") or "").strip() or funding_source
        )
    if "close_spot_source" in market_features:
        close_spot_source = (
            str(market_features.get("close_spot_source") or "").strip() or close_spot_source
        )

    move_bps = (
        ((current_close_f / previous_close_f) - 1.0) * 10_000.0
        if previous_close_f is not None and previous_close_f > 0.0
        else 0.0
    )
    range_bps = (
        ((float(high) - float(low)) / current_close_f) * 10_000.0 if current_close_f > 0.0 else 0.0
    )

    row = {
        "timestamp": _normalize_timestamp(market_features.get("timestamp")),
        "symbol": str(symbol).upper(),
        "timeframe": str(timeframe),
        "close": current_close_f,
        "high": float(high),
        "low": float(low),
        "open": safe_float(
            market_features.get(
                "open", previous_close_f if previous_close_f is not None else current_close_f
            ),
            previous_close_f if previous_close_f is not None else current_close_f,
        ),
        "volume": safe_float(volume, 0.0),
        "quote_volume": safe_float(
            market_features.get("quote_volume", volume), safe_float(volume, 0.0)
        ),
        "close_perp": current_close_f,
        "close_spot": float(close_spot),
        "spread_bps": float(spread_bps),
        "depth_usd": float(depth_usd),
        "open_interest": float(open_interest),
        "oi_notional": float(open_interest),
        "funding_rate_scaled": float(funding_rate_scaled),
        "funding_rate": float(funding_rate_scaled),
        "liquidation_notional": float(liquidation_notional),
        "liquidation_notional_usd": float(liquidation_notional),
        "move_bps": float(move_bps),
        "range_bps": float(range_bps),
        "expected_cost_bps": safe_float(market_features.get("expected_cost_bps"), 0.0),
        "tob_coverage": safe_float(market_features.get("tob_coverage"), 0.0),
        "mark_price": safe_float(
            market_features.get("mark_price", current_close_f), current_close_f
        ),
        "ms_imbalance_24": safe_float(market_features.get("ms_imbalance_24"), 0.0),
        "ms_funding_state": safe_float(market_features.get("ms_funding_state"), float("nan")),
        "ms_funding_confidence": safe_float(
            market_features.get("ms_funding_confidence"), float("nan")
        ),
        "ms_funding_entropy": safe_float(market_features.get("ms_funding_entropy"), float("nan")),
        "ms_spread_state": safe_float(market_features.get("ms_spread_state"), float("nan")),
        "ms_spread_confidence": safe_float(
            market_features.get("ms_spread_confidence"), float("nan")
        ),
        "ms_spread_entropy": safe_float(market_features.get("ms_spread_entropy"), float("nan")),
    }
    source_map = {
        "timestamp": _bool_source_override(market_features, "timestamp"),
        "close": "kline_close",
        "high": _source_override(market_features, "high", high_source),
        "low": _source_override(market_features, "low", low_source),
        "open": _bool_source_override(market_features, "open"),
        "close_perp": "kline_close",
        "close_spot": close_spot_source,
        "spread_bps": spread_source,
        "depth_usd": depth_source,
        "oi_notional": oi_source,
        "funding_rate_scaled": funding_source,
        "liquidation_notional": liquidation_source,
    }

    return LiveRuntimeCoreDetectorInputSurface(
        row=row,
        detector_input_status=_build_detector_input_status(
            supported_event_ids=supported_event_ids,
            source_map=source_map,
        ),
    )


def enrich_runtime_core_detector_history(frame: pd.DataFrame, *, timeframe: str) -> pd.DataFrame:
    out = frame.copy()
    close = pd.to_numeric(out["close"], errors="coerce").astype(float)
    high = pd.to_numeric(out["high"], errors="coerce").astype(float)
    low = pd.to_numeric(out["low"], errors="coerce").astype(float)
    range_bps = pd.to_numeric(out["range_bps"], errors="coerce").astype(float)
    returns = np.log(close / close.shift(1).replace(0.0, np.nan))
    out["rv_96"] = returns.rolling(window=96, min_periods=24).std().fillna(0.0)
    out["range_96"] = range_bps.rolling(window=96, min_periods=24).mean().fillna(range_bps)
    out["range_med_2880"] = (
        range_bps.rolling(window=2880, min_periods=24)
        .median()
        .fillna(range_bps.expanding().median())
    )
    out["close_perp"] = pd.to_numeric(out["close_perp"], errors="coerce").fillna(close)
    out["close_spot"] = pd.to_numeric(out["close_spot"], errors="coerce").fillna(close)
    out["funding_rate_scaled"] = pd.to_numeric(out["funding_rate_scaled"], errors="coerce").fillna(
        0.0
    )
    out["oi_notional"] = pd.to_numeric(out["oi_notional"], errors="coerce").fillna(0.0)
    out["liquidation_notional"] = pd.to_numeric(
        out["liquidation_notional"], errors="coerce"
    ).fillna(0.0)
    bars_per_hour = max(1, round(60 / max(1, _timeframe_minutes(timeframe))))
    out["oi_delta_1h"] = out["oi_notional"] - out["oi_notional"].shift(bars_per_hour).fillna(
        out["oi_notional"]
    )
    out["ms_vol_state"], out["ms_vol_confidence"], out["ms_vol_entropy"] = _vol_state_series(out)
    out["ms_oi_state"], out["ms_oi_confidence"], out["ms_oi_entropy"] = _oi_state_series(out)
    out["high"] = high.fillna(close)
    out["low"] = low.fillna(close)
    return out


def _oi_state_series(frame: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    oi = pd.to_numeric(frame["oi_notional"], errors="coerce").replace(0.0, np.nan).astype(float)
    oi_pct = oi.pct_change(fill_method=None).fillna(0.0)
    oi_mean = oi_pct.rolling(window=96, min_periods=24).mean().fillna(0.0)
    oi_std = oi_pct.rolling(window=96, min_periods=24).std().fillna(1e-8).clip(lower=1e-8)
    oi_z = ((oi_pct - oi_mean) / oi_std).fillna(0.0)
    oi_z_abs = oi_z.abs()
    q70 = oi_z_abs.shift(1).rolling(window=2880, min_periods=24).quantile(0.70).fillna(oi_z_abs)
    q90 = oi_z_abs.shift(1).rolling(window=2880, min_periods=24).quantile(0.90).fillna(q70)
    state = pd.Series(0.0, index=frame.index, dtype=float)
    state = state.mask(oi_z_abs >= q70, 1.0)
    state = state.mask(oi_z_abs >= q90, 2.0)
    confidence = pd.Series(0.70, index=frame.index, dtype=float)
    entropy = pd.Series(0.25, index=frame.index, dtype=float)
    return state, confidence, entropy


def _vol_state_series(frame: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    rv_96 = pd.to_numeric(frame["rv_96"], errors="coerce").fillna(0.0)
    q70 = rv_96.shift(1).rolling(window=2880, min_periods=24).quantile(0.70).fillna(rv_96)
    q90 = rv_96.shift(1).rolling(window=2880, min_periods=24).quantile(0.90).fillna(q70)
    state = pd.Series(0.0, index=frame.index, dtype=float)
    state = state.mask(rv_96 >= q70, 1.0)
    state = state.mask(rv_96 >= q90, 2.0)
    confidence = pd.Series(0.75, index=frame.index, dtype=float)
    entropy = pd.Series(0.20, index=frame.index, dtype=float)
    return state, confidence, entropy


def _build_detector_input_status(
    *,
    supported_event_ids: Sequence[str],
    source_map: Mapping[str, str],
) -> dict[str, Any]:
    per_event: dict[str, Any] = {}
    for event_id in [
        str(item).strip().upper() for item in supported_event_ids if str(item).strip()
    ]:
        bindings = _RUNTIME_CORE_EVENT_INPUT_BINDINGS.get(event_id)
        if bindings is None:
            continue
        missing_inputs: list[str] = []
        approximated_inputs: list[str] = []
        mapping: dict[str, Any] = {}
        for column, rule in bindings.items():
            source = str(source_map.get(column, "derived")).strip() or "derived"
            mode = str(rule.get("mode", "direct"))
            accepted_sources = set(rule.get("accepted_sources", set()))
            depends_on = [str(item) for item in rule.get("depends_on", ())]
            is_missing = False
            is_approx = False
            if mode == "direct":
                if source in {"missing", "configured_default", "current_close_fallback"}:
                    is_missing = True
                elif accepted_sources and source not in accepted_sources:
                    is_missing = True
            elif mode == "derived":
                if any(
                    str(source_map.get(dep, "missing")).strip()
                    in {"missing", "configured_default", "current_close_fallback"}
                    for dep in depends_on
                ):
                    is_missing = True
                elif any(
                    accepted_sources
                    and str(source_map.get(dep, "")).strip() not in accepted_sources
                    for dep in depends_on
                ):
                    is_approx = True
            if is_missing:
                missing_inputs.append(column)
            elif is_approx:
                approximated_inputs.append(column)
            mapping[column] = {
                "mode": mode,
                "source": source,
                "depends_on": depends_on,
                "status": "missing" if is_missing else "approximate" if is_approx else "exact",
            }
        per_event[event_id] = {
            "missing_inputs": missing_inputs,
            "approximated_inputs": approximated_inputs,
            "input_mapping": mapping,
        }
    return {
        "adapter": "governed_runtime_core",
        "per_event": per_event,
        "source_map": dict(source_map),
    }


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
    explicit_microstructure_regime = str(
        market_features.get("microstructure_regime", "") or ""
    ).strip()
    if explicit_microstructure_regime:
        regime_snapshot["microstructure_regime"] = explicit_microstructure_regime
    else:
        spread_bps = safe_float(market_features.get("spread_bps"), float("nan"))
        regime_snapshot["microstructure_regime"] = (
            "healthy" if np.isfinite(spread_bps) and spread_bps <= 5.0 else "degraded"
        )

    primary_event_id = str(detected_event.event_id or detected_event.event_family).strip().upper()
    compat_event_family = (
        str(detected_event.event_family or detected_event.event_id).strip().upper()
    )

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
    provided_episode_ids = [
        str(item or "").strip().upper()
        for item in market_features.get("active_episode_ids", [])
        if str(item or "").strip()
    ]
    active_episode_ids: list[str] = []
    for token in provided_episode_ids + [match.episode_id for match in episode_matches]:
        if token and token not in active_episode_ids:
            active_episode_ids.append(token)

    detector_input_status = market_features.get("detector_input_status", {})
    if not isinstance(detector_input_status, dict):
        detector_input_status = {}
    market_state_quality = {
        "market_state_complete": bool(market_features.get("market_state_complete", False)),
        "is_execution_tradable": bool(market_features.get("is_execution_tradable", False)),
        "non_tradable_reasons": list(market_features.get("non_tradable_reasons", []) or []),
        "ticker_fresh": bool(market_features.get("ticker_fresh", False)),
        "funding_fresh": bool(market_features.get("funding_fresh", False)),
        "open_interest_fresh": bool(market_features.get("open_interest_fresh", False)),
    }

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
        "activation_mode": str(detector_input_status.get("adapter", "heuristic_live_runtime")),
    }

    detector_contract = None
    try:
        detector_contract = get_detector_contract(primary_event_id or compat_event_family)
    except Exception:
        detector_contract = None

    threshold_snapshot = market_features.get(
        "threshold_snapshot",
        detected_event.features.get("threshold_snapshot", {}),
    )
    if not isinstance(threshold_snapshot, dict):
        threshold_snapshot = {}
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
        event_evidence_mode=(
            str(
                getattr(detector_contract, "evidence_mode", "")
                or market_features.get("event_evidence_mode", "")
            )
            .strip()
            .lower()
        ),
        event_role=(
            str(getattr(detector_contract, "role", "trigger")).strip().lower() or "trigger"
        ),
        threshold_snapshot=threshold_snapshot,
        detector_input_status=detector_input_status,
        live_features=dict(market_features),
        regime_snapshot=regime_snapshot | {"market_state_quality": market_state_quality},
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
