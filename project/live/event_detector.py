from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pandas as pd

from project.core.coercion import safe_float
from project.domain.compiled_registry import get_domain_registry
from project.events.detector_contract import DetectorContract
from project.events.data_capabilities import load_data_capability_profile
from project.events.detectors.registry import get_detector_metadata_adapter_class
from project.events.registry import get_detector_contract, list_runtime_eligible_detectors
from project.live.context_builder import (
    build_runtime_core_detector_input_surface,
    enrich_runtime_core_detector_history,
)
from project.live.policy import normalize_live_event_detector_config

_LOG = logging.getLogger(__name__)

_DEFAULT_SUPPORTED_EVENT_IDS: tuple[str, ...] = ()


def _governed_runtime_core_event_ids() -> frozenset[str]:
    """Return detector IDs that may run in governed runtime detection.

    Runtime detectable is intentionally different from trade eligible: context
    detectors must run so the composite thesis layer can see their evidence,
    while non-trade-eligible raw events remain hard-blocked downstream.
    """
    try:
        return frozenset(contract.event_name for contract in list_runtime_eligible_detectors())
    except Exception:
        return frozenset({
            "LIQUIDITY_VACUUM", "LIQUIDITY_VACUUM_RECOVERY",
            "OI_SPIKE_NEGATIVE", "OI_EXPANSION_STRESS", "OI_FLUSH",
            "FUNDING_EXTREME_ONSET", "FUNDING_POS_EXTREME_ONSET", "FUNDING_NEG_EXTREME_ONSET",
            "VOL_SHOCK", "VOL_SPIKE",
        })


@dataclass(frozen=True)
class DetectedEvent:
    event_id: str
    event_family: str
    canonical_regime: str
    event_side: str
    features: dict[str, Any]
    event_confidence: float | None = None
    event_severity: float | None = None
    data_quality_flag: str = "ok"
    trade_eligible: bool = True
    event_version: str = "v2"
    threshold_version: str = "2.0"


class LiveEventDetectionAdapter(ABC):
    adapter_id = "base"

    @abstractmethod
    def detect_events(
        self,
        *,
        symbol: str,
        timeframe: str,
        current_close: float,
        previous_close: float | None,
        volume: float | None = None,
        market_features: Mapping[str, Any] | None = None,
        supported_event_ids: list[str] | None = None,
        supported_event_families: list[str] | None = None,
    ) -> list[DetectedEvent]:
        raise NotImplementedError


def _build_detected_event(
    *,
    event_id: str,
    event_side: str,
    features: Mapping[str, Any],
    event_confidence: float | None = None,
    event_severity: float | None = None,
    data_quality_flag: str = "ok",
    trade_eligible: bool = True,
    event_version: str = "v2",
    threshold_version: str = "2.0",
    event_family: str | None = None,
    canonical_regime: str | None = None,
) -> DetectedEvent:
    normalized_event_id = str(event_id).strip().upper()
    spec = get_domain_registry().get_event(normalized_event_id)
    resolved_regime = str(canonical_regime or "").strip().upper()
    if not resolved_regime and spec is not None:
        resolved_regime = str(spec.canonical_regime or spec.canonical_family).strip().upper()
    compat_family = str(event_family or normalized_event_id).strip().upper() or normalized_event_id
    return DetectedEvent(
        event_id=normalized_event_id,
        event_family=compat_family,
        canonical_regime=resolved_regime,
        event_side=str(event_side).strip().lower() or "conditional",
        features=dict(features),
        event_confidence=event_confidence,
        event_severity=event_severity,
        data_quality_flag=str(data_quality_flag or "ok").strip().lower() or "ok",
        trade_eligible=bool(trade_eligible),
        event_version=event_version,
        threshold_version=threshold_version,
    )


def _move_bps(current_close: float, previous_close: float | None) -> float | None:
    if previous_close is None or previous_close <= 0.0:
        return None
    return ((float(current_close) / float(previous_close)) - 1.0) * 10_000.0


def _threshold_version(config: Mapping[str, Any], default: str = "2.0") -> str:
    return str(config.get("threshold_version", default) or default)


def _normalize_supported_event_ids(
    supported_event_ids: Sequence[str] | None,
    supported_event_families: Sequence[str] | None,
) -> list[str]:
    configured_supported = supported_event_ids if supported_event_ids is not None else supported_event_families
    if configured_supported is None:
        configured_supported = _DEFAULT_SUPPORTED_EVENT_IDS
    return [str(item).strip().upper() for item in list(configured_supported or ()) if str(item).strip()]


def _infer_event_side_from_row(row: Mapping[str, Any]) -> str:
    candidates = (
        safe_float(row.get("move_bps"), 0.0),
        safe_float(row.get("basis_bps"), 0.0),
        safe_float(row.get("funding_rate_scaled"), 0.0),
        safe_float(row.get("funding_rate"), 0.0),
        safe_float(row.get("oi_delta_1h"), 0.0),
    )
    for value in candidates:
        if value > 0.0:
            return "long"
        if value < 0.0:
            return "short"
    return "conditional"


def _detect_vol_shock(
    *,
    symbol: str,
    timeframe: str,
    move_bps: float | None,
    volume: float,
    config: Mapping[str, Any],
) -> DetectedEvent | None:
    if move_bps is None:
        return None
    min_abs_move_bps = float(config.get("vol_shock_min_abs_move_bps", 35.0) or 35.0)
    if abs(move_bps) < min_abs_move_bps:
        return None
    event_side = "long" if move_bps > 0.0 else "short"
    severity = min(1.0, abs(float(move_bps)) / max(min_abs_move_bps * 2.0, 1.0))
    confidence = min(1.0, 0.55 + abs(float(move_bps)) / max(min_abs_move_bps * 4.0, 1.0))
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
        event_confidence=confidence,
        event_severity=severity,
        threshold_version=_threshold_version(config),
    )


def _detect_vol_spike(
    *,
    symbol: str,
    timeframe: str,
    move_bps: float | None,
    volume: float,
    config: Mapping[str, Any],
) -> DetectedEvent | None:
    if move_bps is None:
        return None
    min_abs_move_bps = float(config.get("vol_spike_min_abs_move_bps", 55.0) or 55.0)
    min_volume = float(config.get("vol_spike_min_volume", 50_000.0) or 50_000.0)
    if abs(move_bps) < min_abs_move_bps or volume < min_volume:
        return None
    event_side = "long" if move_bps > 0.0 else "short"
    confidence = min(1.0, 0.6 + (float(volume) / max(min_volume * 4.0, 1.0)))
    severity = min(1.0, abs(float(move_bps)) / max(min_abs_move_bps * 1.5, 1.0))
    return _build_detected_event(
        event_id="VOL_SPIKE",
        event_side=event_side,
        features={
            "symbol": str(symbol).upper(),
            "timeframe": str(timeframe),
            "move_bps": float(move_bps),
            "volume": safe_float(volume, 0.0),
            "detector_strength": min(
                3.0,
                abs(float(move_bps)) / max(min_abs_move_bps, 1.0)
                + (float(volume) / max(min_volume, 1.0)),
            ),
        },
        event_confidence=confidence,
        event_severity=severity,
        threshold_version=_threshold_version(config),
    )


def _detect_liquidity_vacuum(
    *,
    symbol: str,
    timeframe: str,
    move_bps: float | None,
    market_features: Mapping[str, Any],
    config: Mapping[str, Any],
) -> DetectedEvent | None:
    spread_bps = float(market_features.get("spread_bps", 0.0) or 0.0)
    depth_usd = float(market_features.get("depth_usd", 0.0) or 0.0)
    min_spread_bps = float(config.get("liquidity_vacuum_min_spread_bps", 5.0) or 5.0)
    max_depth_usd = float(config.get("liquidity_vacuum_max_depth_usd", 25_000.0) or 25_000.0)
    if spread_bps < min_spread_bps or depth_usd > max_depth_usd:
        return None
    signed_move = float(move_bps or 0.0)
    event_side = "long" if signed_move > 0.0 else "short" if signed_move < 0.0 else "conditional"
    confidence = 0.7 if depth_usd > 0 else 0.45
    severity = min(1.0, (spread_bps / max(min_spread_bps, 1.0)) / 4.0)
    data_quality_flag = "ok" if depth_usd > 0 else "degraded"
    return _build_detected_event(
        event_id="LIQUIDITY_VACUUM",
        event_side=event_side,
        features={
            "symbol": str(symbol).upper(),
            "timeframe": str(timeframe),
            "move_bps": float(signed_move),
            "spread_bps": spread_bps,
            "depth_usd": depth_usd,
            "detector_strength": (spread_bps / max(min_spread_bps, 1.0))
            + (max_depth_usd / max(depth_usd, 1.0)),
        },
        event_confidence=confidence,
        event_severity=severity,
        data_quality_flag=data_quality_flag,
        threshold_version=_threshold_version(config),
    )


def _detect_liquidation_cascade(
    *,
    symbol: str,
    timeframe: str,
    move_bps: float | None,
    market_features: Mapping[str, Any],
    config: Mapping[str, Any],
) -> DetectedEvent | None:
    signed_move = float(move_bps or 0.0)
    oi_delta_fraction = float(
        market_features.get(
            "open_interest_delta_fraction",
            market_features.get("oi_delta_fraction", 0.0),
        )
        or 0.0
    )
    funding_rate = float(market_features.get("funding_rate", 0.0) or 0.0)
    min_abs_move_bps = float(config.get("liquidation_cascade_min_abs_move_bps", 80.0) or 80.0)
    min_abs_oi_drop_fraction = float(
        config.get("liquidation_cascade_min_abs_oi_drop_fraction", 0.03) or 0.03
    )
    min_abs_funding_rate = float(
        config.get("liquidation_cascade_min_abs_funding_rate", 0.0005) or 0.0005
    )
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
        event_confidence=min(
            1.0, 0.7 + abs(oi_delta_fraction) / max(min_abs_oi_drop_fraction * 4.0, 1e-9)
        ),
        event_severity=min(1.0, detector_strength / 6.0),
        threshold_version=_threshold_version(config),
    )


class HeuristicLiveEventDetectionAdapter(LiveEventDetectionAdapter):
    adapter_id = "heuristic"

    def __init__(self, detector_config: Mapping[str, Any] | None = None) -> None:
        self._config = dict(detector_config or {})
        self._data_capability_profile = load_data_capability_profile(self._config.get("data_capability_profile"))

    def detect_events(
        self,
        *,
        symbol: str,
        timeframe: str,
        current_close: float,
        previous_close: float | None,
        volume: float | None = None,
        market_features: Mapping[str, Any] | None = None,
        supported_event_ids: list[str] | None = None,
        supported_event_families: list[str] | None = None,
    ) -> list[DetectedEvent]:
        supported = _normalize_supported_event_ids(supported_event_ids, supported_event_families)
        features = dict(market_features or {})
        move_bps = _move_bps(float(current_close), previous_close)
        vol = float(volume or 0.0)
        detectors = {
            "VOL_SHOCK": lambda: _detect_vol_shock(
                symbol=symbol,
                timeframe=timeframe,
                move_bps=move_bps,
                volume=vol,
                config=self._config,
            ),
            "VOL_SPIKE": lambda: _detect_vol_spike(
                symbol=symbol,
                timeframe=timeframe,
                move_bps=move_bps,
                volume=vol,
                config=self._config,
            ),
            "LIQUIDITY_VACUUM": lambda: _detect_liquidity_vacuum(
                symbol=symbol,
                timeframe=timeframe,
                move_bps=move_bps,
                market_features=features,
                config=self._config,
            ),
            "LIQUIDATION_CASCADE": lambda: _detect_liquidation_cascade(
                symbol=symbol,
                timeframe=timeframe,
                move_bps=move_bps,
                market_features=features,
                config=self._config,
            ),
        }
        detected: list[DetectedEvent] = []
        for family in supported:
            if self._data_capability_profile.detector_disabled(family):
                continue
            detector = detectors.get(family)
            if detector is None:
                continue
            result = detector()
            if result is not None:
                detected.append(result)
        return detected


class GovernedRuntimeCoreEventDetectionAdapter(LiveEventDetectionAdapter):
    adapter_id = "governed_runtime_core"

    def __init__(self, detector_config: Mapping[str, Any] | None = None) -> None:
        self._config = dict(detector_config or {})
        self._data_capability_profile = load_data_capability_profile(self._config.get("data_capability_profile"))
        self._history_limit = max(128, int(self._config.get("history_limit_bars", 4096) or 4096))
        self._history_by_key: dict[tuple[str, str], deque[dict[str, Any]]] = {}
        self._event_history_by_key: dict[tuple[str, str], deque[DetectedEvent]] = {}
        self._warned_missing_inputs: set[tuple[str, str]] = set()

    def detect_events(
        self,
        *,
        symbol: str,
        timeframe: str,
        current_close: float,
        previous_close: float | None,
        volume: float | None = None,
        market_features: Mapping[str, Any] | None = None,
        supported_event_ids: list[str] | None = None,
        supported_event_families: list[str] | None = None,
    ) -> list[DetectedEvent]:
        selected_contracts = self._selected_contracts(
            supported_event_ids=supported_event_ids,
            supported_event_families=supported_event_families,
        )
        if not selected_contracts:
            return []

        input_surface = build_runtime_core_detector_input_surface(
            symbol=symbol,
            timeframe=timeframe,
            current_close=current_close,
            previous_close=previous_close,
            volume=volume,
            market_features=market_features or {},
            supported_event_ids=[contract.event_name for contract in selected_contracts],
        )
        row = input_surface.row
        history = self._append_and_materialize_history(
            symbol=symbol,
            timeframe=timeframe,
            row=row,
        )
        if history.empty:
            return []

        latest_ts = history["timestamp"].iloc[-1]
        detected: list[DetectedEvent] = []
        for contract in selected_contracts:
            detector_cls = get_detector_metadata_adapter_class(contract.event_name)
            if detector_cls is None:
                continue
            event_status = input_surface.detector_input_status.get("per_event", {}).get(
                contract.event_name, {}
            )
            missing = list(event_status.get("missing_inputs", []))
            if missing:
                self._warn_missing_inputs(contract, missing)
                continue
            detector = detector_cls()
            params = dict(self._config)
            params.setdefault("symbol", str(symbol).upper())
            params.setdefault("timeframe", str(timeframe))
            try:
                event_frame = detector.detect_events(history.copy(), params)
            except Exception as exc:
                _LOG.warning(
                    "Governed live event detection failed for %s on %s %s: %s",
                    contract.event_name,
                    str(symbol).upper(),
                    str(timeframe),
                    exc,
                )
                continue
            if event_frame.empty:
                continue
            frame = event_frame.copy()
            for column in ("ts_start", "ts_end"):
                frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
            current_rows = frame[(frame["ts_start"] == latest_ts) | (frame["ts_end"] == latest_ts)]
            if current_rows.empty:
                continue
            for _, event_row in current_rows.iterrows():
                detected.append(
                    self._normalize_governed_event(
                        contract=contract,
                        event_row=event_row,
                        row=row,
                        detector_input_status=input_surface.detector_input_status,
                    )
                )
        detected.extend(self._build_composite_events(symbol=symbol, timeframe=timeframe, current_events=detected, row=row))
        return detected

    def _selected_contracts(
        self,
        *,
        supported_event_ids: Sequence[str] | None,
        supported_event_families: Sequence[str] | None,
    ) -> list[DetectorContract]:
        selected: list[DetectorContract] = []
        eligible = _governed_runtime_core_event_ids()
        requested = _normalize_supported_event_ids(supported_event_ids, supported_event_families)
        profile_runtime = set(getattr(self._data_capability_profile, "runtime_detectable_detectors", frozenset()) or frozenset())
        candidates = requested or sorted(profile_runtime or eligible)
        for event_id in candidates:
            canonical = str(event_id).strip().upper()
            if canonical not in eligible and canonical not in profile_runtime:
                continue
            if self._data_capability_profile.detector_disabled(canonical):
                continue
            contract = get_detector_contract(canonical)
            if not contract.runtime_default and canonical not in profile_runtime:
                continue
            selected.append(contract)
        return selected

    def _append_and_materialize_history(
        self,
        *,
        symbol: str,
        timeframe: str,
        row: Mapping[str, Any],
    ) -> pd.DataFrame:
        key = (str(symbol).upper(), str(timeframe))
        history = self._history_by_key.setdefault(key, deque(maxlen=self._history_limit))
        timestamp = row["timestamp"]
        if history and history[-1].get("timestamp") == timestamp:
            history[-1] = dict(row)
        else:
            history.append(dict(row))

        frame = pd.DataFrame(list(history))
        if frame.empty:
            return frame
        frame = frame.sort_values("timestamp").reset_index(drop=True)
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
        frame = enrich_runtime_core_detector_history(frame, timeframe=timeframe)
        return frame


    def _build_composite_events(
        self,
        *,
        symbol: str,
        timeframe: str,
        current_events: Sequence[DetectedEvent],
        row: Mapping[str, Any],
    ) -> list[DetectedEvent]:
        key = (str(symbol).upper(), str(timeframe))
        history = self._event_history_by_key.setdefault(key, deque(maxlen=max(32, int(self._config.get("composite_event_history_limit", 256) or 256))))
        for event in current_events:
            history.append(event)
        specs = dict(getattr(self._data_capability_profile, "composite_theses", {}) or {})
        if not specs or not history or not current_events:
            return []
        recent = list(history)[-int(self._config.get("composite_lookback_events", 64) or 64):]
        current_ids = {event.event_id for event in current_events}
        def has_event(event_id: str) -> bool:
            return any(ev.event_id == str(event_id).strip().upper() for ev in recent)
        def has_any(group: object) -> bool:
            return any(has_event(str(item)) for item in list(group or []))
        def group_rules_pass(groups: object) -> bool:
            return True if not groups else all(has_any(group) for group in list(groups or []))
        def event_subtype(ev: DetectedEvent) -> str:
            meta = ev.features.get("detector_metadata", {})
            if not isinstance(meta, Mapping): meta = {}
            return str(meta.get("positioning_subtype") or meta.get("flush_subtype") or ev.features.get("positioning_subtype") or ev.features.get("flush_subtype") or "").strip().lower()
        def subtype_pass(spec: Mapping[str, Any]) -> bool:
            exact = str(spec.get("required_subtype") or "").strip().lower()
            any_of = {str(item).strip().lower() for item in list(spec.get("required_subtype_any") or []) if str(item).strip()}
            if not exact and not any_of: return True
            for ev in recent:
                subtype = event_subtype(ev)
                if exact and subtype == exact: return True
                if any_of and subtype in any_of: return True
            return False
        def required_feeds_pass(spec: Mapping[str, Any]) -> bool:
            required = [str(item).strip() for item in list(spec.get("requires_feeds") or []) if str(item).strip()]
            if not required:
                return True
            return all(self._data_capability_profile.feed_available(feed) and pd.notna(row.get(feed)) for feed in required)
        active_liquidity_vacuum = any(ev.event_id == "LIQUIDITY_VACUUM" for ev in recent[-12:])
        composites: list[DetectedEvent] = []
        for thesis_id, raw_spec in specs.items():
            spec = dict(raw_spec or {})
            thesis_token = str(thesis_id).strip().upper()
            required_all = [str(item).strip().upper() for item in list(spec.get("required_all") or []) if str(item).strip()]
            if required_all and not all(has_event(item) for item in required_all): continue
            if not group_rules_pass(spec.get("required_any")): continue
            if not group_rules_pass(spec.get("confirm_any")): continue
            if not subtype_pass(spec): continue
            if not required_feeds_pass(spec): continue
            if str(spec.get("execution_filter") or "").strip().lower() == "no_active_liquidity_vacuum" and active_liquidity_vacuum: continue
            trade_candidate = self._data_capability_profile.trade_candidate(thesis_token)
            related = set(required_all)
            for group_name in ("required_any", "confirm_any"):
                for group in list(spec.get(group_name) or []):
                    related.update(str(item).strip().upper() for item in list(group or []) if str(item).strip())
            if related and not (current_ids & related): continue
            evidence = [ev.event_id for ev in recent if not related or ev.event_id in related]
            latest_side = next((ev.event_side for ev in reversed(recent) if not related or ev.event_id in related), "conditional")
            composites.append(_build_detected_event(
                event_id=thesis_token, event_family="COMPOSITE_THESIS", canonical_regime="COMPOSITE_THESIS", event_side=latest_side,
                features={"symbol": str(symbol).upper(), "timeframe": str(timeframe), "data_capability_profile": self._data_capability_profile.name,
                          "composite_thesis": thesis_token, "evidence_events": evidence[-16:],
                          "triggered_by_current_events": sorted(current_ids), "trade_eligible": trade_candidate,
                          "runtime_role": "trade_candidate" if trade_candidate else "evidence",
                          "detector_metadata": {"composite_router": "data_capability_profile_v1", "trade_eligible": trade_candidate}},
                event_confidence=min(1.0, 0.55 + 0.08 * len(set(evidence))), event_severity=min(1.0, 0.45 + 0.06 * len(evidence)),
                data_quality_flag="ok", trade_eligible=trade_candidate, event_version="composite_v1", threshold_version="profile_composite_v1"))
        return composites

    def _normalize_governed_event(
        self,
        *,
        contract: DetectorContract,
        event_row: Mapping[str, Any],
        row: Mapping[str, Any],
        detector_input_status: Mapping[str, Any],
    ) -> DetectedEvent:
        threshold_snapshot = event_row.get("threshold_snapshot", {})
        if not isinstance(threshold_snapshot, dict):
            threshold_snapshot = {}
        source_features = event_row.get("source_features", {})
        if not isinstance(source_features, dict):
            source_features = {}
        detector_metadata = event_row.get("detector_metadata", {})
        if not isinstance(detector_metadata, dict):
            detector_metadata = {}
        features = dict(row)
        features.update(source_features)
        profile_trade_eligible = self._data_capability_profile.trade_candidate(contract.event_name)
        row_trade_eligible = bool(event_row.get("trade_eligible", True))
        features["trade_eligible"] = bool(profile_trade_eligible and row_trade_eligible)
        features["runtime_role"] = "trade_candidate" if features["trade_eligible"] else "evidence"
        features["data_capability_profile"] = self._data_capability_profile.name
        features["detector_metadata"] = detector_metadata
        features["threshold_snapshot"] = threshold_snapshot
        features["detector_input_status"] = dict(detector_input_status)
        features["required_context_present"] = bool(
            event_row.get("required_context_present", True)
        )
        return _build_detected_event(
            event_id=contract.event_name,
            event_family=contract.event_name,
            canonical_regime=contract.canonical_family,
            event_side=_infer_event_side_from_row(features),
            features=features,
            event_confidence=safe_float(event_row.get("confidence"), 0.0)
            if contract.supports_confidence
            else None,
            event_severity=safe_float(event_row.get("severity"), 0.0)
            if contract.supports_severity
            else None,
            data_quality_flag=(
                str(event_row.get("data_quality_flag", "ok")).strip().lower()
                if contract.supports_quality_flag
                else "ok"
            ),
            trade_eligible=bool(features.get("trade_eligible", True)),
            event_version=contract.event_version,
            threshold_version=str(
                threshold_snapshot.get("version", contract.threshold_schema_version)
                or contract.threshold_schema_version
            ),
        )

    def _warn_missing_inputs(self, contract: DetectorContract, missing: Sequence[str]) -> None:
        key = (contract.event_name, ",".join(sorted(missing)))
        if key in self._warned_missing_inputs:
            return
        self._warned_missing_inputs.add(key)
        _LOG.warning(
            "Governed live detector %s skipped: missing required live inputs %s",
            contract.event_name,
            sorted(missing),
        )


def build_live_event_detection_adapter(
    detector_config: Mapping[str, Any] | None = None,
) -> LiveEventDetectionAdapter:
    config = normalize_live_event_detector_config(detector_config)
    adapter = str(config.get("adapter", "governed_runtime_core")).strip().lower()
    if adapter == "governed_runtime_core":
        return GovernedRuntimeCoreEventDetectionAdapter(config)
    return HeuristicLiveEventDetectionAdapter(config)


def detect_live_events(
    *,
    symbol: str,
    timeframe: str,
    current_close: float,
    previous_close: float | None,
    volume: float | None = None,
    market_features: Mapping[str, Any] | None = None,
    supported_event_ids: list[str] | None = None,
    supported_event_families: list[str] | None = None,
    detector_config: Mapping[str, Any] | None = None,
) -> list[DetectedEvent]:
    adapter = build_live_event_detection_adapter(detector_config)
    return adapter.detect_events(
        symbol=symbol,
        timeframe=timeframe,
        current_close=current_close,
        previous_close=previous_close,
        volume=volume,
        market_features=market_features,
        supported_event_ids=supported_event_ids,
        supported_event_families=supported_event_families,
    )


def detect_live_event(
    *,
    symbol: str,
    timeframe: str,
    current_close: float,
    previous_close: float | None,
    volume: float | None = None,
    market_features: Mapping[str, Any] | None = None,
    supported_event_ids: list[str] | None = None,
    supported_event_families: list[str] | None = None,
    detector_config: Mapping[str, Any] | None = None,
) -> DetectedEvent | None:
    compat_config = dict(detector_config or {})
    events = detect_live_events(
        symbol=symbol,
        timeframe=timeframe,
        current_close=current_close,
        previous_close=previous_close,
        volume=volume,
        market_features=market_features,
        supported_event_ids=supported_event_ids,
        supported_event_families=supported_event_families,
        detector_config=compat_config,
    )
    return events[0] if events else None
