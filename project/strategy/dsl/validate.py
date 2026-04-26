from __future__ import annotations

import pandas as pd

from project.strategy.dsl.references import REGISTRY_SIGNAL_COLUMNS
from project.strategy.dsl.schema import OverlaySpec


def validate_signal_columns(merged: pd.DataFrame, signals: list[str], blueprint_id: str) -> None:
    """
    Validates that the merged frame has all required columns for the entry signals.
    """
    cols = set(merged.columns)
    missing_by_signal: dict[str, list[str]] = {}

    def _has_numeric_values(column: str) -> bool:
        if column not in cols:
            return False
        series = pd.to_numeric(merged[column], errors="coerce")
        return bool(series.notna().any())

    for signal in signals:
        missing: list[str] = []
        if signal in REGISTRY_SIGNAL_COLUMNS:
            if signal not in cols:
                missing.append(signal)
            if missing:
                missing_by_signal[signal] = sorted(set(missing))
            continue
        if signal in {
            "spread_guard_pass",
            "cross_venue_desync_event",
            "cross_venue_consensus_pass",
        } and not _has_numeric_values("spread_bps"):
            missing.append("spread_bps")
        if signal in {
            "funding_extreme_event",
            "funding_normalization_pass",
        } and not _has_numeric_values("funding_rate_scaled"):
            missing.append("funding_rate_scaled")
        if signal in {
            "liquidity_absence_event",
            "liquidity_refill_lag_event",
            "refill_persistence_pass",
            "liquidity_vacuum_event",
        } and not _has_numeric_values("quote_volume"):
            missing.append("quote_volume")
        if signal in {
            "forced_flow_exhaustion_event",
            "liquidity_vacuum_event",
            "breakout_confirmation",
            "range_compression_breakout_event",
        } and not _has_numeric_values("close"):
            missing.append("close")
        if signal in {
            "vol_aftershock_event",
            "vol_shock_relaxation_event",
            "regime_stability_pass",
            "range_compression_breakout_event",
        }:
            has_range = (
                _has_numeric_values("range_96")
                or (_has_numeric_values("high_96") and _has_numeric_values("low_96"))
                or _has_numeric_values("range_ratio")
            )
            if not has_range:
                missing.append("range_96 or (high_96+low_96)")
            if not _has_numeric_values("close") and "close" not in missing:
                missing.append("close")
        if missing:
            missing_by_signal[signal] = sorted(set(missing))

    if missing_by_signal:
        detail = "; ".join(
            f"{name}: {', '.join(cols)}" for name, cols in sorted(missing_by_signal.items())
        )
        raise ValueError(
            f"Blueprint `{blueprint_id}` missing required columns for entry signals -> {detail}"
        )


def validate_overlay_columns(
    frame: pd.DataFrame, overlays: list[OverlaySpec], blueprint_id: str
) -> None:
    """
    Validates that the frame has all required columns for the overlays.
    """
    cols = set(frame.columns)

    def _has_numeric_values(column: str) -> bool:
        if column not in cols:
            return False
        series = pd.to_numeric(frame[column], errors="coerce")
        return bool(series.notna().any())

    missing: dict[str, list[str]] = {}
    for overlay in overlays:
        required: list[str] = []
        if overlay.name == "liquidity_guard":
            required = ["quote_volume"]
        elif overlay.name == "spread_guard":
            required = ["spread_bps"]
        elif overlay.name == "funding_guard":
            required = ["funding_rate_scaled"]
        elif overlay.name == "cross_venue_guard":
            required = ["spread_bps"]
        elif overlay.name in {"risk_throttle", "session_guard"}:
            required = []
        else:
            required = []
        missing_cols = [col for col in required if not _has_numeric_values(col)]
        if missing_cols:
            missing[overlay.name] = sorted(set(missing_cols))

    if missing:
        detail = "; ".join(f"{name}: {', '.join(cols_)}" for name, cols_ in sorted(missing.items()))
        raise ValueError(
            f"Blueprint `{blueprint_id}` missing required columns for overlays -> {detail}"
        )
