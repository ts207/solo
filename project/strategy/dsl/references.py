from __future__ import annotations

from project.events.registry import REGISTRY_BACKED_SIGNALS, _signal_ts_column

KNOWN_ENTRY_SIGNALS = {
    "event_detected",
    "vol_shock_relaxation_event",
    "liquidity_refill_lag_event",
    "liquidity_absence_event",
    "vol_aftershock_event",
    "forced_flow_exhaustion_event",
    "cross_venue_desync_event",
    "liquidity_vacuum_event",
    "funding_extreme_event",
    "range_compression_breakout_event",
    "regime_stability_pass",
    "refill_persistence_pass",
    "spread_guard_pass",
    "oos_validation_pass",
    "cross_venue_consensus_pass",
    "vacuum_refill_confirmation",
    "funding_normalization_pass",
    "breakout_confirmation",
}


def _active_signal_column(signal: str) -> str:
    return f"{signal.removesuffix('_event')}_active"


REGISTRY_SIGNAL_COLUMNS = set()
for signal in REGISTRY_BACKED_SIGNALS:
    REGISTRY_SIGNAL_COLUMNS.add(signal)
    REGISTRY_SIGNAL_COLUMNS.add(_active_signal_column(signal))
    REGISTRY_SIGNAL_COLUMNS.add(_signal_ts_column(signal))

MOMENTUM_BIAS_EVENTS = {
    "vol_shock",
    "cross_venue_desync",
    "oi_spike_positive",
}
CONTRARIAN_BIAS_EVENTS = {
    "liquidity_vacuum",
    "forced_flow_exhaustion",
    "funding_extreme_onset",
    "funding_persistence_trigger",
    "funding_normalization_trigger",
    "oi_spike_negative",
    "oi_flush",
    "liquidation_cascade",
}


def event_direction_bias(event_type: str) -> int:
    """Returns 1 for momentum, -1 for contrarian events."""
    normalized = str(event_type).strip().lower()
    if normalized in CONTRARIAN_BIAS_EVENTS:
        return -1
    if normalized in MOMENTUM_BIAS_EVENTS:
        return 1
    return 1
