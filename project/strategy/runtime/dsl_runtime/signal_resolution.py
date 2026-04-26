from __future__ import annotations

import pandas as pd

from project.strategy.dsl.contract_v1 import resolve_trigger_column
from project.strategy.dsl.references import REGISTRY_SIGNAL_COLUMNS
from project.strategy.dsl.schema import Blueprint


def first_overlay_param(blueprint: Blueprint, overlay_name: str, key: str, default: float) -> float:
    for overlay in blueprint.overlays:
        if overlay.name == overlay_name:
            try:
                return float(overlay.params.get(key, default))
            except (ValueError, TypeError):
                return default
    return default


def signal_mask(signal: str, frame: pd.DataFrame, blueprint: Blueprint) -> pd.Series:
    """
    Evaluates a specific named signal against the current context.
    """
    if signal in REGISTRY_SIGNAL_COLUMNS:
        if signal not in frame.columns:
            raise ValueError(f"Blueprint `{blueprint.id}` missing registry signal column: {signal}")
        return frame[signal].fillna(False).astype(bool)

    if signal == "spread_guard_pass":
        max_spread = first_overlay_param(blueprint, "spread_guard", "max_spread_bps", default=12.0)
        return (frame["spread_abs"] <= max_spread).fillna(False)
    if signal == "cross_venue_consensus_pass":
        max_desync = first_overlay_param(
            blueprint, "cross_venue_guard", "max_desync_bps", default=20.0
        )
        return (frame["spread_abs"] <= max_desync).fillna(False)

    if signal == "funding_normalization_pass":
        funding_available = frame.get("funding_rate_scaled_available")
        if funding_available is None or not funding_available.astype(bool).any():
            raise ValueError(
                f"Blueprint `{blueprint.id}` requires canonical funding_rate_scaled for "
                "funding_normalization_pass"
            )
        max_funding = first_overlay_param(
            blueprint, "funding_guard", "max_abs_funding_bps", default=15.0
        )
        return (frame["funding_bps_abs"] <= max_funding).fillna(False)

    if signal == "refill_persistence_pass":
        return (frame["volume_ratio"] >= 1.0).fillna(False)
    if signal == "vacuum_refill_confirmation":
        return (frame["volume_ratio"] >= 1.0).fillna(False)

    if signal == "regime_stability_pass":
        return (frame["vol_z"].abs() <= 1.0).fillna(False)

    if signal == "breakout_confirmation":
        return (frame["abs_ret_1"] >= frame["abs_ret_q75"]).fillna(False)

    if signal == "event_detected":
        if "event_detected" in frame.columns:
            return frame["event_detected"].fillna(False).astype(bool)
        # A missing event column means no event rows were joined into the frame.
        # Treat that as "no triggers fired", not as an unconditional pass.
        return pd.Series(False, index=frame.index, dtype=bool)

    raise ValueError(
        f"unknown trigger signals: `{signal}` is not a recognized registry or built-in signal"
    )


def signal_list_mask(
    frame: pd.DataFrame, signal_names: list[str], blueprint: Blueprint, signal_kind: str
) -> pd.Series:
    if not signal_names:
        return pd.Series(True, index=frame.index, dtype=bool)

    out = pd.Series(True, index=frame.index, dtype=bool)
    for signal in signal_names:
        out = out & signal_mask(signal=signal, frame=frame, blueprint=blueprint)
    return out.fillna(False)


def compute_trigger_coverage(frame: pd.DataFrame, triggers: list[str]) -> dict[str, object]:
    """
    Computes coverage statistics for entry triggers.
    """
    cols = list(getattr(frame, "columns", []))
    out = {"triggers": {}, "missing": [], "resolved": {}}
    for trig in triggers or []:
        resolved = resolve_trigger_column(str(trig), cols)
        out["resolved"][str(trig)] = resolved
        if resolved is None or resolved not in cols:
            out["missing"].append(str(trig))
            out["triggers"][str(trig)] = {
                "resolved": None,
                "true_count": 0,
                "true_rate": 0.0,
            }
            continue
        s = frame[resolved]
        # tolerate 0/1 ints or bools
        true_count = int((s.astype(bool)).sum())
        true_rate = float((s.astype(bool)).mean()) if len(s) else 0.0
        out["triggers"][str(trig)] = {
            "resolved": resolved,
            "true_count": true_count,
            "true_rate": true_rate,
        }
    out["all_zero"] = (
        all(v.get("true_count", 0) == 0 for v in out["triggers"].values())
        if out["triggers"]
        else True
    )
    return out
