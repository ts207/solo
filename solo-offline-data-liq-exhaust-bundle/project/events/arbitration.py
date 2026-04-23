"""
Event arbitration layer.

Applies suppression rules (from spec/events/compatibility.yaml) and
precedence ordering (from spec/events/precedence.yaml) to a merged event frame.

Returns ArbitrationResult:
  events    -- surviving events, potentially with adjusted tradeability scores
  suppressed -- hard-blocked events with suppression reason attached
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from project.domain.compiled_registry import get_domain_registry
from project.events.shared import format_event_id
from project.spec_registry import load_yaml_relative

_SPEC_DIR = Path(__file__).resolve().parents[2] / "spec" / "events"


def load_compatibility_spec() -> Dict[str, Any]:
    base = load_yaml_relative("spec/events/compatibility.yaml")
    suppression_rules = _merged_suppression_rules(base)
    return {
        **dict(base),
        "suppression_rules": suppression_rules,
        "composite_events": list(base.get("composite_events", []) or []),
    }


def load_precedence_spec() -> Dict[str, Any]:
    base = load_yaml_relative("spec/events/precedence.yaml")
    return {
        **dict(base),
        "family_precedence": list(base.get("family_precedence", []) or []),
        "event_overrides": _merged_precedence_overrides(base),
    }


@dataclass
class ArbitrationResult:
    events: pd.DataFrame
    suppressed: pd.DataFrame = field(default_factory=pd.DataFrame)
    composite_events: pd.DataFrame = field(default_factory=pd.DataFrame)


def _event_local_reason(active_type: str, target_type: str) -> str:
    return (
        f"Event-local arbitration policy: {active_type} suppresses {target_type}."
    )


def _normalize_local_relation_entry(entry: Any) -> Dict[str, Any]:
    if isinstance(entry, str):
        token = str(entry).strip().upper()
        return {"event_type": token} if token else {}
    if isinstance(entry, dict):
        return dict(entry)
    return {}


def _local_suppression_pairs() -> Dict[tuple[str, str], Dict[str, Any]]:
    registry = get_domain_registry()
    pairs: Dict[tuple[str, str], Dict[str, Any]] = {}
    for event in registry.event_definitions.values():
        target_event = event.event_type
        for entry in event.suppressed_by:
            row = _normalize_local_relation_entry(entry)
            active_type = str(row.get("event_type", row.get("when_active", ""))).strip().upper()
            if not active_type:
                continue
            pairs[(active_type, target_event)] = {
                "when_active": active_type,
                "suppress": [target_event],
                "penalty_factor": float(row.get("penalty_factor", 0.5)),
                "block": bool(row.get("block", False)),
                "reason": str(row.get("reason", "")).strip() or _event_local_reason(active_type, target_event),
            }
        for entry in event.suppresses:
            row = _normalize_local_relation_entry(entry)
            suppress_type = str(row.get("event_type", "")).strip().upper()
            if not suppress_type:
                continue
            pairs[(target_event, suppress_type)] = {
                "when_active": target_event,
                "suppress": [suppress_type],
                "penalty_factor": float(row.get("penalty_factor", 0.5)),
                "block": bool(row.get("block", False)),
                "reason": str(row.get("reason", "")).strip() or _event_local_reason(target_event, suppress_type),
            }
    return pairs


def _merged_suppression_rules(base: Dict[str, Any]) -> list[Dict[str, Any]]:
    pair_map: Dict[tuple[str, str], Dict[str, Any]] = {}
    for rule in list(base.get("suppression_rules", []) or []):
        if not isinstance(rule, dict):
            continue
        active_type = str(rule.get("when_active", "")).strip().upper()
        if not active_type:
            continue
        penalty = float(rule.get("penalty_factor", 0.5))
        block = bool(rule.get("block", False))
        reason = str(rule.get("reason", "")).strip()
        for suppress_type in rule.get("suppress", []) or []:
            target = str(suppress_type).strip().upper()
            if not target:
                continue
            pair_map[(active_type, target)] = {
                "when_active": active_type,
                "suppress": [target],
                "penalty_factor": penalty,
                "block": block,
                "reason": reason,
            }
    pair_map.update(_local_suppression_pairs())

    grouped: Dict[tuple[str, float, bool, str], set[str]] = {}
    for payload in pair_map.values():
        key = (
            str(payload["when_active"]).strip().upper(),
            float(payload["penalty_factor"]),
            bool(payload["block"]),
            str(payload["reason"]).strip(),
        )
        grouped.setdefault(key, set()).update(
            str(item).strip().upper() for item in payload.get("suppress", []) if str(item).strip()
        )
    rules: list[Dict[str, Any]] = []
    for when_active, penalty, block, reason in sorted(grouped.keys()):
        rules.append(
            {
                "when_active": when_active,
                "suppress": sorted(grouped[(when_active, penalty, block, reason)]),
                "penalty_factor": penalty,
                "block": block,
                "reason": reason,
            }
        )
    return rules


def _merged_precedence_overrides(base: Dict[str, Any]) -> list[Dict[str, Any]]:
    override_map: Dict[str, Dict[str, Any]] = {}
    for row in list(base.get("event_overrides", []) or []):
        if not isinstance(row, dict):
            continue
        event_type = str(row.get("event_type", "")).strip().upper()
        if event_type:
            override_map[event_type] = dict(row)

    registry = get_domain_registry()
    for event in registry.event_definitions.values():
        if event.precedence_rank <= 0:
            continue
        reason = str(event.raw.get("precedence_reason", "")).strip() or (
            "Event-local precedence declared in the event spec."
        )
        override_map[event.event_type] = {
            "event_type": event.event_type,
            "override_priority": int(event.precedence_rank),
            "rationale": reason,
        }
    return [override_map[event_type] for event_type in sorted(override_map)]


def _events_overlap(df: pd.DataFrame, active_type: str, target_type: str, symbol: str) -> bool:
    """True if any active_type event temporally overlaps any target_type event."""
    active = df[(df["event_type"] == active_type) & (df["symbol"] == symbol)]
    target = df[(df["event_type"] == target_type) & (df["symbol"] == symbol)]
    if active.empty or target.empty:
        return False
    for _, a in active.iterrows():
        a_enter = a.get("enter_ts", a["timestamp"])
        a_exit = a.get("exit_ts", a["timestamp"])
        for _, t in target.iterrows():
            if a_enter <= t["timestamp"] <= a_exit:
                return True
    return False


def arbitrate_events(
    df: pd.DataFrame,
    compat_spec: Dict[str, Any] | None = None,
    prec_spec: Dict[str, Any] | None = None,
) -> ArbitrationResult:
    """
    Apply suppression rules and precedence ordering to an event frame.

    Parameters
    ----------
    df : pd.DataFrame
        Event frame with: event_type, symbol, timestamp, enter_ts, exit_ts,
        event_tradeability_score.
    compat_spec, prec_spec : dict, optional
        Pre-loaded specs; loaded from file if None.

    Returns
    -------
    ArbitrationResult
    """
    if df.empty:
        return ArbitrationResult(events=df.copy(), suppressed=pd.DataFrame())

    if compat_spec is None:
        try:
            compat_spec = load_compatibility_spec()
        except Exception as e:
            warnings.warn(f"Cannot load compatibility spec: {e}; skipping arbitration")
            return ArbitrationResult(events=df.copy())

    if prec_spec is None:
        try:
            prec_spec = load_precedence_spec()
        except Exception as e:
            warnings.warn(f"Cannot load precedence spec: {e}; skipping precedence sort")
            prec_spec = {"family_precedence": [], "event_overrides": []}

    out = df.copy()
    suppressed_rows: List[pd.DataFrame] = []
    symbols = out["symbol"].unique() if "symbol" in out.columns else []

    for rule in compat_spec.get("suppression_rules", []):
        active_type = rule["when_active"]
        suppress_types = rule["suppress"]
        penalty = float(rule.get("penalty_factor", 0.5))
        hard_block = bool(rule.get("block", False))
        reason = rule.get("reason", "")

        for sym in symbols:
            for suppress_type in suppress_types:
                if not _events_overlap(out, active_type, suppress_type, sym):
                    continue
                mask = (out["event_type"] == suppress_type) & (out["symbol"] == sym)
                if not mask.any():
                    continue
                if hard_block:
                    blocked = out[mask].copy()
                    blocked["suppression_reason"] = reason
                    blocked["suppressed_by"] = active_type
                    suppressed_rows.append(blocked)
                    out = out[~mask].copy()
                elif "event_tradeability_score" in out.columns:
                    out.loc[mask, "event_tradeability_score"] = (
                        out.loc[mask, "event_tradeability_score"] * penalty
                    ).clip(0.0, 1.0)

    # Build priority lookup from precedence spec
    fam_prio = {e["family"]: e["priority"] for e in prec_spec.get("family_precedence", [])}
    evt_prio = {
        e["event_type"]: e["override_priority"] for e in prec_spec.get("event_overrides", [])
    }

    def _priority(row) -> int:
        et = str(row.get("event_type", ""))
        for family_key in ("canonical_family", "canonical_regime", "family"):
            family = str(row.get(family_key, "")).strip().upper()
            if family in fam_prio:
                return evt_prio.get(et, fam_prio[family])
        return evt_prio.get(et, 999)

    if not out.empty:
        out["_arb_prio"] = out.apply(_priority, axis=1)
        out = out.sort_values(["symbol", "timestamp", "_arb_prio"], ignore_index=True).drop(
            columns=["_arb_prio"]
        )

    # Implement composite event emission
    composite_rows: List[Dict[str, Any]] = []
    for comp in compat_spec.get("composite_events", []):
        name = comp["name"]
        required = comp["required"]
        window = comp.get("co_occur_window_bars", 24)
        
        for sym in symbols:
            sym_df = out[out["symbol"] == sym].copy()
            if sym_df.empty:
                continue
                
            # Find occurrences where all required events are within window
            # Simplified approach: for each event of first type, look for others
            first_type = required[0]
            others = required[1:]
            
            first_events = sym_df[sym_df["event_type"] == first_type]
            for _, f_evt in first_events.iterrows():
                f_ts = f_evt["timestamp"]
                
                match = True
                matched_evts = [f_evt]
                for other_type in others:
                    # Look for other_type within [f_ts - window*5m, f_ts + window*5m] 
                    # (assuming 5m timeframe, but we can use indices if available or just timestamp diff)
                    # For simplicity, we'll use timestamp diff in minutes assuming 5m bars
                    other_matches = sym_df[
                        (sym_df["event_type"] == other_type) &
                        (sym_df["timestamp"].between(
                            f_ts - pd.Timedelta(seconds=window * 300),
                            f_ts + pd.Timedelta(seconds=window * 300)
                        ))
                    ]
                    if other_matches.empty:
                        match = False
                        break
                    matched_evts.append(other_matches.iloc[0])
                
                if match:
                    # Emit composite event at the latest timestamp of the matched group
                    latest_ts = max(e["timestamp"] for e in matched_evts)
                    # Use integer epoch seconds for ID and count occurrences
                    ts_idx = int(pd.to_datetime(latest_ts, utc=True).timestamp())
                    sub_idx = len([r for r in composite_rows if r["timestamp"] == latest_ts and r["symbol"] == sym])
                    
                    composite_rows.append({
                        "event_type": name,
                        "event_id": format_event_id(name, sym, ts_idx, sub_idx),
                        "signal_column": f"{name.lower()}_event",
                        "symbol": sym,
                        "timestamp": latest_ts,
                        "phenom_enter_ts": latest_ts,
                        "enter_ts": latest_ts,
                        "detected_ts": latest_ts,
                        "signal_ts": latest_ts,
                        "exit_ts": latest_ts,
                        "event_tradeability_score": sum(e.get("event_tradeability_score", 0.5) for e in matched_evts) / len(matched_evts),
                        "composite_source": ",".join(required),
                    })

    composite_df = pd.DataFrame(composite_rows) if composite_rows else pd.DataFrame()
    suppressed_df = (
        pd.concat(suppressed_rows, ignore_index=True) if suppressed_rows else pd.DataFrame()
    )
    return ArbitrationResult(events=out, suppressed=suppressed_df, composite_events=composite_df)
