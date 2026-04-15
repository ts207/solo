from __future__ import annotations

import json
import logging
import re
from typing import Dict, List, Optional, Tuple, Any

import numpy as np
import pandas as pd

from project.core.coercion import safe_float, safe_int, as_bool
from project.strategy.dsl import is_executable_action, is_executable_condition

EVENT_FAMILY_STRATEGY_ROUTING: Dict[str, Dict[str, str]] = {
    "VOL_SHOCK": {
        "execution_family": "breakout_mechanics",
        "base_strategy": "dsl_interpreter_v1",
    },
    "LIQUIDITY_VACUUM": {
        "execution_family": "breakout_mechanics",
        "base_strategy": "dsl_interpreter_v1",
    },
    "FUNDING_EXTREME_ONSET": {
        "execution_family": "carry_imbalance",
        "base_strategy": "dsl_interpreter_v1",
    },
    "FUNDING_PERSISTENCE_TRIGGER": {
        "execution_family": "carry_imbalance",
        "base_strategy": "dsl_interpreter_v1",
    },
    "FUNDING_NORMALIZATION_TRIGGER": {
        "execution_family": "carry_imbalance",
        "base_strategy": "dsl_interpreter_v1",
    },
    "FORCED_FLOW_EXHAUSTION": {
        "execution_family": "exhaustion_overshoot",
        "base_strategy": "dsl_interpreter_v1",
    },
    "CROSS_VENUE_DESYNC": {
        "execution_family": "spread_dislocation",
        "base_strategy": "dsl_interpreter_v1",
    },
    "OI_SPIKE_POSITIVE": {
        "execution_family": "carry_imbalance",
        "base_strategy": "dsl_interpreter_v1",
    },
    "OI_SPIKE_NEGATIVE": {
        "execution_family": "carry_imbalance",
        "base_strategy": "dsl_interpreter_v1",
    },
    "OI_FLUSH": {
        "execution_family": "carry_imbalance",
        "base_strategy": "dsl_interpreter_v1",
    },
    "LIQUIDATION_CASCADE": {
        "execution_family": "carry_imbalance",
        "base_strategy": "dsl_interpreter_v1",
    },
}


def route_event_family(event: str) -> Optional[Dict[str, str]]:
    key = str(event).strip()
    if key in EVENT_FAMILY_STRATEGY_ROUTING:
        return EVENT_FAMILY_STRATEGY_ROUTING.get(key)
    return EVENT_FAMILY_STRATEGY_ROUTING.get(key.upper())


def risk_controls_from_action(action: str) -> Dict[str, object]:
    controls: Dict[str, object] = {
        "entry_delay_bars": 0,
        "size_scale": 1.0,
        "block_entries": False,
        "reentry_mode": "immediate",
    }
    if action.startswith("delay_"):
        controls["entry_delay_bars"] = safe_int(action.split("_")[-1], 0)
        return controls
    if action.startswith("risk_throttle_"):
        controls["size_scale"] = safe_float(action.split("_")[-1], 1.0)
        controls["block_entries"] = bool(controls["size_scale"] <= 0.0)
        return controls
    if action == "entry_gate_skip":
        controls["size_scale"] = 0.0
        controls["block_entries"] = True
        return controls
    if action == "reenable_at_half_life":
        controls["entry_delay_bars"] = 8
        controls["reentry_mode"] = "half_life"
        return controls
    return controls


def infer_condition_from_blueprint(blueprint: Dict[str, object]) -> str:
    entry = blueprint.get("entry", {}) if isinstance(blueprint.get("entry"), dict) else {}
    conditions = entry.get("conditions", []) if isinstance(entry.get("conditions"), list) else []
    for condition in conditions:
        text = str(condition).strip()
        if text:
            return text
    return "all"


def infer_action_from_blueprint(blueprint: Dict[str, object]) -> str:
    overlays = blueprint.get("overlays", []) if isinstance(blueprint.get("overlays"), list) else []
    for overlay in overlays:
        if not isinstance(overlay, dict):
            continue
        if str(overlay.get("name", "")).strip().lower() != "risk_throttle":
            continue
        params = overlay.get("params", {}) if isinstance(overlay.get("params"), dict) else {}
        size_scale = safe_float(params.get("size_scale"), 1.0)
        if size_scale <= 0.0:
            return "entry_gate_skip"
        if abs(size_scale - 1.0) > 1e-9:
            return f"risk_throttle_{size_scale:g}"
    entry = blueprint.get("entry", {}) if isinstance(blueprint.get("entry"), dict) else {}
    delay = safe_int(entry.get("delay_bars"), safe_int(entry.get("entry_delay_bars"), 0))
    if delay > 0:
        return f"delay_{delay}"
    return "no_action"


def symbol_scope_from_row(row: Dict[str, object], symbols: List[str]) -> Dict[str, object]:
    run_symbols = [str(s).strip().upper() for s in symbols if str(s).strip()]
    candidate_symbol = str(row.get("candidate_symbol", "")).strip().upper()
    if not candidate_symbol:
        raw_symbol = str(row.get("symbol", "")).strip().upper()
        if raw_symbol:
            candidate_symbol = raw_symbol
    if not candidate_symbol:
        condition = str(row.get("condition", "")).strip().lower()
        if condition.startswith("symbol_"):
            candidate_symbol = condition.removeprefix("symbol_").upper()
    if not candidate_symbol:
        candidate_symbol = run_symbols[0] if len(run_symbols) == 1 else "ALL"
    return {"candidate_symbol": candidate_symbol, "run_symbols": run_symbols}


def sanitize_id(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", str(value).strip().lower()).strip("_")
