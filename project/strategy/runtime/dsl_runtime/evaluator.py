from __future__ import annotations

import logging
from typing import List

import numpy as np
import pandas as pd

from project.strategy.dsl.schema import Blueprint, ConditionNodeSpec, EntrySpec
from project.strategy.runtime.dsl_runtime.signal_resolution import signal_list_mask

LOGGER = logging.getLogger(__name__)


def condition_mask_node(merged: pd.DataFrame, node: ConditionNodeSpec) -> pd.Series:
    """Evaluates a structured condition node or dynamic expression."""
    if getattr(node, "expression", None):
        import re

        expr = str(node.expression).strip()
        # Strictly allow only column names, math operators, numbers, and basic boolean operators
        # \s includes newlines, so we replace it with [ \t] to prevent multiline injection
        dangerous_tokens = ("import", "eval", "exec", "open", "getattr", "setattr")
        if not re.match(r"^[\w \t\.\+\-\*/<>=&\|~()]+$", expr) or "__" in expr or any(
            token in expr for token in dangerous_tokens
        ):
            LOGGER.error(f"Blocked unsafe or complex expression: '{expr}'")
            return pd.Series(False, index=merged.index)
        try:
            return merged.eval(expr).fillna(False)
        except Exception as e:
            LOGGER.error(f"Failed to evaluate expression '{expr}': {e}")
            return pd.Series(False, index=merged.index)

    if node.feature not in merged.columns:
        raise ValueError(f"Unknown condition feature: {node.feature}")
    series = pd.to_numeric(merged[node.feature], errors="coerce")
    if int(node.lookback_bars) > 0:
        series = series.shift(int(node.lookback_bars))

    op = node.operator
    val = float(node.value)
    if op == ">":
        return (series > val).fillna(False)
    if op == ">=":
        return (series >= val).fillna(False)
    if op == "<":
        return (series < val).fillna(False)
    if op == "<=":
        return (series <= val).fillna(False)
    if op == "==":
        return (series == val).fillna(False)
    if op == "crosses_above":
        prior = series.shift(1)
        return ((prior <= val) & (series > val)).fillna(False)
    if op == "crosses_below":
        prior = series.shift(1)
        return ((prior >= val) & (series < val)).fillna(False)
    if op == "in_range":
        high = float(node.value_high) if node.value_high is not None else val
        return ((series >= val) & (series <= high)).fillna(False)
    if op in {"zscore_gt", "zscore_lt"}:
        window = int(node.window_bars)
        # Shift by 1 to avoid lookahead bias (PIT compliance)
        mean = series.rolling(window, min_periods=window).mean().shift(1)
        raw_std = series.rolling(window, min_periods=window).std().shift(1)
        std = raw_std.replace(0.0, np.nan)
        z = (series - mean) / std
        if op == "zscore_gt":
            return (z > val).fillna(False)
        return (z < val).fillna(False)
    raise ValueError(f"Unsupported condition operator: {op}")


def combined_entry_mask(merged: pd.DataFrame, entry: EntrySpec) -> pd.Series:
    masks: List[pd.Series] = []
    masks.extend(condition_mask_node(merged, node) for node in entry.condition_nodes)
    if not masks:
        return pd.Series(True, index=merged.index, dtype=bool)
    out = masks[0]
    if entry.condition_logic == "any":
        for mask in masks[1:]:
            out = out | mask
    else:
        for mask in masks[1:]:
            out = out & mask
    return out.fillna(False)


def entry_eligibility_mask(
    frame: pd.DataFrame, entry: EntrySpec, blueprint: Blueprint
) -> pd.Series:
    c_mask = combined_entry_mask(frame, entry)
    t_mask = signal_list_mask(frame, entry.triggers, blueprint, signal_kind="trigger")
    f_mask = signal_list_mask(frame, entry.confirmations, blueprint, signal_kind="confirmation")
    return (c_mask & t_mask & f_mask).fillna(False)
