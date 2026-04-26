from __future__ import annotations

import logging

from project.strategy.dsl import is_executable_condition

log = logging.getLogger(__name__)

_BUCKET_PREFIXES = ("severity_bucket_", "quantile_")
_RULE_TEMPLATE_NAMES = (
    "mean_reversion",
    "continuation",
    "carry",
    "breakout",
    "trend_continuation",
    "pullback_entry",
    "exhaustion_fade",
)

_CONDITION_ALIASES = {
    "carry_state_funding_pos": "carry_pos",
    "carry_state_funding_neg": "carry_neg",
    "ms_spread_state_wide": "ms_spread_state_1.0",
    "ms_spread_state_compressed": "ms_spread_state_0.0",
    "ms_trend_state_bullish": "ms_trend_state_1.0",
    "ms_trend_state_bearish": "ms_trend_state_2.0",
    "ms_trend_state_chop": "ms_trend_state_0.0",
}


def _runtime_alias(cond_name: str) -> str:
    return _CONDITION_ALIASES.get(str(cond_name or "").strip().lower(), cond_name)


def condition_for_cond_name(
    cond_name: str,
    *,
    run_symbols=None,
    strict: bool = True,
) -> str:
    name = _runtime_alias(str(cond_name or "").strip())
    if not name or name == "all":
        return "all"

    if any(name.startswith(pfx) for pfx in _BUCKET_PREFIXES):
        return "all"

    if is_executable_condition(name, run_symbols=run_symbols):
        return name

    if strict:
        return "__BLOCKED__"
    return "all"


def condition_routing(
    cond_name: str,
    *,
    run_symbols=None,
    strict: bool = True,
) -> tuple[str, str]:
    name = _runtime_alias(str(cond_name or "").strip())
    if not name or name == "all":
        return "all", "unconditional"

    if any(name.startswith(pfx) for pfx in _BUCKET_PREFIXES):
        return "all", "bucket_non_runtime"

    if is_executable_condition(name, run_symbols=run_symbols):
        return name, "runtime"

    if strict:
        return "__BLOCKED__", "blocked"
    return "all", "permissive_fallback"
