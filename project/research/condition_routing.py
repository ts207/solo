from __future__ import annotations

import logging
from typing import Tuple

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


def condition_for_cond_name(
    cond_name: str,
    *,
    run_symbols=None,
    strict: bool = True,
) -> str:
    name = str(cond_name or "").strip()
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
) -> Tuple[str, str]:
    name = str(cond_name or "").strip()
    if not name or name == "all":
        return "all", "unconditional"

    if any(name.startswith(pfx) for pfx in _BUCKET_PREFIXES):
        return "all", "bucket_non_runtime"

    if is_executable_condition(name, run_symbols=run_symbols):
        return name, "runtime"

    if strict:
        return "__BLOCKED__", "blocked"
    return "all", "permissive_fallback"
