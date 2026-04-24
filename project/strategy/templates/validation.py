from typing import Set

import pandas as pd

# Hardcoded list of inherently PIT-safe columns (price/vol)
CORE_PIT_SAFE_COLUMNS: Set[str] = {
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "spread_bps",
    "spread_abs",
    "funding_rate_scaled",
}


def validate_pit_invariants(signal: pd.Series) -> bool:
    """Return True iff the signal index is strictly monotone increasing.

    A non-monotone index indicates potential lookahead or unsorted data,
    both of which violate point-in-time discipline.
    """
    if signal.empty:
        return True
    return bool(signal.index.is_monotonic_increasing) and not bool(signal.index.duplicated().any())


def check_closed_left_rolling(window: pd.Series) -> bool:
    """Return True iff the rolling window index is monotone increasing.

    A properly constructed closed-left rolling window [T-N, T-1] must have
    a monotone index. A non-monotone window suggests unsorted or incorrectly
    sliced data that could include the current evaluation bar.
    """
    if window.empty:
        return True
    return bool(window.index.is_monotonic_increasing)


from project.domain.compiled_registry import get_domain_registry


def template_kind(template_id: str) -> str:
    token = str(template_id or "").strip()
    if not token:
        return ""
    return get_domain_registry().template_kind(token)


def validate_template_stack(
    primary_template_id: str,
    *,
    filter_template_id: str | None = None,
    execution_template_id: str | None = None,
) -> list[str]:
    errors: list[str] = []
    registry = get_domain_registry()

    primary = str(primary_template_id or "").strip()
    if not primary:
        errors.append("primary template_id must not be empty")
    else:
        primary_kind = registry.template_kind(primary)
        if primary_kind == "filter_template":
            errors.append(
                f"Primary template {primary!r} is a filter template; top-level search units must be expression templates"
            )
        elif primary_kind == "execution_template":
            errors.append(
                f"Primary template {primary!r} is an execution template; top-level search units must be expression templates"
            )
        elif primary_kind not in {"", "expression_template"}:
            errors.append(
                f"Primary template {primary!r} has unsupported template kind {primary_kind!r}"
            )

    if filter_template_id:
        filter_name = str(filter_template_id).strip()
        filter_kind = registry.template_kind(filter_name)
        if filter_kind != "filter_template":
            errors.append(
                f"Auxiliary filter template {filter_name!r} must have template kind 'filter_template', got {filter_kind!r}"
            )

    if execution_template_id:
        execution_name = str(execution_template_id).strip()
        execution_kind = registry.template_kind(execution_name)
        if execution_kind != "execution_template":
            errors.append(
                f"Auxiliary execution template {execution_name!r} must have template kind 'execution_template', got {execution_kind!r}"
            )

    return errors
