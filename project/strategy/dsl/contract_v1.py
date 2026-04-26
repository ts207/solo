from __future__ import annotations

import re
from collections.abc import Iterable

from project.strategy.dsl.schema import ConditionNodeSpec, OverlaySpec


class NonExecutableConditionError(ValueError):
    pass


class NonExecutableActionError(ValueError):
    pass


def derive_action_delay(action: str, robustness: float, time_stop_bars: int) -> int:
    """Derives entry delay bars from action name and robustness."""
    act = str(action).strip().lower()
    if act.startswith("delay_"):
        try:
            return max(0, int(float(act.split("_")[-1])))
        except (ValueError, TypeError):
            return 0
    if act == "reenable_at_half_life":
        return max(1, int(round(time_stop_bars / 2.0)))
    if robustness < 0.5:
        return 12
    if robustness < 0.7:
        return 8
    if robustness < 0.85:
        return 4
    return 0


def action_to_overlays(action: str) -> list[OverlaySpec]:
    """Maps action name to a list of OverlaySpec objects."""
    normalized = str(action or "").strip().lower()
    if normalized == "entry_gate_skip":
        return [OverlaySpec(name="risk_throttle", params={"size_scale": 0.0})]
    if normalized.startswith("risk_throttle_"):
        try:
            scale = float(normalized.split("_")[-1])
        except (ValueError, TypeError):
            scale = 1.0
        scale = max(0.0, min(1.0, scale))
        return [OverlaySpec(name="risk_throttle", params={"size_scale": scale})]
    return []


# Canonical, executable condition names supported by the Strategy DSL compiler.
# NOTE: "research-only" condition names (e.g., age buckets, half-life buckets)
# are intentionally excluded unless/until they are materialized as runtime features.
SESSION_CONDITION_MAP = {
    "session_asia": (0, 7),
    "session_eu": (8, 15),
    "session_us": (16, 23),
}

BULL_BEAR_CONDITION_MAP = {
    "bull_bear_bull": 1,
    "bull_bear_bear": -1,
}

VOL_REGIME_CONDITION_MAP = {
    "vol_regime_low": 0,
    "vol_regime_mid": 1,
    "vol_regime_medium": 1,
    "vol_regime_high": 2,
}

CARRY_STATE_CONDITION_MAP = {
    "carry_pos": 1,
    "carry_neutral": 0,
    "carry_neg": -1,
    "carry_state_pos": 1,
    "carry_state_neutral": 0,
    "carry_state_neg": -1,
}

MS_TREND_CONDITION_MAP = {
    "ms_trend_state_0.0": 0,
    "ms_trend_state_1.0": 1,
    "ms_trend_state_2.0": 2,
}

MS_VOL_CONDITION_MAP = {
    "ms_vol_state_0.0": 0,
    "ms_vol_state_1.0": 1,
    "ms_vol_state_2.0": 2,
    "ms_vol_state_3.0": 3,
}

MS_LIQ_CONDITION_MAP = {
    "ms_liq_state_0.0": 0,
    "ms_liq_state_1.0": 1,
    "ms_liq_state_2.0": 2,
}

MS_OI_CONDITION_MAP = {
    "ms_oi_state_0.0": 0,
    "ms_oi_state_1.0": 1,
    "ms_oi_state_2.0": 2,
}

MS_FUNDING_CONDITION_MAP = {
    "ms_funding_state_0.0": 0,
    "ms_funding_state_1.0": 1,
    "ms_funding_state_2.0": 2,
}

MS_SPREAD_CONDITION_MAP = {
    "ms_spread_state_0.0": 0,
    "ms_spread_state_1.0": 1,
}

# funding_bps is an alias for funding_rate_bps
FUNDING_BPS_CONDITION_MAP = {
    "funding_bps_extreme_pos": 2,
    "funding_bps_extreme_neg": -2,
}

_NUMERIC_CONDITION_PATTERN = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|==|>|<)\s*(-?\d+(?:\.\d+)?)\s*$"
)

# Canonical, executable action names supported end-to-end.
# The compiler currently uses actions primarily to derive delay/cooldown semantics.
_ALLOWED_ACTIONS = {
    "no_action",
    "enter_long_market",
    "enter_short_market",
    "entry_gate_skip",
    "risk_throttle_0.75",
    "risk_throttle_0.5",
    "risk_throttle_0.25",
    "risk_throttle_0.0",
    "delay_0",
    "delay_1",
    "delay_3",
    "delay_8",
    "delay_12",
    "delay_30",
    "reenable_at_half_life",
    "risk_throttle_0",
}


def is_executable_action(action: str) -> bool:
    return str(action or "").strip() in _ALLOWED_ACTIONS


def validate_action(action: str, *, event_type: str, candidate_id: str) -> str:
    value = str(action or "").strip()
    if not value:
        raise NonExecutableActionError(
            f"Empty action for event={event_type}, candidate_id={candidate_id}"
        )
    if value not in _ALLOWED_ACTIONS:
        raise NonExecutableActionError(
            f"Non-executable action for event={event_type}, candidate_id={candidate_id}: `{value}`"
        )
    return value


def normalize_entry_condition(
    condition: object,
    *,
    event_type: str,
    candidate_id: str,
    run_symbols: Iterable[str] | None = None,
) -> tuple[str, list[ConditionNodeSpec], str | None]:
    """Return (canonical_condition_str, condition_nodes, symbol_override)."""

    raw = str(condition if condition is not None else "all")
    lowered = raw.strip().lower()

    if lowered.startswith("all__"):
        raise NonExecutableConditionError(f"legacy all__ prefix is forbidden: {raw}")

    if (
        lowered in {"", "all"}
        or lowered.startswith("severity_bucket_")
        or lowered.startswith("quantile_")
    ):
        return "all", [], None

    # Try registry first
    from project.strategy.dsl.conditions import ConditionRegistry

    nodes = ConditionRegistry.resolve(lowered)
    if nodes is not None:
        return lowered, nodes, None

    # Carry State (Special case or move to registry)
    if lowered in CARRY_STATE_CONDITION_MAP:
        code = float(CARRY_STATE_CONDITION_MAP[lowered])
        return (
            lowered,
            [ConditionNodeSpec(feature="carry_state_code", operator="==", value=code)],
            None,
        )

    # MS state conditions
    _MS_MAPS = [
        (MS_TREND_CONDITION_MAP, "ms_trend_state_code"),
        (MS_VOL_CONDITION_MAP, "ms_vol_state_code"),
        (MS_LIQ_CONDITION_MAP, "ms_liq_state_code"),
        (MS_OI_CONDITION_MAP, "ms_oi_state_code"),
        (MS_FUNDING_CONDITION_MAP, "ms_funding_state_code"),
        (MS_SPREAD_CONDITION_MAP, "ms_spread_state_code"),
    ]
    for ms_map, feature in _MS_MAPS:
        if lowered in ms_map:
            code = float(ms_map[lowered])
            return lowered, [ConditionNodeSpec(feature=feature, operator="==", value=code)], None

    # Handle numeric patterns
    match = _NUMERIC_CONDITION_PATTERN.match(raw)
    if match:
        feature, operator, value = match.groups()
        value_f = float(value)
        return (
            f"{feature}{operator}{value_f:g}",
            [ConditionNodeSpec(feature=feature, operator=operator, value=value_f)],
            None,
        )

    if lowered.startswith("symbol_"):
        symbol = lowered[len("symbol_") :].strip().upper()
        return f"symbol_{symbol}", [], symbol

    raise NonExecutableConditionError(f"Non-executable condition: {raw}")


def is_executable_condition(
    condition: object,
    *,
    run_symbols: Iterable[str] | None = None,
) -> bool:
    lowered = str(condition if condition is not None else "").strip().lower()
    # Research-only buckets are allowed to route to "all", but they are not
    # executable condition names themselves.
    if (
        lowered.startswith("severity_bucket_")
        or lowered.startswith("quantile_")
        or lowered.startswith("all__")
    ):
        return False

    try:
        normalize_entry_condition(
            condition, event_type="_", candidate_id="_", run_symbols=run_symbols
        )
        return True
    except NonExecutableConditionError:
        return False


# ---- Feature allowlist/denylist (runtime safety) ----
# The DSL must never reference research-only or future-looking columns.
# Enforcement is intentionally conservative: unknown feature names are rejected.
ALLOWED_FEATURE_PREFIXES = (
    "vol_",
    "range_",
    "ret_",
    "rvol_",
    "atr_",
    "basis_",
    "spread_",
    "quote_vol_",
    "volume_",
    "oi_",
    "liq_",
    "liquidity_",
    "funding_",
    "carry_",
    "basis_",
    "sentiment_",
    "onchain_",
    "flow_",
    "fp_",
    "mc_",
    "ms_",
    "session_",
    "regime_",
    "bb_",
    "z_",
    "event_",
    "flag_",
    "symbol_",
)

# Explicitly disallow common forward return/outcome tokens and training labels.
DISALLOWED_FEATURE_PATTERNS = (
    r"(^|_)fwd(_|$)",
    r"(^|_)forward(_|$)",
    r"(^|_)future(_|$)",
    r"(^|_)label(_|$)",
    r"(^|_)target(_|$)",
    r"(^|_)y(_|$)",
    r"(^|_)outcome(_|$)",
    r"return_after_costs",  # research metric
    r"mfe",
    r"mae",  # research-only unless explicitly surfaced
)


def _is_allowed_feature_name(name: str) -> bool:
    if not isinstance(name, str) or not name:
        return False
    # denylist first
    for pat in DISALLOWED_FEATURE_PATTERNS:
        if re.search(pat, name, flags=re.IGNORECASE):
            return False
    # allowlist prefixes
    return name.startswith(ALLOWED_FEATURE_PREFIXES)


def validate_feature_references(blueprint: dict) -> None:
    """
    Validate that a blueprint references only allowed feature names.
    Covers:
      - condition_nodes[*].feature
      - executable condition strings of the form "<feature> <op> <value>"
    """
    # condition_nodes
    entry = blueprint.get("entry", {}) or {}
    nodes = entry.get("condition_nodes") or []
    for n in nodes:
        feat = (n or {}).get("feature")
        if feat is None:
            continue
        if not _is_allowed_feature_name(str(feat)):
            raise ValueError(f"Disallowed feature reference in condition_nodes: {feat}")

    # executable strings (best-effort parse)
    conds = entry.get("conditions") or []
    for c in conds:
        if not isinstance(c, str):
            continue
        m = re.match(r"^\s*([A-Za-z0-9_\.]+)\s*(>=|<=|==|!=|>|<)\s*[-+0-9\.eE]+\s*$", c)
        if m:
            feat = m.group(1)
            if not _is_allowed_feature_name(feat):
                raise ValueError(f"Disallowed feature reference in condition string: {feat}")


def resolve_trigger_column(trigger: str, available_columns: list[str]) -> str | None:
    """Resolve a trigger name to an actual boolean column in the runtime frame.

    Resolution order (deterministic):
      1) exact match
      2) common prefix/suffix variations:
         - add/remove 'event_' prefix
         - add/remove 'flag_' prefix
         - add/remove '_flag' suffix
      3) lower/upper tolerant match (exact token, not substring)
    Returns resolved column name or None.
    """
    if not isinstance(trigger, str) or not trigger.strip():
        return None
    trig = trigger.strip()
    cols = list(available_columns or [])
    if trig in cols:
        return trig

    candidates = []
    base = trig
    # strip known prefixes
    for pref in ("event_", "flag_"):
        if base.startswith(pref):
            base = base[len(pref) :]

    # build deterministic candidate list
    variants = [
        trig,
        base,
        f"event_{base}",
        f"flag_{base}",
        f"{base}_flag",
        f"event_{base}_flag",
        f"flag_{base}_flag",
    ]
    # also try toggling suffix for original
    if trig.endswith("_flag"):
        variants.append(trig[:-5])
    else:
        variants.append(trig + "_flag")

    seen = set()
    for v in variants:
        if v and v not in seen:
            candidates.append(v)
            seen.add(v)

    for v in candidates:
        if v in cols:
            return v

    # case-tolerant exact token match
    cols_l = {c.lower(): c for c in cols if isinstance(c, str)}
    if trig.lower() in cols_l:
        return cols_l[trig.lower()]
    for v in candidates:
        if v.lower() in cols_l:
            return cols_l[v.lower()]
    return None
