from __future__ import annotations

from typing import Any, Literal

from project.events.polarity import normalize_event_side, side_to_direction

_VALID_SIDE_POLICIES = {"directional", "contrarian", "both"}


def _direction_sign(value: Any) -> int:
    return side_to_direction(value)


def direction_sign(value: Any) -> int:
    return _direction_sign(value)


def normalize_side_policy(side_policy: str) -> str:
    token = str(side_policy or "").strip().lower()
    if token not in _VALID_SIDE_POLICIES:
        raise ValueError(
            f"Unsupported side_policy={side_policy!r}; expected directional|contrarian|both"
        )
    return token


def resolve_effect_sign(
    *,
    template_verb: str,
    side_policy: str,
    event_direction: Any,
    label_target: str,
    fallback_sign: int = 1,
) -> int:
    target = str(label_target or "").strip().lower()
    if target == "gate":
        return 0

    policy = normalize_side_policy(side_policy)
    evt_sign = _direction_sign(event_direction)
    if evt_sign == 0:
        evt_sign = 1 if int(fallback_sign) >= 0 else -1

    if policy == "directional":
        return int(evt_sign)
    if policy == "contrarian":
        return int(-evt_sign)
    return int(evt_sign)


def resolve_trade_direction(
    *,
    side_policy: str,
    event_side: Any = "unknown",
    event_direction: Any = 0,
    explicit_direction: str | None = None,
    label_target: str = "fwd_return_h",
    fallback_sign: int = 1,
) -> Literal["long", "short", "skip"]:
    if explicit_direction:
        token = str(explicit_direction).strip().lower()
        if token in {"long", "short"}:
            return token  # explicit overrides side policy
    sign_source = event_direction if _direction_sign(event_direction) != 0 else event_side
    sign = resolve_effect_sign(
        template_verb="",
        side_policy=side_policy,
        event_direction=sign_source,
        label_target=label_target,
        fallback_sign=fallback_sign,
    )
    if sign == 0:
        return "skip"
    return "long" if sign > 0 else "short"


def resolve_candidate_action(
    *,
    template_verb: str,
    side_policy: str,
    label_target: str,
    event_direction: Any = 1,
    fallback_sign: int = 1,
) -> str:
    sign = resolve_effect_sign(
        template_verb=template_verb,
        side_policy=side_policy,
        event_direction=event_direction,
        label_target=label_target,
        fallback_sign=fallback_sign,
    )
    if sign == 0:
        return "entry_gate_skip"
    return "enter_long_market" if sign > 0 else "enter_short_market"
