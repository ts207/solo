from __future__ import annotations

from typing import Any


def _direction_sign(value: Any) -> int:
    try:
        num = float(value)
    except (TypeError, ValueError):
        num = 0.0
    if num > 0:
        return 1
    if num < 0:
        return -1
    token = str(value or "").strip().lower()
    if token in {"up", "long", "buy", "bull", "positive", "pos"}:
        return 1
    if token in {"down", "short", "sell", "bear", "negative", "neg"}:
        return -1
    return 0


def normalize_side_policy(side_policy: str) -> str:
    token = str(side_policy or "").strip().lower()
    if token not in {"directional", "contrarian", "both"}:
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

    # Absolute policy semantics:
    # 'directional' follows the event direction.
    # 'contrarian' always flips the event direction.
    if policy == "directional":
        return int(evt_sign)
    if policy == "contrarian":
        return int(-evt_sign)

    # Default 'both' policy — assume directional for effect sign resolution
    return int(evt_sign)


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
