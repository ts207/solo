from __future__ import annotations

from typing import Any

_CONTEXT_LABEL_ALIASES: dict[str, dict[str, str]] = {
    "carry_state": {
        "positive": "funding_pos",
        "pos": "funding_pos",
        "funding_positive": "funding_pos",
        "negative": "funding_neg",
        "neg": "funding_neg",
        "funding_negative": "funding_neg",
        "neutral": "neutral",
    }
}

DIMENSION_VALUE_MAP: dict[str, dict[str, list[object]]] = {
    "ms_trend_state": {
        "chop": [0.0, 0, "0", "0.0", "chop"],
        "bullish": [1.0, 1, "1", "1.0", "bullish"],
        "bearish": [2.0, 2, "2", "2.0", "bearish"],
    },
    "ms_spread_state": {
        "tight": [0.0, 0, "0", "0.0", "tight"],
        "wide": [1.0, 1, "1", "1.0", "wide"],
    },
}


def canonicalize_context_label(family: str, label: Any) -> str:
    family_key = str(family or "").strip().lower()
    token = str(label or "").strip()
    if not token:
        return ""
    alias_map = _CONTEXT_LABEL_ALIASES.get(family_key, {})
    return alias_map.get(token.lower(), token)


def canonicalize_contexts(contexts: dict[str, Any] | None) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for raw_family, raw_values in (contexts or {}).items():
        family = str(raw_family or "").strip()
        if not family:
            continue
        values = raw_values if isinstance(raw_values, (list, tuple, set)) else [raw_values]
        normalized: list[str] = []
        seen: set[str] = set()
        for raw_value in values:
            token = canonicalize_context_label(family, raw_value)
            if token and token not in seen:
                normalized.append(token)
                seen.add(token)
        out[family] = normalized
    return out


def expand_dimension_values(dimension: str, values: list[str]) -> list[object]:
    dimension_key = str(dimension or "").strip().lower()
    mapping = DIMENSION_VALUE_MAP.get(dimension_key)
    if not mapping:
        return list(values)

    expanded: list[object] = []
    seen: set[tuple[str, str]] = set()
    for raw_value in values:
        token = str(raw_value or "").strip()
        if not token:
            continue
        candidates = mapping.get(token.lower(), [raw_value])
        for candidate in candidates:
            marker = (type(candidate).__name__, str(candidate).strip().lower())
            if marker in seen:
                continue
            expanded.append(candidate)
            seen.add(marker)
    return expanded
