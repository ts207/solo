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
