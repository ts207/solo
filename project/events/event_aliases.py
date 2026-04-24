from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
ALIAS_POLICY_PATH = REPO_ROOT / "spec" / "events" / "event_alias_policy.yaml"


@lru_cache(maxsize=1)
def load_event_alias_policy() -> dict[str, Any]:
    payload = yaml.safe_load(ALIAS_POLICY_PATH.read_text(encoding="utf-8")) or {}
    return dict(payload) if isinstance(payload, dict) else {}


def _load_compatibility_aliases() -> dict[str, str]:
    aliases = load_event_alias_policy().get("aliases", {})
    if not isinstance(aliases, dict):
        return {}
    out: dict[str, str] = {}
    for alias, row in aliases.items():
        if not isinstance(row, dict):
            continue
        canonical = str(row.get("canonical_event_type", "")).strip().upper()
        token = str(alias).strip().upper()
        if token and canonical:
            out[token] = canonical
    return out


EVENT_ALIASES = _load_compatibility_aliases()
EXECUTABLE_EVENT_ALIASES = dict(EVENT_ALIASES)


def compatibility_event_aliases() -> tuple[str, ...]:
    return tuple(sorted(EVENT_ALIASES))


def event_alias_policy_rows() -> tuple[dict[str, object], ...]:
    rows = []
    aliases = load_event_alias_policy().get("aliases", {})
    if isinstance(aliases, dict):
        for alias, row in aliases.items():
            if not isinstance(row, dict):
                continue
            rows.append(
                {
                    "alias": str(alias).strip().upper(),
                    "canonical_event_type": str(row.get("canonical_event_type", "")).strip().upper(),
                    "scope": str(row.get("scope", "")).strip(),
                    "planning_identity": bool(row.get("planning_identity", False)),
                    "runtime_identity": bool(row.get("runtime_identity", False)),
                    "promotion_identity": bool(row.get("promotion_identity", False)),
                    "reason": str(row.get("reason", "")).strip(),
                }
            )
    return tuple(sorted(rows, key=lambda row: str(row["alias"])))


def resolve_event_alias(event_type: str) -> str:
    normalized = str(event_type).strip().upper()
    alias_hit = EVENT_ALIASES.get(normalized)
    if alias_hit:
        return alias_hit
    from project.domain.compiled_registry import get_domain_registry
    if get_domain_registry().has_event(normalized):
        return normalized
    return normalized


def resolve_executable_event_alias(event_type: str) -> str:
    normalized = str(event_type).strip().upper()
    return EXECUTABLE_EVENT_ALIASES.get(normalized, normalized)
