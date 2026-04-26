from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def resolve_generated_proposal_controls(
    *,
    templates: Sequence[str],
    horizons_bars: Sequence[int],
    directions: Sequence[str],
    entry_lags: Sequence[int],
    promotion_profile: str = "research",
    run_mode: str = "research",
    search_spec: str = "spec/search_space.yaml",
) -> dict[str, str]:
    """Resolve canonical discovery controls for repo-generated proposals.

    Generated proposals should default to the same broader exploration settings that
    the research stack now supports, while preserving explicit synthetic runs.
    """

    mode = str(run_mode or "research").strip().lower()
    promotion = str(promotion_profile or "research").strip().lower()
    resolved_search_spec = str(search_spec or "spec/search_space.yaml").strip() or "spec/search_space.yaml"

    if mode == "synthetic":
        return {
            "discovery_profile": "synthetic",
            "phase2_gate_profile": "synthetic",
            "search_spec": resolved_search_spec,
        }

    breadth = max(1, len({str(item).strip() for item in templates if str(item).strip()}))
    breadth *= max(1, len({int(item) for item in horizons_bars}))
    breadth *= max(1, len({str(item).strip() for item in directions if str(item).strip()}))
    breadth *= max(1, len({int(item) for item in entry_lags}))

    discovery_profile = "exploratory" if breadth > 1 else "standard"
    phase2_gate_profile = "promotion" if promotion == "deploy" else (
        "discovery" if discovery_profile == "exploratory" else "auto"
    )
    return {
        "discovery_profile": discovery_profile,
        "phase2_gate_profile": phase2_gate_profile,
        "search_spec": resolved_search_spec,
    }


def summarize_viability_for_event(
    viability_report: Mapping[str, Any] | None,
    event_type: str,
) -> dict[str, Any]:
    detectors = (
        viability_report.get("detectors", {}) if isinstance(viability_report, Mapping) else {}
    )
    payload = detectors.get(str(event_type or "").strip().upper(), {})
    if not isinstance(payload, Mapping):
        payload = {}
    blocking = [str(item).strip() for item in payload.get("blocking_columns", []) if str(item).strip()]
    degraded = [str(item).strip() for item in payload.get("degraded_columns", []) if str(item).strip()]
    symbols = payload.get("blocked_symbols", []) if payload.get("status") == "block" else payload.get("warn_symbols", [])
    symbol_list = [str(item).strip().upper() for item in symbols if str(item).strip()]
    return {
        "status": str(payload.get("status", "unknown") or "unknown").strip().lower(),
        "blocking_columns": blocking,
        "degraded_columns": degraded,
        "symbols": symbol_list,
    }
