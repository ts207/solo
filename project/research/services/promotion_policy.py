from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Dict


def _load_dynamic_min_events_by_event(spec_root: str | Path) -> Dict[str, int]:
    path = Path(spec_root) / "spec" / "states" / "state_registry.yaml"
    if not path.exists():
        return {}
    try:
        import yaml

        with path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
    except (ImportError, OSError, UnicodeDecodeError):
        logging.warning("Failed loading state_registry")
        return {}
    except yaml.YAMLError:
        logging.warning("Failed loading state_registry")
        return {}

    out: Dict[str, int] = {}
    default_min = data.get("defaults", {}).get("min_events", 0)
    for state_row in data.get("states", []):
        event_type = state_row.get("source_event_type")
        if event_type:
            out[event_type] = max(
                out.get(event_type, default_min), state_row.get("min_events", default_min)
            )
    return out


def _resolve_promotion_profile(configured_profile: str, source_run_mode: str) -> str:
    profile = str(configured_profile or "auto").strip().lower()
    if profile in {"research", "deploy"}:
        return profile
    if source_run_mode in {"confirmatory", "production", "certification", "promotion", "deploy"}:
        return "deploy"
    return "research"


def _resolve_promotion_policy(
    *,
    config: Any,
    contract: Any,
    source_run_mode: str,
    project_root: Path,
    load_dynamic_min_events_by_event_fn: Callable[[str | Path], Dict[str, int]],
    resolved_policy_cls: type[Any],
) -> Any:
    profile = _resolve_promotion_profile(config.promotion_profile, source_run_mode)
    base_min_events = int(config.min_events)
    dynamic_min_events: Dict[str, int] = {}

    min_net_expectancy_bps = float(
        max(0.0, float(getattr(contract, "min_net_expectancy_bps", 0.0) or 0.0))
    )
    max_fee_plus_slippage_bps = getattr(contract, "max_fee_plus_slippage_bps", None)
    max_daily_turnover_multiple = getattr(contract, "max_daily_turnover_multiple", None)
    require_retail_viability = bool(getattr(contract, "require_retail_viability", False))
    require_low_capital_viability = bool(getattr(contract, "require_low_capital_contract", False))
    enforce_baseline_beats_complexity = True
    enforce_placebo_controls = True
    enforce_timeframe_consensus = True

    if profile == "deploy":
        base_min_events = max(
            base_min_events,
            int(getattr(contract, "min_trade_count", base_min_events) or base_min_events),
            150,
        )
        dynamic_min_events = load_dynamic_min_events_by_event_fn(project_root)
        enforce_baseline_beats_complexity = True
        enforce_placebo_controls = True
        enforce_timeframe_consensus = True
    else:
        min_net_expectancy_bps = min(min_net_expectancy_bps, 1.5)
        require_retail_viability = False
        require_low_capital_viability = False
        enforce_baseline_beats_complexity = True
        enforce_placebo_controls = False
        enforce_timeframe_consensus = False

    use_effective_q_value = profile == "deploy"

    return resolved_policy_cls(
        promotion_profile=profile,
        base_min_events=base_min_events,
        dynamic_min_events=dynamic_min_events,
        min_net_expectancy_bps=min_net_expectancy_bps,
        max_fee_plus_slippage_bps=max_fee_plus_slippage_bps,
        max_daily_turnover_multiple=max_daily_turnover_multiple,
        require_retail_viability=require_retail_viability,
        require_low_capital_viability=require_low_capital_viability,
        enforce_baseline_beats_complexity=enforce_baseline_beats_complexity,
        enforce_placebo_controls=enforce_placebo_controls,
        enforce_timeframe_consensus=enforce_timeframe_consensus,
        use_effective_q_value=use_effective_q_value,
    )
