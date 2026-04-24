"""
Core research discovery and gating logic.
Extracted from pipeline scripts to enable unit testing and clean module boundaries.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from project.core.constants import HORIZON_BARS_BY_TIMEFRAME
from project.domain.compiled_registry import get_domain_registry
from project.research.multiplicity import make_family_id as _canonical_make_family_id

log = logging.getLogger(__name__)

# Compatibility-only helper module retained for legacy callers.
LEGACY_COMPATIBILITY_ONLY = True


def candidate_id_from_hypothesis(
    *,
    hypothesis_id: str,
    symbol: str,
    event_type: str,
) -> str:
    payload = "|".join(
        [
            str(hypothesis_id).strip(),
            str(symbol).strip().upper(),
            str(event_type).strip().upper(),
        ]
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return f"cand_{digest}"


def bool_mask_from_series(series: pd.Series) -> pd.Series:
    """Convert a series containing truthy values into a boolean mask."""
    if series.dtype == bool:
        return series.fillna(False)
    s = series.astype(str).str.strip().lower()
    return s.isin({"true", "1", "1.0", "yes", "y", "pass"}).fillna(False)


def horizon_to_bars(timeframe: str) -> int:
    """Convert a timeframe string to horizon bars using the canonical mapping."""
    return HORIZON_BARS_BY_TIMEFRAME.get(timeframe, 12)


def bars_to_timeframe(horizon_bars: int) -> str:
    """Reverse mapping from bars to timeframe string."""
    for tf, bars in HORIZON_BARS_BY_TIMEFRAME.items():
        if bars == horizon_bars:
            return tf
    return f"{horizon_bars}b"


def _infer_bar_duration_ns(ts_series: pd.Series) -> int:
    """Infer bar duration in nanoseconds from a series of timestamps."""
    if len(ts_series) < 2:
        # Default to 5 minutes in nanoseconds
        return int(pd.Timedelta(minutes=5).asm8.view(np.int64))

    # Use median difference to be robust against gaps
    diffs = ts_series.sort_values().diff().dropna()
    if diffs.empty:
        return int(pd.Timedelta(minutes=5).asm8.view(np.int64))

    median_diff = diffs.median()
    return int(median_diff.asm8.view(np.int64))



def direction_token_to_float(value: object) -> float:
    """Convert direction tokens (1, -1, 'long', etc.) to float."""
    if value is None:
        return 0.0
    try:
        f = float(value)
        if np.isfinite(f):
            return f
    except (TypeError, ValueError):
        pass
    s = str(value).strip().lower()
    if s in {"long", "buy", "up", "1", "pos"}:
        return 1.0
    if s in {"short", "sell", "down", "-1", "neg"}:
        return -1.0
    return 0.0


def return_sign_hint(events_df: pd.DataFrame) -> float:
    """Attempt to infer default direction sign from return columns if available."""
    if events_df.empty:
        return 0.0
    return_columns = ["evt_return_1", "evt_return", "evt_move"]
    return_columns.extend(
        sorted(col for col in events_df.columns if str(col).startswith("return_"))
    )
    for col in return_columns:
        if col in events_df.columns:
            m = events_df[col].mean()
            if abs(m) > 1e-9:
                return 1.0 if m > 0 else -1.0
    for col in ["direction", "direction_sign", "event_direction", "side"]:
        if col not in events_df.columns:
            continue
        signs = events_df[col].map(direction_token_to_float)
        signs = signs[np.isfinite(signs)]
        signs = signs[signs != 0.0]
        if len(signs) == 0:
            continue
        mean_sign = float(np.sign(signs.mean()))
        if mean_sign != 0.0:
            return mean_sign
    return 0.0


def action_name_from_direction(direction: float) -> str:
    """Return 'long' or 'short' based on sign."""
    return "long" if float(direction) >= 0 else "short"


def _legacy_template_direction_sign(template_id: str, requested_sign: float) -> float:
    """Map legacy template direction handling onto compiled operator side-policy."""
    operator = get_domain_registry().get_operator(str(template_id).strip())
    if operator is None or not isinstance(operator.raw, dict):
        return float(requested_sign)
    side_policy = str(operator.raw.get("side_policy", "both")).strip().lower()
    if side_policy == "contrarian":
        return float(-requested_sign)
    return float(requested_sign)


def event_template_map() -> Dict[str, str]:
    """Hardcoded fallback template map for legacy events."""
    return {
        "VOL_SPIKE": "vol_spike_reversion",
        "LIQUIDITY_VACUUM": "liquidity_vacuum_fill",
        "VWAP_CROSS": "vwap_mean_reversion",
        "MOMENTUM_BREAKOUT": "momentum_follow_through",
    }


def default_direction_sign_for_event_type(event_type: str, default: float = 0.0) -> float:
    """Return default sign for known event types."""
    # Logic extracted from phase2_candidate_discovery.py
    if "SHORT" in event_type.upper():
        return -1.0
    if "LONG" in event_type.upper():
        return 1.0
    return default


def infer_event_direction_sign(
    events_df: pd.DataFrame, event_type: str | None = None, default: float = 0.0
) -> float:
    """Combined inference for event direction."""
    if event_type:
        ds = default_direction_sign_for_event_type(event_type, 0.0)
        if ds != 0.0:
            return ds
    hint = return_sign_hint(events_df)
    if hint != 0.0:
        return hint
    return default


def resolve_registry_direction_policy(
    events_df: pd.DataFrame,
    *,
    event_type: str | None = None,
    default: float = 0.0,
) -> Dict[str, Any]:
    default_sign = (
        default_direction_sign_for_event_type(event_type or "", 0.0) if event_type else 0.0
    )
    if default_sign != 0.0:
        return {
            "direction_sign": float(default_sign),
            "policy": "default_event_type",
            "source": "event_type_default",
            "resolved": True,
        }
    inferred = return_sign_hint(events_df)
    if inferred != 0.0:
        return {
            "direction_sign": float(inferred),
            "policy": "directional_inferred",
            "source": "event_data",
            "resolved": True,
        }
    return {
        "direction_sign": float(default),
        "policy": "non_directional_skip",
        "source": "unresolved",
        "resolved": False,
    }


def candidate_return_series(
    events_df: pd.DataFrame, horizon_bars: int, direction_sign: float
) -> pd.Series:
    """Extract forward returns for a candidate, adjusted for direction."""
    col = f"return_{horizon_bars}"
    if col not in events_df.columns:
        return pd.Series(dtype=float)
    return events_df[col] * float(direction_sign)


def series_stats(values: pd.Series, horizon_bars: Optional[int] = None) -> Dict[str, float]:
    """Compute summary statistics for a return series with optional overlap correction."""
    if values.empty:
        return {"n": 0, "mean": 0.0, "std": 0.0, "t_stat": 0.0}
    vals = values.dropna()
    n = len(vals)
    if n == 0:
        return {"n": 0, "mean": 0.0, "std": 0.0, "t_stat": 0.0}

    mu = vals.mean()
    sigma = vals.std()

    # Use robust Newey-West t-stat if horizon_bars is provided to correct for overlap bias
    if horizon_bars is not None and horizon_bars > 1:
        from project.core.stats import newey_west_t_stat_for_mean

        # NW lag choice: h-1 is standard for overlapping returns of length h
        nw_res = newey_west_t_stat_for_mean(vals, max_lag=horizon_bars - 1)
        t_stat = nw_res.t_stat
    else:
        t_stat = (mu / (sigma / np.sqrt(n))) if sigma > 1e-9 and n > 1 else 0.0

    return {"n": n, "mean": mu, "std": sigma, "t_stat": t_stat}


def condition_routing(cond_name: str, *, strict: bool = True) -> Tuple[str, str]:
    """Route a conditioning label to a DSL-safe condition string."""
    from project.research.condition_routing import condition_routing as _condition_routing

    return _condition_routing(cond_name, strict=strict)


def make_family_id(
    symbol: str,
    event_type: str,
    rule: str,
    horizon: str,
    cond_label: str,
    *,
    research_family: Optional[str] = None,
    canonical_family: Optional[str] = None,
    state_id: Optional[str] = None,
) -> str:
    """Compatibility wrapper around the canonical family-id builder."""
    return _canonical_make_family_id(
        symbol,
        event_type,
        rule,
        horizon,
        cond_label,
        research_family=research_family,
        canonical_family=canonical_family,
        state_id=state_id,
    )


def _synthesize_concept_candidates(
    *,
    run_id: str,
    symbol: str,
    events_df: pd.DataFrame,
    features_df: pd.DataFrame | None = None,
    entry_lag_bars: int,
    concept_file: str,
) -> pd.DataFrame:
    """Synthesize candidates from a ControlSpec (concept) file."""
    from project.schemas.control_spec import ControlSpec
    from project.spec_registry import load_yaml_path
    from project.strategy.templates.generator import generate_from_concept

    spec_dict = load_yaml_path(concept_file)
    concept = ControlSpec(**spec_dict)
    generated_specs = generate_from_concept(concept)

    if events_df.empty:
        return pd.DataFrame()

    working = events_df.copy()
    direction_sign = infer_event_direction_sign(
        working, event_type=concept.event_definition.event_type, default=0.0
    )
    use_features = features_df is not None and not features_df.empty
    if use_features:
        from project.research.gating import build_event_return_frame

    rows: List[Dict[str, Any]] = []
    for idx, spec in enumerate(generated_specs):
        horizon_bars = int(spec.params.get("horizon_bars", 24))
        horizon_label = bars_to_timeframe(horizon_bars)
        rule_template = str(spec.entry_signal)
        if use_features:
            return_frame = build_event_return_frame(
                working,
                features_df,
                rule=rule_template,
                horizon=horizon_label,
                canonical_family=concept.event_definition.canonical_family,
                entry_lag_bars=entry_lag_bars,
                horizon_bars_override=horizon_bars,
                stop_loss_bps=spec.stop_loss_bps,
                take_profit_bps=spec.take_profit_bps,
                stop_loss_atr_multipliers=spec.stop_loss_atr_multipliers,
                take_profit_atr_multipliers=spec.take_profit_atr_multipliers,
            )
            returns = (
                return_frame["forward_return"] if not return_frame.empty else pd.Series(dtype=float)
            )
        else:
            returns = candidate_return_series(
                working, horizon_bars=horizon_bars, direction_sign=direction_sign
            )
        stats = series_stats(returns)

        cid = f"{symbol}_{concept.concept_id}_{rule_template}_{horizon_label}_{idx}".upper()
        rows.append(
            {
                "candidate_id": cid,
                "symbol": symbol,
                "event_type": concept.event_definition.event_type,
                "rule_template": rule_template,
                "horizon": horizon_label,
                "horizon_bars": int(horizon_bars),
                "direction": float(direction_sign),
                "expectancy": stats["mean"],
                "expectancy_bps": float(stats["mean"] * 1e4),
                "sample_size": stats["n"],
                "t_stat": stats["t_stat"],
                "family_id": make_family_id(
                    symbol=symbol,
                    event_type=concept.event_definition.event_type,
                    rule=rule_template,
                    horizon=horizon_label,
                    cond_label="all",
                ),
                "run_id": run_id,
                "strategy_id": spec.strategy_id,
                "stop_loss_bps": spec.stop_loss_bps,
                "take_profit_bps": spec.take_profit_bps,
                "stop_loss_atr_multipliers": spec.stop_loss_atr_multipliers,
                "take_profit_atr_multipliers": spec.take_profit_atr_multipliers,
                "spec_params_json": json.dumps(spec.normalize(), sort_keys=True),
            }
        )
    return pd.DataFrame(rows)


def _synthesize_experiment_hypotheses(
    *,
    run_id: str,
    symbol: str,
    events_df: pd.DataFrame,
    features_df: pd.DataFrame | None = None,
    experiment_config: str,
    event_type: str,
    registry_root: str | Path = "project/configs/registries",
    experiment_plan: Any | None = None,
) -> pd.DataFrame:
    """Synthesize candidates from an experiment plan for a specific event trigger context."""
    plan = experiment_plan
    if plan is None:
        import importlib

        experiment_engine = importlib.import_module("project.research.experiment_engine")
        plan = experiment_engine.build_experiment_plan(Path(experiment_config), Path(registry_root))

    # Filter hypotheses that can be evaluated in the context of this event_type
    relevant_hyps = []
    for h in plan.hypotheses:
        t = h.trigger
        if t.trigger_type == "event" and t.event_id == event_type:
            relevant_hyps.append(h)
        elif t.trigger_type == "sequence" and event_type in (t.events or []):
            # Evaluate sequence hypotheses at the timestamps of its member events
            relevant_hyps.append(h)
        elif t.trigger_type == "interaction" and event_type in [t.left, t.right]:
            # Evaluate interaction hypotheses at the timestamps of its member events
            relevant_hyps.append(h)
        elif t.trigger_type in ["state", "transition", "feature_predicate"]:
            # These "regime" triggers are evaluated at ANY event timestamp
            relevant_hyps.append(h)

    if not relevant_hyps or events_df.empty:
        return pd.DataFrame()

    working = events_df.copy()
    if features_df is not None and not features_df.empty:
        from project.research.gating import join_events_to_features

        working = join_events_to_features(working, features_df)

    rows = []

    for h in relevant_hyps:
        # Resolve direction sign
        d_sign = direction_token_to_float(h.direction)

        actual_sign = _legacy_template_direction_sign(h.template_id, d_sign)

        h_bars = horizon_to_bars(h.horizon)

        # 1. Start with full mask
        trigger_mask = pd.Series(True, index=working.index)

        # 2. Evaluate Trigger Type Condition
        t = h.trigger
        if t.trigger_type == "event":
            # If trigger matches current event_type exactly, it's always active here
            # If it doesn't match, we skip (it should have been filtered out anyway)
            if t.event_id != event_type:
                trigger_mask &= False

        elif t.trigger_type == "state":
            state_col = f"state_{t.state_id}"
            if state_col in working.columns:
                trigger_mask &= bool_mask_from_series(working[state_col])
            elif f"market_state_{t.state_id}" in working.columns:
                trigger_mask &= bool_mask_from_series(working[f"market_state_{t.state_id}"])
            else:
                # If state column not found, we cannot evaluate it as active
                trigger_mask &= False

        elif t.trigger_type == "transition":
            to_col = f"state_{t.to_state}"
            prev_from_col = f"prev_state_{t.from_state}"

            if to_col in working.columns and prev_from_col in working.columns:
                # Sprint 2: Strict transition meaning ONLY.
                # Must have previously been in from_state and currently in to_state.
                trigger_mask &= bool_mask_from_series(
                    working[prev_from_col]
                ) & bool_mask_from_series(working[to_col])
            else:
                # Refuse execution if history is missing or column not found.
                trigger_mask &= False

        elif t.trigger_type == "feature_predicate":
            if t.feature in working.columns:
                val = working[t.feature]
                if t.operator == ">":
                    trigger_mask &= val > t.threshold
                elif t.operator == "<":
                    trigger_mask &= val < t.threshold
                elif t.operator == ">=":
                    trigger_mask &= val >= t.threshold
                elif t.operator == "<=":
                    trigger_mask &= val <= t.threshold
                elif t.operator == "==":
                    trigger_mask &= val == t.threshold
            else:
                trigger_mask &= False

        elif t.trigger_type == "sequence":
            # Sequence: [E1, E2, ..., En]
            # We evaluate at the timestamp of the LAST event (En) in the sequence
            if not t.events or t.events[-1] != event_type:
                trigger_mask &= False
            else:
                if len(t.events) >= 2:
                    is_active = (working["event_type"] == t.events[-1]).values
                    target_ts_vals = working["enter_ts"].values.astype(np.int64)
                    bar_duration_ns = _infer_bar_duration_ns(working["enter_ts"])

                    # Iterate backwards from E[n-1] down to E[0]
                    # Note: TriggerSpec.events contains [E1, ..., En]
                    # We start at i = n-2 (event E[n-1]) and find e previous to target
                    for i in range(len(t.events) - 2, -1, -1):
                        prev_e = t.events[i]
                        gap = t.max_gap[i] if (hasattr(t, "max_gap") and t.max_gap and len(t.max_gap) > i) else 1

                        # gap == 0 implies unlimited lookback (or use a large default)
                        if gap <= 0:
                            gap_ns = int(1e18) # ~31 years
                        else:
                            gap_ns = int(gap * bar_duration_ns)

                        prev_times = working[working["event_type"] == prev_e]["enter_ts"].sort_values()
                        if prev_times.empty:
                            is_active = np.zeros_like(is_active, dtype=bool)
                            break

                        prev_ts_vals = prev_times.values.astype(np.int64)
                        # Find latest prev_e timestamp <= current target timestamp
                        idx = np.searchsorted(prev_ts_vals, target_ts_vals, side="left") - 1

                        # Proper sequence requires E[i] to happen STRICTLY BEFORE E[i+1]?
                        # Usually researchers mean 'happened before or at same bar' in discovery.
                        # We use searchsorted with side='left' - 1 which finds latest timestamp < target_ts.
                        # If we want <= target_ts, side='right' - 1.
                        # Canonical discovery uses < (strictly before) into the same event.
                        has_prev = (idx >= 0) & ((target_ts_vals - prev_ts_vals[np.maximum(idx, 0)]) <= gap_ns)
                        is_active &= has_prev

                        # Set target_ts_vals to the matched previous event's timestamp for the next iteration
                        target_ts_vals = np.where(has_prev, prev_ts_vals[np.maximum(idx, 0)], target_ts_vals)

                    trigger_mask &= pd.Series(is_active, index=working.index)
                else:
                    # Single event sequence: invalid or just matches itself
                    trigger_mask &= (working["event_type"] == t.events[0]) if t.events else False

        elif t.trigger_type == "interaction":
            # Interaction: Left OP Right
            # Evaluate at current timestamp if both are active (AND)
            # or if Left is confirmed by Right within lag (CONFIRM)
            if t.op == "and":
                is_left_active = pd.Series(False, index=working.index)
                is_right_active = pd.Series(False, index=working.index)

                for side, active_mask in [(t.left, is_left_active), (t.right, is_right_active)]:
                    if side == event_type:
                        active_mask.update(pd.Series(True, index=working.index))
                    elif f"state_{side}" in working.columns:
                        active_mask.update(bool_mask_from_series(working[f"state_{side}"]))
                    elif f"market_state_{side}" in working.columns:
                        active_mask.update(bool_mask_from_series(working[f"market_state_{side}"]))

                trigger_mask &= is_left_active & is_right_active
            elif t.op in ("confirm", "exclude"):
                is_right_active = (working["event_type"] == t.right).values
                left_times = working[working["event_type"] == t.left]["enter_ts"].sort_values()
                if left_times.empty:
                    has_left = np.zeros(len(working), dtype=bool)
                else:
                    left_ts_vals = left_times.values.astype(np.int64)
                    bar_ts_vals = working["enter_ts"].values.astype(np.int64)
                    bar_duration_ns = _infer_bar_duration_ns(working["enter_ts"])

                    lag_bars = t.lag if hasattr(t, "lag") and t.lag is not None else 1
                    if lag_bars <= 0:
                        lag_ns = int(1e18) # Unlimited
                    else:
                        lag_ns = int(lag_bars * bar_duration_ns)

                    idx = np.searchsorted(left_ts_vals, bar_ts_vals, side="left") - 1
                    has_left = (idx >= 0) & ((bar_ts_vals - left_ts_vals[np.maximum(idx, 0)]) <= lag_ns)

                if t.op == "confirm":
                    trigger_mask &= pd.Series(is_right_active & has_left, index=working.index)
                else:
                    trigger_mask &= pd.Series(is_right_active & ~has_left, index=working.index)
            else:
                trigger_mask &= False

        # 3. Evaluate Context
        if h.context:
            for dim, val in h.context.items():
                if dim in working.columns:
                    trigger_mask &= working[dim].astype(str) == str(val)

        # 4. Compute stats
        if trigger_mask.any():
            matched_events = working[trigger_mask]
            returns = candidate_return_series(
                matched_events, horizon_bars=h_bars, direction_sign=actual_sign
            )
            stats = series_stats(returns, horizon_bars=int(h_bars))

            rows.append(
                {
                    "candidate_id": candidate_id_from_hypothesis(
                        hypothesis_id=h.hypothesis_id(),
                        symbol=symbol,
                        event_type=event_type,
                    ),
                    "symbol": symbol,
                    "event_type": event_type,
                    "trigger_type": t.trigger_type,
                    "rule_template": h.template_id,
                    "horizon": h.horizon,
                    "horizon_bars": int(h_bars),
                    "direction": float(actual_sign),
                    "entry_lag_bars": int(h.entry_lag),
                    "expectancy": stats["mean"],
                    "expectancy_bps": float(stats["mean"] * 1e4),
                    "sample_size": stats["n"],
                    "t_stat": stats["t_stat"],
                    "family_id": h.label(),
                    "run_id": run_id,
                    "program_id": plan.program_id,
                    "hypothesis_id": h.hypothesis_id(),
                }
            )

    return pd.DataFrame(rows)


def _synthesize_registry_candidates(
    *,
    run_id: str,
    symbol: str,
    event_type: str,
    events_df: pd.DataFrame,
    horizon_bars: int,
    entry_lag_bars: int,
    templates: Optional[tuple[str, ...]] = None,
    horizons: Optional[tuple[str, ...]] = None,
    directions: Optional[tuple[str, ...]] = None,
    entry_lags: Optional[tuple[int, ...]] = None,
    search_budget: Optional[int] = None,
) -> pd.DataFrame:
    """Synthesize strategy candidates for an event type with expanded combinations."""
    if events_df.empty:
        return pd.DataFrame()

    working = events_df.copy()

    # Resolve subsets
    sel_templates = list(templates) if templates else ["continuation", "mean_reversion"]
    sel_horizons = list(horizons) if horizons else [bars_to_timeframe(horizon_bars)]
    sel_directions = list(directions) if directions else ["auto"]
    sel_entry_lags = list(entry_lags) if entry_lags else [entry_lag_bars]

    rows: List[Dict[str, Any]] = []

    # Count total combinations to check against budget
    total_combinations = (
        len(sel_templates) * len(sel_horizons) * len(sel_directions) * len(sel_entry_lags)
    )
    if search_budget is not None and total_combinations > search_budget:
        log.warning(
            "Total combinations (%d) exceeds search budget (%d). Truncating.",
            total_combinations,
            search_budget,
        )
        # Simple truncation for now

    count = 0
    for h_label in sel_horizons:
        h_bars = horizon_to_bars(h_label)

        # Precompute direction policy once per event family/horizon block.
        direction_policy = resolve_registry_direction_policy(
            working,
            event_type=event_type,
            default=0.0,
        )
        direction_sign_auto = float(direction_policy["direction_sign"])

        for tpl in sel_templates:
            for d_label in sel_directions:
                for lag in sel_entry_lags:
                    if search_budget is not None and count >= search_budget:
                        break

                    # Resolve direction sign
                    if d_label == "auto":
                        d_sign = direction_sign_auto
                    else:
                        d_sign = direction_token_to_float(d_label)

                    if d_sign == 0.0:
                        continue

                    actual_sign = _legacy_template_direction_sign(tpl, d_sign)

                    # Compute stats for this combination
                    returns = candidate_return_series(
                        working, horizon_bars=h_bars, direction_sign=actual_sign
                    )
                    stats = series_stats(returns)

                    cid = f"{symbol}_{event_type}_{tpl}_{h_label}_{d_label}_{lag}b".upper()
                    rows.append(
                        {
                            "candidate_id": cid,
                            "symbol": symbol,
                            "event_type": event_type,
                            "rule_template": tpl,
                            "horizon": h_label,
                            "horizon_bars": int(h_bars),
                            "direction": float(actual_sign),
                            "entry_lag_bars": int(lag),
                            "expectancy": stats["mean"],
                            "expectancy_bps": float(stats["mean"] * 1e4),
                            "sample_size": stats["n"],
                            "t_stat": stats["t_stat"],
                            "direction_policy": str(direction_policy["policy"]),
                            "direction_resolution_source": str(direction_policy["source"]),
                            "family_id": make_family_id(
                                symbol=symbol,
                                event_type=event_type,
                                rule=tpl,
                                horizon=h_label,
                                cond_label="all",
                                state_id=f"lag{lag}",
                            ),
                            "run_id": run_id,
                        }
                    )
                    count += 1
                if search_budget is not None and count >= search_budget:
                    break
            if search_budget is not None and count >= search_budget:
                break
        if search_budget is not None and count >= search_budget:
            break

    return pd.DataFrame(rows)
