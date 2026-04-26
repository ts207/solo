from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from project.compilers import ExecutableStrategySpec

try:
    from numba import njit
except ImportError:

    def njit(*args, **kwargs):
        if args and callable(args[0]):
            return args[0]
        return lambda f: f


from project.strategy.dsl.normalize import build_blueprint
from project.strategy.dsl.references import event_direction_bias
from project.strategy.dsl.validate import validate_overlay_columns
from project.strategy.runtime.dsl_runtime.evaluator import entry_eligibility_mask
from project.strategy.runtime.dsl_runtime.execution_context import build_signal_frame
from project.strategy.runtime.dsl_runtime.signal_resolution import compute_trigger_coverage


@njit
def generate_positions_numba(
    timestamps: np.ndarray,
    open_prices: np.ndarray,
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    eligible: np.ndarray,
    entry_sides: np.ndarray,
    stop_offsets: np.ndarray,
    target_offsets: np.ndarray,
    allow_intrabar_exits: bool,
    cooldown_bars: int,
    reentry_lockout_bars: int,
    arm_bars_base: int,
    time_stop_bars: int,
    break_even_r: float,
    trailing_stop_type: int,
    trailing_offsets: np.ndarray,
    invalidation_mask: np.ndarray,
    priority_randomisation: bool,
    random_rolls: np.ndarray,
):
    n = len(timestamps)
    positions = np.zeros(n, dtype=np.int32)
    events_timestamps = []
    events_codes = []

    in_pos = 0
    state = 0
    arm_remaining = 0
    entry_idx = -1
    entry_price = np.nan
    risk_per_unit = np.nan
    best_price = np.nan
    stop_price = np.nan
    target_price = np.nan
    cooldown_until = -1

    for i in range(n):
        c = close[i]
        h = high[i]
        l = low[i]
        if in_pos != 0:
            held = i - entry_idx
            invalidate = invalidation_mask[i]
            if in_pos > 0:
                best_price = max(
                    c if np.isnan(best_price) else best_price, h if not np.isnan(h) else c
                )
            else:
                best_price = min(
                    c if np.isnan(best_price) else best_price, l if not np.isnan(l) else c
                )

            if break_even_r > 0 and not np.isnan(risk_per_unit) and risk_per_unit > 0:
                if in_pos > 0:
                    if (best_price - entry_price) / risk_per_unit >= break_even_r:
                        stop_price = max(stop_price, entry_price)
                else:
                    if (entry_price - best_price) / risk_per_unit >= break_even_r:
                        stop_price = min(stop_price, entry_price)

            if trailing_stop_type != 0:
                trail = trailing_offsets[i]
                if trail > 0:
                    if in_pos > 0:
                        stop_price = max(stop_price, best_price - trail)
                    else:
                        stop_price = min(stop_price, best_price + trail)

            should_exit = False
            code = 0
            if held >= time_stop_bars:
                should_exit = True
                code = 2
            elif invalidate:
                should_exit = True
                code = 3
            elif allow_intrabar_exits:
                hit_stop = False
                hit_target = False
                if in_pos > 0 and not np.isnan(l) and not np.isnan(h):
                    if l <= stop_price:
                        hit_stop = True
                    if h >= target_price:
                        hit_target = True
                elif in_pos < 0 and not np.isnan(l) and not np.isnan(h):
                    if h >= stop_price:
                        hit_stop = True
                    if l <= target_price:
                        hit_target = True
                if hit_stop and hit_target:
                    if priority_randomisation and random_rolls[i] > 0.5:
                        should_exit = True
                        code = 5
                    else:
                        should_exit = True
                        code = 4
                elif hit_stop:
                    should_exit = True
                    code = 4
                elif hit_target:
                    should_exit = True
                    code = 5

            if should_exit:
                in_pos = 0
                state = 3
                cooldown_until = i + max(cooldown_bars, reentry_lockout_bars)
                events_timestamps.append(i)
                events_codes.append(code)

        if in_pos == 0:
            if state == 3 and i < cooldown_until:
                positions[i] = 0
                continue
            if state == 3 and i >= cooldown_until:
                state = 0
            if state == 0:
                if eligible[i]:
                    state = 1
                    arm_remaining = arm_bars_base
                    positions[i] = 0
                    continue
                else:
                    positions[i] = 0
                    continue
            if state == 1:
                if arm_remaining > 0:
                    arm_remaining -= 1
                    if arm_remaining > 0:
                        positions[i] = 0
                        continue
                side = entry_sides[i]
                if side == 0 or c <= 0:
                    state = 0
                    positions[i] = 0
                    continue
                stop_off = stop_offsets[i]
                target_off = target_offsets[i]
                if stop_off <= 0 or target_off <= 0:
                    state = 0
                    positions[i] = 0
                    continue
                in_pos = side
                state = 2
                entry_idx = i
                entry_price = open_prices[i]
                risk_per_unit = stop_off
                best_price = open_prices[i]
                if side > 0:
                    stop_price = entry_price - stop_off
                    target_price = entry_price + target_off
                else:
                    stop_price = entry_price + stop_off
                    target_price = entry_price - target_off
                events_timestamps.append(i)
                events_codes.append(1)
        positions[i] = in_pos
    return positions, events_timestamps, events_codes


def _to_float_arr(series: np.ndarray, default: float = 0.0) -> np.ndarray:
    out = np.where(pd.isna(series), default, series)
    out = np.where(np.isinf(out), default, out)
    return out.astype(float)


def _vectorized_offset(frame: pd.DataFrame, kind: str, value: float) -> np.ndarray:
    close = _to_float_arr(frame.get("close", pd.Series(0.0, index=frame.index)).values)
    out = np.zeros(len(frame))
    valid = close > 0
    val = float(value)
    if kind == "percent":
        out[valid] = close[valid] * val
    elif kind == "range_pct":
        high_96 = _to_float_arr(
            frame.get("high_96", pd.Series(np.nan, index=frame.index)).values, np.nan
        )
        low_96 = _to_float_arr(
            frame.get("low_96", pd.Series(np.nan, index=frame.index)).values, np.nan
        )
        rp = np.zeros(len(frame))
        v_rp = (~np.isnan(high_96)) & (~np.isnan(low_96)) & (close > 0)
        rp[v_rp] = np.abs(high_96[v_rp] - low_96[v_rp]) / close[v_rp]
        out[valid] = close[valid] * rp[valid] * val
    else:  # atr
        atr = _to_float_arr(
            frame.get("atr_14", pd.Series(np.nan, index=frame.index)).values, np.nan
        )
        out[valid] = atr[valid] * val
    return np.maximum(0.0, out)


@dataclass
class DslInterpreterV1:
    name: str = "dsl_interpreter_v1"
    required_features: list[str] = field(default_factory=lambda: ["high_96", "low_96"])

    def generate_positions(
        self, bars: pd.DataFrame, features: pd.DataFrame, params: dict
    ) -> pd.Series:
        raw_blueprint = params.get("dsl_blueprint")
        raw_executable = params.get("executable_strategy_spec")
        if raw_blueprint is None and raw_executable is None:
            raise ValueError(
                "dsl_interpreter_v1 requires params.dsl_blueprint or params.executable_strategy_spec"
            )
        if raw_blueprint is None:
            executable = (
                raw_executable
                if isinstance(raw_executable, ExecutableStrategySpec)
                else ExecutableStrategySpec.model_validate(dict(raw_executable))
            )
            raw_blueprint = executable.to_blueprint_dict()
        blueprint = build_blueprint(dict(raw_blueprint))
        bars = bars.copy()
        features = features.copy()
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
        features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)
        merged = features.drop(
            columns=["open", "high", "low", "close", "volume", "quote_volume"], errors="ignore"
        ).merge(
            bars[["timestamp", "open", "high", "low", "close", "volume", "quote_volume"]],
            on="timestamp",
            how="left",
        )
        frame = build_signal_frame(merged)
        validate_overlay_columns(frame, blueprint.overlays, blueprint.id)
        trigger_coverage = compute_trigger_coverage(frame, blueprint.entry.triggers)

        if params.get("fail_on_zero_trigger_coverage") and trigger_coverage.get("all_zero"):
            raise ValueError(
                f"all-zero trigger coverage for blueprint '{blueprint.id}': "
                f"triggers={blueprint.entry.triggers}"
            )

        eligible_mask = entry_eligibility_mask(frame, blueprint.entry, blueprint)

        open_arr = _to_float_arr(frame["open"].values)
        close_arr = _to_float_arr(frame["close"].values)
        high_arr = _to_float_arr(frame["high"].values)
        low_arr = _to_float_arr(frame["low"].values)
        eligible_arr = eligible_mask.values.astype(bool)

        invalidation_arr = np.zeros(len(frame), dtype=bool)
        inv = blueprint.exit.invalidation
        inv_col = str(inv.get("metric", "")).strip()
        if inv_col in frame.columns:
            m_arr = _to_float_arr(frame[inv_col].values)
            val = float(inv.get("value", 0))
            op = str(inv.get("operator", ""))
            if op == ">":
                invalidation_arr = m_arr > val
            elif op == "<":
                invalidation_arr = m_arr < val

        entry_sides = np.zeros(len(frame), dtype=np.int32)
        bias = event_direction_bias(blueprint.event_type)
        if blueprint.direction == "long":
            entry_sides[:] = 1
        elif blueprint.direction == "short":
            entry_sides[:] = -1
        elif blueprint.direction == "both":
            # NOTE: "both" policy creates two separate evaluation frames in the search
            # pipeline, not a single merged frame. This fallback uses bias but the
            # authoritative "both" resolution happens at hypothesis generation time.
            entry_sides[:] = bias if bias != 0 else 1
        elif blueprint.direction == "conditional":
            # Check if there is a specific sign or direction feature attached to the event
            if "sign" in frame.columns:
                entry_sides[:] = np.sign(_to_float_arr(frame["sign"].values))
            else:
                # Fallback to the default bias for this event type
                entry_sides[:] = bias if bias != 0 else 1

        stop_offsets = _vectorized_offset(
            frame, blueprint.exit.stop_type, float(blueprint.exit.stop_value)
        )
        target_offsets = _vectorized_offset(
            frame, blueprint.exit.target_type, float(blueprint.exit.target_value)
        )

        # Deterministic random rolls based on blueprint ID length for reproducible priority randomisation
        import hashlib

        seed = int(hashlib.sha256(blueprint.id.encode("utf-8")).hexdigest()[:8], 16) % (2**31)
        rng = np.random.RandomState(seed)
        random_rolls = rng.rand(len(frame))

        pos_arr, ev_idxs, ev_codes = generate_positions_numba(
            frame["timestamp"].values.astype(np.int64),
            open_arr,
            close_arr,
            high_arr,
            low_arr,
            eligible_arr,
            entry_sides,
            stop_offsets,
            target_offsets,
            bool(params.get("allow_intrabar_exits", False)),
            int(blueprint.entry.cooldown_bars),
            int(blueprint.entry.reentry_lockout_bars),
            max(1, int(blueprint.entry.delay_bars)),
            int(blueprint.exit.time_stop_bars),
            float(blueprint.exit.break_even_r),
            0,
            np.zeros(len(frame)),
            invalidation_arr,
            bool(params.get("priority_randomisation", True)),
            random_rolls,
        )
        out = pd.Series(pos_arr, index=frame["timestamp"], name="position", dtype=int)
        out.attrs["signal_events"] = []  # Placeholder
        out.attrs["strategy_metadata"] = {
            "trigger_coverage": trigger_coverage,
            "blueprint_id": blueprint.id,
            "run_id": blueprint.run_id,
            "contract_source": (
                "executable_strategy_spec"
                if raw_executable is not None and params.get("dsl_blueprint") is None
                else "dsl_blueprint"
            ),
        }
        return out
