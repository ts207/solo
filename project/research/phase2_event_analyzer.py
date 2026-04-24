from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Dict, List, Tuple

import numpy as np
import pandas as pd

from project.core.config import get_data_root
from project.core.feature_schema import feature_dataset_dir_name
from project.io.utils import (
    choose_partition_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
)
from project.spec_registry import load_global_defaults

LOGGER = logging.getLogger(__name__)
MAX_DYNAMIC_CONDITIONS_PER_COLUMN = 12


@dataclass(frozen=True)
class ConditionSpec:
    name: str
    description: str
    mask_fn: Callable[[pd.DataFrame], pd.Series]


@dataclass(frozen=True)
class ActionSpec:
    name: str
    family: str
    params: Dict[str, object]


def _first_existing(df: pd.DataFrame, candidates: List[str]) -> str | None:
    for name in candidates:
        if name in df.columns:
            return name
    return None


def _numeric_non_negative(df: pd.DataFrame, col: str | None, n: int) -> pd.Series:
    if col is None:
        return pd.Series(0.0, index=df.index)
    out = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float).clip(lower=0.0)
    if len(out) != n:
        return pd.Series(0.0, index=df.index)
    return out


def _numeric_any(df: pd.DataFrame, col: str | None, n: int, default: float = np.nan) -> pd.Series:
    if col is None:
        return pd.Series(default, index=df.index)
    out = pd.to_numeric(df[col], errors="coerce").astype(float)
    if len(out) != n:
        return pd.Series(default, index=df.index)
    return out


def build_conditions(events: pd.DataFrame) -> List[ConditionSpec]:
    conds: List[ConditionSpec] = [
        ConditionSpec("all", "all events", lambda d: pd.Series(True, index=d.index))
    ]

    try:
        defaults = load_global_defaults()
        if not isinstance(defaults, dict):
            raise ValueError("global defaults spec must decode to a mapping")
        config_cols = defaults.get("defaults", {}).get("conditioning_cols", [])
        if not isinstance(config_cols, list):
            raise ValueError("defaults.conditioning_cols must be a list")
    except Exception as exc:
        raise RuntimeError(f"Failed to load conditioning config from spec registry: {exc}") from exc

    # Add dynamic categorical conditions from events columns
    for col in config_cols:
        if col in events.columns:
            # Skip handled specially
            if col in ["tod_bucket", "anchor_hour", "t_rv_peak"]:
                continue

            value_counts = events[col].dropna().astype(str).value_counts()
            unique_vals = sorted(
                value_counts.index.tolist(), key=lambda v: (-int(value_counts.get(v, 0)), str(v))
            )
            if len(unique_vals) > MAX_DYNAMIC_CONDITIONS_PER_COLUMN:
                LOGGER.warning(
                    "Capping dynamic conditions for %s at %d of %d distinct values",
                    col,
                    MAX_DYNAMIC_CONDITIONS_PER_COLUMN,
                    len(unique_vals),
                )
                unique_vals = unique_vals[:MAX_DYNAMIC_CONDITIONS_PER_COLUMN]
            for val in unique_vals:
                if val == "nan":
                    continue
                conds.append(
                    ConditionSpec(
                        name=f"{col}_{val}",
                        description=f"{col} == {val}",
                        mask_fn=lambda d, c=col, v=val: d[c].astype(str) == v,
                    )
                )

    if "tod_bucket" in events.columns:
        conds.extend(
            [
                ConditionSpec(
                    "session_asia",
                    "enter hour in [0,7]",
                    lambda d: d["tod_bucket"].between(0, 7, inclusive="both"),
                ),
                ConditionSpec(
                    "session_eu",
                    "enter hour in [8,15]",
                    lambda d: d["tod_bucket"].between(8, 15, inclusive="both"),
                ),
                ConditionSpec(
                    "session_us",
                    "enter hour in [16,23]",
                    lambda d: d["tod_bucket"].between(16, 23, inclusive="both"),
                ),
            ]
        )
    elif "anchor_hour" in events.columns:
        conds.extend(
            [
                ConditionSpec(
                    "session_asia",
                    "enter hour in [0,7]",
                    lambda d: d["anchor_hour"].between(0, 7, inclusive="both"),
                ),
                ConditionSpec(
                    "session_eu",
                    "enter hour in [8,15]",
                    lambda d: d["anchor_hour"].between(8, 15, inclusive="both"),
                ),
                ConditionSpec(
                    "session_us",
                    "enter hour in [16,23]",
                    lambda d: d["anchor_hour"].between(16, 23, inclusive="both"),
                ),
            ]
        )

    if "t_rv_peak" in events.columns:
        conds.extend(
            [
                ConditionSpec(
                    "age_bucket_0_8",
                    "t_rv_peak in [0,8]",
                    lambda d: d["t_rv_peak"].fillna(10**9).between(0, 8, inclusive="both"),
                ),
                ConditionSpec(
                    "age_bucket_9_30",
                    "t_rv_peak in [9,30]",
                    lambda d: d["t_rv_peak"].fillna(10**9).between(9, 30, inclusive="both"),
                ),
                ConditionSpec(
                    "age_bucket_31_96",
                    "t_rv_peak in [31,96]",
                    lambda d: d["t_rv_peak"].fillna(10**9).between(31, 96, inclusive="both"),
                ),
            ]
        )
    if "rv_decay_half_life" in events.columns:
        conds.append(
            ConditionSpec(
                "near_half_life",
                "rv_decay_half_life <= 30",
                lambda d: d["rv_decay_half_life"].fillna(10**9) <= 30,
            )
        )
    if {"t_rv_peak", "duration_bars"}.issubset(events.columns):
        conds.extend(
            [
                ConditionSpec(
                    "fractional_age_0_33",
                    "t_rv_peak / duration_bars <= 0.33",
                    lambda d: (
                        (
                            d["t_rv_peak"].fillna(10**9) / d["duration_bars"].replace(0, np.nan)
                        ).fillna(10**9)
                        <= 0.33
                    ),
                ),
                ConditionSpec(
                    "fractional_age_34_66",
                    "t_rv_peak / duration_bars in (0.33, 0.66]",
                    lambda d: (
                        (
                            (
                                d["t_rv_peak"].fillna(10**9) / d["duration_bars"].replace(0, np.nan)
                            ).fillna(10**9)
                            > 0.33
                        )
                        & (
                            (
                                d["t_rv_peak"].fillna(10**9) / d["duration_bars"].replace(0, np.nan)
                            ).fillna(10**9)
                            <= 0.66
                        )
                    ),
                ),
                ConditionSpec(
                    "fractional_age_67_100",
                    "t_rv_peak / duration_bars > 0.66",
                    lambda d: (
                        (
                            d["t_rv_peak"].fillna(10**9) / d["duration_bars"].replace(0, np.nan)
                        ).fillna(10**9)
                        > 0.66
                    ),
                ),
            ]
        )

    seen = set()
    out = []
    for cond in conds:
        if cond.name in seen:
            continue
        seen.add(cond.name)
        out.append(cond)
    return out


def build_actions() -> List[ActionSpec]:
    return [
        ActionSpec("no_action", "baseline", {}),
        ActionSpec("entry_gate_skip", "entry_gating", {"k": 0.0}),
        ActionSpec("risk_throttle_0.5", "risk_throttle", {"k": 0.5}),
        ActionSpec("risk_throttle_0", "risk_throttle", {"k": 0.0}),
        ActionSpec("delay_0", "timing", {"delay_bars": 0}),
        ActionSpec("delay_8", "timing", {"delay_bars": 8}),
        ActionSpec("delay_30", "timing", {"delay_bars": 30}),
        ActionSpec("reenable_at_half_life", "timing", {"landmark": "rv_decay_half_life"}),
    ]


def candidate_type_from_action(action_name: str) -> str:
    action = str(action_name or "").strip().lower()
    if action == "entry_gate_skip" or action.startswith("risk_throttle_"):
        return "overlay"
    if action == "no_action" or action.startswith("delay_") or action == "reenable_at_half_life":
        return "standalone"
    return "standalone"


def assign_candidate_types_and_overlay_bases(
    candidates: pd.DataFrame, event_type: str
) -> pd.DataFrame:
    if candidates.empty:
        return candidates
    out = candidates.copy()
    action_series = (
        out["action"] if "action" in out.columns else pd.Series("", index=out.index, dtype=str)
    )
    out["candidate_type"] = action_series.astype(str).map(candidate_type_from_action)
    out["overlay_base_candidate_id"] = ""

    no_action_rows = out[action_series.astype(str) == "no_action"]
    base_by_condition: Dict[str, str] = {}
    for _, row in no_action_rows.iterrows():
        cond = str(row.get("condition", "")).strip()
        candidate_id = str(row.get("candidate_id", "")).strip()
        if cond and candidate_id and cond not in base_by_condition:
            base_by_condition[cond] = candidate_id
    fallback_base = f"BASE_TEMPLATE::{str(event_type).strip().lower()}"
    overlay_mask = out["candidate_type"].astype(str) == "overlay"
    for idx in out[overlay_mask].index:
        condition = str(out.at[idx, "condition"]).strip() if "condition" in out.columns else ""
        out.at[idx, "overlay_base_candidate_id"] = base_by_condition.get(condition, fallback_base)
    return out


def attach_forward_opportunity(
    events: pd.DataFrame,
    controls: pd.DataFrame,
    run_id: str,
    symbols: List[str],
    timeframe: str,
    horizon_bars: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    DATA_ROOT = get_data_root()
    if events.empty:
        return events, controls

    out_events = events.copy()
    out_controls = controls.copy()
    if "enter_ts" not in out_events.columns:
        for col in ["anchor_ts", "timestamp"]:
            if col in out_events.columns:
                out_events["enter_ts"] = out_events[col]
                break
    if "enter_idx" not in out_events.columns and "start_idx" in out_events.columns:
        out_events["enter_idx"] = pd.to_numeric(out_events["start_idx"], errors="coerce")
    out_events["enter_ts"] = pd.to_datetime(out_events.get("enter_ts"), utc=True, errors="coerce")

    rows = []
    for symbol in symbols:
        bars_candidates = [
            run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", "perp", symbol, f"bars_{timeframe}"),
            DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / f"bars_{timeframe}",
        ]
        bars_dir = choose_partition_dir(bars_candidates)
        bars = read_parquet(list_parquet_files(bars_dir)) if bars_dir else pd.DataFrame()
        if bars.empty:
            continue
        bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True, errors="coerce")
        bars = bars.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        dupes = int(bars["timestamp"].duplicated(keep="last").sum())
        if dupes > 0:
            logging.warning(
                "Dropping %s duplicate bars for %s (%s) before forward opportunity join.",
                dupes,
                symbol,
                timeframe,
            )
            bars = bars.drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
        close = bars["close"].astype(float)
        fwd_abs_return = (close.shift(-horizon_bars) / close - 1.0).abs()
        rows.append(
            pd.DataFrame(
                {
                    "symbol": symbol,
                    "bar_idx": np.arange(len(bars), dtype=int),
                    "timestamp": bars["timestamp"],
                    "forward_abs_return_h": fwd_abs_return,
                }
            )
        )

    if not rows:
        out_events["forward_abs_return_h"] = np.nan
        out_events["forward_abs_return_h_ctrl"] = np.nan
        out_events["opportunity_value_excess"] = np.nan
        return out_events, out_controls

    fwd = pd.concat(rows, ignore_index=True)
    fwd_ts = fwd.rename(columns={"timestamp": "enter_ts"})

    out_events = out_events.merge(
        fwd_ts[["symbol", "enter_ts", "forward_abs_return_h"]],
        on=["symbol", "enter_ts"],
        how="left",
        validate="many_to_one",
    )
    if "enter_idx" in out_events.columns:
        out_events = out_events.merge(
            fwd[["symbol", "bar_idx", "forward_abs_return_h"]].rename(
                columns={"bar_idx": "enter_idx", "forward_abs_return_h": "forward_abs_return_h_idx"}
            ),
            on=["symbol", "enter_idx"],
            how="left",
            validate="many_to_one",
        )
        out_events["forward_abs_return_h"] = out_events["forward_abs_return_h"].where(
            out_events["forward_abs_return_h"].notna(),
            out_events["forward_abs_return_h_idx"],
        )
        out_events = out_events.drop(columns=["forward_abs_return_h_idx"])

    if (
        not out_controls.empty
        and "event_id" in out_controls.columns
        and "control_idx" in out_controls.columns
    ):
        event_to_symbol = out_events[["event_id", "symbol"]].drop_duplicates()
        if "symbol" not in out_controls.columns:
            out_controls = out_controls.merge(
                event_to_symbol, on="event_id", how="left", validate="many_to_one"
            )
        else:
            out_controls = out_controls.merge(
                event_to_symbol.rename(columns={"symbol": "event_symbol"}),
                on="event_id",
                how="left",
                validate="many_to_one",
            )
            out_controls["symbol"] = out_controls["symbol"].where(
                out_controls["symbol"].notna(),
                out_controls["event_symbol"],
            )
            out_controls = out_controls.drop(columns=["event_symbol"])

        if "symbol" in out_controls.columns:
            out_controls = out_controls.merge(
                fwd[["symbol", "bar_idx", "forward_abs_return_h"]].rename(
                    columns={
                        "bar_idx": "control_idx",
                        "forward_abs_return_h": "forward_abs_return_h_ctrl_row",
                    }
                ),
                on=["symbol", "control_idx"],
                how="left",
                validate="many_to_one",
            )
            ctrl_mean = out_controls.groupby("event_id", as_index=False)[
                "forward_abs_return_h_ctrl_row"
            ].mean()
            out_events = out_events.merge(
                ctrl_mean.rename(
                    columns={"forward_abs_return_h_ctrl_row": "forward_abs_return_h_ctrl"}
                ),
                on="event_id",
                how="left",
            )
        else:
            out_events["forward_abs_return_h_ctrl"] = np.nan
    else:
        out_events["forward_abs_return_h_ctrl"] = np.nan

    out_events["opportunity_value_excess"] = (
        out_events["forward_abs_return_h"] - out_events["forward_abs_return_h_ctrl"]
    )
    out_events["opportunity_value_excess"] = out_events["opportunity_value_excess"].where(
        out_events["opportunity_value_excess"].notna(),
        out_events["forward_abs_return_h"],
    )
    return out_events, out_controls


def attach_event_market_features(
    events: pd.DataFrame,
    run_id: str,
    symbols: List[str],
    timeframe: str = "5m",
) -> pd.DataFrame:
    DATA_ROOT = get_data_root()
    if events.empty:
        return events

    out = events.copy()
    out["enter_ts"] = pd.to_datetime(out.get("enter_ts"), utc=True, errors="coerce")
    if out["enter_ts"].isna().all():
        return out

    context_rows: List[pd.DataFrame] = []
    for symbol in symbols:
        feature_dataset = feature_dataset_dir_name()
        features_candidates = [
            run_scoped_lake_path(
                DATA_ROOT, run_id, "features", "perp", symbol, timeframe, feature_dataset
            ),
            DATA_ROOT / "lake" / "features" / "perp" / symbol / timeframe / feature_dataset,
        ]
        bars_candidates = [
            run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", "perp", symbol, f"bars_{timeframe}"),
            DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / f"bars_{timeframe}",
        ]

        features_src = choose_partition_dir(features_candidates)
        features = (
            read_parquet(list_parquet_files(features_src)) if features_src else pd.DataFrame()
        )
        bars_src = choose_partition_dir(bars_candidates)
        bars = read_parquet(list_parquet_files(bars_src)) if bars_src else pd.DataFrame()

        context_candidates = [
            run_scoped_lake_path(
                DATA_ROOT, run_id, "features", "perp", symbol, timeframe, "market_context"
            ),
            DATA_ROOT / "lake" / "features" / "perp" / symbol / timeframe / "market_context",
            run_scoped_lake_path(DATA_ROOT, run_id, "context", "market_state", symbol, timeframe),
            DATA_ROOT / "lake" / "context" / "market_state" / symbol / timeframe,
        ]
        context_src = choose_partition_dir(context_candidates)
        market_state = (
            read_parquet(list_parquet_files(context_src)) if context_src else pd.DataFrame()
        )

        if features.empty and bars.empty and market_state.empty:
            continue

        if "timestamp" in features.columns:
            features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True, errors="coerce")
        else:
            features["timestamp"] = pd.NaT
        if "timestamp" in bars.columns:
            bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True, errors="coerce")
        else:
            bars["timestamp"] = pd.NaT

        feature_cols = [
            "timestamp",
            "spread_bps",
            "atr_14",
            "quote_volume",
            "funding_rate_scaled",
            "close",
            "high",
            "low",
        ]
        feat = features[[col for col in feature_cols if col in features.columns]].copy()
        if feat.empty:
            feat = pd.DataFrame({"timestamp": pd.Series(dtype="datetime64[ns, UTC]")})
        if "timestamp" not in feat.columns:
            feat["timestamp"] = pd.NaT

        bar_cols = ["timestamp", "close", "high", "low", "quote_volume"]
        bar_view = bars[[col for col in bar_cols if col in bars.columns]].copy()
        if not bar_view.empty:
            feat = feat.merge(bar_view, on="timestamp", how="outer", suffixes=("", "_bar"))
            for col in ["close", "high", "low", "quote_volume"]:
                bar_col = f"{col}_bar"
                if bar_col in feat.columns:
                    if col not in feat.columns:
                        feat[col] = feat[bar_col]
                    else:
                        feat[col] = feat[col].where(feat[col].notna(), feat[bar_col])
                    feat = feat.drop(columns=[bar_col])

        state_cols = [
            "timestamp",
            "vol_regime",
            "vol_regime_code",
            "carry_state",
            "carry_state_code",
            "ms_trend_state",
            "ms_spread_state",
        ]
        state_view = market_state[[col for col in state_cols if col in market_state.columns]].copy()
        if not state_view.empty:
            feat = feat.merge(state_view, on="timestamp", how="outer")

        feat["symbol"] = str(symbol).upper()
        feat["enter_ts"] = pd.to_datetime(feat["timestamp"], utc=True, errors="coerce")
        feat = feat.dropna(subset=["enter_ts"]).drop_duplicates(
            subset=["symbol", "enter_ts"], keep="last"
        )
        keep_cols = [
            "symbol",
            "enter_ts",
            "spread_bps",
            "atr_14",
            "quote_volume",
            "funding_rate_scaled",
            "close",
            "high",
            "low",
            "vol_regime",
            "vol_regime_code",
            "carry_state",
            "carry_state_code",
            "ms_trend_state",
            "ms_spread_state",
        ]
        feat = feat[[col for col in keep_cols if col in feat.columns]]
        context_rows.append(feat)

    if not context_rows:
        return out

    context = pd.concat(context_rows, ignore_index=True).drop_duplicates(
        subset=["symbol", "enter_ts"], keep="last"
    )
    out = out.copy()
    out["enter_ts"] = pd.to_datetime(out["enter_ts"], utc=True, errors="coerce")
    context = context.copy()
    context["enter_ts"] = pd.to_datetime(context["enter_ts"], utc=True, errors="coerce")
    left_parts = []
    for symbol, sub in out.sort_values(["symbol", "enter_ts"]).groupby("symbol", sort=False):
        ctx = context[context["symbol"] == symbol].sort_values("enter_ts")
        if ctx.empty:
            left_parts.append(sub)
            continue
        merged_sub = pd.merge_asof(
            sub, ctx, on="enter_ts", direction="backward", suffixes=("", "_ctx")
        )
        left_parts.append(merged_sub)
    merged = pd.concat(left_parts, ignore_index=True) if left_parts else out.copy()
    for col in [
        "spread_bps",
        "atr_14",
        "quote_volume",
        "funding_rate_scaled",
        "close",
        "high",
        "low",
        "vol_regime",
        "vol_regime_code",
        "carry_state",
        "carry_state_code",
        "ms_trend_state",
        "ms_spread_state",
    ]:
        ctx_col = f"{col}_ctx"
        if ctx_col in merged.columns:
            if col not in merged.columns:
                merged[col] = merged[ctx_col]
            else:
                merged[col] = merged[col].where(merged[col].notna(), merged[ctx_col])
            merged = merged.drop(columns=[ctx_col])
    return merged


def prepare_baseline(events: pd.DataFrame, controls: pd.DataFrame) -> pd.DataFrame:
    out = events.copy()
    out["baseline_mode"] = "event_proxy_only"

    adverse_binary_col = _first_existing(
        out, ["secondary_shock_within_h", "secondary_shock_within", "tail_move_within"]
    )
    adverse_mag_col = _first_existing(out, ["range_pct_96", "range_expansion"])
    opportunity_col = _first_existing(out, ["relaxed_within_96", "forward_abs_return_h"])

    adverse_binary = _numeric_non_negative(out, adverse_binary_col, n=len(out))
    adverse_mag = _numeric_non_negative(out, adverse_mag_col, n=len(out))
    if adverse_binary_col and adverse_mag_col:
        out["adverse_proxy"] = 0.5 * adverse_binary + 0.5 * adverse_mag
    elif adverse_binary_col:
        out["adverse_proxy"] = adverse_binary
    elif adverse_mag_col:
        out["adverse_proxy"] = adverse_mag
    else:
        out["adverse_proxy"] = 0.0

    if opportunity_col:
        out["opportunity_proxy"] = _numeric_non_negative(out, opportunity_col, n=len(out))
    elif adverse_binary_col:
        out["opportunity_proxy"] = (1.0 - adverse_binary).clip(lower=0.0)
    else:
        out["opportunity_proxy"] = 0.0

    time_to_adverse_col = _first_existing(out, ["time_to_secondary_shock", "time_to_tail_move"])
    timing_landmark_col = _first_existing(
        out, ["rv_decay_half_life", "parent_time_to_relax", "time_to_relax"]
    )
    out["time_to_adverse"] = _numeric_any(out, time_to_adverse_col, n=len(out), default=np.nan)
    out["timing_landmark"] = _numeric_any(out, timing_landmark_col, n=len(out), default=np.nan)

    if controls.empty or "event_id" not in controls.columns or "event_id" not in out.columns:
        out["adverse_proxy_ctrl"] = np.nan
        out["opportunity_proxy_ctrl"] = np.nan
        out["adverse_proxy_excess"] = out["adverse_proxy"]
        out["opportunity_proxy_excess"] = out["opportunity_proxy"]
        return out

    numeric_ctrl = controls.groupby("event_id", as_index=False).mean(numeric_only=True)
    ctrl_cols = [
        c
        for c in [
            "secondary_shock_within_h",
            "secondary_shock_within",
            "tail_move_within",
            "range_pct_96",
            "range_expansion",
            "relaxed_within_96",
            "forward_abs_return_h",
            "time_to_secondary_shock",
            "time_to_tail_move",
            "rv_decay_half_life",
            "parent_time_to_relax",
            "time_to_relax",
        ]
        if c in numeric_ctrl.columns
    ]
    if not ctrl_cols:
        out["adverse_proxy_ctrl"] = np.nan
        out["opportunity_proxy_ctrl"] = np.nan
        out["adverse_proxy_excess"] = out["adverse_proxy"]
        out["opportunity_proxy_excess"] = out["opportunity_proxy"]
        return out

    rename_map = {c: f"{c}_ctrl" for c in ctrl_cols}
    merged = out.merge(
        numeric_ctrl[["event_id"] + ctrl_cols].rename(columns=rename_map), on="event_id", how="left"
    )

    adverse_binary_ctrl_col = _first_existing(
        merged,
        ["secondary_shock_within_h_ctrl", "secondary_shock_within_ctrl", "tail_move_within_ctrl"],
    )
    adverse_mag_ctrl_col = _first_existing(merged, ["range_pct_96_ctrl", "range_expansion_ctrl"])
    opportunity_ctrl_col = _first_existing(
        merged, ["relaxed_within_96_ctrl", "forward_abs_return_h_ctrl"]
    )

    adverse_binary_ctrl = _numeric_non_negative(merged, adverse_binary_ctrl_col, n=len(merged))
    adverse_mag_ctrl = _numeric_non_negative(merged, adverse_mag_ctrl_col, n=len(merged))
    if adverse_binary_ctrl_col and adverse_mag_ctrl_col:
        merged["adverse_proxy_ctrl"] = 0.5 * adverse_binary_ctrl + 0.5 * adverse_mag_ctrl
    elif adverse_binary_ctrl_col:
        merged["adverse_proxy_ctrl"] = adverse_binary_ctrl
    elif adverse_mag_ctrl_col:
        merged["adverse_proxy_ctrl"] = adverse_mag_ctrl
    else:
        merged["adverse_proxy_ctrl"] = np.nan

    if opportunity_ctrl_col:
        merged["opportunity_proxy_ctrl"] = _numeric_non_negative(
            merged, opportunity_ctrl_col, n=len(merged)
        )
    else:
        merged["opportunity_proxy_ctrl"] = np.nan

    if "time_to_adverse" not in merged.columns:
        merged["time_to_adverse"] = _numeric_any(
            merged,
            _first_existing(merged, ["time_to_secondary_shock", "time_to_tail_move"]),
            n=len(merged),
            default=np.nan,
        )
    if "timing_landmark" not in merged.columns:
        merged["timing_landmark"] = _numeric_any(
            merged,
            _first_existing(
                merged, ["rv_decay_half_life", "parent_time_to_relax", "time_to_relax"]
            ),
            n=len(merged),
            default=np.nan,
        )

    merged["adverse_proxy_excess"] = merged["adverse_proxy"] - merged["adverse_proxy_ctrl"]
    merged["opportunity_proxy_excess"] = (
        merged["opportunity_proxy"] - merged["opportunity_proxy_ctrl"]
    )
    merged["baseline_mode"] = "matched_controls_excess"
    return merged


def apply_action_proxy(
    sub: pd.DataFrame, action: ActionSpec
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    if "adverse_proxy_excess" in sub.columns:
        adverse = sub["adverse_proxy_excess"].fillna(0).astype(float).to_numpy()
    else:
        sec = sub["secondary_shock_within_h"].fillna(0).astype(float).to_numpy()
        rng = sub["range_pct_96"].fillna(0).astype(float).to_numpy()
        adverse = 0.5 * sec + 0.5 * np.clip(rng, 0.0, None)

    if "opportunity_value_excess" in sub.columns:
        opp_value = sub["opportunity_value_excess"].fillna(0).astype(float).to_numpy()
    elif "opportunity_proxy_excess" in sub.columns:
        opp_value = sub["opportunity_proxy_excess"].fillna(0).astype(float).to_numpy()
    else:
        opp_value = sub["relaxed_within_96"].fillna(0).astype(float).to_numpy()

    if action.name in {"no_action", "delay_0"}:
        expectancy = pd.to_numeric(sub.get("expectancy_proxy"), errors="coerce")
        if expectancy.notna().any():
            base = expectancy.fillna(0.0).to_numpy(dtype=float)
        else:
            opp = pd.to_numeric(sub.get("opportunity_proxy_excess"), errors="coerce").fillna(0.0)
            adv = pd.to_numeric(sub.get("adverse_proxy_excess"), errors="coerce").fillna(0.0)
            base = (opp - adv).to_numpy(dtype=float)
        zeros = np.zeros(len(sub), dtype=float)
        return base, zeros, zeros

    if action.family in {"entry_gating", "risk_throttle"}:
        k = float(action.params.get("k", 1.0))
        exposure_delta = np.full(len(sub), -(1.0 - k), dtype=float)
        adverse_delta = -(1.0 - k) * adverse
        opportunity_delta = exposure_delta * opp_value
        return adverse_delta, opportunity_delta, exposure_delta

    if action.name.startswith("delay_"):
        delay = int(action.params.get("delay_bars", 0))
        t_adverse = (
            _numeric_any(
                sub,
                _first_existing(
                    sub, ["time_to_adverse", "time_to_secondary_shock", "time_to_tail_move"]
                ),
                n=len(sub),
                default=10**9,
            )
            .fillna(10**9)
            .to_numpy()
        )
        adverse_delta = -(t_adverse <= delay).astype(float) * adverse
        exposure_delta = -np.full(len(sub), min(1.0, delay / 96.0), dtype=float)
        opportunity_delta = exposure_delta * opp_value
        return adverse_delta, opportunity_delta, exposure_delta

    if action.name == "reenable_at_half_life":
        t_landmark = (
            _numeric_any(
                sub,
                _first_existing(
                    sub,
                    [
                        "timing_landmark",
                        "rv_decay_half_life",
                        "parent_time_to_relax",
                        "time_to_relax",
                    ],
                ),
                n=len(sub),
                default=10**9,
            )
            .fillna(10**9)
            .to_numpy()
        )
        t_adverse = (
            _numeric_any(
                sub,
                _first_existing(
                    sub, ["time_to_adverse", "time_to_secondary_shock", "time_to_tail_move"]
                ),
                n=len(sub),
                default=10**9,
            )
            .fillna(10**9)
            .to_numpy()
        )
        adverse_delta = -(t_adverse <= t_landmark).astype(float) * adverse
        exposure_delta = -np.clip(t_landmark / 96.0, 0.0, 1.0)
        opportunity_delta = exposure_delta * opp_value
        return adverse_delta, opportunity_delta, exposure_delta

    raise ValueError(f"Unsupported action: {action.name}")


def expectancy_from_effect_vectors(
    adverse_delta_vec: np.ndarray, opp_delta_vec: np.ndarray
) -> float:
    mean_adv = float(np.nanmean(adverse_delta_vec)) if len(adverse_delta_vec) else np.nan
    mean_opp = float(np.nanmean(opp_delta_vec)) if len(opp_delta_vec) else np.nan
    if not np.isfinite(mean_adv):
        return 0.0
    risk_reduction = float(-mean_adv)
    opportunity_cost = float(max(0.0, -mean_opp)) if np.isfinite(mean_opp) else 0.0
    net_benefit = float(risk_reduction - opportunity_cost)
    return float(net_benefit) if np.isfinite(net_benefit) else 0.0


def expectancy_for_action(sub: pd.DataFrame, action: ActionSpec) -> float:
    if sub.empty:
        return 0.0
    if action.name in {"no_action", "delay_0"}:
        base_expectancy = pd.to_numeric(sub.get("expectancy_proxy"), errors="coerce")
        if base_expectancy.notna().any():
            return float(base_expectancy.mean())
        opp = pd.to_numeric(sub.get("opportunity_proxy_excess"), errors="coerce")
        adv = pd.to_numeric(sub.get("adverse_proxy_excess"), errors="coerce")
        proxy = opp.fillna(0.0) - adv.fillna(0.0)
        return float(proxy.mean()) if len(proxy) else 0.0
    adverse_delta_vec, opp_delta_vec, _ = apply_action_proxy(sub, action)
    return expectancy_from_effect_vectors(adverse_delta_vec, opp_delta_vec)


def combine_with_delay_override(sub: pd.DataFrame, action: ActionSpec, delay_bars: int) -> float:
    if sub.empty:
        return 0.0
    base_value = expectancy_for_action(sub, action)
    if int(delay_bars) <= 0:
        return base_value
    delay_action = ActionSpec(
        name=f"delay_{int(delay_bars)}",
        family="timing",
        params={"delay_bars": int(delay_bars)},
    )
    delay_adv, delay_opp, _ = apply_action_proxy(sub, delay_action)
    delay_delta = expectancy_from_effect_vectors(delay_adv, delay_opp)
    return float(base_value + delay_delta)


def compute_drawdown_profile(pnl: np.ndarray) -> float:
    arr = np.asarray(pnl, dtype=float)
    arr = arr[np.isfinite(arr)]
    if len(arr) == 0:
        return 0.0
    cum = np.cumsum(arr)
    running_peak = np.maximum.accumulate(cum)
    drawdown = cum - running_peak
    return float(np.min(drawdown)) if len(drawdown) else 0.0
