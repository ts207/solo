"""
Phase 2 Gating Logic: Expectancy calculation, FDR adjustment, and Drawdown gating.
Refactored to improve testability and separate concerns.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from scipy import stats
except ModuleNotFoundError:
    from project.core.stats import stats

from project.core.stats import bh_adjust, newey_west_t_stat_for_mean
from project.core.constants import parse_horizon_bars
from project.core.validation import ts_ns_utc
from project.research.direction_semantics import resolve_effect_sign
from project.research.holdout_integrity import assert_no_lookahead_join
from project.research.helpers.shrinkage import (
    _asymmetric_tau_days,
    _effective_sample_size,
    _event_direction_from_joined_row,
    _regime_conditioned_tau_days,
    _time_decay_weights,
)

log = logging.getLogger(__name__)

_VALID_SPLIT_LABELS = {"train", "validation", "test"}


def _normalized_split_label(row: Dict[str, Any]) -> str:
    if "evt_split_label" in row:
        raw = row.get("evt_split_label")
    elif "split_label" in row:
        raw = row.get("split_label")
    else:
        return "train"
    label = str(raw).strip().lower()
    return label if label in _VALID_SPLIT_LABELS else "unknown"


def distribution_stats(returns: np.ndarray) -> Dict[str, float]:
    """Compute mean, std, HAC t-stat, p-value for a return distribution."""
    clean = np.asarray(returns, dtype=float)
    clean = clean[np.isfinite(clean)]
    if len(clean) < 2:
        return {"mean": 0.0, "std": 0.0, "t_stat": 0.0, "p_value": 1.0}
    mean = float(np.mean(clean))
    std = float(np.std(clean, ddof=1))
    if std == 0:
        return {"mean": mean, "std": 0.0, "t_stat": 0.0, "p_value": 1.0}

    nw = newey_west_t_stat_for_mean(clean)
    t_stat = float(nw.t_stat) if np.isfinite(nw.t_stat) else mean / (std / np.sqrt(len(clean)))
    p_value = one_sided_p_from_t(t_stat, df=max(len(clean) - 1, 1))
    return {"mean": mean, "std": std, "t_stat": t_stat, "p_value": p_value}


def one_sided_p_from_t(t_stat: float, df: int) -> float:
    """Compute right-tail (one-sided) p-value. Large negative t-stat -> p close to 1.0."""
    if df < 1:
        return 1.0
    return float(stats.t.sf(t_stat, df=df))


def two_sided_p_from_t(t_stat: float, df: int) -> float:
    """
    DEPRECATED: Now aliased to one_sided_p_from_t to ensure all directional hypotheses
    are gated correctly in the research pipeline. Large negative t-stats will now
    receive high p-values (approaching 1.0) rather than low p-values.

    This function will be removed in a future release. Callers must migrate to
    one_sided_p_from_t. In production environments where DeprecationWarnings are
    suppressed, an ERROR-level log is also emitted to ensure visibility.
    """
    import logging as _logging
    import warnings
    _msg = (
        "two_sided_p_from_t is deprecated; use one_sided_p_from_t for directional hypotheses. "
        "Results produced before this function was aliased may have incorrectly passed gating "
        "on strongly negative t-stats."
    )
    warnings.warn(_msg, DeprecationWarning, stacklevel=2)
    _logging.getLogger(__name__).error("DEPRECATED CALL: %s", _msg)
    return one_sided_p_from_t(t_stat, df=df)


def horizon_to_bars(horizon: str) -> int:
    return parse_horizon_bars(horizon, default=12)


def join_events_to_features(
    events_df: pd.DataFrame,
    features_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge event timestamps to the features table using a backward merge.
    """
    ts_col = "enter_ts" if "enter_ts" in events_df.columns else "timestamp"
    if ts_col not in events_df.columns:
        return pd.DataFrame()

    evt = events_df.copy()
    alias_columns = {
        "evt_split_label": "split_label",
        "evt_vol_regime": "vol_regime",
        "evt_liquidity_state": "liquidity_state",
        "evt_market_liquidity_state": "market_liquidity_state",
        "evt_depth_state": "depth_state",
    }
    for src, dst in alias_columns.items():
        if src in evt.columns and dst not in evt.columns:
            evt[dst] = evt[src]
    evt["event_ts"] = ts_ns_utc(evt[ts_col], allow_nat=True)
    evt = evt.dropna(subset=["event_ts"]).sort_values("event_ts").reset_index(drop=True)
    if evt.empty:
        return pd.DataFrame()

    if "timestamp" not in features_df.columns:
        return pd.DataFrame()
    feat = features_df.copy()
    feat["feature_ts"] = ts_ns_utc(feat["timestamp"], allow_nat=True)
    feat = feat.dropna(subset=["feature_ts"]).sort_values("feature_ts").reset_index(drop=True)
    if feat.empty:
        return pd.DataFrame()
    feat["_feature_pos"] = feat.index.astype(int)

    # Use merge_asof: for each event, find the latest feature bar <= event_ts
    extra_evt_cols = [
        col
        for col in (
            "vol_regime",
            "liquidity_state",
            "market_liquidity_state",
            "depth_state",
            "event_direction",
            "direction",
            "signal_direction",
            "flow_direction",
            "breakout_direction",
            "shock_direction",
            "move_direction",
            "leader_direction",
            "return_1",
            "return_sign",
            "sign",
            "polarity",
            "funding_z",
            "basis_z",
            "side",
            "trade_side",
            "direction_label",
            "split_label",
        )
        if col in evt.columns
    ]

    evt_cols = ["event_ts"] + extra_evt_cols
    evt_for_join = evt[evt_cols].rename(columns={c: f"evt_{c}" for c in extra_evt_cols})

    merged = pd.merge_asof(
        evt_for_join,
        feat,
        left_on="event_ts",
        right_on="feature_ts",
        direction="backward",
    )
    assert_no_lookahead_join(
        merged,
        event_ts_col="event_ts",
        feature_ts_col="feature_ts",
        context="project.research.gating.join_events_to_features",
    )
    return merged


def empty_expectancy_stats() -> Dict[str, Any]:
    return {
        "mean_return": 0.0,
        "p_value": 1.0,
        "n_events": 0.0,
        "n_effective": 0.0,
        "stability_pass": False,
        "std_return": 0.0,
        "t_stat": 0.0,
        "time_weight_sum": 0.0,
        "mean_weight_age_days": 0.0,
        "mean_tau_days": 0.0,
        "learning_rate_mean": 0.0,
        "mean_tau_up_days": 0.0,
        "mean_tau_down_days": 0.0,
        "tau_directional_ratio": 0.0,
        "directional_up_share": 0.0,
        "mean_train_return": 0.0,
        "mean_validation_return": 0.0,
        "mean_test_return": 0.0,
        "train_samples": 0,
        "validation_samples": 0,
        "test_samples": 0,
        "t_train": 0.0,
        "t_validation": 0.0,
        "t_test": 0.0,
    }


def _extract_event_returns(
    merged: pd.DataFrame,
    feat_close: np.ndarray,
    entry_lag_bars: int,
    horizon_bars: int,
    shift_labels_k: int,
    rule: str,
    side_policy: str,
    label_target: str,
    canonical_family: str,
) -> Dict[str, List[Any]]:
    event_returns: List[float] = []
    event_ts_list: List[pd.Timestamp] = []
    event_vol_list: List[Any] = []
    event_liq_list: List[Any] = []
    event_dir_list: List[int] = []
    event_split_list: List[str] = []

    for row in merged.to_dict("records"):
        feature_pos = row.get("_feature_pos")
        if pd.isna(feature_pos):
            continue
        pos = int(feature_pos)
        entry_pos = pos + int(entry_lag_bars)
        future_pos = entry_pos + horizon_bars + shift_labels_k
        if (
            pos < 0
            or pos >= len(feat_close)
            or entry_pos < 0
            or entry_pos >= len(feat_close)
            or future_pos >= len(feat_close)
        ):
            continue

        close_t0 = feat_close[entry_pos]
        close_fwd = feat_close[future_pos]
        if close_t0 == 0 or pd.isna(close_t0) or pd.isna(close_fwd):
            continue

        fwd_ret = (close_fwd / close_t0) - 1.0
        event_direction = _event_direction_from_joined_row(
            row, canonical_family=canonical_family, fallback_direction=1
        )
        d_sign = resolve_effect_sign(
            template_verb=str(rule),
            side_policy=str(side_policy or "both"),
            event_direction=event_direction,
            label_target=str(label_target or "fwd_return_h"),
            fallback_sign=1,
        )
        event_returns.append(float(fwd_ret) * d_sign)
        event_ts_list.append(pd.to_datetime(row.get("event_ts"), utc=True))
        event_vol_list.append(row.get("evt_vol_regime", row.get("vol_regime", "")))
        event_liq_list.append(
            row.get(
                "evt_liquidity_state",
                row.get(
                    "evt_market_liquidity_state",
                    row.get(
                        "evt_depth_state",
                        row.get(
                            "liquidity_state",
                            row.get(
                                "market_liquidity_state",
                                row.get("depth_state", ""),
                            ),
                        ),
                    ),
                ),
            )
        )
        event_dir_list.append(event_direction)
        event_split_list.append(_normalized_split_label(row))

    return {
        "returns": event_returns,
        "timestamps": event_ts_list,
        "vol_regimes": event_vol_list,
        "liq_states": event_liq_list,
        "directions": event_dir_list,
        "splits": event_split_list,
    }


def _threshold_from_bps_or_atr(
    *,
    bps: Optional[float],
    atr_multiplier: Optional[float],
    atr_value: object,
    entry_price: float,
) -> Optional[float]:
    candidates: List[float] = []
    if bps is not None and np.isfinite(float(bps)) and float(bps) > 0.0:
        candidates.append(float(bps) / 10_000.0)
    atr_numeric = pd.to_numeric(pd.Series([atr_value]), errors="coerce").iloc[0]
    if (
        atr_multiplier is not None
        and np.isfinite(float(atr_multiplier))
        and float(atr_multiplier) > 0.0
        and pd.notna(atr_numeric)
        and entry_price > 0.0
    ):
        candidates.append(float(float(atr_multiplier) * float(atr_numeric) / entry_price))
    if not candidates:
        return None
    return float(min(candidates))


def _realized_signed_return_from_path(
    *,
    price_path: np.ndarray,
    direction_sign: float,
    stop_loss_bps: Optional[float] = None,
    take_profit_bps: Optional[float] = None,
    stop_loss_atr_multipliers: Optional[float] = None,
    take_profit_atr_multipliers: Optional[float] = None,
    atr_value: object = None,
) -> float:
    if price_path.size < 2:
        return 0.0
    entry_price = float(price_path[0])
    if not np.isfinite(entry_price) or entry_price <= 0.0:
        return 0.0

    signed_path = ((price_path / entry_price) - 1.0) * float(direction_sign)
    stop_threshold = _threshold_from_bps_or_atr(
        bps=stop_loss_bps,
        atr_multiplier=stop_loss_atr_multipliers,
        atr_value=atr_value,
        entry_price=entry_price,
    )
    take_threshold = _threshold_from_bps_or_atr(
        bps=take_profit_bps,
        atr_multiplier=take_profit_atr_multipliers,
        atr_value=atr_value,
        entry_price=entry_price,
    )

    exit_idx = signed_path.size - 1
    for idx in range(1, signed_path.size):
        signed_return = float(signed_path[idx])
        if stop_threshold is not None and signed_return <= -float(stop_threshold):
            exit_idx = idx
            break
        if take_threshold is not None and signed_return >= float(take_threshold):
            exit_idx = idx
            break
    return float(signed_path[exit_idx])


def build_event_return_frame(
    sym_events: pd.DataFrame,
    features_df: pd.DataFrame,
    *,
    rule: str,
    horizon: str,
    side_policy: str = "both",
    label_target: str = "fwd_return_h",
    canonical_family: str = "",
    shift_labels_k: int = 0,
    entry_lag_bars: int = 1,
    horizon_bars_override: Optional[int] = None,
    stop_loss_bps: Optional[float] = None,
    take_profit_bps: Optional[float] = None,
    stop_loss_atr_multipliers: Optional[float] = None,
    take_profit_atr_multipliers: Optional[float] = None,
    cost_bps: float = 0.0,
    direction_override: Optional[float] = None,
) -> pd.DataFrame:
    if sym_events.empty or features_df.empty:
        return pd.DataFrame()
    if int(entry_lag_bars) < 1:
        raise ValueError("entry_lag_bars must be >= 1 to prevent same-bar entry leakage")

    if "timestamp" not in features_df.columns:
        return pd.DataFrame()

    if "close" not in features_df.columns:
        return pd.DataFrame()

    features_sorted = features_df.sort_values("timestamp").reset_index(drop=True)
    merged = join_events_to_features(sym_events, features_sorted)
    feat_close = features_sorted["close"].astype(float).to_numpy()
    return _build_event_return_frame_from_joined(
        merged,
        feat_close,
        rule=rule,
        horizon=horizon,
        side_policy=side_policy,
        label_target=label_target,
        canonical_family=canonical_family,
        shift_labels_k=shift_labels_k,
        entry_lag_bars=entry_lag_bars,
        horizon_bars_override=horizon_bars_override,
        stop_loss_bps=stop_loss_bps,
        take_profit_bps=take_profit_bps,
        stop_loss_atr_multipliers=stop_loss_atr_multipliers,
        take_profit_atr_multipliers=take_profit_atr_multipliers,
        cost_bps=cost_bps,
        direction_override=direction_override,
    )


def _build_event_return_frame_from_joined(
    merged: pd.DataFrame,
    feat_close: np.ndarray,
    *,
    rule: str,
    horizon: str,
    side_policy: str = "both",
    label_target: str = "fwd_return_h",
    canonical_family: str = "",
    shift_labels_k: int = 0,
    entry_lag_bars: int = 1,
    horizon_bars_override: Optional[int] = None,
    stop_loss_bps: Optional[float] = None,
    take_profit_bps: Optional[float] = None,
    stop_loss_atr_multipliers: Optional[float] = None,
    take_profit_atr_multipliers: Optional[float] = None,
    cost_bps: float = 0.0,
    direction_override: Optional[float] = None,
) -> pd.DataFrame:
    if merged.empty:
        return pd.DataFrame()
    if int(entry_lag_bars) < 1:
        raise ValueError("entry_lag_bars must be >= 1 to prevent same-bar entry leakage")

    horizon_bars = (
        int(horizon_bars_override)
        if horizon_bars_override is not None
        else horizon_to_bars(horizon)
    )
    horizon_bars = max(1, int(horizon_bars))
    if feat_close.size == 0 or "close" not in merged.columns:
        return pd.DataFrame()
    records: List[Dict[str, Any]] = []
    per_trade_cost = max(0.0, float(cost_bps)) / 10_000.0

    def _funding_carry_return(row: Dict[str, Any], direction_sign: float) -> tuple[float, bool]:
        for key in ("funding_rate_realized", "funding_rate_scaled", "funding_rate"):
            value = row.get(key)
            if value is None:
                continue
            numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
            if pd.isna(numeric):
                continue
            return float(-direction_sign * float(numeric)), True
        for key in ("funding_rate_bps",):
            value = row.get(key)
            if value is None:
                continue
            numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
            if pd.isna(numeric):
                continue
            return float(-direction_sign * float(numeric) / 10_000.0), True
        return 0.0, False

    for row in merged.to_dict("records"):
        feature_pos = row.get("_feature_pos")
        if pd.isna(feature_pos):
            continue
        pos = int(feature_pos)
        entry_pos = pos + int(entry_lag_bars)
        future_pos = entry_pos + horizon_bars + int(shift_labels_k)
        if (
            pos < 0
            or entry_pos < 0
            or entry_pos >= len(feat_close)
            or future_pos >= len(feat_close)
        ):
            continue

        if direction_override is not None and pd.notna(direction_override):
            direction_sign = float(direction_override)
            event_direction = 1  # dummy for records
        else:
            event_direction = _event_direction_from_joined_row(
                row,
                canonical_family=canonical_family,
                fallback_direction=1,
            )
            direction_sign = resolve_effect_sign(
                template_verb=str(rule),
                side_policy=str(side_policy or "both"),
                event_direction=event_direction,
                label_target=str(label_target or "fwd_return_h"),
                fallback_sign=1,
            )
        price_path = feat_close[entry_pos : future_pos + 1]
        raw_return = _realized_signed_return_from_path(
            price_path=price_path,
            direction_sign=direction_sign,
            stop_loss_bps=stop_loss_bps,
            take_profit_bps=take_profit_bps,
            stop_loss_atr_multipliers=stop_loss_atr_multipliers,
            take_profit_atr_multipliers=take_profit_atr_multipliers,
            atr_value=row.get("atr_14", row.get("atr")),
        )
        funding_carry_return, funding_carry_present = _funding_carry_return(row, direction_sign)
        event_ts = pd.to_datetime(row.get("event_ts"), utc=True, errors="coerce")
        if pd.isna(event_ts):
            continue
        records.append(
            {
                "event_ts": event_ts,
                "cluster_day": event_ts.strftime("%Y-%m-%d"),
                "split_label": _normalized_split_label(row),
                "vol_regime": row.get("evt_vol_regime", row.get("vol_regime", "")),
                "liquidity_state": row.get(
                    "evt_liquidity_state",
                    row.get(
                        "evt_market_liquidity_state",
                        row.get(
                            "evt_depth_state",
                            row.get(
                                "liquidity_state",
                                row.get(
                                    "market_liquidity_state",
                                    row.get("depth_state", ""),
                                ),
                            ),
                        ),
                    ),
                ),
                "event_direction": int(event_direction),
                "direction_sign": float(direction_sign),
                "forward_return_raw": float(raw_return),
                "funding_carry_return": float(funding_carry_return),
                "funding_carry_present": bool(funding_carry_present),
                "cost_return": float(per_trade_cost - funding_carry_return),
                "forward_return": float(raw_return - per_trade_cost + funding_carry_return),
            }
        )
    if not records:
        return pd.DataFrame()
    return pd.DataFrame.from_records(records)


def _calculate_weights(
    ts_series: pd.Series,
    event_vol_list: List[Any],
    event_liq_list: List[Any],
    event_dir_list: List[int],
    canonical_family: str,
    time_decay_tau_seconds: Optional[float],
    time_decay_floor_weight: float,
    regime_conditioned_decay: bool,
    directional_asymmetry_decay: bool,
    regime_tau_smoothing_alpha: float,
    directional_tau_smoothing_alpha: float,
    regime_tau_min_days: float,
    regime_tau_max_days: float,
    directional_tau_default_up_mult: float,
    directional_tau_default_down_mult: float,
    directional_tau_min_ratio: float,
    directional_tau_max_ratio: float,
) -> pd.Series:
    ref_ts = ts_series.max()
    tau_seconds_default = float(time_decay_tau_seconds or (86400.0 * 60.0))

    if bool(regime_conditioned_decay) or bool(directional_asymmetry_decay):
        tau_days_list: List[float] = []
        prev_tau_days: Optional[float] = None
        alpha = max(
            0.0,
            min(
                1.0,
                float(
                    regime_tau_smoothing_alpha
                    if bool(regime_conditioned_decay)
                    else directional_tau_smoothing_alpha
                ),
            ),
        )
        for vol_regime, liq_state, evt_dir in zip(event_vol_list, event_liq_list, event_dir_list):
            base_tau_days = (
                _regime_conditioned_tau_days(
                    canonical_family=canonical_family,
                    vol_regime=vol_regime,
                    liquidity_state=liq_state,
                    base_tau_days_override=tau_seconds_default / 86400.0,
                )
                if bool(regime_conditioned_decay)
                else tau_seconds_default / 86400.0
            )
            base_tau_days = float(
                np.clip(base_tau_days, float(regime_tau_min_days), float(regime_tau_max_days))
            )
            if bool(directional_asymmetry_decay):
                raw_tau_days, _, _, _ = _asymmetric_tau_days(
                    base_tau_days=base_tau_days,
                    canonical_family=canonical_family,
                    direction=int(evt_dir),
                    default_up_mult=directional_tau_default_up_mult,
                    default_down_mult=directional_tau_default_down_mult,
                    min_ratio=directional_tau_min_ratio,
                    max_ratio=directional_tau_max_ratio,
                )
            else:
                raw_tau_days = base_tau_days
            raw_tau_days = float(
                np.clip(raw_tau_days, float(regime_tau_min_days), float(regime_tau_max_days))
            )
            smoothed = (
                raw_tau_days
                if prev_tau_days is None
                else ((1.0 - alpha) * prev_tau_days) + (alpha * raw_tau_days)
            )
            tau_days_list.append(smoothed)
            prev_tau_days = smoothed

        tau_seconds_arr = np.array(tau_days_list, dtype=float) * 86400.0
        age_seconds = (ref_ts - ts_series).dt.total_seconds().fillna(0.0).clip(lower=0.0).values
        return pd.Series(
            np.maximum(
                np.exp(-age_seconds / np.maximum(tau_seconds_arr, 1e-9)),
                float(time_decay_floor_weight),
            )
        )
    else:
        return _time_decay_weights(
            ts_series,
            ref_ts=ref_ts,
            tau_seconds=tau_seconds_default,
            floor_weight=float(time_decay_floor_weight),
        )


def _calculate_tau_diagnostics(
    event_vol_list: List[Any],
    event_liq_list: List[Any],
    event_dir_list: List[int],
    canonical_family: str,
    time_decay_tau_seconds: Optional[float],
    regime_conditioned_decay: bool,
    directional_asymmetry_decay: bool,
    regime_tau_smoothing_alpha: float,
    directional_tau_smoothing_alpha: float,
    regime_tau_min_days: float,
    regime_tau_max_days: float,
    directional_tau_default_up_mult: float,
    directional_tau_default_down_mult: float,
    directional_tau_min_ratio: float,
    directional_tau_max_ratio: float,
) -> Dict[str, float]:
    tau_seconds_default = float(time_decay_tau_seconds or (86400.0 * 60.0))
    tau_days_default = tau_seconds_default / 86400.0
    tau_days_list: List[float] = []
    tau_up_list: List[float] = []
    tau_down_list: List[float] = []
    ratio_list: List[float] = []
    prev_tau_days: Optional[float] = None
    alpha = max(
        0.0,
        min(
            1.0,
            float(
                regime_tau_smoothing_alpha
                if bool(regime_conditioned_decay)
                else directional_tau_smoothing_alpha
            ),
        ),
    )

    for vol_regime, liq_state, evt_dir in zip(event_vol_list, event_liq_list, event_dir_list):
        base_tau_days = (
            _regime_conditioned_tau_days(
                canonical_family=canonical_family,
                vol_regime=vol_regime,
                liquidity_state=liq_state,
                base_tau_days_override=tau_days_default,
            )
            if bool(regime_conditioned_decay)
            else tau_days_default
        )
        base_tau_days = float(
            np.clip(base_tau_days, float(regime_tau_min_days), float(regime_tau_max_days))
        )
        if bool(directional_asymmetry_decay):
            tau_eff, tau_up, tau_down, ratio = _asymmetric_tau_days(
                base_tau_days=base_tau_days,
                canonical_family=canonical_family,
                direction=int(evt_dir),
                default_up_mult=directional_tau_default_up_mult,
                default_down_mult=directional_tau_default_down_mult,
                min_ratio=directional_tau_min_ratio,
                max_ratio=directional_tau_max_ratio,
            )
        else:
            tau_eff = tau_up = tau_down = base_tau_days
            ratio = 1.0
        tau_eff = float(np.clip(tau_eff, float(regime_tau_min_days), float(regime_tau_max_days)))
        smoothed = (
            tau_eff
            if prev_tau_days is None
            else ((1.0 - alpha) * prev_tau_days) + (alpha * tau_eff)
        )
        tau_days_list.append(smoothed)
        tau_up_list.append(float(tau_up))
        tau_down_list.append(float(tau_down))
        ratio_list.append(float(ratio))
        prev_tau_days = smoothed

    if not tau_days_list:
        return {
            "mean_tau_days": 0.0,
            "learning_rate_mean": 0.0,
            "mean_tau_up_days": 0.0,
            "mean_tau_down_days": 0.0,
            "tau_directional_ratio": 0.0,
            "directional_up_share": 0.0,
        }

    up_share = (
        float(np.mean([1.0 if int(x) >= 0 else 0.0 for x in event_dir_list]))
        if event_dir_list
        else 0.0
    )
    return {
        "mean_tau_days": float(np.mean(tau_days_list)),
        "learning_rate_mean": float(alpha),
        "mean_tau_up_days": float(np.mean(tau_up_list)),
        "mean_tau_down_days": float(np.mean(tau_down_list)),
        "tau_directional_ratio": float(np.mean(ratio_list)),
        "directional_up_share": up_share,
    }


def calculate_expectancy_stats(
    sym_events: pd.DataFrame,
    features_df: pd.DataFrame,
    rule: str,
    horizon: str,
    side_policy: str = "both",
    label_target: str = "fwd_return_h",
    canonical_family: str = "",
    shift_labels_k: int = 0,
    entry_lag_bars: int = 1,
    min_samples: int = 30,
    time_decay_enabled: bool = False,
    time_decay_tau_seconds: Optional[float] = None,
    time_decay_floor_weight: float = 0.02,
    regime_conditioned_decay: bool = False,
    regime_tau_smoothing_alpha: float = 0.15,
    regime_tau_min_days: float = 3.0,
    regime_tau_max_days: float = 365.0,
    directional_asymmetry_decay: bool = False,
    directional_tau_smoothing_alpha: float = 0.15,
    directional_tau_min_ratio: float = 1.5,
    directional_tau_max_ratio: float = 3.0,
    directional_tau_default_up_mult: float = 1.25,
    directional_tau_default_down_mult: float = 0.65,
    horizon_bars_override: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Real PIT expectancy calculation.
    """
    if sym_events.empty or features_df.empty:
        return empty_expectancy_stats()

    if int(entry_lag_bars) < 1:
        raise ValueError("entry_lag_bars must be >= 1 to prevent same-bar entry leakage")

    horizon_bars = (
        int(horizon_bars_override)
        if horizon_bars_override is not None
        else horizon_to_bars(horizon)
    )
    horizon_bars = max(1, int(horizon_bars))

    if "timestamp" not in features_df.columns:
         return empty_expectancy_stats()

    # CRITICAL FIX: Sort features by timestamp to ensure integer indices align
    # with the _feature_pos generated by join_events_to_features.
    # We also ensure the index is reset so .values and integer indexing match.
    features_sorted = features_df.sort_values("timestamp").reset_index(drop=True)

    merged = join_events_to_features(sym_events, features_sorted)
    if merged.empty or "close" not in merged.columns:
        return empty_expectancy_stats()

    extracted = _extract_event_returns(
        merged,
        features_sorted["close"].astype(float).values,
        entry_lag_bars,
        horizon_bars,
        shift_labels_k,
        rule,
        side_policy,
        label_target,
        canonical_family,
    )

    event_returns = extracted["returns"]
    if len(event_returns) < min_samples:
        return {
            **empty_expectancy_stats(),
            "n_events": float(len(event_returns)),
            "n_effective": float(len(event_returns)),
        }

    returns_series = pd.Series(event_returns, dtype=float)
    ts_series = pd.to_datetime(pd.Series(extracted["timestamps"]), utc=True)

    # Weighting logic
    if bool(time_decay_enabled):
        weights = _calculate_weights(
            ts_series,
            extracted["vol_regimes"],
            extracted["liq_states"],
            extracted["directions"],
            canonical_family,
            time_decay_tau_seconds,
            time_decay_floor_weight,
            regime_conditioned_decay,
            directional_asymmetry_decay,
            regime_tau_smoothing_alpha,
            directional_tau_smoothing_alpha,
            regime_tau_min_days,
            regime_tau_max_days,
            directional_tau_default_up_mult,
            directional_tau_default_down_mult,
            directional_tau_min_ratio,
            directional_tau_max_ratio,
        )
    else:
        weights = pd.Series(1.0, index=returns_series.index)

    tau_diagnostics = (
        _calculate_tau_diagnostics(
            extracted["vol_regimes"],
            extracted["liq_states"],
            extracted["directions"],
            canonical_family,
            time_decay_tau_seconds,
            regime_conditioned_decay,
            directional_asymmetry_decay,
            regime_tau_smoothing_alpha,
            directional_tau_smoothing_alpha,
            regime_tau_min_days,
            regime_tau_max_days,
            directional_tau_default_up_mult,
            directional_tau_default_down_mult,
            directional_tau_min_ratio,
            directional_tau_max_ratio,
        )
        if bool(time_decay_enabled)
        else {
            "mean_tau_days": 0.0,
            "learning_rate_mean": 0.0,
            "mean_tau_up_days": 0.0,
            "mean_tau_down_days": 0.0,
            "tau_directional_ratio": 0.0,
            "directional_up_share": 0.0,
        }
    )

    n_eff = _effective_sample_size(weights)
    w_sum = float(weights.sum())
    mean_ret = (
        float((returns_series * weights).sum() / w_sum)
        if w_sum > 0
        else float(returns_series.mean())
    )
    std_ret = (
        float(np.sqrt(max(((weights * (returns_series - mean_ret) ** 2).sum()) / w_sum, 0.0)))
        if w_sum > 0
        else float(returns_series.std())
    )
    
    event_split_list = extracted["splits"]
    if any(split == "unknown" for split in event_split_list):
        log.warning(
            "calculate_expectancy_stats encountered invalid split labels; failing closed for gating."
        )
        return {
            **empty_expectancy_stats(),
            "n_events": float(len(event_returns)),
            "n_effective": float(len(event_returns)),
        }

    # B1: gate t-statistic and p-value are computed on train+validation only.
    # The test split is a pure holdout and must not participate in gate decisions.
    # Events with no split_label default to 'train' (conservative: included in gate).
    gate_mask = np.array([s != "test" for s in event_split_list], dtype=bool)
    if gate_mask.any():
        gate_returns = returns_series[gate_mask]
        gate_weights = weights[gate_mask]
    else:
        gate_returns = returns_series
        gate_weights = weights
    n_gate = int(len(gate_returns))

    # Per-split t-statistics for audit and OOS reporting.
    # These are always unweighted (raw t-test) so they are consistent regardless
    # of whether time-decay weighting is enabled in the main gate path.
    def _split_t(
        split_name: str,
    ) -> tuple[float, int]:
        """Return (t_stat, n_events) for the named split."""
        split_returns = [
            r for r, s in zip(event_returns, event_split_list) if s == split_name
        ]
        if len(split_returns) < 2:
            return 0.0, len(split_returns)
        arr = np.asarray(split_returns, dtype=float)
        arr = arr[np.isfinite(arr)]
        if len(arr) < 2 or np.std(arr, ddof=1) < 1e-12:
            return 0.0, len(split_returns)
        nw_s = newey_west_t_stat_for_mean(arr)
        t = float(nw_s.t_stat) if np.isfinite(nw_s.t_stat) else float(
            np.mean(arr) / (np.std(arr, ddof=1) / np.sqrt(len(arr)))
        )
        return t, len(split_returns)

    t_train_val, train_samples = _split_t("train")
    t_val_val, val_samples = _split_t("validation")
    t_test_val, test_samples_n = _split_t("test")

    # B8: use observation count (not ESS) as degrees of freedom for the NW t-test.
    # The weighted NW SE already adjusts for weight concentration; using ESS as df
    # double-penalises inference, making hypothesis comparisons incoherent across
    # equally-weighted and time-decayed estimation paths.
    nw = newey_west_t_stat_for_mean(
        gate_returns,
        weights=gate_weights if bool(time_decay_enabled) else None,
    )
    t_stat = (
        float(nw.t_stat) if np.isfinite(nw.t_stat)
        else float(mean_ret / (std_ret / np.sqrt(max(float(n_gate), 1.0)))) if std_ret > 0 and n_gate > 1
        else 0.0
    )
    p_value = one_sided_p_from_t(t_stat, int(max(n_gate - 1, 1)))

    # Drawdown and results assembly
    dd_result = max_drawdown_gate(event_returns)

    return {
        "mean_return": mean_ret,
        "p_value": p_value,
        "n_events": float(len(event_returns)),
        "n_effective": float(n_eff),
        "stability_pass": bool(dd_result["gate_max_drawdown"]),
        "std_return": std_ret,
        "t_stat": t_stat,
        "time_weight_sum": float(w_sum),
        "mean_weight_age_days": 0.0,   # populated by tau diagnostics if enabled
        "max_drawdown": dd_result["max_drawdown"],
        "gate_max_drawdown": dd_result["gate_max_drawdown"],
        **tau_diagnostics,
        # Per-split means (for OOS direction-match checks in the promotion gate)
        "mean_train_return": float(
            np.mean([r for r, s in zip(event_returns, event_split_list) if s == "train"])
        )
        if "train" in event_split_list
        else 0.0,
        "mean_validation_return": float(
            np.mean([r for r, s in zip(event_returns, event_split_list) if s == "validation"])
        )
        if "validation" in event_split_list
        else 0.0,
        "mean_test_return": float(
            np.mean([r for r, s in zip(event_returns, event_split_list) if s == "test"])
        )
        if "test" in event_split_list
        else 0.0,
        # Per-split t-statistics and sample counts (S2: audit + DSR evidence quality)
        "t_train": t_train_val,
        "t_validation": t_val_val,
        "t_test": t_test_val,
        "train_samples": train_samples,
        "validation_samples": val_samples,
        "test_samples": test_samples_n,
    }


def calculate_expectancy(
    sym_events: pd.DataFrame,
    features_df: pd.DataFrame,
    rule: str,
    horizon: str,
    shift_labels_k: int = 0,
    entry_lag_bars: int = 1,
    min_samples: int = 30,
) -> Tuple[float, float, float, bool]:
    stats_dict = calculate_expectancy_stats(
        sym_events,
        features_df,
        rule,
        horizon,
        shift_labels_k=shift_labels_k,
        entry_lag_bars=entry_lag_bars,
        min_samples=min_samples,
    )
    return (
        float(stats_dict["mean_return"]),
        float(stats_dict["p_value"]),
        float(stats_dict["n_events"]),
        True,
    )


def max_drawdown_gate(returns: List[float], *, max_dd_ratio: float = 3.0) -> dict:
    arr = np.asarray(returns, dtype="float64")
    arr = arr[np.isfinite(arr)]
    if arr.size < 2:
        return {"max_drawdown": 0.0, "dd_to_expectancy_ratio": 0.0, "gate_max_drawdown": True}
    cumulative = np.concatenate([[0.0], np.cumsum(arr)])
    max_dd = float(np.max(np.maximum.accumulate(cumulative) - cumulative))
    abs_mean = abs(float(np.mean(arr)))
    ratio = max_dd / abs_mean if abs_mean > 1e-12 else 0.0
    return {
        "max_drawdown": max_dd,
        "dd_to_expectancy_ratio": ratio,
        "gate_max_drawdown": bool(ratio <= max_dd_ratio),
    }
