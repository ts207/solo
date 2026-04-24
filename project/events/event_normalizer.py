from __future__ import annotations

import json
import logging
from typing import Sequence

import numpy as np
import pandas as pd

from project.core.validation import ts_ns_utc
from project.events.event_specs import (
    _DIRECTION_DEFAULT,
    REGISTRY_EVENT_COLUMNS,
    VALID_DIRECTIONS,
    EventRegistrySpec,
    expected_event_types_for_spec,
)


def filter_phase1_rows_for_event_type(events: pd.DataFrame, event_type: str) -> pd.DataFrame:
    if events.empty or "event_type" not in events.columns:
        return events
    allowed = set(expected_event_types_for_spec(event_type))
    if not allowed:
        return events.iloc[0:0].copy()
    return events[events["event_type"].astype(str).isin(allowed)].copy()


def _empty_registry_events() -> pd.DataFrame:
    out = pd.DataFrame(columns=REGISTRY_EVENT_COLUMNS)
    out["sign"] = pd.to_numeric(out["sign"], errors="coerce").astype("float64")
    return out


def _first_existing_column(df: pd.DataFrame, names: Sequence[str]) -> str | None:
    for name in names:
        if name in df.columns:
            return name
    return None


def _feature_payload(row: pd.Series) -> str:
    keys = [
        "adverse_proxy_excess",
        "opportunity_value_excess",
        "forward_abs_return_h",
        "quote_volume",
        "spread_bps",
        "funding_rate_scaled",
        "range_pct_96",
        "rv_decay_half_life",
        "time_to_secondary_shock",
        "severity",
        "stress_score",
        "depth_drop_pct",
        "shock_return",
        "auc_excess_range",
        "severity_bucket",
        "vol_regime",
        "carry_state",
        "ms_trend_state",
        "ms_spread_state",
    ]
    payload = {}
    for key in keys:
        if key in row.index:
            value = row.get(key)
            if pd.notna(value):
                try:
                    payload[key] = float(value)
                except (TypeError, ValueError):
                    payload[key] = str(value)
    return json.dumps(payload, sort_keys=True)


def normalize_phase1_events(
    events: pd.DataFrame,
    spec: EventRegistrySpec,
    run_id: str,
) -> pd.DataFrame:
    if events.empty:
        return _empty_registry_events()

    out = filter_phase1_rows_for_event_type(events.copy(), spec.event_type)
    if out.empty:
        return _empty_registry_events()

    phenom_col = _first_existing_column(
        out, ["phenom_enter_ts", "anchor_ts", "timestamp", "event_ts", "start_ts", "enter_ts"]
    )
    if phenom_col is None:
        return _empty_registry_events()

    out["phenom_enter_ts"] = ts_ns_utc(out[phenom_col])

    entry_col = _first_existing_column(
        out,
        ["enter_ts", "signal_ts", "trigger_ts", "timestamp", "anchor_ts", "event_ts", "start_ts"],
    )
    if entry_col is not None:
        out["enter_ts"] = ts_ns_utc(out[entry_col])
    else:
        out["enter_ts"] = out["phenom_enter_ts"]

    exit_col = _first_existing_column(
        out,
        [
            "exit_ts",
            "end_ts",
            "event_end_ts",
            "relax_ts",
            "norm_ts",
            "end_time",
            "exit_time",
        ],
    )
    if exit_col is not None:
        out["exit_ts"] = ts_ns_utc(out[exit_col])
    else:
        out["exit_ts"] = out["enter_ts"]

    det_col = _first_existing_column(
        out, ["detected_ts", "detection_ts", "signal_ts", "trigger_ts"]
    )
    if det_col is not None:
        out["detected_ts"] = ts_ns_utc(out[det_col])
    else:
        out["detected_ts"] = out["phenom_enter_ts"]

    sig_col = _first_existing_column(out, ["signal_ts"])
    if sig_col is not None and sig_col != det_col:
        out["signal_ts"] = ts_ns_utc(out[sig_col])
    else:
        out["signal_ts"] = out["detected_ts"]

    eval_col = _first_existing_column(out, ["eval_bar_ts"])
    if eval_col is not None:
        out["eval_bar_ts"] = ts_ns_utc(out[eval_col])
    else:
        out["eval_bar_ts"] = out["phenom_enter_ts"]

    out["timestamp"] = out["signal_ts"]
    out = out.dropna(subset=["timestamp", "enter_ts", "detected_ts", "signal_ts"]).copy()
    if out.empty:
        return _empty_registry_events()
    out["exit_ts"] = out["exit_ts"].where(out["exit_ts"].notna(), out["enter_ts"])
    out["exit_ts"] = out["exit_ts"].where(out["exit_ts"] >= out["enter_ts"], out["enter_ts"])

    if "direction" in out.columns:
        out["direction"] = out["direction"].fillna(_DIRECTION_DEFAULT).astype(str).str.lower().str.strip()

    if "evt_signal_intensity" not in out.columns and "event_score" in out.columns:
        out["evt_signal_intensity"] = out["event_score"]
    elif "evt_signal_intensity" not in out.columns:
        out["evt_signal_intensity"] = 0.0

    if "symbol" not in out.columns:
        out["symbol"] = "ALL"
    out["symbol"] = out["symbol"].fillna("ALL").astype(str).str.upper().replace("", "ALL")

    if "event_id" not in out.columns:
        if "parent_event_id" in out.columns:
            out["event_id"] = out["parent_event_id"].astype(str)
        else:
            out["event_id"] = [f"{spec.event_type}_{idx:08d}" for idx in range(len(out))]
    out["event_id"] = out["event_id"].fillna("").astype(str)
    missing_ids = out["event_id"].str.len() == 0
    if missing_ids.any():
        out.loc[missing_ids, "event_id"] = [
            f"{spec.event_type}_{idx:08d}" for idx in range(int(missing_ids.sum()))
        ]

    for col in ["direction", "sign"]:
        if col not in out.columns:
            out[col] = _DIRECTION_DEFAULT if col == "direction" else np.nan
        if col == "direction":
            out[col] = out[col].fillna(_DIRECTION_DEFAULT).astype(str).str.lower().str.strip()
            out[col] = out[col].replace(
                {
                    "1": "long",
                    "1.0": "long",
                    "-1": "short",
                    "-1.0": "short",
                    "0": "neutral",
                    "0.0": "neutral",
                    "up": "long",
                    "down": "short",
                    "nan": _DIRECTION_DEFAULT,
                }
            )
            invalid = ~out[col].isin(VALID_DIRECTIONS)
            if invalid.any():
                bad_vals = out.loc[invalid, col].unique()[:5]
                logging.getLogger(__name__).warning(
                    "Event %s has %d rows with invalid direction values: %s → defaulting to '%s'",
                    spec.event_type,
                    int(invalid.sum()),
                    list(bad_vals),
                    _DIRECTION_DEFAULT,
                )
                out.loc[invalid, col] = _DIRECTION_DEFAULT
        else:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")
    if "split_label" not in out.columns:
        out["split_label"] = np.nan

    out = out.sort_values(["timestamp", "symbol", "event_id"]).reset_index(drop=True)
    out["features_at_event"] = out.apply(_feature_payload, axis=1)

    result_dict = {
        "run_id": str(run_id),
        "event_type": spec.event_type,
        "signal_column": spec.signal_column,
    }
    for col in REGISTRY_EVENT_COLUMNS:
        if col in result_dict:
            continue
        if col in out.columns:
            result_dict[col] = out[col]
        else:
            result_dict[col] = None

    result = pd.DataFrame(result_dict)
    result = result.drop_duplicates(
        subset=["event_type", "timestamp", "symbol", "event_id"]
    ).reset_index(drop=True)
    return result


def normalize_registry_events_frame(events: pd.DataFrame) -> pd.DataFrame:
    if events is None or events.empty:
        return _empty_registry_events()

    out = events.copy()
    for column in REGISTRY_EVENT_COLUMNS:
        if column not in out.columns:
            out[column] = None
    out = out[REGISTRY_EVENT_COLUMNS].copy()
    out["run_id"] = out["run_id"].fillna("").astype(str)
    out["event_type"] = out["event_type"].fillna("").astype(str)
    out["signal_column"] = out["signal_column"].fillna("").astype(str)

    out["timestamp"] = ts_ns_utc(out["timestamp"])
    out["phenom_enter_ts"] = ts_ns_utc(out.get("phenom_enter_ts"), allow_nat=True).fillna(
        out["timestamp"]
    )
    out["eval_bar_ts"] = ts_ns_utc(out.get("eval_bar_ts"), allow_nat=True).fillna(out["timestamp"])
    out["detected_ts"] = ts_ns_utc(out.get("detected_ts"), allow_nat=True).fillna(
        out["phenom_enter_ts"]
    )
    out["signal_ts"] = ts_ns_utc(out.get("signal_ts"), allow_nat=True).fillna(out["detected_ts"])
    out["enter_ts"] = ts_ns_utc(out.get("enter_ts"), allow_nat=True).fillna(out["signal_ts"])
    out["exit_ts"] = ts_ns_utc(out.get("exit_ts"), allow_nat=True).fillna(out["enter_ts"])

    out["enter_ts"] = out["enter_ts"].where(out["enter_ts"].notna(), out["timestamp"])
    out["phenom_enter_ts"] = out["phenom_enter_ts"].where(
        out["phenom_enter_ts"].notna(), out["enter_ts"]
    )
    out["exit_ts"] = out["exit_ts"].where(out["exit_ts"].notna(), out["enter_ts"])
    out["exit_ts"] = np.maximum(out["exit_ts"], out["enter_ts"])
    out = out.dropna(subset=["timestamp"]).copy()
    out["symbol"] = out["symbol"].fillna("ALL").astype(str).str.upper().replace("", "ALL")
    out["event_id"] = out["event_id"].fillna("").astype(str)
    out["features_at_event"] = out["features_at_event"].fillna("{}").astype(str)
    out["evt_signal_intensity"] = pd.to_numeric(out.get("evt_signal_intensity"), errors="coerce").astype(float).fillna(out.get("event_score", 0.0))
    for col in ("direction",):
        out[col] = out[col].fillna(_DIRECTION_DEFAULT).astype(str)
    for col in ("sign",):
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")
    out = out.drop_duplicates(subset=["event_type", "timestamp", "symbol", "event_id"]).copy()
    out = out.sort_values(["timestamp", "symbol", "event_type", "event_id"]).reset_index(drop=True)
    return out[REGISTRY_EVENT_COLUMNS]
