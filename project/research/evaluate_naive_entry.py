from __future__ import annotations

from project.core.config import get_data_root


def get_research_data_root() -> Path:
    return get_data_root()

import argparse
import logging
import re
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.events.event_normalizer import filter_phase1_rows_for_event_type
from project.events.registry import (
    EVENT_REGISTRY_SPECS,
)
from project.io.utils import ensure_dir, read_parquet, write_parquet
from project.research._family_event_utils import load_features
from project.research.gating import horizon_to_bars
from project.research.search.search_feature_utils import (
    normalize_search_feature_columns as _normalize_search_feature_columns,
)
from project.specs.manifest import finalize_manifest, start_manifest

NUMERIC_CONDITION_PATTERN = re.compile(r"^([a-zA-Z0-9_]+)\s*([><!=]=?)\s*([0-9.-]+)$")


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _condition_mask(events: Any, condition: str) -> pd.Series:
    try:
        cond = str(condition or "all").strip()
        if not hasattr(events, "empty") or events.empty:
            return pd.Series([], dtype=bool)
        if not cond or cond.lower() == "all":
            return pd.Series(True, index=events.index)

        # Ensure we have a DataFrame
        df_events = events if isinstance(events, pd.DataFrame) else pd.DataFrame(events)

        match = NUMERIC_CONDITION_PATTERN.match(cond)
        if match:
            feature, operator, raw_threshold = match.groups()
            threshold = _to_float(raw_threshold, np.nan)

            if feature not in df_events.columns:
                return pd.Series(False, index=df_events.index)

            values = pd.to_numeric(df_events[feature], errors="coerce")

            if operator == ">=":
                return pd.Series(values >= threshold, index=df_events.index)
            if operator == "<=":
                return pd.Series(values <= threshold, index=df_events.index)
            if operator == ">":
                return pd.Series(values > threshold, index=df_events.index)
            if operator == "<":
                return pd.Series(values < threshold, index=df_events.index)
            if operator == "==":
                return pd.Series(values == threshold, index=df_events.index)

        lowered = cond.lower()
        if lowered.startswith("symbol_"):
            symbol = cond[len("symbol_") :].strip().upper()
            if "symbol" in df_events.columns:
                return pd.Series(
                    df_events["symbol"].astype(str).str.upper() == symbol, index=df_events.index
                )

        if lowered in {"session_asia", "session_eu", "session_us"}:
            hour_col = None
            if "tod_bucket" in df_events.columns:
                hour_col = pd.to_numeric(df_events["tod_bucket"], errors="coerce")
            elif "anchor_hour" in df_events.columns:
                hour_col = pd.to_numeric(df_events["anchor_hour"], errors="coerce")
            elif "enter_ts" in df_events.columns:
                hour_col = pd.to_datetime(
                    df_events["enter_ts"], utc=True, errors="coerce"
                ).dt.hour.astype(float)
            if hour_col is None:
                return pd.Series(False, index=df_events.index)
            if lowered == "session_asia":
                res = hour_col.between(0, 7, inclusive="both")
            elif lowered == "session_eu":
                res = hour_col.between(8, 15, inclusive="both")
            else:
                res = hour_col.between(16, 23, inclusive="both")
            return pd.Series(res, index=df_events.index)

        if lowered.startswith("bull_bear_") and "bull_bear" in df_events.columns:
            label = lowered.replace("bull_bear_", "", 1)
            return pd.Series(
                df_events["bull_bear"].astype(str).str.lower() == label, index=df_events.index
            )
        if lowered.startswith("vol_regime_") and "vol_regime" in df_events.columns:
            label = lowered.replace("vol_regime_", "", 1).replace("medium", "mid")
            return pd.Series(
                df_events["vol_regime"].astype(str).str.lower().replace({"medium": "mid"}) == label,
                index=df_events.index,
            )

        return pd.Series(False, index=df_events.index)
    except Exception:
        return pd.Series(False, index=getattr(events, "index", []))


def _load_phase1_events(run_id: str, event_type: str) -> pd.DataFrame:
    spec = EVENT_REGISTRY_SPECS.get(str(event_type))
    report_dir = spec.reports_dir if spec is not None else str(event_type)
    file_name = spec.events_file if spec is not None else f"{event_type}_events.csv"
    path = get_research_data_root() / "reports" / report_dir / run_id / file_name
    if not path.exists():
        return pd.DataFrame()
    try:
        if path.suffix == ".parquet":
            df = read_parquet(path)
        else:
            df = pd.read_csv(path)
        if spec is not None:
            df = filter_phase1_rows_for_event_type(df, spec.event_type)
        return df
    except Exception:
        return pd.DataFrame()


def _load_phase2_candidates(run_id: str) -> pd.DataFrame:
    phase2_root = get_research_data_root() / "reports" / "phase2" / run_id
    if not phase2_root.exists():
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for path in sorted(phase2_root.rglob("phase2_candidates.parquet")):
        try:
            frame = read_parquet(path)
        except Exception:
            continue
        if frame.empty:
            continue
        if "event_type" not in frame.columns:
            event_type = ""
            parts = path.relative_to(phase2_root).parts
            if parts:
                if parts[0] == "search_engine":
                    event_type = "search_engine"
                else:
                    event_type = str(parts[0]).strip()
            frame = frame.copy()
            frame["event_type"] = event_type
        frames.append(frame)

    for path in sorted(phase2_root.rglob("phase2_candidates.csv")):
        parquet_path = path.with_suffix(".parquet")
        if parquet_path.exists():
            continue
        try:
            frame = pd.read_csv(path)
        except Exception:
            continue
        if frame.empty:
            continue
        if "event_type" not in frame.columns:
            event_type = ""
            parts = path.relative_to(phase2_root).parts
            if parts:
                if parts[0] == "search_engine":
                    event_type = "search_engine"
                else:
                    event_type = str(parts[0]).strip()
            frame = frame.copy()
            frame["event_type"] = event_type
        frames.append(frame)

    if not frames:
        return pd.DataFrame()

    candidates = pd.concat(frames, ignore_index=True)
    if "gate_bridge_tradable" in candidates.columns:
        candidates = candidates[
            candidates["gate_bridge_tradable"].fillna(False).astype(bool)
        ].copy()
    elif "gate_all_research" in candidates.columns:
        candidates = candidates[candidates["gate_all_research"].fillna(False).astype(bool)].copy()
    elif "gate_all" in candidates.columns:
        candidates = candidates[candidates["gate_all"].fillna(False).astype(bool)].copy()
    return candidates.reset_index(drop=True)


def _parse_transition_event_type(event_type: str) -> tuple[str, str] | None:
    text = str(event_type or "").strip()
    prefix = "TRANSITION_"
    if not text.startswith(prefix):
        return None
    rest = text[len(prefix) :]
    parts = rest.split("_STATE_", 1)
    if len(parts) != 2:
        return None
    from_state = f"{parts[0]}_STATE"
    to_state = parts[1] if parts[1].endswith("_STATE") else f"{parts[1]}_STATE"
    if not from_state or not to_state:
        return None
    return from_state, to_state


def _build_regime_events(run_id: str, symbol: str, event_type: str, horizon: str) -> pd.DataFrame:
    features = load_features(run_id, symbol, "5m", market="perp")
    if features.empty or "timestamp" not in features.columns:
        return pd.DataFrame()
    features = _normalize_search_feature_columns(features)
    working = features.sort_values("timestamp").reset_index(drop=True).copy()
    working["symbol"] = str(symbol).upper()

    horizon_bars = max(1, int(horizon_to_bars(str(horizon or "5m"))))
    if "close" not in working.columns:
        return pd.DataFrame()
    close = pd.to_numeric(working["close"], errors="coerce")
    forward_close = close.shift(-horizon_bars)
    working["forward_return_h"] = (forward_close / close) - 1.0

    if str(event_type).startswith("STATE_"):
        state_id = str(event_type)[len("STATE_") :]
        state_col = state_id.strip().lower()
        if state_col not in working.columns:
            return pd.DataFrame()
        mask = pd.to_numeric(working[state_col], errors="coerce").fillna(0.0) > 0
    elif str(event_type).startswith("TRANSITION_"):
        parsed = _parse_transition_event_type(event_type)
        if not parsed:
            return pd.DataFrame()
        from_state, to_state = parsed
        from_col = str(from_state).strip().lower()
        to_col = str(to_state).strip().lower()
        if to_col not in working.columns:
            return pd.DataFrame()
        to_now = pd.to_numeric(working[to_col], errors="coerce").fillna(0.0) > 0
        if from_col in working.columns:
            from_prev = (
                pd.to_numeric(working[from_col], errors="coerce").fillna(0.0).shift(1).fillna(0.0)
                > 0
            )
            mask = from_prev & to_now
        else:
            to_prev = (
                pd.to_numeric(working[to_col], errors="coerce").fillna(0.0).shift(1).fillna(0.0) > 0
            )
            mask = (~to_prev) & to_now
    else:
        return pd.DataFrame()

    out = working.loc[mask, ["timestamp", "symbol", "forward_return_h"]].copy()
    if out.empty:
        return out
    out["enter_ts"] = out["timestamp"]
    out["event_type"] = str(event_type)
    return out.dropna(subset=["forward_return_h"]).reset_index(drop=True)


def _load_candidate_events(run_id: str, row: pd.Series) -> pd.DataFrame:
    event_type = str(row.get("event_type", "")).strip()
    events = _load_phase1_events(run_id, event_type)
    if not events.empty:
        return events
    symbol = str(row.get("symbol", "")).strip().upper()
    horizon = str(row.get("horizon", "5m")).strip()
    if symbol and (event_type.startswith("STATE_") or event_type.startswith("TRANSITION_")):
        return _build_regime_events(run_id, symbol, event_type, horizon)
    return pd.DataFrame()


def _pick_return_series(
    events: pd.DataFrame, event_type: str, fallback_expectancy: float
) -> pd.Series:
    signed_cols = ["forward_return_h", "event_return", "future_return_h", "ret_h"]
    for col in signed_cols:
        if col in events.columns:
            series = pd.to_numeric(events[col], errors="coerce")
            if not series.dropna().empty:
                return series.fillna(0.0)
    return pd.Series(fallback_expectancy, index=events.index)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate naive entry performance for discovered candidates"
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--min_trades", type=int, default=20)
    parser.add_argument("--min_expectancy_after_cost", type=float, default=0.0)
    parser.add_argument("--max_drawdown", type=float, default=1.0)
    parser.add_argument("--retail_profile", default="capital_constrained")
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    manifest = start_manifest("evaluate_naive_entry", args.run_id, vars(args), [], [])

    try:
        candidates = _load_phase2_candidates(args.run_id)
        if candidates.empty:
            finalize_manifest(manifest, "success", stats={"candidates": 0})
            return 0

        results = []
        for _, row in candidates.iterrows():
            event_type = str(row.get("event_type", "")).strip()
            events = _load_candidate_events(args.run_id, row)
            if events.empty:
                continue

            condition = str(row.get("condition", "all"))
            mask = _condition_mask(events, condition)

            subset = events[mask].copy()
            if subset.empty:
                continue

            expectancy = _pick_return_series(subset, event_type, 0.0).mean()
            results.append(
                {
                    "candidate_id": row.get("candidate_id"),
                    "hypothesis_id": row.get("hypothesis_id"),
                    "event_type": event_type,
                    "naive_expectancy": float(expectancy),
                    "event_count": len(subset),
                }
            )

        if results:
            eval_df = pd.DataFrame(results)
            out_path = get_research_data_root() / "reports" / "phase2" / args.run_id / "naive_evaluation.parquet"
            ensure_dir(out_path.parent)
            write_parquet(eval_df, out_path)

        finalize_manifest(manifest, "success", stats={"evaluated_hypotheses": len(results)})
        return 0
    except Exception as exc:
        logging.exception("Naive evaluation failed")
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
