from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from project.engine.pnl import compute_pnl_ledger
from project.engine.schema import validate_strategy_frame_schema


def assert_runner_requires_signal_position(frame: pd.DataFrame) -> None:
    validate_strategy_frame_schema(frame, frame_name="regression_position_contract")


def assert_next_open_entry_economics_preserved(
    close: pd.Series, open_: pd.Series, target_position: pd.Series
) -> pd.DataFrame:
    ledger = compute_pnl_ledger(
        target_position, close, open_=open_, execution_mode="next_open", cost_bps=0.0
    )
    changed = ledger["turnover"] > 0
    if not changed.any():
        raise AssertionError("expected at least one turnover event in regression check")
    if abs(float(ledger.loc[changed, "gross_pnl"].iloc[0])) <= 0.0:
        raise AssertionError("entry-bar economics vanished in next_open accounting")
    return ledger


def require_full_market_bars(
    events_df: pd.DataFrame,
    market_df: pd.DataFrame,
    *,
    event_time_col: str = "timestamp",
    market_time_col: str = "timestamp",
) -> None:
    if events_df.empty or market_df.empty:
        raise AssertionError("events and market data must both be non-empty")
    event_times = (
        pd.to_datetime(events_df[event_time_col], utc=True, errors="coerce").dropna().unique()
    )
    market_times = (
        pd.to_datetime(market_df[market_time_col], utc=True, errors="coerce").dropna().unique()
    )
    if len(market_times) <= len(event_times):
        raise AssertionError("market proxy is too sparse relative to events")
    if set(map(str, market_times)) == set(map(str, event_times)):
        raise AssertionError(
            "market proxy matches event timestamps exactly; expected full market bars"
        )


def validate_timeframe_contracts(timeframes: Iterable[str]) -> None:
    allowed = {"1m", "5m", "15m"}
    bad = sorted(set(map(str, timeframes)) - allowed)
    if bad:
        raise AssertionError(f"unsupported timeframe values: {bad}")


def assert_storage_fallback_respected(path: Path) -> None:
    suffix = path.suffix.lower()
    if suffix not in {".csv", ".parquet"}:
        raise AssertionError(f"unexpected storage suffix {suffix}")


def assert_bundle_policy_consistency(audit_df: pd.DataFrame, decisions_df: pd.DataFrame) -> None:
    if audit_df.empty and decisions_df.empty:
        return
    merged = audit_df[["candidate_id", "promotion_decision"]].merge(
        decisions_df[["candidate_id", "promotion_decision"]],
        on="candidate_id",
        suffixes=("_audit", "_decision"),
        how="inner",
    )
    if merged.empty:
        raise AssertionError("no overlapping candidate ids between audit and decisions")
    mismatched = merged[merged["promotion_decision_audit"] != merged["promotion_decision_decision"]]
    if not mismatched.empty:
        raise AssertionError("bundle policy decisions diverge from audit decisions")
