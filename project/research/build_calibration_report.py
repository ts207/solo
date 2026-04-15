"""§4 — Event calibration report.

Produces a per-event-type summary with:
  - count, min_occurrences gate status
  - timing: signal_ts - eval_bar_ts distribution
  - PIT compliance: % events where signal_ts > eval_bar_ts
  - episode stats: merge ratio, mean episode duration
"""

from __future__ import annotations
from project.core.config import get_data_root

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

from project.events.registry import EVENT_REGISTRY_SPECS, load_registry_events
from project.io.utils import ensure_dir, read_parquet, write_parquet

_TIMEFRAME_TO_NS: Dict[str, int] = {
    "5m": 300_000_000_000,
    "15m": 900_000_000_000,
    "1h": 3_600_000_000_000,
}


def _calibration_for_type(
    df: pd.DataFrame,
    event_type: str,
    timeframe_ns: int,
) -> Dict[str, object]:
    """Compute calibration metrics for one event type."""
    sub = df[df["event_type"] == event_type].copy()
    n = len(sub)
    spec = EVENT_REGISTRY_SPECS.get(event_type)
    min_occ = spec.min_occurrences if spec else 0

    row: Dict[str, object] = {
        "event_type": event_type,
        "count": n,
        "min_occurrences": min_occ,
        "passes_min_occurrences": n >= min_occ if min_occ > 0 else True,
    }

    # PIT timing
    for col in ["eval_bar_ts", "signal_ts"]:
        if col not in sub.columns:
            row["pit_compliant_pct"] = np.nan
            row["signal_delay_bars_mean"] = np.nan
            row["signal_delay_bars_median"] = np.nan
            return row

    eval_ns = pd.to_numeric(sub["eval_bar_ts"], errors="coerce")
    signal_ns = pd.to_numeric(sub["signal_ts"], errors="coerce")
    delay_ns = signal_ns - eval_ns
    delay_bars = delay_ns / timeframe_ns

    valid = delay_ns.dropna()
    row["pit_compliant_pct"] = float((valid > 0).mean() * 100) if len(valid) > 0 else np.nan
    row["signal_delay_bars_mean"] = float(delay_bars.mean()) if len(valid) > 0 else np.nan
    row["signal_delay_bars_median"] = float(delay_bars.median()) if len(valid) > 0 else np.nan
    row["signal_delay_bars_min"] = float(delay_bars.min()) if len(valid) > 0 else np.nan
    row["signal_delay_bars_max"] = float(delay_bars.max()) if len(valid) > 0 else np.nan

    # Zero-delay check (potential leak)
    row["zero_delay_count"] = int((valid == 0).sum())
    row["negative_delay_count"] = int((valid < 0).sum())

    # Year distribution
    if "enter_ts" in sub.columns:
        enter = pd.to_datetime(
            pd.to_numeric(sub["enter_ts"], errors="coerce"), unit="ns", utc=True, errors="coerce"
        )
        years = enter.dropna().dt.year
        if not years.empty:
            row["year_min"] = int(years.min())
            row["year_max"] = int(years.max())
            row["year_count"] = int(years.nunique())

    # Symbol distribution
    if "symbol" in sub.columns:
        row["symbol_count"] = int(sub["symbol"].nunique())

    return row


def calibration_report(
    events: pd.DataFrame,
    timeframe_ns: int,
    episodes: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Generate calibration report for all event types."""
    if events.empty:
        return pd.DataFrame()

    rows = []
    for event_type in sorted(events["event_type"].dropna().unique()):
        row = _calibration_for_type(events, event_type, timeframe_ns)

        # Episode stats if available
        if episodes is not None and not episodes.empty and "event_type" in episodes.columns:
            ep_sub = episodes[episodes["event_type"] == event_type]
            if not ep_sub.empty:
                row["episode_count"] = int(len(ep_sub))
                row["merge_ratio"] = (
                    float(row["count"] / len(ep_sub)) if len(ep_sub) > 0 else np.nan
                )
                if "episode_start_ts" in ep_sub.columns and "episode_end_ts" in ep_sub.columns:
                    dur = pd.to_numeric(ep_sub["episode_end_ts"], errors="coerce") - pd.to_numeric(
                        ep_sub["episode_start_ts"], errors="coerce"
                    )
                    dur_bars = dur / timeframe_ns
                    row["episode_duration_bars_mean"] = float(dur_bars.mean())
                    row["episode_duration_bars_median"] = float(dur_bars.median())

        rows.append(row)

    return pd.DataFrame(rows)


def main() -> int:
    DATA_ROOT = get_data_root()
    ap = argparse.ArgumentParser(description="Generate event calibration report.")
    ap.add_argument("--run_id", required=True)
    ap.add_argument("--timeframe", default="5m")
    ap.add_argument("--out_dir", default=None)
    args = ap.parse_args()

    timeframe_ns = _TIMEFRAME_TO_NS.get(args.timeframe)
    if timeframe_ns is None:
        raise ValueError(f"Unsupported timeframe: {args.timeframe}")

    events = load_registry_events(data_root=DATA_ROOT, run_id=args.run_id)
    if events.empty:
        print("No events found.")
        return 0

    # Try to load episodes too
    episodes = None
    ep_path = DATA_ROOT / "events" / args.run_id / "episodes.parquet"
    if ep_path.exists():
        try:
            episodes = read_parquet(ep_path)
        except Exception:
            pass

    report = calibration_report(events, timeframe_ns, episodes)

    out_dir = (
        Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "calibration" / args.run_id
    )
    ensure_dir(out_dir)

    write_parquet(report, out_dir / "event_calibration.parquet")
    report.to_csv(out_dir / "event_calibration.csv", index=False)

    # Print summary
    pit_ok = report["pit_compliant_pct"].mean() if "pit_compliant_pct" in report.columns else np.nan
    zero_leaks = report["zero_delay_count"].sum() if "zero_delay_count" in report.columns else 0
    print(f"Calibration report: {len(report)} event types")
    print(f"  Mean PIT compliance: {pit_ok:.1f}%")
    print(f"  Total zero-delay events: {int(zero_leaks)}")
    print(f"  Output: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
