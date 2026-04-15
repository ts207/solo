"""§3 — Build per-bar episode state parquet from canonicalized episodes.

For each bar in the feature grid, this produces event-type-level columns:
  - {event_type}_active:   1 if bar falls within any episode of that type, 0 otherwise
  - {event_type}_age_bars: bars since episode start (1-indexed), 0 if not active
  - {event_type}_episode_id: episode_id if active, else None

Output is a single parquet indexed by (symbol, timestamp) with one column-set per
event type encountered in the episodes file.
"""

from __future__ import annotations
from project.core.config import get_data_root

import argparse
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

from project.core.feature_schema import feature_dataset_dir_name
from project.events.registry import load_registry_events, write_registry_file
from project.io.utils import (
    choose_partition_dir,
    ensure_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
    write_parquet,
)
from project.specs.manifest import start_manifest, finalize_manifest
from project.research._timeframes import TIMEFRAME_TO_NS


def _load_bar_grid(run_id: str, symbol: str, timeframe: str) -> pd.DataFrame:
    """Load the bar timestamp grid for a symbol."""
    DATA_ROOT = get_data_root()
    feature_dataset = feature_dataset_dir_name()
    candidates = [
        run_scoped_lake_path(
            DATA_ROOT, run_id, "features", "perp", symbol, timeframe, feature_dataset
        ),
        DATA_ROOT / "lake" / "features" / "perp" / symbol / timeframe / feature_dataset,
    ]
    features_dir = choose_partition_dir(candidates)
    files = list_parquet_files(features_dir) if features_dir else []
    if not files:
        return pd.DataFrame()
    df = read_parquet(files)
    if df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return df[["timestamp"]].copy()


def build_episode_state(
    bar_grid: pd.DataFrame,
    episodes: pd.DataFrame,
) -> pd.DataFrame:
    """Build per-bar state frame from episodes.

    Parameters
    ----------
    bar_grid : pd.DataFrame
        Must have a ``timestamp`` column (datetime64, UTC).
    episodes : pd.DataFrame
        Canonicalized episodes with ``episode_start_ts``, ``episode_end_ts``
        (exclusive), ``episode_id``, ``symbol``, and ``event_id`` (used as
        event_type proxy if ``event_type`` is missing). Episode age is derived
        from observed bars, not elapsed wall-clock time.

    Returns
    -------
    pd.DataFrame
        One row per bar, with ``{et}_active``, ``{et}_age_bars``,
        ``{et}_episode_id`` columns per event type.
    """
    out = bar_grid.copy()
    grid_ns = out["timestamp"].astype("int64").to_numpy()

    if episodes.empty:
        return out

    # Determine event type from episode data
    if "event_type" not in episodes.columns:
        # Derive from event_id prefix
        episodes = episodes.copy()
        episodes["event_type"] = (
            episodes["event_id"].astype(str).str.rsplit("_", n=3).str[0].str.upper()
        )

    for event_type in sorted(episodes["event_type"].dropna().unique()):
        prefix = event_type.lower()
        col_active = f"{prefix}_active"
        col_age = f"{prefix}_age_bars"
        col_eid = f"{prefix}_episode_id"

        out[col_active] = np.int8(0)
        out[col_age] = np.int32(0)
        out[col_eid] = None

        et_eps = episodes[episodes["event_type"] == event_type]
        for _, ep in et_eps.iterrows():
            ep_start = int(ep["episode_start_ts"])
            ep_end = int(ep["episode_end_ts"])  # exclusive
            eid = str(ep["episode_id"])

            # Find bars in [ep_start, ep_end)
            mask = (grid_ns >= ep_start) & (grid_ns < ep_end)
            idxs = np.flatnonzero(mask)
            if len(idxs) == 0:
                continue

            out.iloc[idxs, out.columns.get_loc(col_active)] = 1
            out.iloc[idxs, out.columns.get_loc(col_age)] = np.arange(
                1, len(idxs) + 1, dtype=np.int32
            )
            out.iloc[idxs, out.columns.get_loc(col_eid)] = eid

    return out


def main() -> int:
    DATA_ROOT = get_data_root()
    ap = argparse.ArgumentParser(
        description="Build per-bar episode state parquet from canonicalized episodes."
    )
    ap.add_argument("--run_id", required=True)
    ap.add_argument("--symbols", required=True)
    ap.add_argument("--timeframe", default="5m")
    ap.add_argument("--out_dir", default=None)
    ap.add_argument("--log_path", default=None)
    args = ap.parse_args()

    timeframe_ns = TIMEFRAME_TO_NS.get(args.timeframe)
    if timeframe_ns is None:
        raise ValueError(f"Unsupported timeframe: {args.timeframe}")

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "lake" / "features" / "perp"

    manifest = start_manifest(
        "build_episode_state",
        args.run_id,
        {"timeframe": args.timeframe, "symbols": symbols},
        [],
        [],
    )

    try:
        # Load canonicalized episodes
        episodes_path = DATA_ROOT / "events" / args.run_id / "episodes.parquet"
        if not episodes_path.exists():
            print(f"No episodes file at {episodes_path}")
            finalize_manifest(manifest, "skipped")
            return 0
        episodes = read_parquet(episodes_path)

        for symbol in symbols:
            grid = _load_bar_grid(args.run_id, symbol, args.timeframe)
            if grid.empty:
                print(f"No bar grid for {symbol}")
                continue

            sym_episodes = (
                episodes[episodes["symbol"] == symbol].copy()
                if "symbol" in episodes.columns
                else episodes.copy()
            )
            state = build_episode_state(grid, sym_episodes)

            sym_out = out_dir / symbol / args.timeframe / "episode_state"
            ensure_dir(sym_out)
            out_path = sym_out / f"episode_state_{args.run_id}.parquet"
            write_parquet(state, out_path)
            print(f"Wrote {len(state)} rows for {symbol} → {out_path}")

        finalize_manifest(manifest, "success")
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
