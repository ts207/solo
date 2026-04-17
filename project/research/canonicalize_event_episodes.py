from __future__ import annotations
from project.core.config import get_data_root

import argparse
import sys
from dataclasses import dataclass
from typing import Dict, Tuple, Optional

import numpy as np
import pandas as pd
from project import PROJECT_ROOT

REPO_ROOT = PROJECT_ROOT.parent

from project.events.registry import (
    load_registry_events,
    write_registry_file,
    registry_contract_check,
    EVENT_REGISTRY_SPECS,
)
from project.specs.manifest import start_manifest, finalize_manifest
from project.research._timeframes import TIMEFRAME_TO_NS
from project.core.validation import ts_ns_utc
from project.core.validation import assert_monotonic_utc_timestamp


@dataclass(frozen=True)
class EpisodeConfig:
    timeframe_ns: int
    merge_gap_ns: int
    cooldown_ns: int
    anchor_rule: str  # "max_intensity" | "first" | "last"


def _first_existing(df: pd.DataFrame, cols) -> Optional[str]:
    for c in cols:
        if c in df.columns:
            return c
    return None


def _canonicalize_group(g: pd.DataFrame, cfg: EpisodeConfig) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # g is sorted by enter_ts
    if g.empty:
        return g, g

    intensity_col = _first_existing(
        g, ["evt_signal_intensity", "event_score", "intensity", "signal_intensity"]
    )
    if intensity_col is None:
        intensity = np.ones(len(g), dtype="float64")
    else:
        intensity = (
            pd.to_numeric(g[intensity_col], errors="coerce").fillna(0.0).to_numpy(dtype="float64")
        )

    enter_ts = g["enter_ts"].to_numpy(dtype="int64")
    exit_ts = g["exit_ts"].to_numpy(dtype="int64")

    # Basic episode merge: overlaps or adjacency within merge_gap_ns
    # Cooldown: events within cooldown_ns of the previous episode end are skipped
    episodes = []
    anchors = []
    cur_end = exit_ts[0]
    cur_rows = [0]
    cooldown_end_ns: int = 0  # exclusive end of the cooldown window after each episode

    def flush(rows_idx):
        nonlocal episodes, anchors, cooldown_end_ns
        if not rows_idx:
            return
        sub = g.iloc[rows_idx].copy()
        # Ensure we use numpy arrays for faster min/max calculation over small lists
        ep_start = enter_ts[rows_idx].min()
        ep_end = exit_ts[rows_idx].max()

        # Advance cooldown window past this episode
        cooldown_end_ns = ep_end + cfg.cooldown_ns
        # Anchor selection
        if cfg.anchor_rule == "first":
            anchor_row = sub.iloc[0]
        elif cfg.anchor_rule == "last":
            anchor_row = sub.iloc[-1]
        else:
            # max_intensity by absolute value
            if intensity_col is None:
                anchor_row = sub.iloc[0]
            else:
                idx = int(np.nanargmax(np.abs(intensity[rows_idx])))
                anchor_row = sub.iloc[idx]

        anchor_ts = int(
            anchor_row.get("signal_ts", anchor_row.get("detected_ts", anchor_row["enter_ts"]))
        )
        # Bound anchor inside episode
        anchor_ts = max(ep_start, min(anchor_ts, ep_end))

        event_id = str(anchor_row["event_id"])
        symbol = str(anchor_row["symbol"])
        episode_id = f"{event_id}__{symbol}__{ep_start}__{ep_end}"

        ep = {
            "episode_id": episode_id,
            "symbol": symbol,
            "event_id": event_id,
            "episode_start_ts": ep_start,
            "episode_end_ts": ep_end + cfg.timeframe_ns,  # exclusive endpoint
            "anchor_ts": anchor_ts,
            "rows_merged": int(len(sub)),
        }
        # Preserve key metadata columns if present
        for c in ["signal_column", "direction", "sign", "split_label"]:
            if c in sub.columns:
                ep[c] = sub[c].iloc[0]
        if intensity_col is not None:
            ep["episode_anchor_intensity"] = float(
                pd.to_numeric(anchor_row[intensity_col], errors="coerce") or 0.0
            )

        episodes.append(ep)

        # Anchor-event row: keep original registry schema columns but shift enter_ts to anchor_ts, exit_ts to episode_end_ts
        a = anchor_row.to_dict()
        a["episode_id"] = episode_id
        a["enter_ts"] = anchor_ts
        a["exit_ts"] = ep_end
        # Keep a consistent 'signal_ts' if present; otherwise set to anchor
        if "signal_ts" in a:
            a["signal_ts"] = anchor_ts
        anchors.append(a)

    for i in range(1, len(g)):
        n_start = enter_ts[i]
        n_end = exit_ts[i]

        # Skip events that fall inside the cooldown window of the previous episode
        if cfg.cooldown_ns > 0 and n_start < cooldown_end_ns:
            continue

        # Merge if overlapping or within merge_gap
        if n_start <= cur_end + cfg.merge_gap_ns:
            cur_end = max(cur_end, n_end)
            cur_rows.append(i)
        else:
            flush(cur_rows)
            # After flush cooldown_end_ns is updated; skip if new event still in cooldown
            if cfg.cooldown_ns > 0 and n_start < cooldown_end_ns:
                cur_end = n_end
                cur_rows = []
                continue
            cur_end = n_end
            cur_rows = [i]
    flush(cur_rows)

    ep_df = pd.DataFrame(episodes)
    anchor_df = pd.DataFrame(anchors)
    return ep_df, anchor_df


def canonicalize_event_episodes(
    events: pd.DataFrame, cfg: EpisodeConfig
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if events.empty:
        return events.copy(), events.copy()

    # Ensure required timestamp columns exist as int64 nanoseconds
    for col in ["enter_ts", "exit_ts", "detected_ts", "signal_ts"]:
        if col in events.columns:
            events[col] = pd.to_numeric(events[col], errors="coerce").fillna(0).astype("int64")
    if "enter_ts" not in events.columns:
        raise ValueError("events missing enter_ts")
    if "exit_ts" not in events.columns:
        events["exit_ts"] = events["enter_ts"]

    # Fill missing exit_ts with enter_ts (instantaneous events)
    events["exit_ts"] = events["exit_ts"].fillna(events["enter_ts"]).astype("int64")
    events["enter_ts"] = events["enter_ts"].fillna(events["exit_ts"]).astype("int64")
    if "detected_ts" in events.columns:
        events["detected_ts"] = events["detected_ts"].fillna(events["enter_ts"]).astype("int64")
    else:
        events["detected_ts"] = events["enter_ts"].astype("int64")
    if "signal_ts" in events.columns:
        events["signal_ts"] = events["signal_ts"].fillna(events["enter_ts"]).astype("int64")
    else:
        events["signal_ts"] = events["enter_ts"].astype("int64")

    # Sanity: monotonic within symbol? not globally; enforce sorting
    events = events.sort_values(["symbol", "event_id", "enter_ts", "exit_ts"]).reset_index(
        drop=True
    )

    ep_parts = []
    anchor_parts = []
    for (symbol, event_id), g in events.groupby(["symbol", "event_id"], sort=False):
        g = g.sort_values("enter_ts").reset_index(drop=True)
        ep_df, anchor_df = _canonicalize_group(g, cfg)
        ep_parts.append(ep_df)
        anchor_parts.append(anchor_df)

    episodes = pd.concat(ep_parts, ignore_index=True) if ep_parts else pd.DataFrame()
    anchors = pd.concat(anchor_parts, ignore_index=True) if anchor_parts else pd.DataFrame()

    # Ensure ts columns are int64
    for c in ["episode_start_ts", "episode_end_ts", "anchor_ts"]:
        if c in episodes.columns:
            episodes[c] = pd.to_numeric(episodes[c], errors="coerce").fillna(0).astype("int64")

    return episodes, anchors


def main() -> int:
    DATA_ROOT = get_data_root()
    ap = argparse.ArgumentParser(
        description="Canonicalize Phase1 event triggers into non-overlapping episodes and anchor events."
    )
    ap.add_argument("--run_id", required=True)
    ap.add_argument("--timeframe", default="5m")
    ap.add_argument(
        "--merge_gap_bars",
        type=int,
        default=None,
        help="Merge events if gap <= this many bars. Overrides spec default.",
    )
    ap.add_argument(
        "--cooldown_bars",
        type=int,
        default=None,
        help="Cooldown bars between episodes. Overrides spec default.",
    )
    ap.add_argument(
        "--anchor_rule",
        default=None,
        choices=["max_intensity", "first", "last"],
        help="Anchor selection rule. Overrides spec default.",
    )
    ap.add_argument(
        "--min_occurrences",
        type=int,
        default=None,
        help="Minimum event count to keep an event_type. Overrides spec default.",
    )
    ap.add_argument(
        "--event_type",
        default="all",
        help="Optional: restrict to one event_type if the registry was built per type.",
    )
    args = ap.parse_args()

    timeframe_ns = TIMEFRAME_TO_NS.get(str(args.timeframe).lower())
    if timeframe_ns is None:
        raise ValueError(f"Unsupported timeframe: {args.timeframe}")

    # Resolve per-event spec defaults (CLI overrides take precedence)
    spec = EVENT_REGISTRY_SPECS.get(args.event_type) if args.event_type != "all" else None
    merge_gap_bars = (
        args.merge_gap_bars
        if args.merge_gap_bars is not None
        else (spec.merge_gap_bars if spec else 1)
    )
    cooldown_bars = (
        args.cooldown_bars
        if args.cooldown_bars is not None
        else (spec.cooldown_bars if spec else 0)
    )
    anchor_rule = (
        args.anchor_rule
        if args.anchor_rule is not None
        else (spec.anchor_rule if spec else "max_intensity")
    )
    min_occurrences = (
        args.min_occurrences
        if args.min_occurrences is not None
        else (spec.min_occurrences if spec else 0)
    )

    cfg = EpisodeConfig(
        timeframe_ns=timeframe_ns,
        merge_gap_ns=merge_gap_bars * timeframe_ns,
        cooldown_ns=cooldown_bars * timeframe_ns,
        anchor_rule=anchor_rule,
    )

    manifest = start_manifest(
        stage_name="canonicalize_event_episodes",
        run_id=args.run_id,
        params={
            "timeframe": args.timeframe,
            "merge_gap_bars": merge_gap_bars,
            "cooldown_bars": cooldown_bars,
            "anchor_rule": anchor_rule,
            "min_occurrences": min_occurrences,
            "event_type": str(args.event_type),
        },
        inputs=[],
        outputs=[],
    )

    events = load_registry_events(data_root=DATA_ROOT, run_id=args.run_id)
    if args.event_type != "all" and "event_type" in events.columns:
        events = events[events["event_type"] == args.event_type].copy()

    # §2.3: min_occurrences pruning gate
    if min_occurrences > 0 and not events.empty and "event_type" in events.columns:
        counts = events.groupby("event_type").size()
        keep = counts[counts >= min_occurrences].index
        dropped = set(counts.index) - set(keep)
        if dropped:
            print(f"Pruning event_types with < {min_occurrences} occurrences: {sorted(dropped)}")
        events = events[events["event_type"].isin(keep)].copy()

    # Contract check on input (best-effort)
    try:
        registry_contract_check(events)
    except Exception:
        pass

    episodes, anchor_events = canonicalize_event_episodes(events, cfg)

    # Write under registry root
    out_paths = {}
    out_paths["episodes_path"] = write_registry_file(DATA_ROOT, args.run_id, "episodes", episodes)
    out_paths["episode_anchors_path"] = write_registry_file(
        DATA_ROOT, args.run_id, "episode_anchors", anchor_events
    )

    manifest["outputs"] = [{"path": str(p)} for p in out_paths.values()]
    finalize_manifest(manifest, status="success")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
