import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple


def _normalize_gap_value(gap, sample_diff):
    if isinstance(sample_diff, pd.Timedelta):
        if isinstance(gap, pd.Timedelta):
            return gap
        return pd.to_timedelta(gap)
    return gap


def detect_sequences(
    df: pd.DataFrame, events: List[str], max_gaps: List[int], sequence_name: str
) -> pd.DataFrame:
    """
    Detect causal chains of events in an event stream.

    df: must contain ['symbol', 'event_type', 'signal_ts']
    events: ordered list of event IDs, e.g. [A, B, C]
    max_gaps: max gap between signal_ts of adjacent events
    """
    if len(events) < 2:
        return pd.DataFrame()

    if len(max_gaps) != len(events) - 1:
        raise ValueError("max_gaps must have length len(events) - 1")

    results = []

    for symbol, group in df.groupby("symbol"):
        group = group.sort_values("signal_ts").reset_index(drop=True)

        # Get indices for each event type
        event_indices = {
            etype: group.index[group["event_type"] == etype].tolist() for etype in set(events)
        }

        for start_idx in event_indices.get(events[0], []):
            current_path = [start_idx]
            if _find_next_step(group, events, max_gaps, 1, current_path, event_indices):
                last_idx = current_path[-1]
                results.append(
                    {
                        "symbol": symbol,
                        "sequence_name": sequence_name,
                        "enter_ts": group.loc[start_idx, "signal_ts"],
                        "signal_ts": group.loc[last_idx, "signal_ts"],
                        "event_ids": ",".join(events),
                    }
                )

    return pd.DataFrame(results)


def _find_next_step(
    group: pd.DataFrame,
    events: List[str],
    max_gaps: List[int],
    step: int,
    current_path: List[int],
    event_indices: Dict[str, List[int]],
) -> bool:
    if step == len(events):
        return True

    last_idx = current_path[-1]
    last_ts = group.loc[last_idx, "signal_ts"]
    next_event = events[step]
    gap = max_gaps[step - 1]

    # Potential candidates for next event
    candidates = [
        i for i in event_indices.get(next_event, []) if group.loc[i, "signal_ts"] > last_ts
    ]

    for cand_idx in candidates:
        ts_diff = group.loc[cand_idx, "signal_ts"] - last_ts
        normalized_gap = _normalize_gap_value(gap, ts_diff)
        lower_bound = pd.Timedelta(0) if isinstance(ts_diff, pd.Timedelta) else 0
        if lower_bound < ts_diff <= normalized_gap:
            current_path.append(cand_idx)
            if _find_next_step(group, events, max_gaps, step + 1, current_path, event_indices):
                return True
            current_path.pop()

    return False
