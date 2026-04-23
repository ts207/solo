import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple


class InteractionOp:
    AND = "and"
    OR = "or"
    CONFIRM = "confirm"
    EXCLUDE = "exclude"


def _within_lag(l_ts: pd.Timestamp, r_ts: pd.Timestamp, lag: any) -> bool:
    """Check if r_ts is within lag of l_ts."""
    delta = r_ts - l_ts
    if isinstance(lag, pd.Timedelta):
        return pd.Timedelta(0) < delta <= lag

    if hasattr(delta, "total_seconds"):
        diff = delta.total_seconds() / 60
    else:
        diff = float(delta)
    return 0 < diff <= float(lag)


def detect_interactions(
    df: pd.DataFrame, left_id: str, right_id: str, op: str, lag: any, interaction_name: str
) -> pd.DataFrame:
    """
    Detect intersections between two mechanisms (events/states).
    lag can be int (minutes) or pd.Timedelta.
    """
    results = []

    for symbol, group in df.groupby("symbol"):
        group = group.sort_values("signal_ts")

        left_mask = group["event_type"] == left_id
        right_mask = group["event_type"] == right_id

        left_ts = group.loc[left_mask, "signal_ts"].tolist()
        right_ts = group.loc[right_mask, "signal_ts"].tolist()

        if op == InteractionOp.AND:
            for l_ts in left_ts:
                for r_ts in right_ts:
                    if _within_lag(l_ts, r_ts, lag):
                        results.append(
                            {
                                "symbol": symbol,
                                "interaction_id": interaction_name,
                                "signal_ts": l_ts,
                            }
                        )
                        break
        elif op == InteractionOp.CONFIRM:
            for l_ts in left_ts:
                for r_ts in right_ts:
                    if _within_lag(l_ts, r_ts, lag):
                        results.append(
                            {
                                "symbol": symbol,
                                "interaction_id": interaction_name,
                                "signal_ts": r_ts,
                            }
                        )
                        break
        elif op == InteractionOp.EXCLUDE:
            for l_ts in left_ts:
                has_right = any(_within_lag(l_ts, r_ts, lag) for r_ts in right_ts)
                if not has_right:
                    results.append(
                        {
                            "symbol": symbol,
                            "interaction_id": interaction_name,
                            "signal_ts": l_ts,
                        }
                    )
        elif op == InteractionOp.OR:
            for l_ts in left_ts:
                results.append(
                    {"symbol": symbol, "interaction_id": interaction_name, "signal_ts": l_ts}
                )
            for r_ts in right_ts:
                results.append(
                    {"symbol": symbol, "interaction_id": interaction_name, "signal_ts": r_ts}
                )

    return pd.DataFrame(results)
