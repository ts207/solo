from __future__ import annotations

from project.research.helpers.events import (
    EVENT_COLUMNS,
    rolling_z_score,
    rows_for_event,
)
from project.research.helpers.events import (
    merge_event_artifacts as merge_event_csv,
)


def rolling_z(series, window):
    return rolling_z_score(series, window)


from project.research.phase2 import (
    clear_feature_cache as _clear_feature_cache_impl,
)
from project.research.phase2 import (
    load_features as _load_features_impl,
)


def clear_feature_cache() -> None:
    """Clear the global feature cache used during tests."""
    _clear_feature_cache_impl()


def load_features(
    run_id: str,
    symbol: str,
    timeframe: str = "5m",
    higher_timeframes: list[str] | None = None,
    market: str = "perp",
    data_root: Path | None = None,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    from project.core.config import get_data_root

    return _load_features_impl(
        data_root=data_root or get_data_root(),
        run_id=run_id,
        symbol=symbol,
        timeframe=timeframe,
        higher_timeframes=higher_timeframes,
        market=market,
        start=start,
        end=end,
    )


import argparse
import inspect
import json
import sys
from pathlib import Path
from typing import Callable, Iterable

import numpy as np
import pandas as pd

from project.core.config import get_data_root
from project.events.registry import EVENT_REGISTRY_SPECS


def _call_maybe_with_args(fn: Callable, df, event_type: str, symbol: str):
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        params = {}
    kwargs = {}
    if "df" in params:
        kwargs["df"] = df
    if "event_type" in params:
        kwargs["event_type"] = event_type
    if "symbol" in params:
        kwargs["symbol"] = symbol

    # If the function takes **kwargs or any other names, it might fail if we don't handle them correctly
    # But for our known cases, this is enough.
    # To be safe, we'll only pass what's in params
    call_args = {k: v for k, v in kwargs.items() if k in params and k != "df"}

    return fn(df, **call_args)


def run_family_analyzer(
    *,
    family_name: str,
    event_types: Iterable[str],
    event_mask_fn: Callable,
    event_score_fn: Callable,
    default_min_spacing: int = 1,
    min_spacing_fn: Callable | None = None,
    load_features_fn: Callable = load_features,
) -> int:
    DATA_ROOT = get_data_root()
    parser = argparse.ArgumentParser(
        description=f"Family-specific analyzer for {family_name} events"
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--event_type", required=True)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--seed", type=int, default=13)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    event_type = str(args.event_type).strip().upper()
    allowed = {str(v).strip().upper() for v in event_types}
    if event_type not in allowed:
        print(f"Unsupported {family_name} event_type: {event_type}", file=sys.stderr)
        return 1
    spec = EVENT_REGISTRY_SPECS.get(event_type)
    if spec is None:
        print(f"Unknown event_type: {event_type}", file=sys.stderr)
        return 1

    out_dir = (
        Path(args.out_dir)
        if args.out_dir
        else DATA_ROOT / "reports" / spec.reports_dir / args.run_id
    )
    out_path = out_dir / spec.events_file

    events_parts = []
    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    for symbol in symbols:
        features = load_features_fn(
            run_id=str(args.run_id), symbol=symbol, timeframe=str(args.timeframe)
        )
        if features.empty:
            continue

        # New pattern: detectors might return a whole DataFrame
        # But run_family_analyzer expects a mask and then calls rows_for_event
        # To minimize disruption, we'll check if event_mask_fn returns a DataFrame or Series
        result = _call_maybe_with_args(event_mask_fn, features, event_type, symbol)

        if isinstance(result, pd.DataFrame):
            # If it's a DataFrame, it's already a set of events
            part = result
        else:
            # Traditional behavior: it's a mask
            mask = result
            spacing = int(
                min_spacing_fn(event_type, spec)
                if min_spacing_fn is not None
                else default_min_spacing
            )
            part = rows_for_event(
                features,
                symbol=symbol,
                event_type=event_type,
                mask=mask,
                event_score=_call_maybe_with_args(event_score_fn, features, event_type, symbol),
                min_spacing=max(1, spacing),
                log_path=args.log_path,
                seed=args.seed,
            )
        if not part.empty:
            # Merge feature columns needed for evaluation (e.g. for condition masking)
            # We merge based on eval_bar_ts matching the original features timestamp
            feature_cols = [
                c for c in features.columns if c not in part.columns or c == "timestamp"
            ]
            part = part.merge(
                features[feature_cols],
                left_on="eval_bar_ts",
                right_on="timestamp",
                how="left",
                suffixes=("", "_feat"),
            )
            events_parts.append(part)

    new_df = (
        pd.concat(events_parts, ignore_index=True)
        if events_parts
        else pd.DataFrame(columns=EVENT_COLUMNS)
    )
    new_df = merge_event_csv(out_path, event_type=event_type, new_df=new_df)
    summary = {
        "run_id": str(args.run_id),
        "event_type": event_type,
        "rows": int(len(new_df[new_df["event_type"].astype(str) == event_type]))
        if not new_df.empty
        else 0,
        "events_file": str(out_path),
    }
    (out_dir / f"{event_type.lower()}_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    print(f"Wrote {summary['rows']} rows for {event_type} to {out_path}")
    return 0


def safe_severity_quantiles(df: pd.DataFrame, col: str = "severity") -> pd.Series:
    """Compute severity buckets top_20pct, top_10pct, extreme_5pct safely (Finding 84)."""
    if df.empty:
        return pd.Series(dtype=str)

    vals = df[col].dropna()
    if vals.empty:
        return pd.Series("base", index=df.index)

    qs = vals.quantile([0.8, 0.9, 0.95]).values
    # Ensure quantiles are strictly increasing to avoid cut errors
    # (e.g. if all values are the same, qs will be the same)
    unique_qs = np.unique(np.concatenate([[-np.inf], qs, [np.inf]]))

    # We want base, top_20pct, top_10pct, extreme_5pct
    # Use explicit bins and labels
    labels = ["base", "top_20pct", "top_10pct", "extreme_5pct"]
    # If not enough unique quantiles, collapse to base
    if len(unique_qs) < 5:
        return pd.Series("base", index=df.index)

    return (
        pd.cut(df[col], bins=unique_qs, labels=labels, include_lowest=True)
        .astype(str)
        .fillna("base")
    )
