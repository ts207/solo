"""
Phase 2 Research Services: Event preparation, Gating, and Feature Loading.
Extracted from pipeline scripts to improve testability and separate concerns.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from project.core.coercion import safe_float, safe_int, as_bool
from project.core.feature_schema import feature_dataset_dir_name
from project.core.constants import parse_horizon_bars
from project.core.timeframes import normalize_timeframe, timeframe_to_minutes
from project.domain.compiled_registry import get_domain_registry
from project.events.registry import load_registry_episode_anchors
from project.features.assembly import filter_time_window, prune_partition_files_by_window
from project.research.validation import assign_split_labels as _validation_assign_split_labels
from project.research.services.phase2_diagnostics import (
    attach_prepare_events_diagnostics,
    build_prepare_events_diagnostics,
    split_counts as phase2_split_counts,
)
from project.io.utils import (
    HAS_PYARROW,
    choose_partition_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
)
from project.specs.ontology import ontology_spec_paths
from project.research.holdout_integrity import assert_holdout_split_integrity

log = logging.getLogger(__name__)

# Cache for feature DataFrames during tests to reduce disk I/O
_FEATURE_CACHE: Dict[Tuple[str, str, str, str, str, str, Tuple[str, ...]], pd.DataFrame] = {}


def _schema_columns_for_parquet_files(files: List[Path]) -> list[str] | None:
    if not HAS_PYARROW:
        return None
    if not files or any(path.suffix != ".parquet" for path in files):
        return None
    try:
        import pyarrow.parquet as pq

        ordered: list[str] = []
        seen: set[str] = set()
        for path in files:
            for column in pq.ParquetFile(path).schema.names:
                if column not in seen:
                    ordered.append(column)
                    seen.add(column)
        return ordered
    except Exception:
        return None


def _read_new_context_columns(files: List[Path], existing_columns: set[str]) -> pd.DataFrame:
    schema_columns = _schema_columns_for_parquet_files(files)
    if schema_columns is None:
        return read_parquet(files)
    if "timestamp" not in schema_columns:
        return pd.DataFrame()
    columns = ["timestamp"] + [
        column
        for column in schema_columns
        if column != "timestamp" and column not in existing_columns
    ]
    if len(columns) == 1:
        return pd.DataFrame(columns=["timestamp"])
    return read_parquet(files, columns=columns)


def _normalize_timestamp_order(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    valid = df["timestamp"].notna()
    if not bool(valid.all()):
        df = df.loc[valid].copy()
    if df.empty:
        return pd.DataFrame()
    if not df["timestamp"].is_monotonic_increasing:
        return df.sort_values("timestamp").reset_index(drop=True)
    if isinstance(df.index, pd.RangeIndex) and df.index.start == 0 and df.index.step == 1:
        return df
    return df.reset_index(drop=True)


def clear_feature_cache() -> None:
    """Clear the global feature cache used during tests."""
    _FEATURE_CACHE.clear()


def load_template_verb_lexicon(repo_root: Path) -> Dict[str, Any]:
    """Load the template verb lexicon from the compiled domain registry."""
    del repo_root
    return {"operators": get_domain_registry().operator_rows()}


def operator_registry(verb_lexicon: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Extract operator definitions from the verb lexicon."""
    operators = verb_lexicon.get("operators", {})
    if isinstance(operators, dict) and operators:
        return {
            str(key).strip(): dict(value)
            for key, value in operators.items()
            if str(key).strip() and isinstance(value, dict)
        }
    return get_domain_registry().operator_rows()


def validate_operator_for_event(
    *,
    template_verb: str,
    canonical_family: str,
    operator_registry_map: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Validate that a template verb is compatible with an event family."""
    op = operator_registry_map.get(str(template_verb).strip(), {})
    if not op:
        raise ValueError(f"Missing operator definition for template verb: {template_verb}")
    compatible = op.get("compatible_families", [])
    if isinstance(compatible, list) and compatible:
        allowed = {str(x).strip().upper() for x in compatible if str(x).strip()}
        if str(canonical_family).strip().upper() not in allowed:
            raise ValueError(
                f"Template verb {template_verb} is incompatible with family {canonical_family}; "
                f"allowed={sorted(allowed)}"
            )
    return op


def load_features(
    data_root: Path,
    run_id: str,
    symbol: str,
    timeframe: str = "5m",
    higher_timeframes: List[str] | None = None,
    market: str = "perp",
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    """Load and merge feature partitions for a symbol/run/market."""
    tf = str(timeframe or "5m").strip().lower() or "5m"
    mkt = str(market or "perp").strip().lower()

    # Check cache if in test environment
    is_test = os.getenv("PYTEST_CURRENT_TEST") is not None
    htf_key = tuple(sorted(higher_timeframes)) if higher_timeframes else ()
    cache_key = (run_id, symbol, tf, mkt, str(start or ""), str(end or ""), htf_key)
    if is_test and cache_key in _FEATURE_CACHE:
        return _FEATURE_CACHE[cache_key].copy()

    feature_dataset = feature_dataset_dir_name()
    candidates = [
        run_scoped_lake_path(data_root, run_id, "features", mkt, symbol, tf, feature_dataset),
        data_root / "lake" / "features" / mkt / symbol / tf / feature_dataset,
    ]
    features_dir = choose_partition_dir(candidates)
    if not features_dir:
        return pd.DataFrame()
    files = prune_partition_files_by_window(list_parquet_files(features_dir), start=start, end=end)
    if not files:
        return pd.DataFrame()
    df = read_parquet(files)
    if df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()

    out = filter_time_window(_normalize_timestamp_order(df), start=start, end=end)
    if out.empty:
        return pd.DataFrame()

    # 1. Merge Market State Context
    ms_candidates = [
        run_scoped_lake_path(data_root, run_id, "features", mkt, symbol, tf, "market_context"),
        data_root / "lake" / "features" / mkt / symbol / tf / "market_context",
    ]
    ms_dir = choose_partition_dir(ms_candidates)
    if ms_dir:
        ms_files = prune_partition_files_by_window(list_parquet_files(ms_dir), start=start, end=end)
        if ms_files:
            ms_df = _read_new_context_columns(ms_files, set(out.columns))
            if not ms_df.empty and "timestamp" in ms_df.columns:
                ms_df = filter_time_window(_normalize_timestamp_order(ms_df), start=start, end=end)
                # Filter to only new columns
                new_cols = [c for c in ms_df.columns if c not in out.columns or c == "timestamp"]
                out = pd.merge_asof(
                    out,
                    ms_df[new_cols],
                    on="timestamp",
                    direction="backward",
                )

    # 2. Merge Microstructure Context
    micro_candidates = [
        run_scoped_lake_path(data_root, run_id, "features", mkt, symbol, tf, "microstructure"),
        data_root / "lake" / "features" / mkt / symbol / tf / "microstructure",
    ]
    micro_dir = choose_partition_dir(micro_candidates)
    if micro_dir:
        micro_files = prune_partition_files_by_window(
            list_parquet_files(micro_dir),
            start=start,
            end=end,
        )
        if micro_files:
            micro_df = _read_new_context_columns(micro_files, set(out.columns))
            if not micro_df.empty and "timestamp" in micro_df.columns:
                micro_df = filter_time_window(
                    _normalize_timestamp_order(micro_df),
                    start=start,
                    end=end,
                )
                # Filter to only new columns
                new_cols = [c for c in micro_df.columns if c not in out.columns or c == "timestamp"]
                out = pd.merge_asof(
                    out,
                    micro_df[new_cols],
                    on="timestamp",
                    direction="backward",
                )

    if higher_timeframes:
        for htf in higher_timeframes:
            if htf == tf:
                continue
            htf_df = load_features(
                data_root,
                run_id,
                symbol,
                timeframe=htf,
                market=mkt,
                start=start,
                end=end,
            )
            if htf_df.empty:
                continue

            htf_df = htf_df.rename(
                columns={c: f"{c}_{htf}" for c in htf_df.columns if c != "timestamp"}
            )
            out = pd.merge_asof(
                out,
                htf_df,
                on="timestamp",
                direction="backward",
            )

    if is_test and not out.empty:
        _FEATURE_CACHE[cache_key] = out.copy()

    return out


def prepare_events_dataframe(
    *,
    data_root: Path,
    run_id: str,
    event_type: str | List[str],
    symbols: List[str],
    event_registry_specs: Dict[str, Any],
    horizons: List[str],
    entry_lag_bars: int,
    fam_config: Dict[str, Any],
    logger: logging.Logger,
    run_mode: str = "research",
    timeframe: str = "5m",
) -> pd.DataFrame:
    """Prepare the events dataframe for Phase 2: loading, splitting, and merging context."""
    timeframe = normalize_timeframe(timeframe or "5m")
    bar_duration_minutes = int(timeframe_to_minutes(timeframe))

    event_types = [event_type] if isinstance(event_type, str) else event_type

    all_events_frames = []

    # We will accumulate these flags over all event types. If ANY was loaded from fallback, we set it to True.
    loaded_from_fallback_file = False

    for et in event_types:
        if et not in event_registry_specs:
            logger.warning(f"Event type {et} not found in specs. Skipping.")
            if isinstance(event_type, str):
                # Only raise if it was strictly requested as a single string (legacy behavior)
                raise KeyError(event_type)
            continue

        spec = event_registry_specs[et]
        phase1_reports_root = data_root / "reports" / spec.reports_dir / run_id
        events_path = phase1_reports_root / spec.events_file

        df = load_registry_episode_anchors(
            data_root=data_root,
            run_id=run_id,
            event_type=et,
            symbols=symbols,
        )

        if df.empty:
            try:
                df = _read_csv_or_parquet(events_path)
                if not df.empty:
                    loaded_from_fallback_file = True
                symbol_set = {str(s).strip().upper() for s in symbols if str(s).strip()}
                if symbol_set and not df.empty and "symbol" in df.columns:
                    df = df[df["symbol"].astype(str).str.upper().isin(symbol_set)].copy()
            except pd.errors.EmptyDataError:
                df = pd.DataFrame()

        if not df.empty:
            if "event_type" not in df.columns:
                df["event_type"] = et
            all_events_frames.append(df)

    resplit_attempted = False
    holdout_integrity_failed = False
    returned_empty_due_to_holdout = False

    if not all_events_frames:
        # Return empty with diagnostics
        empty_df = pd.DataFrame()
        return attach_prepare_events_diagnostics(
            empty_df,
            build_prepare_events_diagnostics(
                run_id=run_id,
                event_type=str(event_type),
                symbols_requested=list(symbols),
                raw_event_count=0,
                canonical_episode_count=0,
                split_counts_payload={"train": 0, "validation": 0, "test": 0},
                loaded_from_fallback_file=False,
                holdout_integrity_failed=False,
                resplit_attempted=False,
                returned_empty_due_to_holdout=False,
                min_validation_events=max(
                    1, safe_int(fam_config.get("min_holdout_validation_events", 1), 1)
                ),
                min_test_events=max(1, safe_int(fam_config.get("min_holdout_test_events", 1), 1)),
                returned_rows=0,
            ),
        )

    events_df = pd.concat(all_events_frames, ignore_index=True)
    raw_event_count = int(len(events_df))

    if "symbol" not in events_df.columns:
        # Edge case: all loaded dfs missing symbol
        return events_df  # Diagnostics attached below anyway if needed

    # ... rest of the function remains similar but handles combined events_df ...

    if "enter_ts" not in events_df.columns:
        for col in ("timestamp", "anchor_ts", "event_ts"):
            if col in events_df.columns:
                events_df["enter_ts"] = events_df[col]
                break
        if "enter_ts" not in events_df.columns:
            logger.warning(
                "No enter_ts (or fallback) found in events. Market state conditioning skipped."
            )
            return attach_prepare_events_diagnostics(
                events_df,
                build_prepare_events_diagnostics(
                    run_id=run_id,
                    event_type=event_type,
                    symbols_requested=list(symbols),
                    raw_event_count=raw_event_count,
                    canonical_episode_count=int(len(events_df)),
                    split_counts_payload=phase2_split_counts(events_df),
                    loaded_from_fallback_file=loaded_from_fallback_file,
                    holdout_integrity_failed=False,
                    resplit_attempted=False,
                    returned_empty_due_to_holdout=False,
                    min_validation_events=max(
                        1, safe_int(fam_config.get("min_holdout_validation_events", 1), 1)
                    ),
                    min_test_events=max(
                        1, safe_int(fam_config.get("min_holdout_test_events", 1), 1)
                    ),
                    returned_rows=int(len(events_df)),
                ),
            )

    events_df["enter_ts"] = pd.to_datetime(events_df["enter_ts"], utc=True, errors="coerce")

    # Split labels assignment
    if "split_label" not in events_df.columns or events_df["split_label"].isna().all():
        max_h = max([parse_horizon_bars(h, default=0) for h in horizons] or [0])
        purge_bars = int(max_h) + int(entry_lag_bars)
        events_df = assign_event_split_labels(
            events_df,
            time_col="enter_ts",
            train_frac=float(fam_config.get("train_frac", 0.6)),
            validation_frac=float(fam_config.get("validation_frac", 0.2)),
            embargo_days=int(fam_config.get("embargo_days", 0)),
            purge_bars=purge_bars,
            bar_duration_minutes=bar_duration_minutes,
            run_mode=run_mode,
        )

    # Feature audit and context merge (condensed for brevity, should use audited_join)
    from project.features.audit import FeatureAuditRegistry

    audit_registry = FeatureAuditRegistry()

    merged_dfs: List[pd.DataFrame] = []
    for sym in events_df["symbol"].dropna().unique():
        sym_events = events_df[events_df["symbol"] == sym].copy()
        null_enter_ts = sym_events[sym_events["enter_ts"].isna()].copy()
        sym_events = (
            sym_events.dropna(subset=["enter_ts"]).sort_values("enter_ts").reset_index(drop=True)
        )
        if sym_events.empty:
            if not null_enter_ts.empty:
                merged_dfs.append(null_enter_ts)
            continue

        # Market State merge
        ms_df = pd.DataFrame()
        ms_candidates = [
            run_scoped_lake_path(
                data_root, run_id, "features", "perp", sym, timeframe, "market_context"
            ),
            data_root / "lake" / "features" / "perp" / sym / timeframe / "market_context",
        ]
        ms_dir = choose_partition_dir(ms_candidates)
        if ms_dir:
            ms_files = list_parquet_files(ms_dir)
            if ms_files:
                ms_df = read_parquet(ms_files)
        if ms_df.empty:
            legacy_ms_paths = [
                run_scoped_lake_path(
                    data_root, run_id, "context", "market_state", sym, f"{timeframe}.parquet"
                ),
                data_root / "lake" / "context" / "market_state" / sym / f"{timeframe}.parquet",
            ]
            for ms_path in legacy_ms_paths:
                if ms_path.exists():
                    ms_df = _read_csv_or_parquet(ms_path)
                    break
        if not ms_df.empty and "timestamp" in ms_df.columns:
            ms_df["timestamp"] = pd.to_datetime(ms_df["timestamp"], utc=True, errors="coerce")
            from project.core.audited_join import audited_merge_asof

            sym_events = audited_merge_asof(
                sym_events,
                ms_df,
                left_on="enter_ts",
                right_on="timestamp",
                direction="backward",
                tolerance=pd.Timedelta("1h"),
                feature_name="market_state",
                stale_threshold_seconds=3600,
                audit_registry=audit_registry,
                symbol=sym,
                run_id=run_id,
            )
            if "timestamp" in sym_events.columns:
                sym_events = sym_events.drop(columns=["timestamp"])

        # Microstructure merge
        micro_df = pd.DataFrame()
        micro_candidates = [
            run_scoped_lake_path(
                data_root, run_id, "features", "perp", sym, timeframe, "microstructure"
            ),
            data_root / "lake" / "features" / "perp" / sym / timeframe / "microstructure",
        ]
        micro_dir = choose_partition_dir(micro_candidates)
        if micro_dir:
            micro_files = list_parquet_files(micro_dir)
            if micro_files:
                micro_df = read_parquet(micro_files)
        if micro_df.empty:
            legacy_micro_paths = [
                run_scoped_lake_path(
                    data_root, run_id, "context", "microstructure", sym, f"{timeframe}.parquet"
                ),
                data_root / "lake" / "context" / "microstructure" / sym / f"{timeframe}.parquet",
            ]
            for micro_path in legacy_micro_paths:
                if micro_path.exists():
                    micro_df = _read_csv_or_parquet(micro_path)
                    break
        if not micro_df.empty and "timestamp" in micro_df.columns:
            micro_df["timestamp"] = pd.to_datetime(micro_df["timestamp"], utc=True, errors="coerce")
            from project.core.audited_join import audited_merge_asof

            sym_events = audited_merge_asof(
                sym_events,
                micro_df,
                left_on="enter_ts",
                right_on="timestamp",
                direction="backward",
                tolerance=pd.Timedelta("15min"),
                feature_name="microstructure",
                stale_threshold_seconds=900,
                audit_registry=audit_registry,
                symbol=sym,
                run_id=run_id,
            )
            if "timestamp" in sym_events.columns:
                sym_events = sym_events.drop(columns=["timestamp"])

        if not null_enter_ts.empty:
            sym_events = (
                pd.concat([sym_events, null_enter_ts], ignore_index=True)
                .sort_values("enter_ts", na_position="last")
                .reset_index(drop=True)
            )
        merged_dfs.append(sym_events)

    if merged_dfs:
        events_df = pd.concat(merged_dfs, ignore_index=True)

    # Feature audit artifacts
    audit_dir = data_root / "reports" / "feature_audit" / run_id
    audit_registry.write_artifacts(audit_dir)

    # Holdout validation and fail-closed logic
    min_validation_events = max(1, safe_int(fam_config.get("min_holdout_validation_events", 1), 1))
    min_test_events = max(1, safe_int(fam_config.get("min_holdout_test_events", 1), 1))

    split_diag = assert_holdout_split_integrity(
        events_df, time_col="enter_ts", split_col="split_label"
    )
    validation_count = safe_int(split_diag.get("counts", {}).get("validation", 0), 0)
    test_count = safe_int(split_diag.get("counts", {}).get("test", 0), 0)

    if validation_count < min_validation_events or test_count < min_test_events:
        holdout_integrity_failed = True
        is_promo = str(run_mode).lower() in {
            "production",
            "certification",
            "promotion",
            "deploy",
        }
        if is_promo:
            raise ValueError(f"Holdout fail-closed for {event_type}: insufficent OOS events.")

        # When we only have a fallback file (no registry anchors), keep the
        # data but warn – we don't want to silently discard all events that
        # were successfully materialized to disk.
        if loaded_from_fallback_file:
            logger.info(
                "Holdout integrity check failed for %s (val=%s, test=%s) using fallback events; "
                "returning non-promotable training-only events.",
                event_type,
                validation_count,
                test_count,
            )
            return attach_prepare_events_diagnostics(
                events_df,
                build_prepare_events_diagnostics(
                    run_id=run_id,
                    event_type=event_type,
                    symbols_requested=list(symbols),
                    raw_event_count=raw_event_count,
                    canonical_episode_count=int(len(events_df)),
                    split_counts_payload=phase2_split_counts(events_df),
                    loaded_from_fallback_file=loaded_from_fallback_file,
                    holdout_integrity_failed=holdout_integrity_failed,
                    resplit_attempted=False,
                    returned_empty_due_to_holdout=False,
                    min_validation_events=min_validation_events,
                    min_test_events=min_test_events,
                    returned_rows=int(len(events_df)),
                ),
            )

        # Research-mode safeguard: attempt deterministic resplit for degenerate
        # incoming labels, then fail-closed if we still have no OOS coverage.
        logger.warning(
            "Holdout integrity check failed for %s (val=%s, test=%s)",
            event_type,
            validation_count,
            test_count,
        )

        try:
            resplit_attempted = True
            max_h = max([parse_horizon_bars(h, default=0) for h in horizons] or [0])
            purge_bars = int(max_h) + int(entry_lag_bars)
            events_df = assign_event_split_labels(
                events_df,
                time_col="enter_ts",
                train_frac=float(fam_config.get("train_frac", 0.6)),
                validation_frac=float(fam_config.get("validation_frac", 0.2)),
                embargo_days=int(fam_config.get("embargo_days", 0)),
                purge_bars=purge_bars,
                bar_duration_minutes=bar_duration_minutes,
                run_mode=run_mode,
            )
            split_diag = assert_holdout_split_integrity(
                events_df, time_col="enter_ts", split_col="split_label"
            )
            validation_count = safe_int(split_diag.get("counts", {}).get("validation", 0), 0)
            test_count = safe_int(split_diag.get("counts", {}).get("test", 0), 0)
        except Exception:
            # If resplitting fails, treat this configuration as non-promotable
            # by returning an empty frame to avoid leaking unstable splits.
            returned_empty_due_to_holdout = True
            empty = events_df.iloc[0:0].copy()
            return attach_prepare_events_diagnostics(
                empty,
                build_prepare_events_diagnostics(
                    run_id=run_id,
                    event_type=event_type,
                    symbols_requested=list(symbols),
                    raw_event_count=raw_event_count,
                    canonical_episode_count=int(len(events_df)),
                    split_counts_payload=phase2_split_counts(events_df),
                    loaded_from_fallback_file=loaded_from_fallback_file,
                    holdout_integrity_failed=holdout_integrity_failed,
                    resplit_attempted=resplit_attempted,
                    returned_empty_due_to_holdout=returned_empty_due_to_holdout,
                    min_validation_events=min_validation_events,
                    min_test_events=min_test_events,
                    returned_rows=0,
                ),
            )

        if validation_count < min_validation_events or test_count < min_test_events:
            # Even after resplitting we have no usable OOS data; fail-closed in
            # research mode by returning an empty frame.
            returned_empty_due_to_holdout = True
            empty = events_df.iloc[0:0].copy()
            return attach_prepare_events_diagnostics(
                empty,
                build_prepare_events_diagnostics(
                    run_id=run_id,
                    event_type=event_type,
                    symbols_requested=list(symbols),
                    raw_event_count=raw_event_count,
                    canonical_episode_count=int(len(events_df)),
                    split_counts_payload=phase2_split_counts(events_df),
                    loaded_from_fallback_file=loaded_from_fallback_file,
                    holdout_integrity_failed=holdout_integrity_failed,
                    resplit_attempted=resplit_attempted,
                    returned_empty_due_to_holdout=returned_empty_due_to_holdout,
                    min_validation_events=min_validation_events,
                    min_test_events=min_test_events,
                    returned_rows=0,
                ),
            )

    return attach_prepare_events_diagnostics(
        events_df,
        build_prepare_events_diagnostics(
            run_id=run_id,
            event_type=event_type,
            symbols_requested=list(symbols),
            raw_event_count=raw_event_count,
            canonical_episode_count=int(len(events_df)),
            split_counts_payload=phase2_split_counts(events_df),
            loaded_from_fallback_file=loaded_from_fallback_file,
            holdout_integrity_failed=holdout_integrity_failed,
            resplit_attempted=resplit_attempted,
            returned_empty_due_to_holdout=returned_empty_due_to_holdout,
            min_validation_events=min_validation_events,
            min_test_events=min_test_events,
            returned_rows=int(len(events_df)),
        ),
    )


def assign_event_split_labels(
    events: pd.DataFrame,
    *,
    time_col: str = "enter_ts",
    train_frac: float = 0.6,
    validation_frac: float = 0.2,
    embargo_days: int = 0,
    embargo_bars: int = 0,
    purge_bars: int = 0,
    bar_duration_minutes: int = 5,
    run_mode: str = "research",
) -> pd.DataFrame:
    """Assign train/validation/test labels using the validation split engine.

    Backward compatibility is preserved by accepting ``embargo_days`` and converting it
    into bars when ``embargo_bars`` is not supplied.
    """
    if events.empty or time_col not in events.columns:
        return events
    out = events.copy()
    ts = pd.to_datetime(out[time_col], utc=True, errors="coerce")
    out[time_col] = ts
    valid = ts.notna()
    if valid.sum() < 2:
        out["split_label"] = "train"
        out["non_promotable"] = True
        return out

    if int(embargo_bars) <= 0 and int(embargo_days) > 0:
        embargo_bars = int(round((24 * 60 * int(embargo_days)) / max(1, int(bar_duration_minutes))))

    try:
        out = _validation_assign_split_labels(
            out,
            time_col=time_col,
            train_frac=float(train_frac),
            validation_frac=float(validation_frac),
            embargo_bars=int(embargo_bars),
            purge_bars=int(purge_bars),
            bar_duration_minutes=int(bar_duration_minutes),
            split_col="split_label",
        )
        assert_holdout_split_integrity(out, time_col=time_col, split_col="split_label")
    except Exception:
        if str(run_mode).lower() in {"production", "promotion"}:
            raise
        log.warning(
            "Event split assignment failed in research mode; marking rows non-promotable",
            exc_info=True,
        )
        out["split_label"] = "train"
        out["non_promotable"] = True
    return out


def populate_fail_reasons(df: pd.DataFrame) -> pd.DataFrame:
    """Populate primary failure reasons for rejected candidates.

    When ``gate_phase2_final`` is False, prefer:

    1. Explicit ``fail_reasons`` token if present.
    2. Out-of-sample gates (``gate_oos_*``).
    3. Retail gates (``gate_retail_*``).
    4. Any other specific gate that failed.
    5. Fallback to ``gate_phase2_final`` if no other signal is available.
    """
    if df.empty:
        return df

    gate_cols = [c for c in df.columns if c.startswith("gate_")]
    oos_gates = [c for c in gate_cols if c.startswith("gate_oos_")]
    retail_gates = [c for c in gate_cols if c.startswith("gate_retail_")]

    df["fail_gate_primary"] = ""
    df["fail_reason_primary"] = ""

    for idx, row in df.iterrows():
        if bool(row.get("gate_phase2_final", True)):
            continue

        # 1) Explicit fail_reasons token wins if present.
        token = str(row.get("fail_reasons", "") or "").strip()
        if token:
            df.at[idx, "fail_gate_primary"] = token
            df.at[idx, "fail_reason_primary"] = f"failed_{token}"
            continue

        chosen_gate: str | None = None

        # 2) Out-of-sample gates (min samples, validation, etc.).
        for gate in oos_gates:
            if not bool(row.get(gate, True)):
                chosen_gate = gate
                break

        # 3) Retail gates if no OOS gate was the culprit.
        if chosen_gate is None:
            for gate in retail_gates:
                if not bool(row.get(gate, True)):
                    chosen_gate = gate
                    break

        # 4) Any other specific gate, excluding the aggregate phase2 flag.
        if chosen_gate is None:
            for gate in gate_cols:
                if gate == "gate_phase2_final":
                    continue
                if not bool(row.get(gate, True)):
                    chosen_gate = gate
                    break

        # 5) Fallback if none of the above signalled a specific gate.
        if chosen_gate is None:
            chosen_gate = "gate_phase2_final"

        df.at[idx, "fail_gate_primary"] = chosen_gate
        df.at[idx, "fail_reason_primary"] = f"failed_{chosen_gate}"

    return df


def write_gate_summary(df: pd.DataFrame, out_path: Path) -> None:
    """Write Phase 2 gating summary to JSON."""
    if df.empty:
        return
    gate_cols = [c for c in df.columns if c.startswith("gate_")]
    summary = {
        "candidates_total": len(df),
        "pass_all_gates": int(df.get("gate_phase2_final", pd.Series(0)).sum()),
        "per_gate_pass_count": {
            c: int(df[c].sum()) for c in gate_cols if df[c].dtype in (bool, int)
        },
        "per_gate_fail_count": {
            c: int((~df[c].astype(bool)).sum()) for c in gate_cols if df[c].dtype in (bool, int)
        },
    }
    with open(out_path, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)


def _read_csv_or_parquet(path: Path) -> pd.DataFrame:
    """Internal helper to load data from CSV or Parquet."""
    if not path.exists():
        return pd.DataFrame()
    try:
        if path.suffix.lower() == ".parquet":
            try:
                return pd.read_parquet(path)
            except Exception as parquet_exc:
                csv_path = path.with_suffix(".csv")
                if csv_path.exists():
                    try:
                        return pd.read_csv(csv_path)
                    except Exception as csv_exc:
                        log.warning(
                            "Failed to read tabular artifact %s via parquet (%s) and CSV fallback %s (%s)",
                            path,
                            parquet_exc,
                            csv_path,
                            csv_exc,
                        )
                        return pd.DataFrame()
                log.warning("Failed to read tabular artifact %s: %s", path, parquet_exc)
                return pd.DataFrame()
        return pd.read_csv(path)
    except Exception as exc:
        log.warning("Failed to read tabular artifact %s: %s", path, exc)
        return pd.DataFrame()
