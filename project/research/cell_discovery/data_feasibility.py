from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from project.core.feature_schema import feature_dataset_dir_name
from project.events.detectors.registry import get_detector_class
from project.features.assembly import filter_time_window, prune_partition_files_by_window
from project.io.utils import (
    atomic_write_json,
    choose_partition_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
)
from project.research.cell_discovery.data_contract import (
    contract_summary,
    required_condition_keys,
    required_event_types,
)
from project.research.context_labels import expand_dimension_values
from project.research.cell_discovery.models import DataFeasibilityResult, DiscoveryRegistry
from project.research.cell_discovery.paths import paths_for_run
from project.research.condition_key_contract import (
    load_symbol_joined_condition_contract,
    missing_condition_keys,
)
from project.research.feature_surface_viability import (
    _load_bars_for_viability,
    analyze_feature_surface_viability,
)
from project.research.validation.splits import build_repeated_walkforward_splits

_COVERAGE_WARN_RATIO = 0.80


def _load_partitioned_frame(path_dir: Path | None, *, start: str | None, end: str | None) -> pd.DataFrame:
    if path_dir is None:
        return pd.DataFrame()
    files = prune_partition_files_by_window(list_parquet_files(path_dir), start=start, end=end)
    if not files:
        return pd.DataFrame()
    frame = read_parquet(files)
    return filter_time_window(frame, start=start, end=end)


def _load_feature_frame(*, data_root: Path, run_id: str, symbol: str, timeframe: str, start: str, end: str) -> pd.DataFrame:
    dataset = feature_dataset_dir_name()
    root = choose_partition_dir(
        [
            run_scoped_lake_path(data_root, run_id, "features", "perp", symbol, timeframe, dataset),
            Path(data_root) / "lake" / "features" / "perp" / symbol / timeframe / dataset,
        ]
    )
    return _load_partitioned_frame(root, start=start, end=end)


def _load_market_context_frame(*, data_root: Path, run_id: str, symbol: str, timeframe: str, start: str, end: str) -> pd.DataFrame:
    root = choose_partition_dir(
        [
            run_scoped_lake_path(data_root, run_id, "features", "perp", symbol, timeframe, "market_context"),
            Path(data_root) / "lake" / "features" / "perp" / symbol / timeframe / "market_context",
        ]
    )
    return _load_partitioned_frame(root, start=start, end=end)


def _normalize_timestamps(values: pd.Series | list | pd.Index) -> pd.Series:
    ts = pd.to_datetime(values, utc=True, errors="coerce")
    if isinstance(ts, pd.DatetimeIndex):
        ts = pd.Series(ts)
    return ts.dropna().sort_values().drop_duplicates().reset_index(drop=True)


def _expected_timestamps(*, start: str, end: str, timeframe: str) -> pd.Series:
    if not start or not end:
        return pd.Series(dtype="datetime64[ns, UTC]")
    freq = "5min" if timeframe == "5m" else timeframe
    return pd.Series(pd.date_range(start=start, end=end, freq=freq, tz="UTC"))


def _synthetic_split_status(*, start: str, end: str, timeframe: str) -> dict[str, Any]:
    timestamps = _expected_timestamps(start=start, end=end, timeframe=timeframe)
    if timestamps.empty:
        return {"status": "unknown", "reason": "start/end not provided", "mode": "synthetic"}
    folds = build_repeated_walkforward_splits(
        timestamps,
        train_bars=120,
        validation_bars=40,
        test_bars=40,
        step_bars=40,
        min_folds=1,
        max_folds=3,
    )
    return {
        "status": "pass" if folds else "block",
        "mode": "synthetic",
        "fold_count": len(folds),
        "coverage_ratio": 1.0,
        "bar_count": int(len(timestamps)),
        "expected_bar_count": int(len(timestamps)),
    }


def _split_status(*, data_root: Path, run_id: str, symbols: list[str], timeframe: str, start: str, end: str) -> dict[str, Any]:
    per_symbol: dict[str, Any] = {}
    statuses: list[str] = []
    used_real = False
    expected = _expected_timestamps(start=start, end=end, timeframe=timeframe)
    expected_count = int(len(expected))
    for symbol in symbols:
        bars, source = _load_bars_for_viability(
            data_root=data_root,
            run_id=run_id,
            market="perp",
            symbol=symbol,
            timeframe=timeframe,
            start=start,
            end=end,
        )
        if bars.empty or "timestamp" not in bars.columns:
            synthetic = _synthetic_split_status(start=start, end=end, timeframe=timeframe)
            synthetic.update({"bars_source": source, "reason": "bars_unavailable_fallback_synthetic"})
            per_symbol[symbol] = synthetic
            statuses.append(str(synthetic.get("status", "unknown")))
            continue
        ts = _normalize_timestamps(bars["timestamp"])
        used_real = True
        if ts.empty:
            per_symbol[symbol] = {
                "status": "block",
                "mode": "real",
                "bars_source": source,
                "reason": "no_valid_timestamps",
                "bar_count": 0,
                "expected_bar_count": expected_count,
                "coverage_ratio": 0.0,
                "fold_count": 0,
            }
            statuses.append("block")
            continue
        folds = build_repeated_walkforward_splits(
            ts,
            train_bars=120,
            validation_bars=40,
            test_bars=40,
            step_bars=40,
            min_folds=1,
            max_folds=3,
        )
        coverage_ratio = float(len(ts) / expected_count) if expected_count > 0 else 1.0
        if not folds:
            status = "block"
        elif coverage_ratio < _COVERAGE_WARN_RATIO:
            status = "warn"
        else:
            status = "pass"
        per_symbol[symbol] = {
            "status": status,
            "mode": "real",
            "bars_source": source,
            "bar_count": int(len(ts)),
            "expected_bar_count": expected_count,
            "coverage_ratio": coverage_ratio,
            "fold_count": len(folds),
            "timestamp_min_utc": str(ts.iloc[0]),
            "timestamp_max_utc": str(ts.iloc[-1]),
        }
        statuses.append(status)
    if not statuses:
        return {"status": "unknown", "reason": "no_symbols", "per_symbol": {}}
    if all(status == "block" for status in statuses):
        overall = "block"
    elif any(status == "warn" for status in statuses) or any(status == "block" for status in statuses):
        overall = "warn"
    elif all(status == "pass" for status in statuses):
        overall = "pass"
    else:
        overall = "unknown"
    return {
        "status": overall,
        "mode": "real" if used_real else "synthetic",
        "expected_bar_count": expected_count,
        "per_symbol": per_symbol,
    }


def _status_rank(status: str) -> int:
    order = {"pass": 0, "warn": 1, "unknown": 2, "block": 3}
    return order.get(str(status or "unknown"), 2)


def _worst_status(statuses: list[str]) -> str:
    if not statuses:
        return "unknown"
    return max((str(status or "unknown") for status in statuses), key=_status_rank)


def _event_symbol_status(
    feature_surface: dict[str, Any],
    *,
    event_type: str,
    symbol: str,
) -> dict[str, Any]:
    symbol_payload = dict((feature_surface.get("symbols") or {}).get(symbol, {}) or {})
    detectors = dict(symbol_payload.get("detectors", {}) or {})
    event_payload = dict(detectors.get(event_type, {}) or {})
    if not event_payload:
        aggregate = dict((feature_surface.get("detectors") or {}).get(event_type, {}) or {})
        status = str(aggregate.get("status", "unknown") or "unknown")
        return {
            "status": status,
            "blocking_columns": [],
            "degraded_columns": [],
            "reason": "symbol_event_status_missing",
        }
    return {
        "status": str(event_payload.get("status", "unknown") or "unknown"),
        "blocking_columns": list(event_payload.get("blocking_columns", []) or []),
        "degraded_columns": list(event_payload.get("degraded_columns", []) or []),
        "required_columns": list(event_payload.get("required_columns", []) or []),
    }


def _condition_symbol_status(
    condition_payload: dict[str, Any],
    *,
    symbol: str,
    required_keys: list[str],
) -> dict[str, Any]:
    payload = dict(condition_payload.get(symbol, {}) or {})
    missing = missing_condition_keys(required_keys, payload.get("available_keys", set()))
    if not payload:
        return {
            "status": "unknown",
            "missing_condition_keys": sorted(required_keys),
            "required_condition_keys": sorted(required_keys),
        }
    if missing:
        return {
            "status": "block",
            "missing_condition_keys": sorted(missing),
            "required_condition_keys": sorted(required_keys),
        }
    # Treat condition-key feasibility as cell-local: a symbol can be missing keys for some
    # context cells without blocking unrelated cells (including unconditional).
    payload_status = str(payload.get("status", "unknown") or "unknown")
    return {
        "status": "unknown" if payload_status == "unknown" else "pass",
        "missing_condition_keys": [],
        "required_condition_keys": sorted(required_keys),
    }


def _normalize_scalar(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def _value_mask(series: pd.Series, values: list[str]) -> pd.Series:
    if series.empty:
        return pd.Series([], dtype=bool)
    numeric_targets: list[float] = []
    all_numeric = True
    for value in values:
        try:
            numeric_targets.append(float(str(value).strip()))
        except Exception:
            all_numeric = False
            break
    coerced = pd.to_numeric(series, errors="coerce")
    if all_numeric and coerced.notna().any():
        return coerced.isin(numeric_targets)
    return series.map(_normalize_scalar).isin({_normalize_scalar(value) for value in values})


def _context_state_mask(context_frame: pd.DataFrame, context: Any | None) -> pd.Series:
    if context_frame.empty:
        return pd.Series([], dtype=bool)
    if context is None:
        return pd.Series(True, index=context_frame.index, dtype=bool)
    if context.dimension and context.dimension in context_frame.columns:
        vals = expand_dimension_values(context.dimension, list(context.values))
        return _value_mask(context_frame[context.dimension], vals)
    if context.required_feature_key and context.required_feature_key in context_frame.columns:
        series = context_frame[context.required_feature_key]
        numeric = pd.to_numeric(series, errors="coerce")
        if numeric.notna().any():
            return numeric.fillna(0.0) > 0.0
        return _value_mask(series, list(context.values))
    return pd.Series(False, index=context_frame.index, dtype=bool)


def _detected_events(feature_frame: pd.DataFrame, *, event_type: str, symbol: str) -> tuple[pd.DataFrame, str | None]:
    if feature_frame.empty:
        return pd.DataFrame(), "feature_frame_missing"
    detector_cls = get_detector_class(event_type)
    if detector_cls is None:
        return pd.DataFrame(), "detector_unavailable"
    try:
        detector = detector_cls()
        events = detector.detect(feature_frame.copy(), symbol=symbol)
    except Exception as exc:  # pragma: no cover - best-effort diagnostic path
        return pd.DataFrame(), f"detect_failed:{exc}"
    if events is None or events.empty or "timestamp" not in events.columns:
        return pd.DataFrame(columns=["timestamp"]), None
    out = events.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    out = out.dropna(subset=["timestamp"]).drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
    return out, None


def _support_status(
    *,
    feature_frame: pd.DataFrame,
    context_frame: pd.DataFrame,
    atom: Any,
    context: Any | None,
    symbol: str,
    min_support: int,
    detection_cache: dict[tuple[str, str], tuple[pd.DataFrame, str | None]],
) -> dict[str, Any]:
    if feature_frame.empty:
        return {"status": "not_evaluated", "reason": "feature_artifact_unavailable"}
    cache_key = (symbol, atom.event_type)
    if cache_key not in detection_cache:
        detection_cache[cache_key] = _detected_events(feature_frame, event_type=atom.event_type, symbol=symbol)
    events, event_error = detection_cache[cache_key]
    event_count = int(len(events))
    if event_error:
        return {
            "status": "not_evaluated",
            "reason": event_error,
            "event_count": event_count,
        }
    if context is None:
        support_count = event_count
        state_row_count = int(len(feature_frame))
        state_density_ratio = 1.0 if len(feature_frame) else 0.0
    else:
        if context_frame.empty:
            return {
                "status": "not_evaluated",
                "reason": "market_context_unavailable",
                "event_count": event_count,
            }
        mask = _context_state_mask(context_frame, context)
        if len(mask) != len(context_frame):
            mask = pd.Series(False, index=context_frame.index, dtype=bool)
        state_row_count = int(mask.sum())
        state_density_ratio = float(state_row_count / len(context_frame)) if len(context_frame) else 0.0
        if state_row_count <= 0:
            return {
                "status": "block",
                "reason": "blocked_zero_state_density",
                "event_count": event_count,
                "support_count": 0,
                "state_row_count": 0,
                "state_density_ratio": 0.0,
            }
        eligible_ts = set(pd.to_datetime(context_frame.loc[mask, "timestamp"], utc=True, errors="coerce").dropna())
        support_count = int(events["timestamp"].isin(eligible_ts).sum()) if event_count else 0
    support_ratio = float(support_count / event_count) if event_count else 0.0
    if support_count <= 0:
        return {
            "status": "block",
            "reason": "blocked_zero_event_support",
            "event_count": event_count,
            "support_count": support_count,
            "support_ratio": support_ratio,
            "state_row_count": state_row_count,
            "state_density_ratio": state_density_ratio,
        }
    if support_count < int(max(1, min_support)):
        return {
            "status": "warn",
            "reason": "warn_low_support",
            "event_count": event_count,
            "support_count": support_count,
            "support_ratio": support_ratio,
            "state_row_count": state_row_count,
            "state_density_ratio": state_density_ratio,
        }
    return {
        "status": "pass",
        "reason": "support_ok",
        "event_count": event_count,
        "support_count": support_count,
        "support_ratio": support_ratio,
        "state_row_count": state_row_count,
        "state_density_ratio": state_density_ratio,
    }


def _cell_matrix(
    *,
    registry: DiscoveryRegistry,
    symbols: list[str],
    feature_surface: dict[str, Any],
    condition_payload: dict[str, Any],
    split_payload: dict[str, Any],
    data_root: Path,
    run_id: str,
    timeframe: str,
    start: str,
    end: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    feature_frames = {
        symbol: _load_feature_frame(
            data_root=data_root,
            run_id=run_id,
            symbol=symbol,
            timeframe=timeframe,
            start=start,
            end=end,
        )
        for symbol in symbols
    }
    context_frames = {
        symbol: _load_market_context_frame(
            data_root=data_root,
            run_id=run_id,
            symbol=symbol,
            timeframe=timeframe,
            start=start,
            end=end,
        )
        for symbol in symbols
    }
    detection_cache: dict[tuple[str, str], tuple[pd.DataFrame, str | None]] = {}
    min_support = int(getattr(registry.ranking_policy, "min_support", 30))
    split_by_symbol = dict(split_payload.get("per_symbol", {}) or {})
    for atom in registry.event_atoms:
        contexts = [None, *registry.context_cells]
        for context in contexts:
            context_cell = context.cell_id if context is not None else "unconditional"
            required_condition_keys = []
            if context is not None:
                required_condition_keys = [
                    key
                    for key in (context.dimension, context.required_feature_key)
                    if str(key or "").strip()
                ]
            for symbol in symbols:
                event_status = _event_symbol_status(
                    feature_surface,
                    event_type=atom.event_type,
                    symbol=symbol,
                )
                condition_status = _condition_symbol_status(
                    condition_payload,
                    symbol=symbol,
                    required_keys=required_condition_keys,
                )
                split_detail = dict(split_by_symbol.get(symbol, {}) or {})
                split_status = str(split_detail.get("status", split_payload.get("status", "unknown")) or "unknown")
                support = _support_status(
                    feature_frame=feature_frames.get(symbol, pd.DataFrame()),
                    context_frame=context_frames.get(symbol, pd.DataFrame()),
                    atom=atom,
                    context=context,
                    symbol=symbol,
                    min_support=min_support,
                    detection_cache=detection_cache,
                )
                blocked_reasons: list[str] = []
                if event_status["status"] == "block":
                    blocked_reasons.append("blocked_missing_data")
                elif event_status["status"] == "unknown":
                    blocked_reasons.append("blocked_unknown_event_surface")
                if condition_status["status"] == "block":
                    blocked_reasons.append("blocked_missing_condition_keys")
                elif condition_status["status"] == "unknown" and required_condition_keys:
                    blocked_reasons.append("blocked_unknown_condition_keys")
                if split_status == "block":
                    blocked_reasons.append("blocked_missing_forward_window")
                elif split_status == "unknown":
                    blocked_reasons.append("blocked_unknown_forward_window")
                if support.get("status") == "block":
                    blocked_reasons.append(str(support.get("reason", "blocked_zero_event_support")))
                statuses = [event_status["status"], condition_status["status"], split_status]
                if str(support.get("status", "")).strip() not in {"", "not_evaluated"}:
                    statuses.append(str(support.get("status")))
                status = "block" if blocked_reasons else _worst_status(statuses)
                rows.append(
                    {
                        "event_atom_id": atom.atom_id,
                        "event_type": atom.event_type,
                        "event_family": atom.event_family,
                        "context_cell": context_cell,
                        "executability_class": (
                            "unconditional"
                            if context is None
                            else context.executability_class
                        ),
                        "symbol": symbol,
                        "status": status,
                        "blocked_reasons": sorted(set(blocked_reasons)),
                        "event_status": event_status,
                        "condition_status": condition_status,
                        "split_status": split_status,
                        "split_detail": split_detail,
                        "support": support,
                    }
                )
    return rows


def verify_data_contract(
    *,
    registry: DiscoveryRegistry,
    run_id: str,
    data_root: Path,
    symbols: list[str],
    timeframe: str,
    start: str = "",
    end: str = "",
) -> DataFeasibilityResult:
    paths = paths_for_run(data_root=data_root, run_id=run_id)
    paths.run_dir.mkdir(parents=True, exist_ok=True)
    events = required_event_types(registry)
    keys = required_condition_keys(registry)
    feature_surface = analyze_feature_surface_viability(
        data_root=data_root,
        run_id=run_id,
        symbols=symbols,
        timeframe=timeframe,
        start=start,
        end=end,
        event_types=events,
    )
    condition_payload: dict[str, Any] = {}
    for symbol in symbols:
        try:
            contract = load_symbol_joined_condition_contract(
                data_root=data_root,
                run_id=run_id,
                symbol=symbol,
                timeframe=timeframe,
            )
            missing = missing_condition_keys(keys, contract.get("keys", set()))
            condition_payload[symbol] = {
                "status": "pass" if not missing else "block",
                "missing_condition_keys": sorted(missing),
                "available_key_count": len(contract.get("keys", set())),
                "available_keys": sorted(contract.get("keys", set())),
            }
        except Exception as exc:
            condition_payload[symbol] = {
                "status": "unknown",
                "error": str(exc),
                "missing_condition_keys": keys,
            }

    try:
        split_payload = _split_status(
            data_root=data_root,
            run_id=run_id,
            symbols=symbols,
            timeframe=timeframe,
            start=start,
            end=end,
        )
    except Exception as exc:
        split_payload = {"status": "block", "reason": str(exc), "per_symbol": {}}

    matrix = _cell_matrix(
        registry=registry,
        symbols=symbols,
        feature_surface=feature_surface,
        condition_payload=condition_payload,
        split_payload=split_payload,
        data_root=data_root,
        run_id=run_id,
        timeframe=timeframe,
        start=start,
        end=end,
    )
    cell_status_counts = {
        status: sum(1 for row in matrix if row["status"] == status)
        for status in ("pass", "warn", "unknown", "block")
    }
    blocked_by_reason: dict[str, int] = {}
    support_status_counts: dict[str, int] = {}
    for row in matrix:
        support_status = str((row.get("support") or {}).get("status", "not_evaluated") or "not_evaluated")
        support_status_counts[support_status] = support_status_counts.get(support_status, 0) + 1
        for reason in row["blocked_reasons"]:
            blocked_by_reason[reason] = blocked_by_reason.get(reason, 0) + 1

    statuses = [
        str(feature_surface.get("status", "unknown")),
        *(str(item.get("status", "unknown")) for item in condition_payload.values()),
        str(split_payload.get("status", "unknown")),
    ]
    if matrix and all(row["status"] == "block" for row in matrix):
        status = "block"
    elif "block" in statuses or any(row["status"] == "block" for row in matrix):
        status = "warn"
    elif "warn" in statuses or "unknown" in statuses or any(
        row["status"] in {"warn", "unknown"} for row in matrix
    ):
        status = "warn"
    else:
        status = "pass"

    payload = {
        **contract_summary(registry),
        "schema_version": "edge_cell_data_contract_v2",
        "run_id": run_id,
        "symbols": symbols,
        "timeframe": timeframe,
        "start": start,
        "end": end,
        "status": status,
        "feature_surface": feature_surface,
        "condition_keys": condition_payload,
        "split_feasibility": split_payload,
        "cell_feasibility": matrix,
        "cell_status_counts": cell_status_counts,
        "support_status_counts": dict(sorted(support_status_counts.items())),
        "blocked_by_reason": dict(sorted(blocked_by_reason.items())),
        "blocked_reasons": sorted(reason for reason, count in blocked_by_reason.items() if count > 0),
    }
    atomic_write_json(paths.data_contract_path, payload)
    return DataFeasibilityResult(
        status=status,
        report_path=paths.data_contract_path,
        payload=payload,
    )
