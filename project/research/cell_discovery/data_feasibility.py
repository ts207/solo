from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from project.io.utils import atomic_write_json
from project.research.cell_discovery.data_contract import (
    contract_summary,
    required_condition_keys,
    required_event_types,
)
from project.research.cell_discovery.models import DataFeasibilityResult, DiscoveryRegistry
from project.research.cell_discovery.paths import paths_for_run
from project.research.condition_key_contract import (
    load_symbol_joined_condition_contract,
    missing_condition_keys,
)
from project.research.feature_surface_viability import analyze_feature_surface_viability
from project.research.validation.splits import build_repeated_walkforward_splits


def _split_status(*, start: str, end: str, timeframe: str) -> dict[str, Any]:
    if not start or not end:
        return {"status": "unknown", "reason": "start/end not provided"}
    freq = "5min" if timeframe == "5m" else timeframe
    timestamps = pd.date_range(start=start, end=end, freq=freq, tz="UTC")
    folds = build_repeated_walkforward_splits(
        pd.Series(timestamps),
        train_bars=120,
        validation_bars=40,
        test_bars=40,
        step_bars=40,
        min_folds=1,
        max_folds=3,
    )
    return {"status": "pass" if folds else "block", "fold_count": len(folds)}


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
    return {
        "status": str(payload.get("status", "unknown") or "unknown"),
        "missing_condition_keys": [],
        "required_condition_keys": sorted(required_keys),
    }


def _cell_matrix(
    *,
    registry: DiscoveryRegistry,
    symbols: list[str],
    feature_surface: dict[str, Any],
    condition_payload: dict[str, Any],
    split_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    split_status = str(split_payload.get("status", "unknown") or "unknown")
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
                statuses = [event_status["status"], condition_status["status"], split_status]
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
    missing_any: set[str] = set()
    for symbol in symbols:
        try:
            contract = load_symbol_joined_condition_contract(
                data_root=data_root,
                run_id=run_id,
                symbol=symbol,
                timeframe=timeframe,
            )
            missing = missing_condition_keys(keys, contract.get("keys", set()))
            missing_any.update(missing)
            condition_payload[symbol] = {
                "status": "pass" if not missing else "block",
                "missing_condition_keys": sorted(missing),
                "available_key_count": len(contract.get("keys", set())),
                "available_keys": sorted(contract.get("keys", set())),
            }
        except Exception as exc:
            missing_any.update(keys)
            condition_payload[symbol] = {
                "status": "unknown",
                "error": str(exc),
                "missing_condition_keys": keys,
            }

    try:
        split_payload = _split_status(start=start, end=end, timeframe=timeframe)
    except Exception as exc:
        split_payload = {"status": "block", "reason": str(exc)}

    matrix = _cell_matrix(
        registry=registry,
        symbols=symbols,
        feature_surface=feature_surface,
        condition_payload=condition_payload,
        split_payload=split_payload,
    )
    cell_status_counts = {
        status: sum(1 for row in matrix if row["status"] == status)
        for status in ("pass", "warn", "unknown", "block")
    }
    blocked_by_reason: dict[str, int] = {}
    for row in matrix:
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
        "blocked_by_reason": dict(sorted(blocked_by_reason.items())),
        "blocked_reasons": sorted(
            reason
            for reason, enabled in {
                "blocked_missing_data": bool(blocked_by_reason.get("blocked_missing_data")),
                "blocked_missing_forward_window": bool(
                    blocked_by_reason.get("blocked_missing_forward_window")
                ),
                "blocked_missing_condition_keys": bool(
                    blocked_by_reason.get("blocked_missing_condition_keys")
                ),
                "blocked_unknown_event_surface": bool(
                    blocked_by_reason.get("blocked_unknown_event_surface")
                ),
                "blocked_unknown_condition_keys": bool(
                    blocked_by_reason.get("blocked_unknown_condition_keys")
                ),
                "blocked_unknown_forward_window": bool(
                    blocked_by_reason.get("blocked_unknown_forward_window")
                ),
            }.items()
            if enabled
        ),
    }
    atomic_write_json(paths.data_contract_path, payload)
    return DataFeasibilityResult(
        status=status,
        report_path=paths.data_contract_path,
        payload=payload,
    )
