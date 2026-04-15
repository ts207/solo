from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from project.io.utils import read_parquet
from project.research.services.pathing import resolve_phase2_candidates_path


BASE_STRUCTURAL_KEY_COLUMNS = ["symbol", "event_type", "direction", "rule_template", "horizon"]
STRICT_OPTIONAL_STRUCTURAL_KEY_COLUMNS = [
    "entry_lag_bars",
    "stop_loss_bps",
    "take_profit_bps",
    "stop_loss_atr_multipliers",
    "take_profit_atr_multipliers",
    "state_id",
    "canonical_regime",
    "regime_bucket",
    "context_label",
    "contexts_json",
    "fee_bps_per_side",
    "slippage_bps_per_fill",
    "cost_bps",
    "round_trip_cost_bps",
    "cost_config_digest",
    "after_cost_includes_funding_carry",
    "cost_model_source",
]
STRICT_COST_IDENTITY_COLUMNS = [
    "fee_bps_per_side",
    "slippage_bps_per_fill",
    "cost_bps",
    "round_trip_cost_bps",
    "cost_config_digest",
    "after_cost_includes_funding_carry",
    "cost_model_source",
]
STRUCTURAL_KEY_COLUMNS = list(BASE_STRUCTURAL_KEY_COLUMNS)


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return read_parquet(path)
    except Exception:
        return pd.DataFrame()


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _pass_like(value: Any) -> bool:
    text = str(value).strip().lower()
    return text in {"1", "true", "t", "yes", "y", "on", "pass", "passed", "tradable"}


def _column_has_signal(frame: pd.DataFrame, column: str) -> bool:
    if frame.empty or column not in frame.columns:
        return False
    series = frame[column]
    if series.empty:
        return False
    if pd.api.types.is_numeric_dtype(series):
        return bool(pd.to_numeric(series, errors="coerce").notna().any())
    normalized = series.astype(str).str.strip().str.lower()
    return bool((~series.isna() & ~normalized.isin({"", "nan", "none", "null", "[]", "{}"})).any())


def _normalize_key_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        return json.dumps(sorted(str(item) for item in value), separators=(",", ":"))
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if not pd.isna(numeric):
        return f"{float(numeric):.10g}"
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "null"} else text


def _resolve_structural_key_columns(origin: pd.DataFrame, target: pd.DataFrame) -> List[str]:
    columns = list(BASE_STRUCTURAL_KEY_COLUMNS)
    for column in STRICT_OPTIONAL_STRUCTURAL_KEY_COLUMNS:
        if _column_has_signal(origin, column) or _column_has_signal(target, column):
            columns.append(column)
    return columns


def _search_engine_gate_pass(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype=bool)
    required = [
        "gate_oos_validation",
        "gate_after_cost_positive",
        "gate_after_cost_stressed_positive",
        "gate_bridge_tradable",
        "gate_multiplicity",
        "gate_c_regime_stable",
    ]
    if not all(col in frame.columns for col in required):
        return pd.Series(False, index=frame.index)
    mask = pd.Series(True, index=frame.index)
    for col in required:
        mask &= frame[col].fillna(False).astype(bool)
    return mask


def _normalize_origin_candidates(frame: pd.DataFrame, *, key_columns: List[str]) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    if "gate_bridge_tradable" in out.columns:
        out = out[out["gate_bridge_tradable"].apply(_pass_like)].copy()
    for col in key_columns:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].map(_normalize_key_value)
    return out


def _normalize_target_candidates(frame: pd.DataFrame, *, key_columns: List[str]) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    for col in key_columns:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].map(_normalize_key_value)
    cost_identity_complete = pd.Series(True, index=out.index)
    for col in STRICT_COST_IDENTITY_COLUMNS:
        if col not in out.columns:
            out[col] = ""
        normalized = out[col].map(_normalize_key_value)
        out[col] = normalized
        cost_identity_complete &= normalized.astype(str).str.strip().ne("")
    out["confirmatory_cost_identity_complete"] = cost_identity_complete.astype(bool)
    out["confirmatory_gate_pass"] = _search_engine_gate_pass(out).astype(bool)
    out["confirmatory_bridge_pass"] = (
        out["gate_bridge_tradable"].fillna(False).astype(bool)
        if "gate_bridge_tradable" in out.columns
        else False
    )
    out["confirmatory_strict_pass"] = (
        out["confirmatory_gate_pass"]
        & out.get("gate_multiplicity_strict", pd.Series(False, index=out.index))
        .fillna(False)
        .astype(bool)
        & out["confirmatory_cost_identity_complete"].astype(bool)
    )
    if "q_value" in out.columns:
        out["_q_sort"] = pd.to_numeric(out["q_value"], errors="coerce").fillna(1.0)
    else:
        out["_q_sort"] = 1.0
    out = out.sort_values(
        by=[
            "confirmatory_gate_pass",
            "confirmatory_bridge_pass",
            "confirmatory_strict_pass",
            "_q_sort",
        ],
        ascending=[False, False, False, True],
    )
    out = out.drop_duplicates(subset=key_columns, keep="first").copy()
    return out.drop(columns=["_q_sort"], errors="ignore")


def _fail_reasons_from_row(row: Dict[str, Any]) -> List[str]:
    reasons: List[str] = []
    if not row:
        return ["missing_in_target"]
    if not bool(row.get("confirmatory_gate_pass", False)):
        if not bool(row.get("gate_oos_validation", False)):
            reasons.append("oos_validation_fail")
        if not bool(row.get("gate_after_cost_positive", False)):
            reasons.append("after_cost_negative")
        if not bool(row.get("gate_after_cost_stressed_positive", False)):
            reasons.append("stressed_after_cost_negative")
        if not bool(row.get("confirmatory_bridge_pass", False)):
            reasons.append("bridge_fail")
        if not bool(row.get("gate_c_regime_stable", False)):
            reasons.append("regime_unstable")
        if not bool(row.get("gate_multiplicity_strict", False)):
            reasons.append("multiplicity_not_strict")
    return reasons or ["passed"]


def _load_run_manifest(data_root: Path, run_id: str) -> Dict[str, Any]:
    return _read_json(data_root / "runs" / run_id / "run_manifest.json")


def _parse_iso_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _month_key(value: date) -> str:
    return f"{value.year:04d}-{value.month:02d}"


def _iter_month_keys(start: date, end: date) -> List[str]:
    cursor = date(start.year, start.month, 1)
    finish = date(end.year, end.month, 1)
    out: List[str] = []
    while cursor <= finish:
        out.append(_month_key(cursor))
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
    return out


def _next_month_key(value: date) -> str:
    if value.month == 12:
        return f"{value.year + 1:04d}-01"
    return f"{value.year:04d}-{value.month + 1:02d}"


def _list_symbol_funding_months(data_root: Path, symbol: str) -> List[str]:
    months: set[str] = set()
    raw_root = data_root / "lake" / "raw"
    funding_roots = [
        raw_root / "perp" / symbol / "funding",
        raw_root / "perp" / symbol / "fundingRate",
    ]
    if raw_root.exists():
        for venue_dir in sorted(path for path in raw_root.iterdir() if path.is_dir()):
            funding_roots.append(venue_dir / "perp" / symbol / "funding")
            funding_roots.append(venue_dir / "perp" / symbol / "fundingRate")
    for funding_root in funding_roots:
        for path in funding_root.glob("year=*/month=*"):
            try:
                year = int(path.parent.name.split("=")[1])
                month = int(path.name.split("=")[1])
            except (IndexError, ValueError):
                continue
            months.add(f"{year:04d}-{month:02d}")
    return sorted(months)


def _run_symbols(manifest: Dict[str, Any]) -> List[str]:
    symbols = manifest.get("normalized_symbols")
    if isinstance(symbols, list) and symbols:
        return [str(symbol) for symbol in symbols if str(symbol).strip()]
    raw = str(manifest.get("symbols", "")).strip()
    if not raw:
        return []
    return [token.strip() for token in raw.split(",") if token.strip()]


def _candidate_target_run_ids(data_root: Path) -> List[str]:
    out: List[str] = []
    reports_root = data_root / "reports" / "phase2"
    if not reports_root.exists():
        return out
    for run_dir in sorted(path for path in reports_root.iterdir() if path.is_dir()):
        candidate_path = resolve_phase2_candidates_path(data_root=data_root, run_id=run_dir.name)
        if candidate_path.exists():
            out.append(run_dir.name)
    return out


def _is_synthetic_like_manifest(manifest: Dict[str, Any], run_id: str) -> bool:
    run_name = str(run_id).strip().lower()
    if "synth" in run_name or "synthetic" in run_name:
        return True
    cli_argv = manifest.get("config_resolution", {}).get("cli_argv", [])
    tokens = " ".join(str(token).strip().lower() for token in cli_argv)
    return "synthetic" in tokens or "synthetic_truth" in tokens


def plan_confirmatory_window(
    *,
    data_root: Path,
    origin_run_id: str,
) -> Dict[str, Any]:
    manifest = _load_run_manifest(data_root, origin_run_id)
    origin_start = _parse_iso_date(manifest.get("start"))
    origin_end = _parse_iso_date(manifest.get("end"))
    symbols = _run_symbols(manifest)
    funding_by_symbol = {
        symbol: _list_symbol_funding_months(data_root, symbol) for symbol in symbols
    }
    common_funding_months = (
        sorted(set.intersection(*(set(months) for months in funding_by_symbol.values())))
        if funding_by_symbol
        else []
    )

    target_runs: List[Dict[str, Any]] = []
    for run_id in _candidate_target_run_ids(data_root):
        if run_id == origin_run_id:
            continue
        target_manifest = _load_run_manifest(data_root, run_id)
        if _is_synthetic_like_manifest(target_manifest, run_id):
            continue
        target_start = _parse_iso_date(target_manifest.get("start"))
        target_end = _parse_iso_date(target_manifest.get("end"))
        target_symbols = _run_symbols(target_manifest)
        if not target_start or not target_end or not target_symbols:
            continue
        if symbols and set(target_symbols) != set(symbols):
            continue
        target_months = _iter_month_keys(target_start, target_end)
        funding_covered = all(
            all(month in funding_by_symbol.get(symbol, []) for month in target_months)
            for symbol in symbols
        )
        days_from_origin_end = None
        if origin_end is not None:
            days_from_origin_end = (target_start - origin_end).days
        target_runs.append(
            {
                "run_id": run_id,
                "start": target_start.isoformat(),
                "end": target_end.isoformat(),
                "months": target_months,
                "days_from_origin_end": days_from_origin_end,
                "funding_covered": funding_covered,
                "is_forward": bool(origin_end is not None and target_start > origin_end),
            }
        )

    forward_local_targets = [
        row for row in target_runs if row["is_forward"] and row["funding_covered"]
    ]
    forward_local_targets.sort(key=lambda row: (row["days_from_origin_end"], row["run_id"]))

    missing_forward_month = None
    suggested_forward_month = None
    if origin_end is not None:
        candidate = _next_month_key(origin_end)
        suggested_forward_month = candidate
        if candidate not in common_funding_months:
            missing_forward_month = candidate

    if forward_local_targets:
        readiness = "ready"
    elif missing_forward_month is None and suggested_forward_month is not None:
        readiness = "ready_to_run_forward_confirmatory"
    else:
        readiness = "blocked_by_missing_forward_data"

    return {
        "origin_run_id": origin_run_id,
        "origin_window": {
            "start": origin_start.isoformat() if origin_start else None,
            "end": origin_end.isoformat() if origin_end else None,
            "symbols": symbols,
            "months": _iter_month_keys(origin_start, origin_end)
            if origin_start and origin_end
            else [],
        },
        "local_funding_months_by_symbol": funding_by_symbol,
        "local_common_funding_months": common_funding_months,
        "target_runs_considered": target_runs,
        "forward_local_targets": forward_local_targets,
        "nearest_forward_local_target": forward_local_targets[0] if forward_local_targets else None,
        "suggested_forward_month": suggested_forward_month,
        "next_required_funding_month": missing_forward_month,
        "readiness": readiness,
    }


def compare_confirmatory_candidates(
    *,
    data_root: Path,
    origin_run_id: str,
    target_run_id: str,
) -> Dict[str, Any]:
    origin_path = (
        data_root
        / "reports"
        / "edge_candidates"
        / origin_run_id
        / "edge_candidates_normalized.parquet"
    )
    target_path = (
        resolve_phase2_candidates_path(data_root=data_root, run_id=target_run_id)
    )

    origin_raw = _read_parquet(origin_path)
    target_raw = _read_parquet(target_path)
    key_columns = _resolve_structural_key_columns(origin_raw, target_raw)
    origin = _normalize_origin_candidates(origin_raw, key_columns=key_columns)
    target = _normalize_target_candidates(target_raw, key_columns=key_columns)
    shared_cost_identity_complete = all(
        _column_has_signal(origin_raw, column) and _column_has_signal(target_raw, column)
        for column in STRICT_COST_IDENTITY_COLUMNS
    )
    strict_matching_blocked = not shared_cost_identity_complete
    strict_matching_blocking_reasons = (
        [
            "confirmatory cost identity incomplete: "
            + ", ".join(
                column
                for column in STRICT_COST_IDENTITY_COLUMNS
                if not (_column_has_signal(origin_raw, column) and _column_has_signal(target_raw, column))
            )
        ]
        if strict_matching_blocked
        else []
    )

    origin_summary = {
        "candidate_count": int(len(origin)),
        "structural_key_count": int(len(origin[key_columns].drop_duplicates()))
        if not origin.empty
        else 0,
    }
    target_summary = {
        "candidate_count": int(len(target)),
        "cost_identity_complete_count": int(
            target.get("confirmatory_cost_identity_complete", pd.Series(dtype=bool)).sum()
        )
        if not target.empty
        else 0,
        "bridge_pass_count": int(
            target.get("confirmatory_bridge_pass", pd.Series(dtype=bool)).sum()
        )
        if not target.empty
        else 0,
        "gate_pass_count": int(target.get("confirmatory_gate_pass", pd.Series(dtype=bool)).sum())
        if not target.empty
        else 0,
        "strict_pass_count": int(
            target.get("confirmatory_strict_pass", pd.Series(dtype=bool)).sum()
        )
        if not target.empty
        else 0,
    }

    if origin.empty or target.empty:
        return {
            "origin_run_id": origin_run_id,
            "target_run_id": target_run_id,
            "structural_key_columns": key_columns,
            "strict_matching_blocked": strict_matching_blocked,
            "strict_matching_blocking_reasons": strict_matching_blocking_reasons,
            "origin_summary": origin_summary,
            "target_summary": target_summary,
            "matched_summary": {
                "matched_structural_rows": 0,
                "matched_structural_keys": 0,
                "matched_cost_identity_complete_count": 0,
                "matched_bridge_pass_count": 0,
                "matched_gate_pass_count": 0,
                "matched_strict_pass_count": 0,
            },
            "matched_candidates": [],
            "origin_path": str(origin_path),
            "target_path": str(target_path),
        }

    origin_keyed = origin.drop_duplicates(subset=key_columns).copy()
    target_keyed = target.copy()
    merged = origin_keyed.merge(
        target_keyed,
        on=key_columns,
        how="inner",
        suffixes=("_origin", "_target"),
    )

    matched_summary = {
        "matched_structural_rows": int(len(merged)),
        "matched_structural_keys": int(len(merged[key_columns].drop_duplicates()))
        if not merged.empty
        else 0,
        "matched_cost_identity_complete_count": int(
            merged.get("confirmatory_cost_identity_complete", pd.Series(dtype=bool)).sum()
        )
        if not merged.empty
        else 0,
        "matched_bridge_pass_count": int(
            merged.get("confirmatory_bridge_pass", pd.Series(dtype=bool)).sum()
        )
        if not merged.empty
        else 0,
        "matched_gate_pass_count": int(
            merged.get("confirmatory_gate_pass", pd.Series(dtype=bool)).sum()
        )
        if not merged.empty
        else 0,
        "matched_strict_pass_count": int(
            merged.get("confirmatory_strict_pass", pd.Series(dtype=bool)).sum()
        )
        if not merged.empty
        else 0,
    }

    matched_candidates: List[Dict[str, Any]] = []
    if not merged.empty:
        for row in merged.to_dict(orient="records"):
            matched_candidates.append(
                {
                    "candidate_id_origin": row.get("candidate_id_origin"),
                    "candidate_id_target": row.get("candidate_id_target"),
                    "symbol": row.get("symbol"),
                    "event_type": row.get("event_type"),
                    "direction": row.get("direction"),
                    "rule_template": row.get("rule_template"),
                    "horizon": row.get("horizon"),
                    "origin_q_value": row.get("q_value_origin"),
                    "target_q_value": row.get("q_value_target"),
                    "target_bridge_pass": bool(row.get("confirmatory_bridge_pass", False)),
                    "target_gate_pass": bool(row.get("confirmatory_gate_pass", False)),
                    "target_strict_pass": bool(row.get("confirmatory_strict_pass", False)),
                    "target_bridge_eval_status": row.get("bridge_eval_status"),
                }
            )

    return {
        "origin_run_id": origin_run_id,
        "target_run_id": target_run_id,
        "structural_key_columns": key_columns,
        "strict_matching_blocked": strict_matching_blocked,
        "strict_matching_blocking_reasons": strict_matching_blocking_reasons,
        "origin_summary": origin_summary,
        "target_summary": target_summary,
        "matched_summary": matched_summary,
        "matched_candidates": matched_candidates,
        "origin_path": str(origin_path),
        "target_path": str(target_path),
    }


def build_adjacent_survivorship_payload(
    *,
    data_root: Path,
    origin_run_id: str,
    target_run_id: str,
) -> Dict[str, Any]:
    origin_path = (
        data_root
        / "reports"
        / "edge_candidates"
        / origin_run_id
        / "edge_candidates_normalized.parquet"
    )
    target_path = (
        resolve_phase2_candidates_path(data_root=data_root, run_id=target_run_id)
    )
    origin_raw = _read_parquet(origin_path)
    target_raw = _read_parquet(target_path)
    key_columns = _resolve_structural_key_columns(origin_raw, target_raw)
    origin = (
        _normalize_origin_candidates(origin_raw, key_columns=key_columns)
        .drop_duplicates(subset=key_columns)
        .copy()
    )
    target = _normalize_target_candidates(target_raw, key_columns=key_columns).copy()

    target_index: Dict[tuple[str, ...], Dict[str, Any]] = {}
    if not target.empty:
        for row in target.to_dict(orient="records"):
            key = tuple(str(row.get(col, "")) for col in key_columns)
            target_index[key] = row

    rows: List[Dict[str, Any]] = []
    for row in origin.to_dict(orient="records"):
        key = tuple(str(row.get(col, "")) for col in key_columns)
        target_row = target_index.get(key, {})
        reasons = _fail_reasons_from_row(target_row)
        rows.append(
            {
                **{col: row.get(col) for col in key_columns},
                "origin_candidate_id": row.get("candidate_id"),
                "origin_q_value": row.get("q_value"),
                "origin_after_cost_expectancy_per_trade": row.get(
                    "after_cost_expectancy_per_trade"
                ),
                "target_candidate_id": target_row.get("candidate_id"),
                "target_q_value": target_row.get("q_value"),
                "target_after_cost_expectancy_per_trade": target_row.get(
                    "after_cost_expectancy_per_trade"
                ),
                "target_stressed_after_cost_expectancy_per_trade": target_row.get(
                    "stressed_after_cost_expectancy_per_trade"
                ),
                "target_gate_pass": bool(target_row.get("confirmatory_gate_pass", False)),
                "target_bridge_pass": bool(target_row.get("confirmatory_bridge_pass", False)),
                "target_regime_stable": bool(target_row.get("gate_c_regime_stable", False)),
                "failure_reasons": reasons,
                "survived_adjacent_window": bool(target_row.get("confirmatory_gate_pass", False)),
            }
        )

    failure_reason_counts: Dict[str, int] = {}
    for row in rows:
        for reason in row["failure_reasons"]:
            failure_reason_counts[reason] = failure_reason_counts.get(reason, 0) + 1

    families: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        family = str(row.get("event_type", ""))
        bucket = families.setdefault(
            family,
            {"origin_count": 0, "survived_count": 0, "failure_reason_counts": {}},
        )
        bucket["origin_count"] += 1
        if row["survived_adjacent_window"]:
            bucket["survived_count"] += 1
        for reason in row["failure_reasons"]:
            bucket["failure_reason_counts"][reason] = (
                bucket["failure_reason_counts"].get(reason, 0) + 1
            )

    return {
        "origin_run_id": origin_run_id,
        "target_run_id": target_run_id,
        "structural_key_columns": key_columns,
        "origin_survivor_count": int(len(origin)),
        "adjacent_survivor_count": int(sum(1 for row in rows if row["survived_adjacent_window"])),
        "failure_reason_counts": failure_reason_counts,
        "primary_event_ids": list(families.keys()),
        "by_primary_event_id": families,
        "compat_grouping_aliases": {
            "by_event_family": "by_primary_event_id",
        },
        "by_event_family": families,
        "candidate_rows": rows,
        "origin_path": str(origin_path),
        "target_path": str(target_path),
    }


def write_confirmatory_candidate_report(
    *,
    data_root: Path,
    origin_run_id: str,
    target_run_id: str,
    out_dir: Path | None = None,
) -> Path:
    payload = compare_confirmatory_candidates(
        data_root=data_root,
        origin_run_id=origin_run_id,
        target_run_id=target_run_id,
    )
    report_dir = (
        out_dir
        if out_dir is not None
        else data_root
        / "reports"
        / "confirmatory_comparison"
        / target_run_id
        / f"vs_{origin_run_id}"
    )
    report_dir.mkdir(parents=True, exist_ok=True)
    out_path = report_dir / "confirmatory_candidates.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    
    # NEW: Run Validation Stage for the target run (the confirmatory run)
    try:
        from project.research.services.evaluation_service import ValidationService
        val_svc = ValidationService(data_root=data_root)
        # Load the target candidates table
        tables = val_svc.load_candidate_tables(target_run_id)
        candidates_df = tables.get("phase2_candidates", pd.DataFrame())
        if not candidates_df.empty:
            val_svc.run_validation_stage(
                run_id=target_run_id,
                candidates_df=candidates_df
            )
    except Exception as exc:
        logging.warning("Failed to run validation stage in confirmatory report: %s", exc)

    return out_path


def write_adjacent_survivorship_report(
    *,
    data_root: Path,
    origin_run_id: str,
    target_run_id: str,
    out_dir: Path | None = None,
) -> Path:
    payload = build_adjacent_survivorship_payload(
        data_root=data_root,
        origin_run_id=origin_run_id,
        target_run_id=target_run_id,
    )
    report_dir = (
        out_dir
        if out_dir is not None
        else data_root / "reports" / "adjacent_survivorship" / target_run_id / f"vs_{origin_run_id}"
    )
    report_dir.mkdir(parents=True, exist_ok=True)
    out_path = report_dir / "adjacent_survivorship.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path


def write_confirmatory_window_plan(
    *,
    data_root: Path,
    origin_run_id: str,
    out_dir: Path | None = None,
) -> Path:
    payload = plan_confirmatory_window(
        data_root=data_root,
        origin_run_id=origin_run_id,
    )
    report_dir = (
        out_dir
        if out_dir is not None
        else data_root / "reports" / "confirmatory_plan" / origin_run_id
    )
    report_dir.mkdir(parents=True, exist_ok=True)
    out_path = report_dir / "confirmatory_window_plan.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path


def build_confirmatory_workflow_payload(
    *,
    data_root: Path,
    origin_run_id: str,
    target_run_id: str | None = None,
) -> Dict[str, Any]:
    window_plan = plan_confirmatory_window(
        data_root=data_root,
        origin_run_id=origin_run_id,
    )
    comparison: Dict[str, Any] = {}
    workflow_status = "planning_only"
    next_action = "inspect_confirmatory_plan"
    blocking_reason = ""

    target = str(target_run_id or "").strip()
    if not target:
        readiness = str(window_plan.get("readiness", "")).strip()
        if readiness == "blocked_by_missing_forward_data":
            workflow_status = "blocked"
            next_action = "ingest_forward_data"
            blocking_reason = str(window_plan.get("next_required_funding_month", "")).strip()
        elif readiness in {"ready", "ready_to_run_forward_confirmatory"}:
            workflow_status = "ready_for_confirmatory_run"
            next_action = "run_confirmatory"
        return {
            "origin_run_id": origin_run_id,
            "target_run_id": None,
            "workflow_status": workflow_status,
            "next_action": next_action,
            "blocking_reason": blocking_reason,
            "window_plan": window_plan,
            "comparison": comparison,
        }

    comparison = compare_confirmatory_candidates(
        data_root=data_root,
        origin_run_id=origin_run_id,
        target_run_id=target,
    )
    if comparison.get("strict_matching_blocked"):
        blocking_reason = "; ".join(comparison.get("strict_matching_blocking_reasons", []))
        return {
            "origin_run_id": origin_run_id,
            "target_run_id": target,
            "workflow_status": "blocked",
            "next_action": "repair_confirmatory_cost_identity",
            "blocking_reason": blocking_reason,
            "window_plan": window_plan,
            "comparison": comparison,
        }
    matched = dict(comparison.get("matched_summary", {}))
    matched_rows = int(matched.get("matched_structural_rows", 0))
    matched_gate_pass = int(matched.get("matched_gate_pass_count", 0))
    matched_strict_pass = int(matched.get("matched_strict_pass_count", 0))

    if matched_strict_pass > 0:
        workflow_status = "confirmatory_strict_pass"
        next_action = "promotion_review"
    elif matched_gate_pass > 0:
        workflow_status = "confirmatory_pass"
        next_action = "review_for_promotion"
    elif matched_rows > 0:
        workflow_status = "confirmatory_failed"
        next_action = "review_fail_reasons"
    else:
        workflow_status = "no_structural_match"
        next_action = "reframe_confirmatory_slice"

    return {
        "origin_run_id": origin_run_id,
        "target_run_id": target,
        "workflow_status": workflow_status,
        "next_action": next_action,
        "blocking_reason": "",
        "window_plan": window_plan,
        "comparison": comparison,
    }


def write_confirmatory_workflow_report(
    *,
    data_root: Path,
    origin_run_id: str,
    target_run_id: str | None = None,
    out_dir: Path | None = None,
) -> Path:
    payload = build_confirmatory_workflow_payload(
        data_root=data_root,
        origin_run_id=origin_run_id,
        target_run_id=target_run_id,
    )
    if target_run_id:
        report_dir = (
            out_dir
            if out_dir is not None
            else data_root
            / "reports"
            / "confirmatory_workflow"
            / str(target_run_id)
            / f"vs_{origin_run_id}"
        )
    else:
        report_dir = (
            out_dir
            if out_dir is not None
            else data_root / "reports" / "confirmatory_workflow" / origin_run_id
        )
    report_dir.mkdir(parents=True, exist_ok=True)
    out_path = report_dir / "confirmatory_workflow.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out_path
