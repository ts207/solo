from __future__ import annotations

import json
import math
import re
import warnings
from pathlib import Path
from typing import Any

import pandas as pd

from project.research import results_index

YEAR_CONCENTRATION_LIMIT = 0.50


def _data_root(path: Path | str | None) -> Path:
    return Path(path or "data")


def _phase2_dir(data_root: Path, run_id: str) -> Path:
    return data_root / "reports" / "phase2" / run_id


def _read_table(path: Path) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        warnings.warn(f"Could not read year split table {path}: {exc}", stacklevel=2)
        return pd.DataFrame()


def _to_float(value: Any) -> float | None:
    return results_index._to_float(value)


def _to_int(value: Any) -> int | None:
    return results_index._to_int(value)


def _round(value: float | None, digits: int = 4) -> float | None:
    return None if value is None else round(float(value), digits)


def _safe_name(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    return text.strip("_") or "candidate"


def _candidate_frame(data_root: Path, run_id: str) -> pd.DataFrame:
    return _read_table(_phase2_dir(data_root, run_id) / "phase2_candidates.parquet")


def _event_timestamp_frame(data_root: Path, run_id: str) -> pd.DataFrame:
    return _read_table(_phase2_dir(data_root, run_id) / "phase2_candidate_event_timestamps.parquet")


def _candidate_trace_frame(data_root: Path, run_id: str) -> pd.DataFrame:
    frames = []
    for path in sorted((data_root / "reports" / "candidate_traces" / run_id).glob("*_traces.parquet")):
        frame = _read_table(path)
        if not frame.empty:
            frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "pass", "passed", "tradable"}


def _select_candidate(df: pd.DataFrame, candidate_id: str | None) -> dict[str, Any]:
    if df.empty:
        return {}
    selected = df.copy()
    if candidate_id:
        masks = [
            selected[column].astype(str) == str(candidate_id)
            for column in ("candidate_id", "hypothesis_id")
            if column in selected.columns
        ]
        if masks:
            matched = selected[masks[0]]
            for mask in masks[1:]:
                matched = pd.concat([matched, selected[mask]], ignore_index=True)
            matched = matched.drop_duplicates()
            if not matched.empty:
                selected = matched
    if "gate_bridge_tradable" in selected.columns:
        bridge = selected[selected["gate_bridge_tradable"].map(_bool_value)]
        if not bridge.empty:
            selected = bridge
    score_column = "t_stat_net" if "t_stat_net" in selected.columns else "t_stat"
    if score_column in selected.columns:
        selected = selected.assign(_score=pd.to_numeric(selected[score_column], errors="coerce"))
        selected = selected.sort_values("_score", ascending=False, na_position="last")
    return selected.iloc[0].to_dict()


def _candidate_ids(candidate: dict[str, Any], candidate_id: str | None) -> set[str]:
    ids = {str(candidate_id or "").strip()}
    for column in ("candidate_id", "hypothesis_id"):
        value = str(candidate.get(column, "") or "").strip()
        if value:
            ids.add(value)
    return {value for value in ids if value}


def _filter_events(events: pd.DataFrame, ids: set[str]) -> pd.DataFrame:
    if events.empty or not ids:
        return events
    masks = [
        events[column].astype(str).isin(ids)
        for column in ("candidate_id", "hypothesis_id")
        if column in events.columns
    ]
    if not masks:
        return events
    mask = masks[0]
    for next_mask in masks[1:]:
        mask = mask | next_mask
    return events[mask].copy()


def _normalize_event_columns(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return events
    out = events.copy()
    if "event_timestamp" not in out.columns and "event_ts" in out.columns:
        out["event_timestamp"] = out["event_ts"]
    return out


def _event_return_column(events: pd.DataFrame) -> str | None:
    for column in (
        "net_return_bps",
        "return_net_bps",
        "forward_return_net_bps",
        "signed_return_bps",
        "mean_return_net_bps",
    ):
        if column in events.columns:
            return column
    return None


def _t_stat(values: pd.Series) -> float | None:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if len(clean) < 2:
        return None
    std = float(clean.std(ddof=1))
    if std == 0.0 or math.isnan(std):
        return None
    return float(clean.mean()) / (std / math.sqrt(len(clean)))


def _yearly_rows(events: pd.DataFrame) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    timestamps = pd.to_datetime(events["event_timestamp"], utc=True, errors="coerce")
    working = events.assign(_event_timestamp=timestamps).dropna(subset=["_event_timestamp"]).copy()
    working["year"] = working["_event_timestamp"].dt.year.astype(int)
    return_column = _event_return_column(working)
    if return_column:
        working["_return_net_bps"] = pd.to_numeric(working[return_column], errors="coerce")
    else:
        working["_return_net_bps"] = pd.NA

    total_events = len(working)
    yearly: list[dict[str, Any]] = []
    yearly_pnl: dict[int, float] = {}
    for year, group in working.groupby("year", sort=True):
        returns = pd.to_numeric(group["_return_net_bps"], errors="coerce").dropna()
        event_count = len(group)
        trade_count = len(group)
        pnl = float(returns.sum()) if not returns.empty else None
        if pnl is not None:
            yearly_pnl[int(year)] = pnl
        yearly.append(
            {
                "year": int(year),
                "event_count": event_count,
                "trade_count": trade_count,
                "mean_return_net_bps": _round(float(returns.mean())) if not returns.empty else None,
                "median_return_net_bps": _round(float(returns.median()))
                if not returns.empty
                else None,
                "t_stat_net": _round(_t_stat(returns)),
                "hit_rate": _round(float((returns > 0).mean())) if not returns.empty else None,
                "event_share": _round(event_count / total_events if total_events else None),
                "pnl_share": None,
            }
        )

    total_abs_pnl = sum(abs(value) for value in yearly_pnl.values())
    if total_abs_pnl > 0:
        for row in yearly:
            row["pnl_share"] = _round(abs(yearly_pnl.get(int(row["year"]), 0.0)) / total_abs_pnl)

    max_event = max(yearly, key=lambda row: float(row.get("event_share") or 0.0), default={})
    max_pnl = max(yearly, key=lambda row: float(row.get("pnl_share") or 0.0), default={})
    concentration = {
        "max_event_share": max_event.get("event_share"),
        "max_event_year": max_event.get("year"),
        "max_pnl_share": max_pnl.get("pnl_share"),
        "max_pnl_year": max_pnl.get("year") if max_pnl.get("pnl_share") is not None else None,
        "return_stats_available": bool(return_column),
    }
    return yearly, concentration


def _classify(
    *, yearly: list[dict[str, Any]], concentration: dict[str, Any]
) -> tuple[str, str, str, str, list[dict[str, str]]]:
    checks = [
        {
            "id": "event_timestamps_present",
            "status": "pass" if yearly else "fail",
            "detail": f"year_count={len(yearly)}",
        },
        {
            "id": "max_event_share_lte_50pct",
            "status": "pass"
            if (concentration.get("max_event_share") or 0.0) <= YEAR_CONCENTRATION_LIMIT
            else "fail",
            "detail": f"max_event_share={concentration.get('max_event_share')}",
        },
        {
            "id": "max_pnl_share_lte_50pct",
            "status": "unknown"
            if concentration.get("max_pnl_share") is None
            else "pass"
            if float(concentration["max_pnl_share"]) <= YEAR_CONCENTRATION_LIMIT
            else "fail",
            "detail": f"max_pnl_share={concentration.get('max_pnl_share')}",
        },
    ]
    if not yearly:
        return "blocked", "unsupported", "review", "missing candidate event timestamps", checks
    if any(check["status"] == "fail" for check in checks[1:]):
        return (
            "fail",
            "year_conditional",
            "park",
            "year_pnl_concentration",
            checks,
        )
    if concentration.get("return_stats_available"):
        return (
            "pass",
            "general_candidate",
            "review",
            "year support is not concentrated above 50%",
            checks,
        )
    return (
        "pass",
        "general_candidate",
        "review",
        "year_split_event_support_pass; PnL stability not tested because per-event return stats are unavailable",
        checks,
    )


def build_year_split_report(
    *, run_id: str, candidate_id: str | None = None, data_root: Path | str | None = None
) -> dict[str, Any]:
    resolved_data_root = _data_root(data_root)
    candidate = _select_candidate(_candidate_frame(resolved_data_root, run_id), candidate_id)
    ids = _candidate_ids(candidate, candidate_id)
    trace_events = _filter_events(_candidate_trace_frame(resolved_data_root, run_id), ids)
    timestamp_events = _filter_events(_event_timestamp_frame(resolved_data_root, run_id), ids)
    events = trace_events if not trace_events.empty else timestamp_events
    events = _normalize_event_columns(events)
    if "event_timestamp" not in events.columns:
        events = pd.DataFrame(columns=["event_timestamp"])
    yearly, concentration = _yearly_rows(events) if not events.empty else ([], {})
    status, classification, decision, reason, checks = _classify(
        yearly=yearly, concentration=concentration
    )

    resolved_candidate_id = (
        str(candidate_id or "")
        or str(candidate.get("candidate_id", "") or "")
        or str(candidate.get("hypothesis_id", "") or "")
    )
    if status == "pass" and classification == "general_candidate":
        next_safe_command = "Run specificity v1 before validation or promotion."
    elif classification == "year_conditional":
        next_safe_command = "Park unless an ex-ante regime thesis is declared before retesting."
    else:
        next_safe_command = "Inspect source artifacts before continuing."

    return {
        "schema_version": "year_split_v1",
        "run_id": run_id,
        "candidate_id": resolved_candidate_id,
        "status": status,
        "classification": classification,
        "decision": decision,
        "reason": reason,
        "totals": {
            "event_count": int(sum(row["event_count"] for row in yearly)),
            "trade_count": int(sum(row["trade_count"] for row in yearly)),
            "return_stats_available": bool(concentration.get("return_stats_available")),
        },
        "concentration": concentration,
        "interpretation": "year_split_event_support_pass" if status == "pass" else status,
        "limitation": (
            "Year split proves event support is not dominated by one year; "
            "it does not prove PnL is not dominated by one year unless return_stats_available is true."
        ),
        "years": yearly,
        "blocking_checks": checks,
        "next_safe_command": next_safe_command,
    }


def report_path(data_root: Path | str | None, run_id: str, candidate_id: str | None = None) -> Path:
    candidate_part = _safe_name(candidate_id or "candidate")
    return (
        _data_root(data_root) / "reports" / "regime" / run_id / f"{candidate_part}_year_split.json"
    )


def write_year_split_report(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_year_split(
    *, run_id: str, candidate_id: str | None = None, data_root: Path | str | None = None
) -> dict[str, Any]:
    report = build_year_split_report(run_id=run_id, candidate_id=candidate_id, data_root=data_root)
    write_year_split_report(
        report,
        report_path(
            data_root, run_id, str(report.get("candidate_id", "") or candidate_id or "candidate")
        ),
    )
    return report
