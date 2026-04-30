from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from project.research.regime_baselines import (
    DEFAULT_TIMEFRAME,
    discover_market_context_run,
    load_market_context,
)

SCHEMA_VERSION = "funding_data_triage_v1"
DEFAULT_OUTPUT_ROOT = Path(__file__).resolve().parents[2] / "data" / "reports" / "funding_data_triage"
FUNDING_FIELDS = ("funding_rate_scaled", "funding_abs_pct")
EXPECTED_UPDATE_GAP_HOURS = 8.0
CADENCE_TOLERANCE_MULTIPLIER = 1.5
MATERIAL_STALE_MULTIPLIER = 3.0
MIN_HISTORY_DAYS_FOR_CONSTANT_INVALID = 60.0

ROW_COLUMNS = [
    "schema_version",
    "run_id",
    "source_run_id",
    "symbol",
    "field",
    "present",
    "row_count",
    "non_null_count",
    "coverage_ratio",
    "distinct_count",
    "timestamp_count",
    "median_update_gap_hours",
    "p95_update_gap_hours",
    "expected_update_gap_hours",
    "forward_fill_detected",
    "stale_ratio_raw",
    "stale_ratio_funding_adjusted",
    "classification",
    "recommended_action",
    "timeframe",
    "first_timestamp",
    "last_timestamp",
    "last_update_timestamp",
    "last_update_lag_hours",
    "history_days",
    "aligned_to_funding_interval",
    "aligned_update_ratio",
    "market_context_funding_fields",
    "source_funding_fields",
    "recomputable_from",
]


@dataclass(frozen=True)
class FundingDataTriageRequest:
    run_id: str
    symbols: tuple[str, ...]
    data_root: Path
    source_run_id: str | None = None
    timeframe: str = DEFAULT_TIMEFRAME


def default_run_id(now: datetime | None = None) -> str:
    ts = (now or datetime.now(UTC)).strftime("%Y%m%dT%H%M%SZ")
    return f"funding_data_triage_{ts}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _timestamp(value: pd.Timestamp | None) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).isoformat()


def _hours(delta: pd.Timedelta | None) -> float | None:
    if delta is None or pd.isna(delta):
        return None
    return float(pd.Timedelta(delta).total_seconds() / 3600.0)


def _history_days(first_ts: pd.Timestamp | None, last_ts: pd.Timestamp | None) -> float | None:
    if first_ts is None or last_ts is None:
        return None
    return float((last_ts - first_ts).total_seconds() / 86400.0)


def _stable_label(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True)
    return str(value)


def _distinct_count(series: pd.Series) -> int:
    non_null = series.dropna()
    try:
        return int(non_null.nunique(dropna=True))
    except TypeError:
        return int(non_null.map(_stable_label).nunique(dropna=True))


def _stale_ratio(series: pd.Series) -> float | None:
    values = series.dropna()
    if len(values) <= 1:
        return 0.0 if len(values) == 1 else None
    return float(values.eq(values.shift()).iloc[1:].sum() / (len(values) - 1))


def _funding_columns(frame: pd.DataFrame) -> list[str]:
    return sorted(column for column in frame.columns if "funding" in str(column).lower())


def discover_source_funding_fields(data_root: Path, *, source_run_id: str, symbol: str) -> list[str]:
    run_root = data_root / "lake" / "runs" / source_run_id
    if not source_run_id or not run_root.exists():
        return []
    fields: set[str] = set()
    patterns = [
        f"**/*{symbol}*funding*.parquet",
        "**/funding/**/*.parquet",
        "**/*funding*.parquet",
    ]
    for pattern in patterns:
        for path in sorted(run_root.glob(pattern))[:50]:
            try:
                frame = pd.read_parquet(path)
            except Exception:
                continue
            fields.update(_funding_columns(frame))
    return sorted(fields)


def _update_timestamps(frame: pd.DataFrame, field: str) -> pd.Series:
    subset = frame[["timestamp", field]].copy()
    subset["timestamp"] = pd.to_datetime(subset["timestamp"], utc=True, errors="coerce")
    subset[field] = pd.to_numeric(subset[field], errors="coerce")
    subset = subset.dropna(subset=["timestamp", field]).sort_values("timestamp")
    if subset.empty:
        return pd.Series(dtype="datetime64[ns, UTC]")
    changed = subset[field].ne(subset[field].shift())
    return subset.loc[changed, "timestamp"].reset_index(drop=True)


def _gap_stats(update_ts: pd.Series) -> tuple[float | None, float | None]:
    if len(update_ts) <= 1:
        return None, None
    gaps = update_ts.diff().dropna().dt.total_seconds() / 3600.0
    if gaps.empty:
        return None, None
    return float(gaps.median()), float(gaps.quantile(0.95))


def _aligned_update_ratio(update_ts: pd.Series, *, expected_gap_hours: float) -> float | None:
    if update_ts.empty:
        return None
    expected_seconds = expected_gap_hours * 3600.0
    timestamps = pd.to_datetime(update_ts, utc=True, errors="coerce").dropna()
    if timestamps.empty:
        return None
    seconds = (
        timestamps.dt.hour.astype(float) * 3600.0
        + timestamps.dt.minute.astype(float) * 60.0
        + timestamps.dt.second.astype(float)
    )
    offset = seconds.mod(expected_seconds)
    distance = pd.concat([offset, expected_seconds - offset], axis=1).min(axis=1)
    return float((distance <= 15 * 60.0).sum() / len(distance))


def classify_funding_field(
    frame: pd.DataFrame,
    *,
    run_id: str,
    source_run_id: str,
    symbol: str,
    timeframe: str,
    field: str,
    source_funding_fields: list[str] | None = None,
    companion_rate_classification: str | None = None,
) -> dict[str, Any]:
    source_funding_fields = source_funding_fields or []
    market_context_funding_fields = _funding_columns(frame)
    if frame.empty or field not in frame.columns:
        classification = "missing"
        recomputable_from = ""
        if field == "funding_abs_pct" and companion_rate_classification == "valid_stepwise":
            classification = "recomputable"
            recomputable_from = "funding_rate_scaled"
        return {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "source_run_id": source_run_id,
            "symbol": symbol,
            "field": field,
            "present": False,
            "row_count": 0,
            "non_null_count": 0,
            "coverage_ratio": 0.0,
            "distinct_count": 0,
            "timestamp_count": 0,
            "median_update_gap_hours": None,
            "p95_update_gap_hours": None,
            "expected_update_gap_hours": EXPECTED_UPDATE_GAP_HOURS,
            "forward_fill_detected": False,
            "stale_ratio_raw": None,
            "stale_ratio_funding_adjusted": None,
            "classification": classification,
            "recommended_action": "recompute funding_abs_pct from funding_rate_scaled" if recomputable_from else "repair funding source/materialization",
            "timeframe": timeframe,
            "first_timestamp": None,
            "last_timestamp": None,
            "last_update_timestamp": None,
            "last_update_lag_hours": None,
            "history_days": None,
            "aligned_to_funding_interval": False,
            "aligned_update_ratio": None,
            "market_context_funding_fields": market_context_funding_fields,
            "source_funding_fields": source_funding_fields,
            "recomputable_from": recomputable_from,
        }

    timestamps = pd.to_datetime(frame.get("timestamp"), utc=True, errors="coerce")
    values = pd.to_numeric(frame[field], errors="coerce")
    valid_mask = timestamps.notna() & values.notna()
    row_count = int(len(frame))
    non_null_count = int(values.notna().sum())
    coverage_ratio = float(non_null_count / row_count) if row_count else 0.0
    distinct_count = _distinct_count(values)
    first_ts = pd.Timestamp(timestamps[valid_mask].min()) if valid_mask.any() else None
    last_ts = pd.Timestamp(timestamps[valid_mask].max()) if valid_mask.any() else None
    dataset_last = pd.Timestamp(timestamps.dropna().max()) if timestamps.notna().any() else None
    history_days = _history_days(first_ts, last_ts)
    update_ts = _update_timestamps(frame, field)
    median_gap, p95_gap = _gap_stats(update_ts)
    stale_ratio_raw = _stale_ratio(values)
    aligned_ratio = _aligned_update_ratio(update_ts, expected_gap_hours=EXPECTED_UPDATE_GAP_HOURS)
    aligned = bool(aligned_ratio is not None and aligned_ratio >= 0.80)
    last_update_ts = pd.Timestamp(update_ts.iloc[-1]) if not update_ts.empty else None
    last_update_lag_hours = (
        _hours(dataset_last - last_update_ts)
        if dataset_last is not None and last_update_ts is not None
        else None
    )
    cadence_limit = EXPECTED_UPDATE_GAP_HOURS * CADENCE_TOLERANCE_MULTIPLIER
    true_stale_limit = EXPECTED_UPDATE_GAP_HOURS * MATERIAL_STALE_MULTIPLIER
    cadence_ok = (
        median_gap is not None
        and p95_gap is not None
        and median_gap <= cadence_limit
        and p95_gap <= true_stale_limit
    )
    forward_fill_detected = bool(
        stale_ratio_raw is not None
        and stale_ratio_raw >= 0.20
        and cadence_ok
        and len(update_ts) > 1
    )
    stale_ratio_funding_adjusted = 0.0 if cadence_ok else 1.0 if len(update_ts) <= 1 else None

    recomputable_from = ""
    if row_count == 0 or coverage_ratio == 0:
        classification = "missing"
        action = "repair funding source/materialization"
    elif non_null_count == 0:
        classification = "missing"
        action = "repair funding source/materialization"
    elif distinct_count <= 1 and (history_days or 0.0) >= MIN_HISTORY_DAYS_FOR_CONSTANT_INVALID:
        classification = "invalid"
        action = "inspect source funding; constant multi-month funding is implausible"
    elif last_update_lag_hours is not None and last_update_lag_hours > true_stale_limit:
        classification = "true_stale"
        action = "repair funding source freshness before funding research"
    elif not cadence_ok:
        classification = "true_stale" if len(update_ts) > 1 else "invalid"
        action = "inspect funding timestamp cadence and source materialization"
    else:
        classification = "valid_stepwise"
        action = "treat funding as cadence-aware in data quality audit"

    if (
        field == "funding_abs_pct"
        and classification in {"missing", "true_stale", "invalid"}
        and companion_rate_classification == "valid_stepwise"
    ):
        classification = "recomputable"
        recomputable_from = "funding_rate_scaled"
        action = "recompute funding_abs_pct from funding_rate_scaled"

    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "source_run_id": source_run_id,
        "symbol": symbol,
        "field": field,
        "present": True,
        "row_count": row_count,
        "non_null_count": non_null_count,
        "coverage_ratio": coverage_ratio,
        "distinct_count": distinct_count,
        "timestamp_count": int(len(update_ts)),
        "median_update_gap_hours": median_gap,
        "p95_update_gap_hours": p95_gap,
        "expected_update_gap_hours": EXPECTED_UPDATE_GAP_HOURS,
        "forward_fill_detected": forward_fill_detected,
        "stale_ratio_raw": stale_ratio_raw,
        "stale_ratio_funding_adjusted": stale_ratio_funding_adjusted,
        "classification": classification,
        "recommended_action": action,
        "timeframe": timeframe,
        "first_timestamp": _timestamp(first_ts),
        "last_timestamp": _timestamp(last_ts),
        "last_update_timestamp": _timestamp(last_update_ts),
        "last_update_lag_hours": last_update_lag_hours,
        "history_days": history_days,
        "aligned_to_funding_interval": aligned,
        "aligned_update_ratio": aligned_ratio,
        "market_context_funding_fields": market_context_funding_fields,
        "source_funding_fields": source_funding_fields,
        "recomputable_from": recomputable_from,
    }


def run_funding_data_triage(request: FundingDataTriageRequest) -> pd.DataFrame:
    source_run_id = request.source_run_id or discover_market_context_run(
        request.data_root,
        symbols=request.symbols,
        timeframe=request.timeframe,
    )
    source_run_id = source_run_id or ""
    rows: list[dict[str, Any]] = []
    for symbol in request.symbols:
        frame = (
            load_market_context(
                request.data_root,
                source_run_id=source_run_id,
                symbol=symbol,
                timeframe=request.timeframe,
            )
            if source_run_id
            else pd.DataFrame()
        )
        source_fields = discover_source_funding_fields(
            request.data_root,
            source_run_id=source_run_id,
            symbol=symbol,
        )
        rate_row = classify_funding_field(
            frame,
            run_id=request.run_id,
            source_run_id=source_run_id,
            symbol=symbol,
            timeframe=request.timeframe,
            field="funding_rate_scaled",
            source_funding_fields=source_fields,
        )
        rows.append(rate_row)
        rows.append(
            classify_funding_field(
                frame,
                run_id=request.run_id,
                source_run_id=source_run_id,
                symbol=symbol,
                timeframe=request.timeframe,
                field="funding_abs_pct",
                source_funding_fields=source_fields,
                companion_rate_classification=str(rate_row["classification"]),
            )
        )
    return pd.DataFrame(rows, columns=ROW_COLUMNS)


def write_funding_data_triage_outputs(rows: pd.DataFrame, *, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    records = [_json_ready(row) for row in rows.to_dict(orient="records")]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "run_id": records[0]["run_id"] if records else "",
        "source_run_id": records[0]["source_run_id"] if records else "",
        "row_count": len(records),
        "rows": records,
    }
    (output_dir / "funding_data_triage.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    parquet_rows = rows.copy()
    for column in ("market_context_funding_fields", "source_funding_fields"):
        parquet_rows[column] = parquet_rows[column].map(lambda item: json.dumps(item or []))
    parquet_rows.to_parquet(output_dir / "funding_data_triage.parquet", index=False)
    write_funding_data_triage_markdown(rows, output_dir=output_dir)


def write_funding_data_triage_markdown(rows: pd.DataFrame, *, output_dir: Path) -> None:
    run_id = str(rows.iloc[0]["run_id"]) if not rows.empty else ""
    lines = [
        "# Funding Data Triage",
        "",
        f"- run_id: `{run_id}`",
        f"- row_count: `{len(rows)}`",
        "",
        "| symbol | field | classification | median_gap_h | p95_gap_h | stale_raw | stale_adjusted | action |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for _, row in rows.iterrows():
        lines.append(
            "| "
            f"{row['symbol']} | {row['field']} | {row['classification']} | "
            f"{'' if pd.isna(row['median_update_gap_hours']) else round(float(row['median_update_gap_hours']), 2)} | "
            f"{'' if pd.isna(row['p95_update_gap_hours']) else round(float(row['p95_update_gap_hours']), 2)} | "
            f"{'' if pd.isna(row['stale_ratio_raw']) else round(float(row['stale_ratio_raw']), 3)} | "
            f"{'' if pd.isna(row['stale_ratio_funding_adjusted']) else round(float(row['stale_ratio_funding_adjusted']), 3)} | "
            f"{row['recommended_action']} |"
        )
    (output_dir / "funding_data_triage.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
