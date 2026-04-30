from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from project.research.mechanisms import DEFAULT_REGISTRY_PATH, load_mechanism_registry
from project.research.regime_baselines import (
    DEFAULT_TIMEFRAME,
    discover_market_context_run,
    load_market_context,
)

SCHEMA_VERSION = "data_quality_audit_v1"
MECHANISM_SCHEMA_VERSION = "mechanism_data_quality_v1"
DEFAULT_OUTPUT_ROOT = Path(__file__).resolve().parents[2] / "data" / "reports" / "data_quality_audit"
MIN_HISTORY_DAYS = 180.0
MIN_NON_NULL_COUNT = 1000
MIN_COVERAGE_RATIO = 0.80
MAX_STALE_RATIO = 0.20
CONSTANT_SYNTHETIC_RATIO = 0.95
MATERIALLY_OLDER_FRACTION = 0.20

FIELD_EXPECTATIONS = {
    "funding_rate_scaled": {
        "kind": "stepwise_cadence",
        "allow_zero_heavy": False,
        "expected_update_gap_hours": 8.0,
        "max_update_gap_multiplier": 2.5,
    },
    "funding_abs_pct": {
        "kind": "stepwise_cadence",
        "allow_zero_heavy": False,
        "expected_update_gap_hours": 8.0,
        "max_update_gap_multiplier": 2.5,
    },
    "oi_notional": {"kind": "continuous", "allow_zero_heavy": False},
    "oi_delta_1h": {"kind": "continuous", "allow_zero_heavy": True},
    "rv_96": {"kind": "continuous", "allow_zero_heavy": False},
    "rv_percentile_24h": {"kind": "bounded", "allow_zero_heavy": False, "allow_sticky": True},
    "spread_bps": {"kind": "continuous", "allow_zero_heavy": False},
    "slippage_bps": {"kind": "continuous", "allow_zero_heavy": True},
    "market_depth": {"kind": "continuous", "allow_zero_heavy": False},
    "basis_zscore": {"kind": "continuous", "allow_zero_heavy": True},
    "liquidation_notional": {"kind": "event_sparse", "allow_zero_heavy": True},
    "volume": {"kind": "continuous", "allow_zero_heavy": False},
    "order_book": {"kind": "object_or_proxy", "allow_zero_heavy": False},
}
KNOWN_PROXY_FIELDS = {"basis_zscore"}

MECHANISM_REQUIRED_OBSERVABLES = {
    "funding_squeeze": [
        "funding_rate_scaled",
        "funding_abs_pct",
        "oi_notional",
        "rv_96",
        "basis_zscore",
        "volume",
    ],
    "forced_flow_reversal": [
        "oi_notional",
        "oi_delta_1h",
        "rv_96",
        "spread_bps",
        "liquidation_notional",
        "volume",
    ],
    "liquidation_exhaustion_rebound": [
        "liquidation_notional",
        "spread_bps",
        "market_depth",
        "rv_96",
        "volume",
    ],
    "liquidity_vacuum": [
        "spread_bps",
        "slippage_bps",
        "market_depth",
        "volume",
    ],
    "stop_run_reversal": [
        "spread_bps",
        "market_depth",
        "volume",
        "order_book",
    ],
    "volatility_compression_release": [
        "rv_96",
        "rv_percentile_24h",
        "spread_bps",
        "volume",
    ],
}

FIELD_COLUMNS = [
    "schema_version",
    "run_id",
    "source_run_id",
    "symbol",
    "timeframe",
    "field",
    "present",
    "non_null_count",
    "row_count",
    "coverage_ratio",
    "distinct_count",
    "zero_ratio",
    "stale_ratio",
    "first_timestamp",
    "last_timestamp",
    "history_days",
    "cadence_aware",
    "expected_update_gap_hours",
    "median_update_gap_hours",
    "p95_update_gap_hours",
    "last_update_gap_hours",
    "funding_adjusted_stale_ratio",
    "valid_stepwise_cadence",
    "classification",
    "reason",
]

CLASSIFICATION_RANK = {
    "real": 0,
    "proxy": 1,
    "insufficient_history": 2,
    "stale": 3,
    "synthetic": 4,
    "missing": 5,
}
DATA_BLOCKING_CLASSIFICATIONS = {"missing", "stale", "synthetic", "insufficient_history"}


@dataclass(frozen=True)
class DataQualityAuditRequest:
    run_id: str
    symbols: tuple[str, ...]
    data_root: Path
    source_run_id: str | None = None
    timeframe: str = DEFAULT_TIMEFRAME


def default_run_id(now: datetime | None = None) -> str:
    ts = (now or datetime.now(UTC)).strftime("%Y%m%dT%H%M%SZ")
    return f"data_quality_audit_{ts}"


def _timestamp(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).isoformat()


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


def _numeric_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _zero_ratio(series: pd.Series) -> float | None:
    numeric = _numeric_series(series).dropna()
    if numeric.empty:
        return None
    return float((numeric == 0).sum() / len(numeric))


def _dominant_ratio(series: pd.Series) -> float | None:
    non_null = series.dropna()
    if non_null.empty:
        return None
    try:
        counts = non_null.value_counts(dropna=True)
    except TypeError:
        counts = non_null.map(_stable_value_label).value_counts(dropna=True)
    if counts.empty:
        return None
    return float(counts.iloc[0] / len(non_null))


def _distinct_count(series: pd.Series) -> int:
    non_null = series.dropna()
    try:
        return int(non_null.nunique(dropna=True))
    except TypeError:
        return int(non_null.map(_stable_value_label).nunique(dropna=True))


def _stable_value_label(item: Any) -> str:
    if isinstance(item, (dict, list)):
        return json.dumps(item, sort_keys=True)
    return str(item)


def _stale_ratio(series: pd.Series, *, allow_zero_heavy: bool) -> float | None:
    values = series.dropna()
    if allow_zero_heavy:
        numeric = _numeric_series(values)
        values = values[numeric.ne(0).fillna(True)]
    if len(values) <= 1:
        return 0.0 if len(values) == 1 else None
    return float(values.eq(values.shift()).iloc[1:].sum() / (len(values) - 1))


def _funding_adjusted_stale_metrics(
    frame: pd.DataFrame,
    field: str,
    *,
    expected_update_gap_hours: float = 8.0,
    max_update_gap_multiplier: float = 2.5,
) -> dict[str, float | bool | None]:
    empty = {
        "cadence_aware": True,
        "expected_update_gap_hours": expected_update_gap_hours,
        "median_update_gap_hours": None,
        "p95_update_gap_hours": None,
        "last_update_gap_hours": None,
        "funding_adjusted_stale_ratio": None,
        "valid_stepwise_cadence": False,
    }
    if frame.empty or "timestamp" not in frame.columns or field not in frame.columns:
        return empty

    subset = frame[["timestamp", field]].copy()
    subset["timestamp"] = pd.to_datetime(subset["timestamp"], utc=True, errors="coerce")
    subset[field] = pd.to_numeric(subset[field], errors="coerce")
    subset = subset.dropna(subset=["timestamp", field]).sort_values("timestamp")
    if subset.empty:
        return empty

    if "funding_event_ts" in frame.columns:
        event_ts = pd.to_datetime(frame["funding_event_ts"], utc=True, errors="coerce").dropna()
        updates = event_ts.drop_duplicates().sort_values().reset_index(drop=True)
    else:
        changed = subset[field].ne(subset[field].shift())
        updates = subset.loc[changed, "timestamp"].reset_index(drop=True)
    if len(updates) <= 1:
        empty["funding_adjusted_stale_ratio"] = 1.0
        return empty

    gaps = updates.diff().dropna().dt.total_seconds() / 3600.0
    median_gap = float(gaps.median())
    p95_gap = float(gaps.quantile(0.95))
    dataset_last = pd.Timestamp(subset["timestamp"].max())
    last_update = pd.Timestamp(updates.iloc[-1])
    last_update_gap = float((dataset_last - last_update).total_seconds() / 3600.0)
    median_limit = expected_update_gap_hours * 1.5
    max_limit = expected_update_gap_hours * max_update_gap_multiplier
    valid = bool(
        median_gap <= median_limit
        and p95_gap <= max_limit
        and last_update_gap <= max_limit
    )
    return {
        "cadence_aware": True,
        "expected_update_gap_hours": expected_update_gap_hours,
        "median_update_gap_hours": median_gap,
        "p95_update_gap_hours": p95_gap,
        "last_update_gap_hours": last_update_gap,
        "funding_adjusted_stale_ratio": 0.0 if valid else 1.0,
        "valid_stepwise_cadence": valid,
    }


def _history_days(first_timestamp: pd.Timestamp | None, last_timestamp: pd.Timestamp | None) -> float | None:
    if first_timestamp is None or last_timestamp is None:
        return None
    return float((last_timestamp - first_timestamp).total_seconds() / 86400.0)


def _field_source_marked_synthetic(field: str) -> bool:
    token = field.lower()
    return "synthetic" in token or token.endswith("_default") or token.endswith("_filled")


def _latest_dataset_timestamp(frame: pd.DataFrame) -> pd.Timestamp | None:
    if frame.empty or "timestamp" not in frame.columns:
        return None
    timestamps = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce").dropna()
    if timestamps.empty:
        return None
    return pd.Timestamp(timestamps.max())


def classify_field(
    frame: pd.DataFrame,
    *,
    run_id: str,
    source_run_id: str,
    symbol: str,
    timeframe: str,
    field: str,
    proxy_fields: set[str] | None = None,
) -> dict[str, Any]:
    expectation = FIELD_EXPECTATIONS[field]
    proxy_fields = proxy_fields or set()
    dataset_last = _latest_dataset_timestamp(frame)
    cadence_aware = expectation.get("kind") == "stepwise_cadence"
    empty_cadence = {
        "cadence_aware": bool(cadence_aware),
        "expected_update_gap_hours": expectation.get("expected_update_gap_hours"),
        "median_update_gap_hours": None,
        "p95_update_gap_hours": None,
        "last_update_gap_hours": None,
        "funding_adjusted_stale_ratio": None,
        "valid_stepwise_cadence": False,
    }
    if frame.empty or field not in frame.columns:
        return {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "source_run_id": source_run_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "field": field,
            "present": False,
            "non_null_count": 0,
            "row_count": 0,
            "coverage_ratio": 0.0,
            "distinct_count": 0,
            "zero_ratio": None,
            "stale_ratio": None,
            "first_timestamp": None,
            "last_timestamp": None,
            "history_days": None,
            **empty_cadence,
            "classification": "missing",
            "reason": "field is absent from market_context" if field not in frame.columns else "market_context has no rows",
        }

    series = frame[field]
    row_count = int(len(frame))
    non_null = series.dropna()
    non_null_count = int(len(non_null))
    coverage_ratio = float(non_null_count / row_count) if row_count else 0.0
    distinct_count = _distinct_count(series)
    zero_ratio = _zero_ratio(series)
    stale_ratio = _stale_ratio(series, allow_zero_heavy=bool(expectation["allow_zero_heavy"]))
    cadence_metrics = (
        _funding_adjusted_stale_metrics(
            frame,
            field,
            expected_update_gap_hours=float(expectation.get("expected_update_gap_hours", 8.0)),
            max_update_gap_multiplier=float(expectation.get("max_update_gap_multiplier", 2.5)),
        )
        if cadence_aware
        else empty_cadence
    )

    valid_frame = frame.loc[series.notna()].copy()
    if "timestamp" in valid_frame.columns and not valid_frame.empty:
        timestamps = pd.to_datetime(valid_frame["timestamp"], utc=True, errors="coerce").dropna()
        first_ts = pd.Timestamp(timestamps.min()) if not timestamps.empty else None
        last_ts = pd.Timestamp(timestamps.max()) if not timestamps.empty else None
    else:
        first_ts = None
        last_ts = None
    history_days = _history_days(first_ts, last_ts)

    dominant_ratio = _dominant_ratio(series)
    continuous_kind = expectation["kind"] in {"continuous", "bounded"}
    implausibly_low_distinct = continuous_kind and distinct_count <= 2
    constant_default_filled = (
        continuous_kind
        and dominant_ratio is not None
        and dominant_ratio >= CONSTANT_SYNTHETIC_RATIO
        and not bool(expectation["allow_zero_heavy"] and zero_ratio is not None and zero_ratio >= CONSTANT_SYNTHETIC_RATIO)
    )
    last_materially_old = False
    if dataset_last is not None and last_ts is not None and first_ts is not None:
        dataset_history = _history_days(first_ts, dataset_last) or 0.0
        if dataset_history >= MIN_HISTORY_DAYS:
            lag_days = float((dataset_last - last_ts).total_seconds() / 86400.0)
            last_materially_old = lag_days >= max(1.0, dataset_history * MATERIALLY_OLDER_FRACTION)

    if row_count == 0 or coverage_ratio == 0:
        classification = "missing"
        reason = "field has zero coverage"
    elif history_days is None or history_days < MIN_HISTORY_DAYS:
        classification = "insufficient_history"
        reason = f"history_days={history_days} is below {MIN_HISTORY_DAYS:g}"
    elif non_null_count < MIN_NON_NULL_COUNT:
        classification = "insufficient_history"
        reason = f"non_null_count={non_null_count} is below {MIN_NON_NULL_COUNT}"
    elif _field_source_marked_synthetic(field) or implausibly_low_distinct or constant_default_filled:
        classification = "synthetic"
        reason = "field appears synthetic or default-filled"
    elif cadence_aware and bool(cadence_metrics["valid_stepwise_cadence"]):
        if field in proxy_fields:
            classification = "proxy"
            reason = "field is stepwise on expected funding cadence but marked as proxy"
        else:
            classification = "real"
            reason = "field is stepwise on expected funding cadence"
    elif (stale_ratio is not None and stale_ratio >= (MAX_STALE_RATIO * 4.0 if expectation.get("allow_sticky", False) else MAX_STALE_RATIO)) or last_materially_old:
        classification = "stale"
        reason = "field is stale relative to market_context history"
    elif field in proxy_fields or expectation["kind"] == "object_or_proxy":
        classification = "proxy"
        reason = "field is marked as proxy or derived approximation"
    elif coverage_ratio >= MIN_COVERAGE_RATIO and history_days >= MIN_HISTORY_DAYS:
        classification = "real"
        reason = "field passes coverage and history thresholds"
    else:
        classification = "insufficient_history"
        reason = "field does not meet real-data thresholds"

    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "source_run_id": source_run_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "field": field,
        "present": True,
        "non_null_count": non_null_count,
        "row_count": row_count,
        "coverage_ratio": coverage_ratio,
        "distinct_count": distinct_count,
        "zero_ratio": zero_ratio,
        "stale_ratio": stale_ratio,
        "first_timestamp": _timestamp(first_ts),
        "last_timestamp": _timestamp(last_ts),
        "history_days": history_days,
        **cadence_metrics,
        "classification": classification,
        "reason": reason,
    }


def discover_proxy_fields(data_root: Path) -> set[str]:
    proxy_fields: set[str] = set(KNOWN_PROXY_FIELDS)
    for field in FIELD_EXPECTATIONS:
        if "proxy" in field.lower():
            proxy_fields.add(field)
    inventory_path = data_root / "reports" / "regime_event_inventory" / "regime_event_inventory.parquet"
    if inventory_path.exists():
        try:
            inventory = pd.read_parquet(inventory_path)
            for _, row in inventory.iterrows():
                risk_text = " ".join(
                    str(row.get(column, "") or "")
                    for column in ("known_data_risk", "data_quality_risk", "source_quality")
                ).lower()
                if "proxy" not in risk_text:
                    continue
                features = row.get("features_required") or []
                if not isinstance(features, list):
                    continue
                proxy_fields.update(str(item) for item in features if str(item) in FIELD_EXPECTATIONS)
        except Exception:
            pass
    return proxy_fields


def run_data_quality_audit(
    request: DataQualityAuditRequest,
    *,
    proxy_fields: set[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    source_run_id = request.source_run_id or discover_market_context_run(
        request.data_root,
        symbols=request.symbols,
        timeframe=request.timeframe,
    )
    source_run_id = source_run_id or ""
    proxies = discover_proxy_fields(request.data_root) if proxy_fields is None else proxy_fields
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
        for field in FIELD_EXPECTATIONS:
            rows.append(
                classify_field(
                    frame,
                    run_id=request.run_id,
                    source_run_id=source_run_id,
                    symbol=symbol,
                    timeframe=request.timeframe,
                    field=field,
                    proxy_fields=proxies,
                )
            )
    frame = pd.DataFrame(rows, columns=FIELD_COLUMNS)
    return frame, build_mechanism_data_quality(
        frame,
        run_id=request.run_id,
    )


def _worst_classification(classifications: list[str]) -> str:
    if not classifications:
        return "missing"
    return max(classifications, key=lambda item: CLASSIFICATION_RANK.get(str(item), 99))


def build_mechanism_data_quality(field_rows: pd.DataFrame, *, run_id: str) -> dict[str, Any]:
    registry = load_mechanism_registry(DEFAULT_REGISTRY_PATH)
    mechanisms: list[dict[str, Any]] = []
    for mechanism_id, required in MECHANISM_REQUIRED_OBSERVABLES.items():
        entry = registry.mechanisms.get(mechanism_id)
        status = entry.status if entry is not None else "draft"
        relevant = field_rows[field_rows["field"].isin(required)] if not field_rows.empty else pd.DataFrame()
        classifications = [str(item) for item in relevant.get("classification", [])]
        blocked_fields = sorted(
            set(
                str(row["field"])
                for _, row in relevant.iterrows()
                if str(row.get("classification")) in DATA_BLOCKING_CLASSIFICATIONS
            )
        )
        proxy_fields = sorted(
            set(
                str(row["field"])
                for _, row in relevant.iterrows()
                if str(row.get("classification")) == "proxy"
            )
        )
        missing_required = sorted(set(required) - set(str(item) for item in relevant.get("field", [])))
        blocked_fields = sorted(set(blocked_fields) | set(missing_required))
        worst = _worst_classification(classifications + (["missing"] if missing_required else []))
        if status != "active":
            decision = "draft_only"
            reason = "mechanism is not active"
        elif blocked_fields:
            decision = "data_blocked"
            reason = f"blocked core observables: {', '.join(blocked_fields)}"
        elif proxy_fields:
            decision = "paper_blocked"
            reason = f"proxy core observables: {', '.join(proxy_fields)}"
        else:
            decision = "research_allowed"
            reason = "all required core observables are real"
        mechanisms.append(
            {
                "mechanism_id": mechanism_id,
                "status": status,
                "required_observables": list(required),
                "worst_classification": worst,
                "data_quality_decision": decision,
                "blocked_fields": blocked_fields,
                "proxy_fields": proxy_fields,
                "reason": reason,
            }
        )
    return {
        "schema_version": MECHANISM_SCHEMA_VERSION,
        "run_id": run_id,
        "mechanisms": mechanisms,
    }


def write_data_quality_audit_outputs(
    field_rows: pd.DataFrame,
    mechanism_payload: dict[str, Any],
    *,
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = [_json_ready(row) for row in field_rows.to_dict(orient="records")]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "run_id": rows[0]["run_id"] if rows else mechanism_payload.get("run_id", ""),
        "source_run_id": rows[0]["source_run_id"] if rows else "",
        "row_count": len(rows),
        "rows": rows,
    }
    (output_dir / "data_quality_audit.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    field_rows.to_parquet(output_dir / "data_quality_audit.parquet", index=False)
    (output_dir / "mechanism_data_quality.json").write_text(
        json.dumps(_json_ready(mechanism_payload), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_data_quality_audit_markdown(field_rows, mechanism_payload, output_dir=output_dir)


def write_data_quality_audit_markdown(
    field_rows: pd.DataFrame,
    mechanism_payload: dict[str, Any],
    *,
    output_dir: Path,
) -> None:
    lines = [
        "# Data Quality Audit",
        "",
        f"- run_id: `{mechanism_payload.get('run_id', '')}`",
        f"- row_count: `{len(field_rows)}`",
        "",
        "## Field Classifications",
        "",
        "| symbol | field | classification | coverage | history_days | reason |",
        "| --- | --- | --- | ---: | ---: | --- |",
    ]
    for _, row in field_rows.iterrows():
        lines.append(
            "| "
            f"{row['symbol']} | {row['field']} | {row['classification']} | "
            f"{float(row['coverage_ratio']):.3f} | "
            f"{'' if pd.isna(row['history_days']) else round(float(row['history_days']), 2)} | "
            f"{row['reason']} |"
        )
    lines.extend(
        [
            "",
            "## Mechanisms",
            "",
            "| mechanism | status | decision | worst | blocked_fields | proxy_fields |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    for item in mechanism_payload.get("mechanisms", []):
        lines.append(
            "| "
            f"{item['mechanism_id']} | {item['status']} | {item['data_quality_decision']} | "
            f"{item['worst_classification']} | {', '.join(item['blocked_fields'])} | "
            f"{', '.join(item['proxy_fields'])} |"
        )
    (output_dir / "data_quality_audit.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
