from __future__ import annotations

import ast
import glob
import json
import warnings
from pathlib import Path
from typing import Any

import pandas as pd

from project.research import results_index
from project.research.year_split import _safe_name

SCHEMA_VERSION = "candidate_traces_v1"
TRACE_COLUMNS = [
    "run_id",
    "candidate_id",
    "symbol",
    "event_id",
    "template_id",
    "context_key",
    "context_value",
    "direction",
    "horizon_bars",
    "event_ts",
    "entry_ts",
    "exit_ts",
    "entry_price",
    "exit_price",
    "gross_return_bps",
    "cost_bps",
    "net_return_bps",
    "context_pass",
    "entry_lag_bars",
    "source_artifact",
]
RETURN_COLUMNS = (
    "net_return_bps",
    "return_net_bps",
    "forward_return_net_bps",
    "signed_return_bps",
)


def _data_root(path: Path | str | None) -> Path:
    return Path(path or "data")


def _phase2_dir(data_root: Path, run_id: str) -> Path:
    return data_root / "reports" / "phase2" / run_id


def _read_table(path: Path) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        warnings.warn(f"Could not read candidate trace table {path}: {exc}", stacklevel=2)
        return pd.DataFrame()


def _to_float(value: Any) -> float | None:
    return results_index._to_float(value)


def _to_int(value: Any) -> int | None:
    return results_index._to_int(value)


def _horizon_bars(value: Any) -> int | None:
    return results_index._horizon_bars(value)


def _norm(value: Any) -> str:
    if results_index._is_missing(value):
        return ""
    return str(value).strip().lower()


def _id_norm(value: Any) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def _ids_match(left: Any, right: Any) -> bool:
    left_text = str(left or "").strip()
    right_text = str(right or "").strip()
    if not left_text or not right_text:
        return False
    if left_text == right_text:
        return True
    return _id_norm(left_text) == _id_norm(right_text)


def _parse_context(value: Any) -> tuple[str, str]:
    if results_index._is_missing(value):
        return "", ""
    if isinstance(value, dict):
        payload = value
    else:
        text = str(value).strip()
        if not text:
            return "", ""
        if "=" in text and not text.startswith("{"):
            key, raw = text.split("=", 1)
            return key.strip().upper(), raw.strip().upper()
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            try:
                payload = ast.literal_eval(text)
            except (SyntaxError, ValueError):
                return "", text.upper()
    if not isinstance(payload, dict):
        return "", ""
    for key, raw in sorted(payload.items()):
        if not results_index._is_missing(raw):
            return str(key).strip().upper(), str(raw).strip().upper()
    return "", ""


def _candidate_frame(data_root: Path, run_id: str) -> pd.DataFrame:
    return _read_table(_phase2_dir(data_root, run_id) / "phase2_candidates.parquet")


def _evaluated_hypotheses_frame(data_root: Path, run_id: str) -> pd.DataFrame:
    frames = []
    for path in sorted((_phase2_dir(data_root, run_id) / "hypotheses").glob("*/evaluated_hypotheses.parquet")):
        frame = _read_table(path)
        if not frame.empty:
            frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _evaluation_results_frame(data_root: Path, run_id: str) -> pd.DataFrame:
    pattern = str(data_root / "artifacts" / "experiments" / "*" / run_id / "evaluation_results.parquet")
    frames = [_read_table(Path(path)) for path in sorted(glob.glob(pattern))]
    valid = [frame for frame in frames if not frame.empty]
    return pd.concat(valid, ignore_index=True) if valid else pd.DataFrame()


def _select_metadata_row(df: pd.DataFrame, ids: set[str]) -> dict[str, Any]:
    if df.empty:
        return {}
    selected = df.copy()
    masks = []
    for column in ("candidate_id", "hypothesis_id", "strategy_id"):
        if column in selected.columns:
            masks.append(selected[column].map(lambda value: any(_ids_match(value, item) for item in ids)))
    if masks:
        mask = masks[0]
        for next_mask in masks[1:]:
            mask = mask | next_mask
        matched = selected[mask].copy()
        if not matched.empty:
            selected = matched
    return selected.iloc[0].to_dict()


def _select_candidate(
    data_root: Path,
    run_id: str,
    candidate_id: str,
) -> tuple[dict[str, Any], set[str]]:
    candidates = _candidate_frame(data_root, run_id)
    if candidates.empty:
        return {}, {candidate_id}
    selected = candidates.copy()
    masks = []
    for column in ("candidate_id", "hypothesis_id", "strategy_id"):
        if column in selected.columns:
            masks.append(selected[column].map(lambda value: _ids_match(value, candidate_id)))
    if masks:
        mask = masks[0]
        for next_mask in masks[1:]:
            mask = mask | next_mask
        matched = selected[mask].copy()
        if not matched.empty:
            selected = matched
    score_column = "t_stat_net" if "t_stat_net" in selected.columns else "t_stat"
    if score_column in selected.columns:
        selected = selected.assign(_score=pd.to_numeric(selected[score_column], errors="coerce"))
        selected = selected.sort_values("_score", ascending=False, na_position="last")
    row = selected.iloc[0].to_dict()
    ids = {candidate_id}
    for column in ("candidate_id", "hypothesis_id", "strategy_id"):
        value = str(row.get(column, "") or "").strip()
        if value:
            ids.add(value)
    return row, ids


def _candidate_context(candidate: dict[str, Any]) -> tuple[str, str]:
    for column in ("context", "context_cell", "context_signature", "context_slice"):
        key, value = _parse_context(candidate.get(column))
        if key or value:
            return key, value
    return "", ""


def _candidate_profile(
    data_root: Path,
    run_id: str,
    candidate_id: str,
) -> tuple[dict[str, Any], set[str]]:
    candidate, ids = _select_candidate(data_root, run_id, candidate_id)
    for supplement in (
        _select_metadata_row(_evaluated_hypotheses_frame(data_root, run_id), ids),
        _select_metadata_row(_evaluation_results_frame(data_root, run_id), ids),
    ):
        for key, value in supplement.items():
            if results_index._is_missing(candidate.get(key)) and not results_index._is_missing(value):
                candidate[key] = value
    context_key, context_value = _candidate_context(candidate)
    profile = {
        "run_id": run_id,
        "candidate_id": candidate_id,
        "symbol": str(candidate.get("symbol", "") or ""),
        "event_id": str(
            results_index._first_present(candidate, ["event_id", "event_type", "event"]) or ""
        ),
        "template_id": str(
            results_index._first_present(candidate, ["template_id", "rule_template", "template"])
            or ""
        ),
        "context_key": context_key,
        "context_value": context_value,
        "direction": str(candidate.get("direction", "") or ""),
        "horizon_bars": _horizon_bars(
            results_index._first_present(candidate, ["horizon_bars", "horizon", "horizon_label"])
        ),
        "entry_lag_bars": _to_int(
            results_index._first_present(candidate, ["entry_lag_bars", "entry_lag"])
        )
        or 1,
        "cost_bps": _to_float(
            results_index._first_present(
                candidate,
                ["expected_cost_bps_per_trade", "cost_bps", "funding_cost_bps_per_trade"],
            )
        )
        or 0.0,
    }
    return profile, ids


def _candidate_event_frame(data_root: Path, run_id: str, ids: set[str]) -> pd.DataFrame:
    path = _phase2_dir(data_root, run_id) / "phase2_candidate_event_timestamps.parquet"
    if not path.exists():
        return pd.DataFrame()
    events = _read_table(path)
    if events.empty:
        return events
    masks = []
    for column in ("candidate_id", "hypothesis_id", "strategy_id"):
        if column in events.columns:
            masks.append(events[column].map(lambda value: any(_ids_match(value, item) for item in ids)))
    if masks:
        mask = masks[0]
        for next_mask in masks[1:]:
            mask = mask | next_mask
        events = events[mask].copy()
    return events


def _trace_source_paths(data_root: Path, run_id: str) -> list[Path]:
    phase2 = _phase2_dir(data_root, run_id)
    return [
        phase2 / "edge_cell_pnl_traces.parquet",
        phase2 / "edge_cell_trigger_traces.parquet",
    ]


def _event_level_table_paths(data_root: Path, run_id: str) -> list[Path]:
    paths = []
    paths.extend(Path(path) for path in sorted(glob.glob(str(data_root / "artifacts" / "experiments" / "*" / run_id / "evaluation_results.parquet"))))
    paths.extend(
        sorted((_phase2_dir(data_root, run_id) / "hypotheses").glob("*/evaluated_hypotheses.parquet"))
    )
    return paths


def _return_column(frame: pd.DataFrame) -> str | None:
    for column in RETURN_COLUMNS:
        if column in frame.columns:
            return column
    return None


def _timestamp_column(frame: pd.DataFrame) -> str | None:
    for column in ("event_ts", "event_timestamp", "timestamp"):
        if column in frame.columns:
            return column
    return None


def _filter_by_ids(frame: pd.DataFrame, ids: set[str]) -> pd.DataFrame:
    if frame.empty or not ids:
        return frame
    masks = []
    for column in ("candidate_id", "hypothesis_id", "strategy_id"):
        if column in frame.columns:
            masks.append(frame[column].map(lambda value: any(_ids_match(value, item) for item in ids)))
    if not masks:
        return frame
    mask = masks[0]
    for next_mask in masks[1:]:
        mask = mask | next_mask
    return frame[mask].copy()


def _normalize_existing_trace(
    frame: pd.DataFrame,
    *,
    profile: dict[str, Any],
    ids: set[str],
    source_artifact: str,
) -> pd.DataFrame:
    filtered = _filter_by_ids(frame, ids)
    return_col = _return_column(filtered)
    ts_col = _timestamp_column(filtered)
    if filtered.empty or return_col is None or ts_col is None:
        return pd.DataFrame(columns=TRACE_COLUMNS)
    event_ts = pd.to_datetime(filtered[ts_col], utc=True, errors="coerce")
    rows = []
    for idx, row in filtered.assign(_event_ts=event_ts).dropna(subset=["_event_ts"]).iterrows():
        net = _to_float(row.get(return_col))
        if net is None:
            continue
        cost = _to_float(row.get("cost_bps")) or float(profile["cost_bps"])
        gross = _to_float(row.get("gross_return_bps"))
        if gross is None:
            gross = net + cost
        rows.append(
            {
                **profile,
                "event_ts": row["_event_ts"],
                "entry_ts": pd.to_datetime(row.get("entry_ts"), utc=True, errors="coerce"),
                "exit_ts": pd.to_datetime(row.get("exit_ts"), utc=True, errors="coerce"),
                "entry_price": _to_float(row.get("entry_price")),
                "exit_price": _to_float(row.get("exit_price")),
                "gross_return_bps": float(gross),
                "cost_bps": float(cost),
                "net_return_bps": float(net),
                "context_pass": bool(row.get("context_pass", True)),
                "source_artifact": source_artifact,
            }
        )
    return pd.DataFrame(rows, columns=TRACE_COLUMNS)


def _feature_paths(data_root: Path, run_id: str, symbol: str) -> list[Path]:
    base = data_root / "lake" / "runs" / run_id / "features" / "perp" / symbol / "5m"
    market = sorted((base / "market_context").glob("year=*/month=*/*.parquet"))
    if market:
        return market
    return sorted((base / "features_feature_schema_v2").glob("year=*/month=*/*.parquet"))


def _load_feature_frame(data_root: Path, run_id: str, symbol: str) -> pd.DataFrame:
    frames = []
    for path in _feature_paths(data_root, run_id, symbol):
        frame = _read_table(path)
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if "timestamp" not in df.columns or "close" not in df.columns:
        return pd.DataFrame()
    return df.sort_values("timestamp").reset_index(drop=True)


def _context_pass(row: pd.Series, context_key: str, context_value: str) -> bool:
    if not context_key:
        return True
    column = context_key.lower()
    if column in row.index:
        value = row[column]
        if _norm(value) == _norm(context_value):
            return True
    if context_key == "VOL_REGIME" and "high_vol_regime" in row.index and context_value == "HIGH":
        return bool(row["high_vol_regime"])
    return False


def _build_from_event_timestamps(
    *,
    data_root: Path,
    run_id: str,
    profile: dict[str, Any],
    ids: set[str],
) -> pd.DataFrame:
    events = _candidate_event_frame(data_root, run_id, ids)
    symbol = str(profile.get("symbol") or "")
    features = _load_feature_frame(data_root, run_id, symbol)
    if events.empty or features.empty:
        return pd.DataFrame(columns=TRACE_COLUMNS)
    if profile.get("horizon_bars") is None:
        return pd.DataFrame(columns=TRACE_COLUMNS)
    timestamps = pd.to_datetime(features["timestamp"], utc=True, errors="coerce")
    features = features.assign(_timestamp=timestamps).dropna(subset=["_timestamp"]).reset_index(drop=True)
    feature_ts = features["_timestamp"]
    close = pd.to_numeric(features["close"], errors="coerce")
    direction_sign = -1.0 if str(profile.get("direction", "")).lower() == "short" else 1.0
    horizon = int(profile["horizon_bars"])
    lag = int(profile["entry_lag_bars"])
    rows = []
    for _, event in events.iterrows():
        event_ts = pd.to_datetime(event.get("event_timestamp"), utc=True, errors="coerce")
        if pd.isna(event_ts):
            continue
        event_pos = int(feature_ts.searchsorted(event_ts, side="left"))
        entry_pos = event_pos + lag
        exit_pos = entry_pos + horizon
        if event_pos >= len(features) or entry_pos >= len(features) or exit_pos >= len(features):
            continue
        entry_price = _to_float(close.iloc[entry_pos])
        exit_price = _to_float(close.iloc[exit_pos])
        if entry_price is None or exit_price is None or entry_price == 0.0:
            continue
        gross = ((exit_price / entry_price) - 1.0) * 10_000.0 * direction_sign
        cost = float(profile["cost_bps"])
        feature_row = features.iloc[event_pos]
        rows.append(
            {
                **profile,
                "event_ts": event_ts,
                "entry_ts": feature_ts.iloc[entry_pos],
                "exit_ts": feature_ts.iloc[exit_pos],
                "entry_price": float(entry_price),
                "exit_price": float(exit_price),
                "gross_return_bps": float(gross),
                "cost_bps": cost,
                "net_return_bps": float(gross - cost),
                "context_pass": _context_pass(
                    feature_row,
                    str(profile.get("context_key", "") or ""),
                    str(profile.get("context_value", "") or ""),
                ),
                "source_artifact": "phase2_candidate_event_timestamps+lake_market_context",
            }
        )
    return pd.DataFrame(rows, columns=TRACE_COLUMNS)


def _extract_trace_frame(
    *,
    data_root: Path,
    run_id: str,
    candidate_id: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    profile, ids = _candidate_profile(data_root, run_id, candidate_id)
    if not profile.get("event_id"):
        return pd.DataFrame(columns=TRACE_COLUMNS), {
            "status": "blocked",
            "reason": "missing_candidate_metadata",
            "source_artifact": "",
        }

    for path in _trace_source_paths(data_root, run_id):
        if not path.exists():
            continue
        traces = _normalize_existing_trace(
            _read_table(path),
            profile=profile,
            ids=ids,
            source_artifact=str(path),
        )
        if not traces.empty:
            return traces, {"status": "extracted", "source_artifact": str(path)}

    for path in _event_level_table_paths(data_root, run_id):
        if not path.exists():
            continue
        traces = _normalize_existing_trace(
            _read_table(path),
            profile=profile,
            ids=ids,
            source_artifact=str(path),
        )
        if not traces.empty:
            return traces, {"status": "extracted", "source_artifact": str(path)}

    traces = _build_from_event_timestamps(
        data_root=data_root,
        run_id=run_id,
        profile=profile,
        ids=ids,
    )
    if not traces.empty:
        return traces, {
            "status": "extracted",
            "source_artifact": "phase2_candidate_event_timestamps+lake_market_context",
        }

    return pd.DataFrame(columns=TRACE_COLUMNS), {
        "status": "blocked",
        "reason": "insufficient_trace_source",
        "source_artifact": "",
    }


def trace_paths(data_root: Path | str | None, run_id: str, candidate_id: str) -> tuple[Path, Path]:
    base = _data_root(data_root) / "reports" / "candidate_traces" / run_id
    safe = _safe_name(candidate_id)
    return base / f"{safe}_traces.parquet", base / f"{safe}_traces.json"


def extract_candidate_traces(
    *,
    run_id: str,
    candidate_id: str,
    data_root: Path | str | None = None,
) -> dict[str, Any]:
    resolved_data_root = _data_root(data_root)
    traces, meta = _extract_trace_frame(
        data_root=resolved_data_root,
        run_id=run_id,
        candidate_id=candidate_id,
    )
    parquet_path, json_path = trace_paths(resolved_data_root, run_id, candidate_id)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    if not traces.empty:
        traces.to_parquet(parquet_path, index=False)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "candidate_id": candidate_id,
        "status": meta.get("status", "blocked"),
        "reason": meta.get("reason", ""),
        "row_count": int(len(traces)),
        "source_artifact": meta.get("source_artifact", ""),
        "parquet_path": str(parquet_path) if parquet_path.exists() else "",
        "columns": TRACE_COLUMNS,
        "next_safe_command": (
            "Rerun specificity and year split."
            if not traces.empty
            else "Inspect pipeline trace generation before validation."
        ),
    }
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload
