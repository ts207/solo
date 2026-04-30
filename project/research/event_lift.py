from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from project.research.regime_baselines import (
    DEFAULT_TIMEFRAME,
    _context_mask,
    _cost_series,
    _max_drawdown_bps,
    _safe_float,
    _suppress_overlap,
    _t_stat,
    _year_stats,
    discover_market_context_run,
    load_market_context,
    validate_regime_filters,
)
from project.research.regime_event_inventory import load_authoritative_event_registry

SCHEMA_VERSION = "event_lift_v1"
DEFAULT_DATA_ROOT = Path(__file__).resolve().parents[2] / "data"
DEFAULT_OUTPUT_ROOT = DEFAULT_DATA_ROOT / "reports" / "event_lift"
DEFAULT_SCORECARD_PATH = DEFAULT_DATA_ROOT / "reports" / "regime_baselines" / "regime_scorecard.parquet"
DEFAULT_OVERLAP_POLICY = "suppress"
ENTRY_LAGS = (0, 1, 2, 3)
MIN_EVENT_EFFECTIVE_N = 20
MATERIAL_TIMING_BPS = 1.0
LOGGER = logging.getLogger(__name__)

CONTROL_COLUMNS = [
    "unconditional_all",
    "regime_only_all",
    "regime_only_matched_non_event",
    "event_only",
    "event_plus_regime",
    "opposite_direction",
    "entry_lags",
]

ROW_COLUMNS = [
    "schema_version",
    "run_id",
    "mechanism_id",
    "regime_id",
    "scorecard_decision",
    "audit_only",
    "promotion_eligible",
    "event_id",
    "symbol",
    "direction",
    "horizon_bars",
    "controls",
    "lift",
    "year_stats",
    "max_year_pnl_share",
    "mean_net_bps_2x_cost",
    "classification",
    "decision",
    "reason",
]


class EventLiftGateError(ValueError):
    """Raised before output creation when a regime is not scorecard-eligible."""


@dataclass(frozen=True)
class EventLiftRequest:
    run_id: str
    mechanism_id: str
    regime_id: str
    event_id: str
    symbol: str
    direction: str
    horizon_bars: int
    data_root: Path = DEFAULT_DATA_ROOT
    source_run_id: str | None = None
    event_source_run_id: str | None = None
    timeframe: str = DEFAULT_TIMEFRAME
    overlap_policy: str = DEFAULT_OVERLAP_POLICY
    allow_nonviable_regime_audit: bool = False


def parse_regime_id(regime_id: str) -> dict[str, str]:
    filters: dict[str, str] = {}
    for part in str(regime_id or "").split("+"):
        token = part.strip()
        if not token:
            continue
        if "=" not in token:
            raise ValueError(f"Invalid regime filter token: {token}")
        dimension, value = token.split("=", 1)
        filters[dimension.strip()] = value.strip()
    if not filters:
        raise ValueError("regime_id must include at least one dimension=value filter")
    validate_regime_filters(filters)
    return filters


def load_scorecard(data_root: Path) -> pd.DataFrame:
    base = data_root / "reports" / "regime_baselines"
    parquet_path = base / "regime_scorecard.parquet"
    json_path = base / "regime_scorecard.json"
    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    if json_path.exists():
        payload = json.loads(json_path.read_text(encoding="utf-8"))
        return pd.DataFrame(payload.get("rows") or [])
    return pd.DataFrame()


def scorecard_decision_for_regime(data_root: Path, regime_id: str) -> str:
    scorecard = load_scorecard(data_root)
    if scorecard.empty or "regime_id" not in scorecard.columns:
        return "missing_scorecard"
    matches = scorecard[scorecard["regime_id"].astype(str) == str(regime_id)]
    if matches.empty:
        return "missing_scorecard_row"
    return str(matches.iloc[0].get("decision") or "")


def enforce_scorecard_gate(request: EventLiftRequest) -> str:
    decision = scorecard_decision_for_regime(request.data_root, request.regime_id)
    if decision != "allow_event_lift" and not request.allow_nonviable_regime_audit:
        raise EventLiftGateError(
            f"regime_id={request.regime_id} is not eligible for event lift; "
            f"scorecard decision={decision}"
        )
    return decision


def _empty_stats(reason: str = "") -> dict[str, Any]:
    return {
        "n": 0,
        "effective_n": 0,
        "mean_gross_bps": None,
        "mean_net_bps": None,
        "median_net_bps": None,
        "t_stat_net": None,
        "hit_rate": None,
        "p25_net_bps": None,
        "p75_net_bps": None,
        "max_drawdown_bps": None,
        "cost_bps": None,
        "mean_net_bps_2x_cost": None,
        "reason": reason,
    }


def _summarize_sample(df: pd.DataFrame, *, horizon_bars: int, reason: str = "") -> dict[str, Any]:
    if df.empty:
        return _empty_stats(reason)
    sampled = _suppress_overlap(df.copy(), horizon_bars)
    if sampled.empty:
        return _empty_stats(reason or "no rows remain after overlap suppression")
    return {
        "n": len(df),
        "effective_n": len(sampled),
        "mean_gross_bps": _safe_float(sampled["gross_bps"].mean()),
        "mean_net_bps": _safe_float(sampled["net_bps"].mean()),
        "median_net_bps": _safe_float(sampled["net_bps"].median()),
        "t_stat_net": _t_stat(sampled["net_bps"]),
        "hit_rate": _safe_float((sampled["net_bps"] > 0).mean()),
        "p25_net_bps": _safe_float(sampled["net_bps"].quantile(0.25)),
        "p75_net_bps": _safe_float(sampled["net_bps"].quantile(0.75)),
        "max_drawdown_bps": _max_drawdown_bps(sampled["net_bps"]),
        "cost_bps": _safe_float(sampled["cost_bps"].mean()),
        "mean_net_bps_2x_cost": _safe_float(sampled["net_bps_2x_cost"].mean()),
        "reason": reason,
    }


def _mean_net(control: dict[str, Any]) -> float | None:
    return _safe_float(control.get("mean_net_bps"))


def _lift(left: dict[str, Any], right: dict[str, Any]) -> float | None:
    left_mean = _mean_net(left)
    right_mean = _mean_net(right)
    if left_mean is None or right_mean is None:
        return None
    return float(left_mean - right_mean)


def classify_event_lift(
    *,
    controls: dict[str, Any],
    max_year_pnl_share: float | None,
    mean_net_bps_2x_cost: float | None,
) -> tuple[str, str, str]:
    event_plus = controls.get("event_plus_regime") or {}
    matched = controls.get("regime_only_matched_non_event") or {}
    event_only = controls.get("event_only") or {}
    unconditional = controls.get("unconditional_all") or {}
    opposite = controls.get("opposite_direction") or {}

    event_plus_mean = _mean_net(event_plus)
    matched_mean = _mean_net(matched)
    event_only_mean = _mean_net(event_only)
    unconditional_mean = _mean_net(unconditional)
    opposite_mean = _mean_net(opposite)
    effective_n = int(event_plus.get("effective_n") or 0)

    if effective_n < MIN_EVENT_EFFECTIVE_N or event_plus_mean is None:
        return "insufficient_support", "data_repair", "event_plus_regime support is insufficient"
    if event_plus_mean < 0:
        return "negative", "kill", "event_plus_regime mean_net_bps < 0"
    if opposite_mean is not None and opposite_mean > 0:
        return "direction_invalid", "kill", "opposite_direction is positive"

    base_lag = None
    late_best = None
    for item in controls.get("entry_lags") or []:
        lag = int(item.get("lag_bars") or 0)
        mean = _mean_net(item)
        if lag == 0:
            base_lag = mean
        elif lag in {2, 3} and mean is not None:
            late_best = mean if late_best is None else max(late_best, mean)
    if base_lag is not None and late_best is not None and late_best > base_lag + MATERIAL_TIMING_BPS:
        return "timing_unstable", "park", "entry_lag_2_or_3 beats base materially"

    if matched_mean is not None and event_plus_mean <= matched_mean:
        return "regime_proxy", "park", "event_plus_regime does not beat matched regime-only control"
    if event_only_mean is not None and event_plus_mean <= event_only_mean:
        return "context_not_additive", "park", "event_plus_regime does not beat event_only"
    if unconditional_mean is not None and event_plus_mean <= unconditional_mean:
        return "negative", "kill", "event_plus_regime does not beat unconditional_all"
    if max_year_pnl_share is not None and max_year_pnl_share > 0.50:
        return "year_conditional", "park", "positive result is one-year dominated"
    if mean_net_bps_2x_cost is None or mean_net_bps_2x_cost < 0:
        return "negative", "kill", "2x cost stress fails"
    return "event_specific", "advance_to_mechanism_proposal", "passes initial event-lift controls"


def _timestamp_column(df: pd.DataFrame) -> str | None:
    for column in ("timestamp", "signal_ts", "detected_ts", "eval_bar_ts", "ts_start"):
        if column in df.columns:
            return column
    return None


def _event_paths(
    data_root: Path,
    *,
    reports_dir: str,
    events_file: str,
    event_source_run_id: str | None,
) -> list[Path]:
    base = data_root / "reports" / reports_dir
    if event_source_run_id:
        path = base / event_source_run_id / events_file
        return [path] if path.exists() else []
    if not base.exists():
        return []
    return sorted(base.glob(f"*/{events_file}"), key=lambda item: (item.stat().st_mtime, str(item)), reverse=True)


def load_event_timestamps(
    data_root: Path,
    *,
    event_id: str,
    symbol: str,
    event_source_run_id: str | None = None,
) -> tuple[pd.Series, str]:
    registry = load_authoritative_event_registry()
    event = registry.get(str(event_id).strip().upper())
    if not event:
        return pd.Series(dtype="datetime64[ns, UTC]"), f"{event_id} is not in the authoritative registry"
    reports_dir = str(event.get("reports_dir") or "")
    events_file = str(event.get("events_file") or "")
    signal_column = str(event.get("signal_column") or "")
    if not reports_dir or not events_file:
        return pd.Series(dtype="datetime64[ns, UTC]"), "event registry lacks reports_dir/events_file"

    frames: list[pd.DataFrame] = []
    for path in _event_paths(
        data_root,
        reports_dir=reports_dir,
        events_file=events_file,
        event_source_run_id=event_source_run_id,
    ):
        try:
            frames.append(pd.read_parquet(path))
        except Exception as exc:
            LOGGER.warning("Could not read event parquet %s: %s", path, exc)
    if not frames:
        return pd.Series(dtype="datetime64[ns, UTC]"), "missing materialized event file"

    events = pd.concat(frames, ignore_index=True)
    if "symbol" in events.columns:
        events = events[events["symbol"].astype(str) == str(symbol)]
    event_token = str(event_id).strip().upper()
    if "event_type" in events.columns:
        events = events[events["event_type"].astype(str).str.upper() == event_token]
    elif "event_name" in events.columns:
        events = events[events["event_name"].astype(str).str.upper() == event_token]
    elif signal_column in events.columns:
        events = events[pd.to_numeric(events[signal_column], errors="coerce").fillna(0) > 0]

    ts_col = _timestamp_column(events)
    if not ts_col:
        return pd.Series(dtype="datetime64[ns, UTC]"), "materialized event file has no timestamp column"
    timestamps = pd.to_datetime(events[ts_col], utc=True, errors="coerce").dropna().drop_duplicates()
    timestamps = timestamps.sort_values().reset_index(drop=True)
    reason = "" if not timestamps.empty else "event file contains no matching timestamps"
    return timestamps, reason


def _prepare_return_frame(
    features: pd.DataFrame,
    *,
    direction: str,
    horizon_bars: int,
) -> tuple[pd.DataFrame, str]:
    if features.empty:
        return pd.DataFrame(), "missing prices"
    if "timestamp" not in features.columns or "close" not in features.columns:
        return pd.DataFrame(), "missing prices"
    costs, cost_source = _cost_series(features)
    if costs is None:
        return pd.DataFrame(), "missing cost fields"

    working = features.copy().reset_index(drop=True)
    working["timestamp"] = pd.to_datetime(working["timestamp"], utc=True, errors="coerce")
    close = pd.to_numeric(working["close"], errors="coerce")
    future_close = close.shift(-int(horizon_bars))
    direction_sign = 1.0 if direction == "long" else -1.0
    working["_pos"] = range(len(working))
    working["gross_bps"] = direction_sign * ((future_close / close) - 1.0) * 10_000.0
    working["cost_bps"] = costs
    working["net_bps"] = working["gross_bps"] - working["cost_bps"]
    working["net_bps_2x_cost"] = working["gross_bps"] - (2.0 * working["cost_bps"])
    working = working.dropna(subset=["timestamp", "gross_bps", "cost_bps"])
    return working.reset_index(drop=True), f"cost_source={cost_source}"


def _event_position_mask(working: pd.DataFrame, event_timestamps: pd.Series) -> pd.Series:
    if working.empty or event_timestamps.empty:
        return pd.Series(False, index=working.index)
    event_values = set(pd.to_datetime(event_timestamps, utc=True, errors="coerce").dropna())
    return working["timestamp"].isin(event_values).fillna(False)


def _lag_mask(working: pd.DataFrame, base_event_mask: pd.Series, lag_bars: int) -> pd.Series:
    selected_positions = set((working.loc[base_event_mask, "_pos"].astype(int) + int(lag_bars)).tolist())
    return working["_pos"].astype(int).isin(selected_positions)


def _matched_non_event_sample(
    working: pd.DataFrame,
    *,
    regime_mask: pd.Series,
    event_mask: pd.Series,
    event_plus: pd.DataFrame,
    horizon_bars: int,
) -> pd.DataFrame:
    if event_plus.empty:
        return working.iloc[0:0].copy()

    event_positions = set(working.loc[event_mask, "_pos"].astype(int).tolist())
    cooldown_positions: set[int] = set()
    for pos in event_positions:
        cooldown_positions.update(range(pos, pos + int(horizon_bars) + 1))

    candidates = working[
        regime_mask
        & ~working["_pos"].astype(int).isin(event_positions)
        & ~working["_pos"].astype(int).isin(cooldown_positions)
    ].copy()
    if candidates.empty:
        return candidates

    event_year_counts = (
        pd.to_datetime(event_plus["timestamp"], utc=True, errors="coerce").dt.year.value_counts().sort_index()
    )
    samples: list[pd.DataFrame] = []
    for year, count in event_year_counts.items():
        year_candidates = candidates[
            pd.to_datetime(candidates["timestamp"], utc=True, errors="coerce").dt.year == int(year)
        ].copy()
        year_candidates = _suppress_overlap(year_candidates, horizon_bars)
        samples.append(year_candidates.head(int(count)))
    if not samples:
        return working.iloc[0:0].copy()
    return pd.concat(samples, ignore_index=True)


def build_event_lift_controls(
    *,
    features: pd.DataFrame,
    filters: dict[str, str],
    event_timestamps: pd.Series,
    direction: str,
    horizon_bars: int,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], float | None, float | None, str]:
    working, return_reason = _prepare_return_frame(
        features,
        direction=direction,
        horizon_bars=horizon_bars,
    )
    if working.empty:
        controls = {column: _empty_stats(return_reason) for column in CONTROL_COLUMNS if column != "entry_lags"}
        controls["entry_lags"] = [dict(_empty_stats(return_reason), lag_bars=lag) for lag in ENTRY_LAGS]
        lift = {
            "event_plus_regime_vs_regime_only_all_bps": None,
            "event_plus_regime_vs_regime_only_matched_bps": None,
            "event_plus_regime_vs_event_only_bps": None,
            "event_plus_regime_vs_unconditional_bps": None,
        }
        return controls, lift, {}, None, None, return_reason

    regime_mask, context_reason = _context_mask(working, filters)
    if regime_mask is None:
        regime_mask = pd.Series(False, index=working.index)
    event_mask = _event_position_mask(working, event_timestamps)
    event_plus_mask = regime_mask & event_mask
    event_plus = working[event_plus_mask].copy()
    matched = _matched_non_event_sample(
        working,
        regime_mask=regime_mask,
        event_mask=event_mask,
        event_plus=event_plus,
        horizon_bars=horizon_bars,
    )

    opposite_working = working.copy()
    opposite_working["gross_bps"] = -opposite_working["gross_bps"]
    opposite_working["net_bps"] = opposite_working["gross_bps"] - opposite_working["cost_bps"]
    opposite_working["net_bps_2x_cost"] = opposite_working["gross_bps"] - (
        2.0 * opposite_working["cost_bps"]
    )

    controls = {
        "unconditional_all": _summarize_sample(working, horizon_bars=horizon_bars),
        "regime_only_all": _summarize_sample(
            working[regime_mask].copy(),
            horizon_bars=horizon_bars,
            reason=context_reason or "",
        ),
        "regime_only_matched_non_event": _summarize_sample(
            matched,
            horizon_bars=horizon_bars,
            reason="same-year non-event regime control",
        ),
        "event_only": _summarize_sample(working[event_mask].copy(), horizon_bars=horizon_bars),
        "event_plus_regime": _summarize_sample(
            event_plus,
            horizon_bars=horizon_bars,
            reason=context_reason or "",
        ),
        "opposite_direction": _summarize_sample(
            opposite_working[event_plus_mask].copy(),
            horizon_bars=horizon_bars,
            reason="opposite sign on event_plus_regime rows",
        ),
    }
    entry_lags: list[dict[str, Any]] = []
    for lag in ENTRY_LAGS:
        lagged = working[regime_mask & _lag_mask(working, event_mask, lag)].copy()
        entry_lags.append(dict(_summarize_sample(lagged, horizon_bars=horizon_bars), lag_bars=lag))
    controls["entry_lags"] = entry_lags

    event_plus_sampled = _suppress_overlap(event_plus.copy(), horizon_bars)
    year_stats, max_year_pnl_share, _positive_year_count = _year_stats(event_plus_sampled)
    mean_net_bps_2x_cost = controls["event_plus_regime"]["mean_net_bps_2x_cost"]
    lift = {
        "event_plus_regime_vs_regime_only_all_bps": _lift(
            controls["event_plus_regime"],
            controls["regime_only_all"],
        ),
        "event_plus_regime_vs_regime_only_matched_bps": _lift(
            controls["event_plus_regime"],
            controls["regime_only_matched_non_event"],
        ),
        "event_plus_regime_vs_event_only_bps": _lift(controls["event_plus_regime"], controls["event_only"]),
        "event_plus_regime_vs_unconditional_bps": _lift(
            controls["event_plus_regime"],
            controls["unconditional_all"],
        ),
    }
    return controls, lift, year_stats, max_year_pnl_share, mean_net_bps_2x_cost, return_reason


def _empty_result(
    request: EventLiftRequest,
    *,
    scorecard_decision: str,
    reason: str,
) -> dict[str, Any]:
    controls = {column: _empty_stats(reason) for column in CONTROL_COLUMNS if column != "entry_lags"}
    controls["entry_lags"] = [dict(_empty_stats(reason), lag_bars=lag) for lag in ENTRY_LAGS]
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": request.run_id,
        "mechanism_id": request.mechanism_id,
        "regime_id": request.regime_id,
        "scorecard_decision": scorecard_decision,
        "audit_only": bool(request.allow_nonviable_regime_audit),
        "promotion_eligible": False,
        "event_id": request.event_id,
        "symbol": request.symbol,
        "direction": request.direction,
        "horizon_bars": int(request.horizon_bars),
        "controls": controls,
        "lift": {
            "event_plus_regime_vs_regime_only_all_bps": None,
            "event_plus_regime_vs_regime_only_matched_bps": None,
            "event_plus_regime_vs_event_only_bps": None,
            "event_plus_regime_vs_unconditional_bps": None,
        },
        "year_stats": {},
        "max_year_pnl_share": None,
        "mean_net_bps_2x_cost": None,
        "classification": "audit_only" if request.allow_nonviable_regime_audit else "insufficient_support",
        "decision": "audit_only" if request.allow_nonviable_regime_audit else "data_repair",
        "reason": reason,
    }


def run_event_lift(request: EventLiftRequest) -> dict[str, Any]:
    if request.direction not in {"long", "short"}:
        raise ValueError("direction must be long or short")
    filters = parse_regime_id(request.regime_id)
    scorecard_decision = enforce_scorecard_gate(request)

    source_run_id = request.source_run_id or discover_market_context_run(
        request.data_root,
        symbols=(request.symbol,),
        timeframe=request.timeframe,
    )
    features = (
        load_market_context(
            request.data_root,
            source_run_id=source_run_id,
            symbol=request.symbol,
            timeframe=request.timeframe,
        )
        if source_run_id
        else pd.DataFrame()
    )
    event_timestamps, event_reason = load_event_timestamps(
        request.data_root,
        event_id=request.event_id,
        symbol=request.symbol,
        event_source_run_id=request.event_source_run_id,
    )
    if features.empty:
        result = _empty_result(request, scorecard_decision=scorecard_decision, reason="missing prices")
    else:
        controls, lift, year_stats, max_share, mean_2x_cost, return_reason = build_event_lift_controls(
            features=features,
            filters=filters,
            event_timestamps=event_timestamps,
            direction=request.direction,
            horizon_bars=request.horizon_bars,
        )
        if request.allow_nonviable_regime_audit:
            classification, decision, reason = "audit_only", "audit_only", "audit-only nonviable regime override"
        else:
            classification, decision, reason = classify_event_lift(
                controls=controls,
                max_year_pnl_share=max_share,
                mean_net_bps_2x_cost=mean_2x_cost,
            )
        if event_reason and reason:
            reason = f"{reason}; {event_reason}"
        elif event_reason:
            reason = event_reason
        elif return_reason and reason:
            reason = f"{reason}; {return_reason}"
        result = {
            "schema_version": SCHEMA_VERSION,
            "run_id": request.run_id,
            "mechanism_id": request.mechanism_id,
            "regime_id": request.regime_id,
            "scorecard_decision": scorecard_decision,
            "audit_only": bool(request.allow_nonviable_regime_audit),
            "promotion_eligible": bool(
                not request.allow_nonviable_regime_audit
                and classification == "event_specific"
                and decision == "advance_to_mechanism_proposal"
            ),
            "event_id": request.event_id,
            "symbol": request.symbol,
            "direction": request.direction,
            "horizon_bars": int(request.horizon_bars),
            "controls": controls,
            "lift": lift,
            "year_stats": year_stats,
            "max_year_pnl_share": max_share,
            "mean_net_bps_2x_cost": mean_2x_cost,
            "classification": classification,
            "decision": decision,
            "reason": reason,
        }
    if request.allow_nonviable_regime_audit:
        result["audit_only"] = True
        result["promotion_eligible"] = False
        result["classification"] = "audit_only"
        result["decision"] = "audit_only"
    return result


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def write_event_lift_outputs(result: dict[str, Any], *, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "row_count": 1,
        "rows": [_json_ready(result)],
    }
    (output_dir / "event_lift.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    frame = pd.DataFrame([result], columns=ROW_COLUMNS)
    try:
        frame.to_parquet(output_dir / "event_lift.parquet", index=False)
    except Exception:
        parquet_frame = frame.copy()
        for column in ("controls", "lift", "year_stats"):
            parquet_frame[column] = parquet_frame[column].map(lambda item: json.dumps(_json_ready(item), sort_keys=True))
        parquet_frame.to_parquet(output_dir / "event_lift.parquet", index=False)
    write_event_lift_markdown(result, output_dir=output_dir)


def write_event_lift_markdown(result: dict[str, Any], *, output_dir: Path) -> None:
    controls = result.get("controls") or {}
    lines = [
        "# Event Lift",
        "",
        f"- run_id: `{result['run_id']}`",
        f"- mechanism_id: `{result['mechanism_id']}`",
        f"- regime_id: `{result['regime_id']}`",
        f"- event_id: `{result['event_id']}`",
        f"- scorecard_decision: `{result['scorecard_decision']}`",
        f"- audit_only: `{result['audit_only']}`",
        f"- promotion_eligible: `{result['promotion_eligible']}`",
        f"- classification: `{result['classification']}`",
        f"- decision: `{result['decision']}`",
        f"- reason: `{result['reason']}`",
        "",
        "## Controls",
        "",
    ]
    for key in [
        "unconditional_all",
        "regime_only_all",
        "regime_only_matched_non_event",
        "event_only",
        "event_plus_regime",
        "opposite_direction",
    ]:
        control = controls.get(key) or {}
        lines.append(
            "- "
            f"{key}: n={control.get('n')} effective_n={control.get('effective_n')} "
            f"mean_net_bps={control.get('mean_net_bps')} t_stat_net={control.get('t_stat_net')}"
        )
    lines.extend(["", "## Entry Lags", ""])
    lines.extend(
        (
            "- "
            f"lag={item.get('lag_bars')}: n={item.get('n')} effective_n={item.get('effective_n')} "
            f"mean_net_bps={item.get('mean_net_bps')}"
        )
        for item in controls.get("entry_lags") or []
    )
    (output_dir / "event_lift.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
