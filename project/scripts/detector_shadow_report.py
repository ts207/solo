from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.events.data_capabilities import load_data_capability_profile
from project.events.detectors.exhaustion import FailedContinuationDetector
from project.events.detectors.liquidity_base import (
    LiquidityVacuumDetectorV2,
    LiquidityVacuumRecoveryDetectorV2,
)
from project.events.detectors.positioning_base import (
    FundingExtremeOnsetDetectorV2,
    FundingNegExtremeOnsetDetectorV2,
    FundingPosExtremeOnsetDetectorV2,
    OIExpansionStressDetectorV2,
    OIFlushDetectorV2,
    OISpikeNegativeDetectorV2,
)
from project.events.detectors.volatility_base import VolShockDetectorV2


DEFAULT_SYMBOLS = ("BTCUSDT", "ETHUSDT")
DEFAULT_YEARS = (2022, 2023, 2024)
DEFAULT_HORIZONS = (6, 12, 24, 48, 96)


def _read_many(repo_root: Path, patterns: list[str]) -> pd.DataFrame:
    files: list[Path] = []
    for pattern in patterns:
        files.extend(sorted(repo_root.glob(pattern)))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(path) for path in files], ignore_index=True).sort_values("timestamp")


def _rolling_pct_rank(series: pd.Series, window: int = 2880, min_periods: int = 288) -> pd.Series:
    return series.rolling(window, min_periods=min_periods).rank(pct=True).fillna(0.0) * 100.0


def _prepare_symbol_frame(repo_root: Path, symbol: str, years: list[int]) -> pd.DataFrame:
    bars = _read_many(
        repo_root,
        [
            f"data/lake/raw/bybit/perp/{symbol}/ohlcv_5m/year={year}/month=*/ohlcv_{symbol}_5m_{year}-*.parquet"
            for year in years
        ],
    )
    oi = _read_many(
        repo_root,
        [
            f"data/lake/raw/bybit/perp/{symbol}/open_interest/year={year}/month=*/oi_{symbol}_{year}-*.parquet"
            for year in years
        ],
    )
    funding = _read_many(
        repo_root,
        [
            f"data/lake/raw/bybit/perp/{symbol}/funding/year={year}/month=*/funding_{symbol}_{year}-*.parquet"
            for year in years
        ],
    )
    if bars.empty or oi.empty or funding.empty:
        raise RuntimeError(
            f"missing required bars/OI/funding for {symbol}: "
            f"bars={len(bars)} oi={len(oi)} funding={len(funding)}"
        )
    for frame in (bars, oi, funding):
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    bars = bars.sort_values("timestamp").drop_duplicates("timestamp")
    oi = oi.sort_values("timestamp").drop_duplicates("timestamp")
    funding = funding.sort_values("timestamp").drop_duplicates("timestamp")

    df = bars.merge(oi[["timestamp", "open_interest"]], on="timestamp", how="left")
    df = pd.merge_asof(
        df.sort_values("timestamp"),
        funding[["timestamp", "funding_rate"]].sort_values("timestamp"),
        on="timestamp",
        direction="backward",
    )
    df["open_interest"] = pd.to_numeric(df["open_interest"], errors="coerce").ffill()
    df["funding_rate_scaled"] = pd.to_numeric(df["funding_rate"], errors="coerce").ffill().fillna(0.0)
    df["funding_abs"] = df["funding_rate_scaled"].abs()
    df["funding_abs_pct"] = _rolling_pct_rank(df["funding_abs"])
    df["oi_notional"] = df["open_interest"] * pd.to_numeric(df["close"], errors="coerce")

    oi_delta_abs = np.log(df["oi_notional"].replace(0.0, np.nan)).diff().abs()
    oi_rank = _rolling_pct_rank(oi_delta_abs)
    df["ms_oi_state"] = np.select([oi_rank >= 80.0, oi_rank <= 20.0], [2.0, 0.0], default=1.0)
    df["ms_oi_confidence"] = 0.80
    df["ms_oi_entropy"] = 0.20

    close = pd.to_numeric(df["close"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    ret = close.pct_change()
    df["rv_96"] = ret.rolling(96, min_periods=12).std() * math.sqrt(96)
    df["range_96"] = ((high - low) / close.replace(0.0, np.nan)).abs().rolling(96, min_periods=12).mean()
    df["range_med_2880"] = df["range_96"].rolling(2880, min_periods=288).median()
    vol_rank = _rolling_pct_rank(df["rv_96"])
    df["ms_vol_state"] = np.select([vol_rank >= 75.0, vol_rank >= 45.0], [2.0, 1.0], default=0.0)
    df["ms_vol_confidence"] = 0.80
    df["ms_vol_entropy"] = 0.20
    df["shadow_vol_regime"] = np.select(
        [df["ms_vol_state"] >= 2.0, df["ms_vol_state"] >= 1.0],
        ["high_vol", "mid_vol"],
        default="low_vol",
    )
    df["shadow_year"] = df["timestamp"].dt.year.astype(str)
    df["shadow_month"] = df["timestamp"].dt.strftime("%Y-%m")
    df["shadow_funding_sign"] = np.select(
        [df["funding_rate_scaled"] > 0.0, df["funding_rate_scaled"] < 0.0],
        ["positive", "negative"],
        default="zero",
    )
    return df.reset_index(drop=True)


def _event_timestamp_column(events: pd.DataFrame) -> str | None:
    for column in ("ts_start", "timestamp", "signal_ts", "eval_bar_ts"):
        if column in events.columns:
            return column
    return None


def _event_metadata(row: pd.Series) -> dict[str, Any]:
    for column in ("detector_metadata", "metadata"):
        value = row.get(column, {})
        if isinstance(value, dict):
            return value
    return {}


def _standardize_events(events: pd.DataFrame, event_id: str, symbol: str, frame: pd.DataFrame) -> list[dict[str, Any]]:
    if events is None or events.empty:
        return []
    ts_column = _event_timestamp_column(events)
    if ts_column is None:
        return []
    indexed = frame.set_index("timestamp")
    rows: list[dict[str, Any]] = []
    for _, row in events.iterrows():
        ts = pd.to_datetime(row[ts_column], utc=True, errors="coerce")
        if pd.isna(ts):
            continue
        metadata = _event_metadata(row)
        source_features = row.get("source_features", {})
        if not isinstance(source_features, dict):
            source_features = {}
        side = str(row.get("event_side", "") or "").lower()
        if not side:
            direction = str(row.get("direction", "") or "").lower()
            side = "bullish" if direction == "up" else "bearish" if direction == "down" else "neutral"
        if ts in indexed.index:
            state = indexed.loc[ts]
            if isinstance(state, pd.DataFrame):
                state = state.iloc[-1]
        else:
            state = pd.Series(dtype=object)
        rows.append(
            {
                "event_id": event_id,
                "symbol": symbol,
                "timestamp": ts,
                "side": side,
                "data_quality_flag": str(row.get("data_quality_flag", "ok") or "ok").lower(),
                "detector_output_trade_eligible": bool(row.get("trade_eligible", metadata.get("trade_eligible", True))),
                "metadata": metadata,
                "source_features": source_features,
                "regime": str(state.get("shadow_vol_regime", "unknown")),
                "year": str(state.get("shadow_year", ts.year)),
                "month": str(state.get("shadow_month", ts.strftime("%Y-%m"))),
                "funding_sign": str(state.get("shadow_funding_sign", "unknown")),
                "oi_subtype": str(
                    metadata.get("positioning_subtype")
                    or metadata.get("flush_subtype")
                    or "unknown"
                ),
                "close": float(state.get("close", np.nan)),
            }
        )
    return rows


def _run_detector(detector_cls: type[Any], event_id: str, frame: pd.DataFrame, symbol: str) -> tuple[list[dict[str, Any]], str | None]:
    try:
        events = detector_cls().detect_events(frame, {"symbol": symbol, "timeframe": "5m"})
    except Exception as exc:
        return [], str(exc)
    return _standardize_events(events, event_id, symbol, frame), None


def _mark_event_column(frame: pd.DataFrame, events: list[dict[str, Any]], event_id: str, column: str) -> None:
    timestamps = {event["timestamp"] for event in events if event["event_id"] == event_id}
    frame[column] = frame["timestamp"].isin(timestamps)


def _build_composites(
    *,
    symbol: str,
    events: list[dict[str, Any]],
    profile: Any,
    data_feeds_available: set[str],
    missing_feed_blockers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_ts: dict[pd.Timestamp, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        by_ts[event["timestamp"]].append(event)

    blocked_thesis_feeds: dict[str, list[str]] = {}
    for thesis_id, raw_spec in profile.composite_theses.items():
        thesis = str(thesis_id).strip().upper()
        spec = dict(raw_spec or {})
        missing = [
            str(feed)
            for feed in list(spec.get("requires_feeds") or [])
            if not profile.feed_available(str(feed)) or str(feed) not in data_feeds_available
        ]
        if missing:
            blocked_thesis_feeds[thesis] = missing
            missing_feed_blockers.append({"symbol": symbol, "thesis": thesis, "missing_feeds": missing})

    history: deque[dict[str, Any]] = deque(maxlen=256)
    composites: list[dict[str, Any]] = []
    for ts in sorted(by_ts):
        current = by_ts[ts]
        history.extend(current)
        recent = list(history)[-64:]
        current_ids = {event["event_id"] for event in current}

        def has_event(event_id: str) -> bool:
            token = str(event_id).strip().upper()
            return any(event["event_id"] == token for event in recent)

        def has_any(group: list[str]) -> bool:
            return any(has_event(item) for item in group or [])

        def grouped_rules_pass(groups: list[list[str]] | None) -> bool:
            return True if not groups else all(has_any(group) for group in groups)

        active_liquidity_vacuum = any(event["event_id"] == "LIQUIDITY_VACUUM" for event in recent[-12:])
        for thesis_id, raw_spec in profile.composite_theses.items():
            thesis = str(thesis_id).strip().upper()
            if thesis in blocked_thesis_feeds:
                continue
            spec = dict(raw_spec or {})
            required_all = [str(item).strip().upper() for item in list(spec.get("required_all") or []) if str(item).strip()]
            if required_all and not all(has_event(item) for item in required_all):
                continue
            if not grouped_rules_pass(spec.get("required_any")):
                continue
            if not grouped_rules_pass(spec.get("confirm_any")):
                continue
            exact = str(spec.get("required_subtype") or "").strip().lower()
            any_subtype = {str(item).strip().lower() for item in list(spec.get("required_subtype_any") or []) if str(item).strip()}
            if exact or any_subtype:
                subtypes = {event.get("oi_subtype", "") for event in recent}
                if exact and exact not in subtypes:
                    continue
                if any_subtype and not (any_subtype & subtypes):
                    continue
            if str(spec.get("execution_filter") or "").strip().lower() == "no_active_liquidity_vacuum" and active_liquidity_vacuum:
                continue
            related = set(required_all)
            for group_name in ("required_any", "confirm_any"):
                for group in spec.get(group_name) or []:
                    related.update(str(item).strip().upper() for item in group)
            if related and not (current_ids & related):
                continue
            evidence = [event for event in recent if not related or event["event_id"] in related]
            latest = evidence[-1] if evidence else current[-1]
            composites.append(
                {
                    **latest,
                    "event_id": thesis,
                    "data_quality_flag": "ok",
                    "detector_output_trade_eligible": profile.trade_candidate(thesis),
                    "metadata": {
                        "composite_thesis": thesis,
                        "evidence_events": [event["event_id"] for event in evidence[-16:]],
                        "trade_eligible": profile.trade_candidate(thesis),
                    },
                }
            )
    return composites


def _attach_forward_returns(events: list[dict[str, Any]], frame: pd.DataFrame, horizons: list[int], cost_bps: float) -> None:
    close = pd.to_numeric(frame["close"], errors="coerce").reset_index(drop=True)
    ts_to_idx = {ts: idx for idx, ts in enumerate(frame["timestamp"])}
    for event in events:
        idx = ts_to_idx.get(event["timestamp"])
        side_mult = 1.0 if event["side"] == "bullish" else -1.0 if event["side"] == "bearish" else np.nan
        for horizon in horizons:
            gross = np.nan
            long_gross = np.nan
            if idx is not None and idx + horizon < len(close):
                long_gross = float(((close.iloc[idx + horizon] / close.iloc[idx]) - 1.0) * 10000.0)
            if np.isfinite(long_gross) and np.isfinite(side_mult):
                gross = long_gross * side_mult
            event[f"fwd_{horizon}b_bps"] = gross
            event[f"fwd_{horizon}b_net_bps"] = gross - cost_bps if np.isfinite(gross) else np.nan
            event[f"fwd_{horizon}b_inverted_bps"] = -gross if np.isfinite(gross) else np.nan
            event[f"fwd_{horizon}b_inverted_net_bps"] = -gross - cost_bps if np.isfinite(gross) else np.nan
            event[f"fwd_{horizon}b_long_bps"] = long_gross
            event[f"fwd_{horizon}b_long_net_bps"] = long_gross - cost_bps if np.isfinite(long_gross) else np.nan
            event[f"fwd_{horizon}b_short_bps"] = -long_gross if np.isfinite(long_gross) else np.nan
            event[f"fwd_{horizon}b_short_net_bps"] = -long_gross - cost_bps if np.isfinite(long_gross) else np.nan


def _t_stat(values: list[float]) -> float | None:
    clean = [value for value in values if np.isfinite(value)]
    if len(clean) < 2:
        return None
    std = float(np.std(clean, ddof=1))
    if std <= 0.0:
        return None
    return float(np.mean(clean) / (std / math.sqrt(len(clean))))


def _return_summary(values: list[float]) -> dict[str, Any]:
    clean = [value for value in values if np.isfinite(value)]
    return {"n": len(clean), "mean_bps": float(np.mean(clean)) if clean else None, "t_stat": _t_stat(clean)}


def _horizon_direction_summary(rows: list[dict[str, Any]], horizon: int, cost_bps: float) -> dict[str, Any]:
    gross = [event.get(f"fwd_{horizon}b_bps", np.nan) for event in rows]
    net = [event.get(f"fwd_{horizon}b_net_bps", np.nan) for event in rows]
    inverted_net = [-value - cost_bps for value in gross if np.isfinite(value)]
    gross_clean = [value for value in gross if np.isfinite(value)]
    net_clean = [value for value in net if np.isfinite(value)]
    gross_mean = float(np.mean(gross_clean)) if gross_clean else None
    net_mean = float(np.mean(net_clean)) if net_clean else None
    return {
        "event_count": len(rows),
        "configured_direction": {
            "gross_bps": _return_summary(gross),
            "net_bps": _return_summary(net),
        },
        "inverted_direction": {
            "net_bps": _return_summary(inverted_net),
        },
        "long": {
            "gross_bps": _return_summary([event.get(f"fwd_{horizon}b_long_bps", np.nan) for event in rows]),
            "net_bps": _return_summary([event.get(f"fwd_{horizon}b_long_net_bps", np.nan) for event in rows]),
        },
        "short": {
            "gross_bps": _return_summary([event.get(f"fwd_{horizon}b_short_bps", np.nan) for event in rows]),
            "net_bps": _return_summary([event.get(f"fwd_{horizon}b_short_net_bps", np.nan) for event in rows]),
        },
        "cost_survival_ratio": (
            float(net_mean / gross_mean)
            if gross_mean is not None and gross_mean > 0.0 and net_mean is not None
            else None
        ),
        "by_symbol": {
            key: _return_summary([event.get(f"fwd_{horizon}b_net_bps", np.nan) for event in rows if event["symbol"] == key])
            for key in sorted({event["symbol"] for event in rows})
        },
        "by_year": {
            key: _return_summary([event.get(f"fwd_{horizon}b_net_bps", np.nan) for event in rows if event["year"] == key])
            for key in sorted({event["year"] for event in rows})
        },
    }


def _summarize_group(
    events: list[dict[str, Any]],
    horizons: list[int],
    cost_bps: float,
    trade_candidates: set[str],
    paper_approved: set[str],
    live_approved: set[str],
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for event_id in sorted({event["event_id"] for event in events}):
        rows = [event for event in events if event["event_id"] == event_id]
        gross_24 = [event.get("fwd_24b_bps", np.nan) for event in rows]
        net_24 = [event.get("fwd_24b_net_bps", np.nan) for event in rows]
        gross_mean_24 = float(np.nanmean(gross_24)) if rows else None
        net_mean_24 = float(np.nanmean(net_24)) if rows else None
        direction_original = net_24
        direction_inverted = [-value for value in gross_24 if np.isfinite(value)]
        direction_inverted = [value - cost_bps for value in direction_inverted]
        long_rows = [event for event in rows if event["side"] == "bullish"]
        short_rows = [event for event in rows if event["side"] == "bearish"]
        horizon_diagnostics = {
            f"{horizon}b": _horizon_direction_summary(rows, horizon, cost_bps)
            for horizon in horizons
        }
        out[event_id] = {
            "count": len(rows),
            "trade_eligible_count": sum(1 for event in rows if event["event_id"] in trade_candidates and event["detector_output_trade_eligible"]),
            "paper_approved_count": sum(1 for event in rows if event["event_id"] in paper_approved),
            "live_approved_count": sum(1 for event in rows if event["event_id"] in live_approved),
            "detector_output_trade_eligible_count": sum(1 for event in rows if event["detector_output_trade_eligible"]),
            "context_only_count": sum(1 for event in rows if event["event_id"] not in trade_candidates),
            "composite_count": sum(1 for event in rows if event["event_id"] in trade_candidates),
            "data_quality_flag_distribution": dict(Counter(event["data_quality_flag"] for event in rows)),
            "regime_distribution": dict(Counter(event["regime"] for event in rows)),
            "month_count": len({event["month"] for event in rows}),
            "top_months": dict(Counter(event["month"] for event in rows).most_common(5)),
            "avg_forward_returns_bps": {
                f"{horizon}b": float(np.nanmean([event.get(f"fwd_{horizon}b_bps", np.nan) for event in rows]))
                for horizon in horizons
            },
            "avg_cost_adjusted_forward_returns_bps": {
                f"{horizon}b": float(np.nanmean([event.get(f"fwd_{horizon}b_net_bps", np.nan) for event in rows]))
                for horizon in horizons
            },
            "net_24b_mean_bps": net_mean_24,
            "gross_24b_mean_bps": gross_mean_24,
            "net_24b_t_stat": _t_stat(net_24),
            "cost_survival_ratio_24b": (
                float(net_mean_24 / gross_mean_24)
                if gross_mean_24 is not None and gross_mean_24 > 0.0 and net_mean_24 is not None
                else None
            ),
            "direction_diagnostics_24b": {
                "original": _return_summary(direction_original),
                "inverted": _return_summary(direction_inverted),
                "long_only": _return_summary([event.get("fwd_24b_net_bps", np.nan) for event in long_rows]),
                "short_only": _return_summary([event.get("fwd_24b_net_bps", np.nan) for event in short_rows]),
            },
            "horizon_direction_diagnostics": horizon_diagnostics,
            "horizon_diagnostics": horizon_diagnostics,
            "by_symbol_net_24b": {
                key: _return_summary([event.get("fwd_24b_net_bps", np.nan) for event in rows if event["symbol"] == key])
                for key in sorted({event["symbol"] for event in rows})
            },
            "by_year_net_24b": {
                key: _return_summary([event.get("fwd_24b_net_bps", np.nan) for event in rows if event["year"] == key])
                for key in sorted({event["year"] for event in rows})
            },
            "by_vol_regime_net_24b": {
                key: _return_summary([event.get("fwd_24b_net_bps", np.nan) for event in rows if event["regime"] == key])
                for key in sorted({event["regime"] for event in rows})
            },
            "by_funding_sign_net_24b": {
                key: _return_summary([event.get("fwd_24b_net_bps", np.nan) for event in rows if event["funding_sign"] == key])
                for key in sorted({event["funding_sign"] for event in rows})
            },
            "by_oi_subtype_net_24b": {
                key: _return_summary([event.get("fwd_24b_net_bps", np.nan) for event in rows if event["oi_subtype"] == key])
                for key in sorted({event["oi_subtype"] for event in rows})
            },
        }
    return out


def _net_summary_at(diagnostics: dict[str, Any], horizon_key: str, direction_key: str) -> dict[str, Any]:
    return dict(
        diagnostics.get(horizon_key, {})
        .get(direction_key, {})
        .get("net_bps", {})
        or {}
    )


def _best_direction_horizon(row: dict[str, Any], horizons: list[int]) -> dict[str, Any]:
    best = {
        "direction": "configured",
        "horizon_bars": None,
        "net_bps": None,
        "t_stat": None,
        "event_count": int(row.get("count", 0) or 0),
    }
    diagnostics = dict(row.get("horizon_direction_diagnostics") or {})
    for horizon in horizons:
        horizon_key = f"{horizon}b"
        for label, key in (("configured", "configured_direction"), ("inverted", "inverted_direction")):
            summary = _net_summary_at(diagnostics, horizon_key, key)
            mean = summary.get("mean_bps")
            if mean is None:
                continue
            if best["net_bps"] is None or float(mean) > float(best["net_bps"]):
                best = {
                    "direction": label,
                    "horizon_bars": int(horizon),
                    "net_bps": float(mean),
                    "t_stat": summary.get("t_stat"),
                    "event_count": int(summary.get("n") or row.get("count", 0) or 0),
                }
    return best


def _best_forced_side_horizon(row: dict[str, Any], horizons: list[int]) -> dict[str, Any]:
    best = {"side": None, "horizon_bars": None, "net_bps": None, "t_stat": None}
    diagnostics = dict(row.get("horizon_direction_diagnostics") or {})
    for horizon in horizons:
        horizon_key = f"{horizon}b"
        for side in ("long", "short"):
            summary = _net_summary_at(diagnostics, horizon_key, side)
            mean = summary.get("mean_bps")
            if mean is None:
                continue
            if best["net_bps"] is None or float(mean) > float(best["net_bps"]):
                best = {
                    "side": side,
                    "horizon_bars": int(horizon),
                    "net_bps": float(mean),
                    "t_stat": summary.get("t_stat"),
                }
    return best


def _positive_breakdown_keys(breakdown: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    for key, summary in breakdown.items():
        if float((summary or {}).get("mean_bps") or 0.0) > 0.0:
            keys.append(str(key))
    return sorted(keys)


def _candidate_recommendation(
    *,
    event_id: str,
    row: dict[str, Any],
    horizons: list[int],
    missing_feed_blockers: list[dict[str, Any]],
) -> dict[str, Any]:
    blockers = [
        blocker
        for blocker in missing_feed_blockers
        if str(blocker.get("thesis", "")).strip().upper() == event_id
    ]
    best = _best_direction_horizon(row, horizons)
    forced = _best_forced_side_horizon(row, horizons)
    recommendation = {
        "event_id": event_id,
        "status": "research_only",
        "best_direction": best["direction"],
        "best_horizon_bars": best["horizon_bars"],
        "net_bps": best["net_bps"],
        "t_stat": best["t_stat"],
        "event_count": int(row.get("count", 0) or 0),
        "paper_approved": False,
        "live_approved": False,
        "best_forced_side": forced,
        "reason": "diagnostic_only_requires_fresh_validation",
    }
    if blockers:
        recommendation.update(
            {
                "status": "needs_book_data",
                "reason": "missing_required_feeds",
                "missing_feed_blockers": blockers,
            }
        )
        return recommendation
    if int(row.get("count", 0) or 0) == 0:
        recommendation.update({"status": "kill", "reason": "no_events_created"})
        return recommendation

    best_net = best["net_bps"]
    best_t = best["t_stat"]
    if best_net is None or best_net <= 0.0:
        recommendation.update({"status": "kill", "reason": "no_positive_configured_or_inverted_horizon"})
        return recommendation
    if best_t is None or best_t <= 2.0:
        reason = "positive_but_t_stat_below_gate"
        forced_t = forced.get("t_stat")
        forced_net = forced.get("net_bps")
        if forced_net is not None and forced_net > 0.0 and forced_t is not None and forced_t > 2.0:
            reason = "configured_direction_not_validated_forced_side_is_diagnostic_only"
        recommendation.update({"status": "research_only", "reason": reason})
        return recommendation

    horizon_key = f"{best['horizon_bars']}b"
    diagnostics = dict(row.get("horizon_direction_diagnostics") or {}).get(horizon_key, {})
    by_symbol = dict(diagnostics.get("by_symbol") or {})
    by_year = dict(diagnostics.get("by_year") or {})
    positive_symbols = _positive_breakdown_keys(by_symbol)
    positive_years = _positive_breakdown_keys(by_year)
    all_symbols_positive = bool(by_symbol) and len(positive_symbols) == len(by_symbol)
    enough_years_positive = len(positive_years) >= 2
    recommendation.update(
        {
            "positive_symbols": positive_symbols,
            "positive_years": positive_years,
            "by_symbol": by_symbol,
            "by_year": by_year,
        }
    )
    if all_symbols_positive and enough_years_positive:
        recommendation.update(
            {
                "status": "fresh_validation_candidate",
                "reason": "positive_diagnostic_requires_fresh_validation_before_paper_approval",
            }
        )
        return recommendation
    if positive_symbols:
        recommendation.update(
            {
                "status": "fresh_validation_candidate",
                "suggested_variant": f"{event_id}_H{best['horizon_bars']}_{positive_symbols[0]}",
                "suggested_symbol_scope": positive_symbols,
                "reason": "positive_diagnostic_is_symbol_scoped_requires_fresh_validation",
            }
        )
        return recommendation
    recommendation.update({"status": "research_only", "reason": "positive_aggregate_but_breakdowns_are_not_positive"})
    return recommendation


def _build_candidate_recommendations(
    *,
    counts: dict[str, Any],
    profile: Any,
    horizons: list[int],
    missing_feed_blockers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            **_candidate_recommendation(
                event_id=event_id,
                row=dict(counts.get(event_id, {}) or {}),
                horizons=horizons,
                missing_feed_blockers=missing_feed_blockers,
            ),
            **({"status": "kill", "reason": "killed_by_profile"} if event_id in profile.killed_events else {}),
        }
        for event_id in sorted(profile.trade_candidate_events)
    ]


def _variant_net_key(direction: str, horizon: int) -> str:
    token = str(direction or "configured").strip().lower()
    if token == "long":
        return f"fwd_{horizon}b_long_net_bps"
    if token == "short":
        return f"fwd_{horizon}b_short_net_bps"
    if token == "inverted":
        return f"fwd_{horizon}b_inverted_net_bps"
    return f"fwd_{horizon}b_net_bps"


def _variant_gross_key(direction: str, horizon: int) -> str:
    token = str(direction or "configured").strip().lower()
    if token == "long":
        return f"fwd_{horizon}b_long_bps"
    if token == "short":
        return f"fwd_{horizon}b_short_bps"
    if token == "inverted":
        return f"fwd_{horizon}b_inverted_bps"
    return f"fwd_{horizon}b_bps"


def _top_share(values: list[str]) -> float | None:
    if not values:
        return None
    return float(Counter(values).most_common(1)[0][1] / len(values))


def _build_research_variant_validations(
    *,
    profile: Any,
    events: list[dict[str, Any]],
    cost_bps: float,
    years: list[int],
) -> list[dict[str, Any]]:
    validations: list[dict[str, Any]] = []
    variants = dict(getattr(profile, "research_candidate_variants", {}) or {})
    for variant_id, raw_spec in sorted(variants.items()):
        spec = dict(raw_spec or {})
        event_id = str(spec.get("event_id") or variant_id).strip().upper()
        base_event = str(spec.get("base_event") or "").strip().upper()
        symbols = {str(item).strip().upper() for item in list(spec.get("symbol_scope") or []) if str(item).strip()}
        horizon = int(spec.get("horizon_bars") or 0)
        direction = str(spec.get("direction") or "configured").strip().lower()
        net_key = _variant_net_key(direction, horizon)
        gross_key = _variant_gross_key(direction, horizon)
        rows = [
            event
            for event in events
            if event["event_id"] == base_event
            and (not symbols or event["symbol"] in symbols)
            and np.isfinite(event.get(net_key, np.nan))
        ]
        net_values = [event.get(net_key, np.nan) for event in rows]
        gross_values = [event.get(gross_key, np.nan) for event in rows]
        net_summary = _return_summary(net_values)
        gross_summary = _return_summary(gross_values)
        gross_mean = gross_summary.get("mean_bps")
        net_mean = net_summary.get("mean_bps")
        cost_survival = (
            float(net_mean / gross_mean)
            if gross_mean is not None and gross_mean > 0.0 and net_mean is not None
            else None
        )
        by_symbol = {
            key: _return_summary([event.get(net_key, np.nan) for event in rows if event["symbol"] == key])
            for key in sorted({event["symbol"] for event in rows})
        }
        by_year = {
            key: _return_summary([event.get(net_key, np.nan) for event in rows if event["year"] == key])
            for key in sorted({event["year"] for event in rows})
        }
        by_month = {
            key: _return_summary([event.get(net_key, np.nan) for event in rows if event["month"] == key])
            for key in sorted({event["month"] for event in rows})
        }
        data_quality = dict(Counter(event["data_quality_flag"] for event in rows))
        top_month_share = _top_share([event["month"] for event in rows])
        enough_events = len(rows) >= 100
        net_pass = net_mean is not None and net_mean > 0.0
        t_pass = net_summary.get("t_stat") is not None and float(net_summary["t_stat"]) > 2.0
        cost_pass = cost_survival is not None and cost_survival >= 0.8
        symbol_pass = bool(by_symbol) and all(float((summary or {}).get("mean_bps") or 0.0) > 0.0 for summary in by_symbol.values())
        if len(years) <= 1:
            year_pass = bool(by_year) and all(float((summary or {}).get("mean_bps") or 0.0) > 0.0 for summary in by_year.values())
        else:
            year_pass = sum(1 for summary in by_year.values() if float((summary or {}).get("mean_bps") or 0.0) > 0.0) >= 2
        concentration_pass = top_month_share is not None and top_month_share <= 0.50
        data_quality_pass = not rows or set(data_quality.keys()) <= {"ok"}
        configured_direction_pass = direction == "configured"
        gate_pass = all(
            [
                enough_events,
                net_pass,
                t_pass,
                cost_pass,
                symbol_pass,
                year_pass,
                concentration_pass,
                data_quality_pass,
                configured_direction_pass,
            ]
        )
        failures = []
        if not enough_events: failures.append("insufficient_events")
        if not net_pass: failures.append("net_bps_not_positive")
        if not t_pass: failures.append("t_stat_not_above_2")
        if not cost_pass: failures.append("cost_survival_below_0_8")
        if not symbol_pass: failures.append("symbol_scope_not_positive")
        if not year_pass: failures.append("year_split_not_positive")
        if not concentration_pass: failures.append("month_concentration_too_high")
        if not data_quality_pass: failures.append("data_quality_not_clean")
        if not configured_direction_pass: failures.append("not_configured_direction")
        validations.append(
            {
                "event_id": event_id,
                "base_event": base_event,
                "status": "paper_eligible_candidate" if gate_pass else "fresh_validation_candidate",
                "paper_approved": False,
                "live_approved": False,
                "requires_fresh_validation": bool(spec.get("requires_fresh_validation", True)),
                "symbol_scope": sorted(symbols),
                "horizon_bars": horizon,
                "bar_interval": str(spec.get("bar_interval") or ""),
                "direction": direction,
                "cost_round_trip_bps": float(spec.get("cost_round_trip_bps") or cost_bps),
                "event_count": len(rows),
                "net_bps": net_summary.get("mean_bps"),
                "gross_bps": gross_summary.get("mean_bps"),
                "t_stat": net_summary.get("t_stat"),
                "cost_survival_ratio": cost_survival,
                "top_month_share": top_month_share,
                "data_quality_flag_distribution": data_quality,
                "by_symbol": by_symbol,
                "by_year": by_year,
                "by_month": by_month,
                "gate_pass": gate_pass,
                "gate_failures": failures,
                "reason": "validation_gate_passed_but_profile_approval_unchanged" if gate_pass else "validation_gate_failed_or_diagnostic_only",
            }
        )
    return validations


def build_report(
    *,
    repo_root: Path,
    profile_name: str,
    symbols: list[str],
    years: list[int],
    horizons: list[int],
    cost_bps: float,
) -> dict[str, Any]:
    profile = load_data_capability_profile(profile_name)
    trade_candidates = {event_id for event_id in profile.trade_candidate_events if profile.trade_candidate(event_id)}
    all_events: list[dict[str, Any]] = []
    input_summary: dict[str, Any] = {}
    detector_errors: dict[str, list[dict[str, str]]] = defaultdict(list)
    missing_feed_blockers: list[dict[str, Any]] = []

    first_pass = [
        ("VOL_SHOCK", VolShockDetectorV2),
        ("FAILED_CONTINUATION", FailedContinuationDetector),
        ("LIQUIDITY_VACUUM", LiquidityVacuumDetectorV2),
        ("LIQUIDITY_VACUUM_RECOVERY", LiquidityVacuumRecoveryDetectorV2),
    ]
    second_pass = [
        ("OI_FLUSH", OIFlushDetectorV2),
        ("OI_EXPANSION_STRESS", OIExpansionStressDetectorV2),
        ("OI_SPIKE_NEGATIVE", OISpikeNegativeDetectorV2),
        ("FUNDING_EXTREME_ONSET", FundingExtremeOnsetDetectorV2),
        ("FUNDING_POS_EXTREME_ONSET", FundingPosExtremeOnsetDetectorV2),
        ("FUNDING_NEG_EXTREME_ONSET", FundingNegExtremeOnsetDetectorV2),
    ]
    for symbol in symbols:
        frame = _prepare_symbol_frame(repo_root, symbol, years)
        input_summary[symbol] = {
            "rows": int(len(frame)),
            "start": str(frame["timestamp"].min()),
            "end": str(frame["timestamp"].max()),
            "has_depth_usd": bool("depth_usd" in frame.columns),
            "has_spread_bps": bool("spread_bps" in frame.columns),
            "source": "data/lake/raw/bybit/perp",
        }
        symbol_events: list[dict[str, Any]] = []
        for event_id, detector_cls in first_pass:
            rows, error = _run_detector(detector_cls, event_id, frame, symbol)
            if error:
                detector_errors[event_id].append({"symbol": symbol, "error": error})
            symbol_events.extend(rows)
        _mark_event_column(frame, symbol_events, "VOL_SHOCK", "vol_shock")
        _mark_event_column(frame, symbol_events, "FAILED_CONTINUATION", "failed_continuation")
        _mark_event_column(frame, symbol_events, "LIQUIDITY_VACUUM", "liquidity_vacuum")
        _mark_event_column(frame, symbol_events, "LIQUIDITY_VACUUM_RECOVERY", "liquidity_vacuum_recovery")

        for event_id, detector_cls in second_pass:
            rows, error = _run_detector(detector_cls, event_id, frame, symbol)
            if error:
                detector_errors[event_id].append({"symbol": symbol, "error": error})
            symbol_events.extend(rows)

        symbol_events.extend(
            _build_composites(
                symbol=symbol,
                events=symbol_events,
                profile=profile,
                data_feeds_available={column for column in ("depth_usd", "spread_bps") if column in frame.columns},
                missing_feed_blockers=missing_feed_blockers,
            )
        )
        _attach_forward_returns(symbol_events, frame, horizons, cost_bps)
        all_events.extend(symbol_events)

    counts = _summarize_group(
        all_events,
        horizons,
        cost_bps,
        trade_candidates,
        set(profile.paper_approved_events),
        set(profile.live_approved_events),
    )
    candidate_recommendations = _build_candidate_recommendations(
        counts=counts,
        profile=profile,
        horizons=horizons,
        missing_feed_blockers=missing_feed_blockers,
    )
    research_variant_validations = _build_research_variant_validations(
        profile=profile,
        events=all_events,
        cost_bps=cost_bps,
        years=years,
    )
    requested = [
        "OI_FLUSH",
        "OI_EXPANSION_STRESS",
        "LIQUIDITY_VACUUM_RECOVERY",
        "FUNDING_CROWDING_BREAK",
        "SHORT_BUILD_CONTINUATION",
        "SQUEEZE_RISK_REVERSAL",
        "OI_FLUSH_REVERSAL",
    ]
    empty_count = {
        "count": 0,
        "trade_eligible_count": 0,
        "paper_approved_count": 0,
        "live_approved_count": 0,
        "detector_output_trade_eligible_count": 0,
        "context_only_count": 0,
        "composite_count": 0,
        "horizon_direction_diagnostics": {
            f"{horizon}b": _horizon_direction_summary([], horizon, cost_bps)
            for horizon in horizons
        },
    }
    empty_count["horizon_diagnostics"] = empty_count["horizon_direction_diagnostics"]
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "profile": profile.name,
        "scope": {"symbols": symbols, "years": years, "timeframe": "5m", "horizons": horizons, "cost_bps": cost_bps},
        "input_summary": input_summary,
        "runtime_selector_inspection": {
            "runtime_detectable_detectors": sorted(profile.runtime_detectable_detectors),
            "context_only_detectors": sorted(profile.context_only_detectors),
            "never_trade_standalone": sorted(profile.never_trade_standalone),
            "trade_candidate_events": sorted(profile.trade_candidate_events),
            "paper_approved_events": sorted(profile.paper_approved_events),
            "live_approved_events": sorted(profile.live_approved_events),
            "research_only_events": sorted(profile.research_only_events),
            "killed_events": sorted(profile.killed_events),
            "rejected_events": sorted(profile.rejected_events),
            "research_candidate_variants": profile.research_candidate_variants,
            "composite_theses": sorted(profile.composite_theses.keys()),
        },
        "detector_errors_or_skips": dict(detector_errors),
        "missing_feed_blockers": missing_feed_blockers[:100],
        "missing_feed_blocker_count": len(missing_feed_blockers),
        "candidate_recommendations": candidate_recommendations,
        "research_variant_validations": research_variant_validations,
        "counts_by_detector": counts,
        "detectors": counts,
        "requested_key_counts": {key: counts.get(key, empty_count) for key in requested},
        "totals": {
            "events": len(all_events),
            "composite_events": sum(1 for event in all_events if event["event_id"] in trade_candidates),
            "trade_eligible_events": sum(
                1 for event in all_events if event["event_id"] in trade_candidates and event["detector_output_trade_eligible"]
            ),
            "paper_approved_events": sum(1 for event in all_events if event["event_id"] in profile.paper_approved_events),
            "live_approved_events": sum(1 for event in all_events if event["event_id"] in profile.live_approved_events),
            "research_only_events": sum(1 for event in all_events if event["event_id"] in profile.research_only_events),
            "context_or_evidence_events": sum(1 for event in all_events if event["event_id"] not in trade_candidates),
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build no-liquidation detector shadow report")
    parser.add_argument("--profile", default="no_liquidations_v1")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--years", default=",".join(str(year) for year in DEFAULT_YEARS))
    parser.add_argument("--horizons", default=",".join(str(horizon) for horizon in DEFAULT_HORIZONS))
    parser.add_argument("--cost-bps", type=float, default=6.0)
    parser.add_argument("--output", default="data/reports/detectors/no_liquidations_v1/shadow_report.json")
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[2]
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
    years = [int(item.strip()) for item in args.years.split(",") if item.strip()]
    horizons = [int(item.strip()) for item in args.horizons.split(",") if item.strip()]
    report = build_report(
        repo_root=repo_root,
        profile_name=args.profile,
        symbols=symbols,
        years=years,
        horizons=horizons,
        cost_bps=float(args.cost_bps),
    )
    output = repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
    print(json.dumps({"status": "pass", "output": str(output), "totals": report["totals"]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
