from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from project.core.config import get_data_root
from project.core.context_quality import summarize_context_quality
from project.core.feature_registry import ensure_market_context_feature_definitions_registered
from project.core.feature_schema import feature_dataset_dir_name
from project.core.logging_utils import build_stage_log_handlers
from project.features.context_states import (
    calculate_ms_funding_probabilities,
    calculate_ms_liq_probabilities,
    calculate_ms_oi_probabilities,
    calculate_ms_spread_probabilities,
    calculate_ms_trend_probabilities,
    calculate_ms_vol_probabilities,
    encode_context_state_code,
)
from project.features.funding_persistence import build_funding_persistence_state
from project.io.utils import (
    choose_partition_dir,
    ensure_dir,
    lake_cache_key,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
    write_cache_key,
    write_parquet,
)
from project.specs.manifest import finalize_manifest, start_manifest

_FUNDING_PERSIST_WINDOW = 12  # bars
_HIGH_VOL_PCT = 80
_LOW_VOL_PCT = 20
_SPREAD_ELEVATED_Z = 1.5
_CROWDING_OI_DELTA_PCT = 0.05
_VOL_REGIME_LABELS: dict[float, str] = {
    0.0: "low",
    1.0: "mid",
    2.0: "high",
    3.0: "shock",
}
_CARRY_STATE_LABELS: dict[float, str] = {
    -1.0: "funding_neg",
    0.0: "neutral",
    1.0: "funding_pos",
}


def _context_quality_report_path(
    data_root: Path,
    *,
    run_id: str,
    market: str,
    symbol: str,
    timeframe: str,
) -> Path:
    return (
        data_root
        / "reports"
        / "context_quality"
        / run_id
        / market
        / symbol
        / timeframe
        / "context_quality_report_v1.json"
    )


def _write_context_quality_report(path: Path, payload: dict[str, object]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _normalize_percentile_scale(series: pd.Series) -> pd.Series:
    out = pd.to_numeric(series, errors="coerce").astype(float)
    non_null = out.dropna()
    if not non_null.empty and float(non_null.abs().max()) <= 1.0:
        out = out * 100.0
    return out


def _normalize_utc_timestamp_column(
    frame: pd.DataFrame,
    *,
    column: str = "timestamp",
    frame_name: str,
) -> pd.DataFrame:
    if column not in frame.columns:
        raise ValueError(f"missing {column} column in {frame_name}")
    out = frame.copy()
    out[column] = pd.to_datetime(out[column], utc=True, errors="coerce")
    if out[column].isna().all():
        raise ValueError(f"{frame_name}.{column} normalized to all-null timestamps")
    return out


def _label_from_state_code(series: pd.Series, mapping: dict[float, str]) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").astype(float)
    labels = numeric.map(mapping)
    return labels.where(labels.notna(), pd.NA)


def _written_path(write_result: object, requested_path: Path) -> Path:
    if isinstance(write_result, tuple) and write_result:
        candidate = write_result[0]
        if isinstance(candidate, Path):
            return candidate
    if isinstance(write_result, Path):
        return write_result
    return requested_path


def _filter_time_window(
    frame: pd.DataFrame,
    *,
    start: str | None,
    end: str | None,
) -> pd.DataFrame:
    if frame.empty or "timestamp" not in frame.columns or (not start and not end):
        return frame
    out = frame.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    if start:
        start_ts = pd.Timestamp(start, tz="UTC")
        out = out[out["timestamp"] >= start_ts]
    if end:
        end_ts = pd.Timestamp(end, tz="UTC")
        out = out[out["timestamp"] < end_ts]
    return out.reset_index(drop=True)


def _build_market_context(symbol: str, features: pd.DataFrame) -> pd.DataFrame:
    ensure_market_context_feature_definitions_registered()
    if "funding_rate_scaled" not in features.columns:
        raise ValueError(f"missing funding_rate_scaled for {symbol}")
    features["funding_rate_scaled"] = pd.to_numeric(
        features["funding_rate_scaled"], errors="coerce"
    ).astype(float)
    if features["funding_rate_scaled"].isna().any():
        missing_count = int(features["funding_rate_scaled"].isna().sum())
        total_rows = len(features)
        gap_pct = (missing_count / total_rows) if total_rows else 0.0
        if missing_count == total_rows:
            logging.warning(
                "funding_rate_scaled unavailable for %s; defaulting all %s/%s rows to 0.0",
                symbol,
                missing_count,
                total_rows,
            )
        else:
            logging.warning(
                "funding_rate_scaled contains %s/%s missing rows (%.2f%%) for %s; defaulting gaps to 0.0",
                missing_count,
                total_rows,
                gap_pct * 100.0,
                symbol,
            )
        features["funding_rate_scaled"] = features["funding_rate_scaled"].fillna(0.0)

    out = _normalize_utc_timestamp_column(features, frame_name=f"{symbol}_features")
    close = pd.to_numeric(out.get("close", pd.Series(np.nan, index=out.index)), errors="coerce")
    if "close_perp" not in out.columns:
        out["close_perp"] = close
    else:
        out["close_perp"] = pd.to_numeric(out["close_perp"], errors="coerce").where(
            pd.to_numeric(out["close_perp"], errors="coerce").notna(),
            close,
        )
    if "close_spot" not in out.columns and "spot_close" in out.columns:
        out["close_spot"] = pd.to_numeric(out["spot_close"], errors="coerce")

    # funding_rate_bps
    out["funding_rate_bps"] = out["funding_rate_scaled"] * 10_000.0
    funding_probs = calculate_ms_funding_probabilities(out["funding_rate_bps"])
    out = pd.concat([out, funding_probs], axis=1)

    fp_state = build_funding_persistence_state(
        out[["timestamp", "funding_rate_scaled"]],
        symbol=symbol,
    )
    fp_state = _normalize_utc_timestamp_column(fp_state, frame_name=f"{symbol}_funding_persistence")
    fp_cols = [col for col in fp_state.columns if col != "timestamp"]
    out = out.merge(fp_state[["timestamp", *fp_cols]], on="timestamp", how="left")

    # carry_state_code: +1 positive funding, 0 neutral funding, -1 negative funding
    out["carry_state_code"] = np.where(
        out["funding_rate_scaled"] > 0,
        1.0,
        np.where(out["funding_rate_scaled"] < 0, -1.0, 0.0),
    ).astype(float)
    out["carry_state"] = _label_from_state_code(out["carry_state_code"], _CARRY_STATE_LABELS)

    out["funding_persistence_state"] = (
        pd.to_numeric(out.get("fp_active", 0.0), errors="coerce").fillna(0.0) > 0
    ).astype(float)
    funding_positive = out["funding_rate_scaled"] > 0
    funding_negative = out["funding_rate_scaled"] < 0
    funding_persistent = out["funding_persistence_state"] > 0
    funding_extreme = pd.to_numeric(out.get("ms_funding_state", 0.0), errors="coerce").fillna(0.0) >= 2.0
    out["funding_phase"] = "neutral"
    out.loc[funding_positive & funding_extreme, "funding_phase"] = "positive_onset"
    out.loc[funding_negative & funding_extreme, "funding_phase"] = "negative_onset"
    out.loc[funding_positive & funding_persistent, "funding_phase"] = "positive_persistent"
    out.loc[funding_negative & funding_persistent, "funding_phase"] = "negative_persistent"

    # vol regime: use rv_96 percentile if available, else rv_pct_17280
    if "rv_pct_17280" in out.columns:
        rv_pct = _normalize_percentile_scale(out["rv_pct_17280"])
        vol_probs = calculate_ms_vol_probabilities(rv_pct)
        out = pd.concat([out, vol_probs], axis=1)
        out["vol_regime_code"] = pd.to_numeric(out["ms_vol_state"], errors="coerce").astype(float)
        out["vol_regime"] = _label_from_state_code(out["vol_regime_code"], _VOL_REGIME_LABELS)
        out["high_vol_regime"] = (out["ms_vol_state"] >= 2.0).astype(float)
        out["low_vol_regime"] = (out["ms_vol_state"] == 0.0).astype(float)
    else:
        out["ms_vol_state"] = np.nan
        out["vol_regime_code"] = np.nan
        out["vol_regime"] = pd.Series(pd.NA, index=out.index, dtype="object")
        out["prob_vol_low"] = np.nan
        out["prob_vol_mid"] = np.nan
        out["prob_vol_high"] = np.nan
        out["prob_vol_shock"] = np.nan
        out["ms_vol_confidence"] = np.nan
        out["ms_vol_entropy"] = np.nan
        out["high_vol_regime"] = 0.0
        out["low_vol_regime"] = 0.0

    # spread_elevated_state
    if "spread_zscore" in out.columns:
        spread_z = pd.to_numeric(out["spread_zscore"], errors="coerce").astype(float)
        spread_probs = calculate_ms_spread_probabilities(spread_z)
        out = pd.concat([out, spread_probs], axis=1)
        out["spread_elevated_state"] = (out["ms_spread_state"] >= 1.0).astype(float)
    else:
        out["ms_spread_state"] = np.nan
        out["prob_spread_tight"] = np.nan
        out["prob_spread_wide"] = np.nan
        out["ms_spread_confidence"] = np.nan
        out["ms_spread_entropy"] = np.nan
        out["spread_elevated_state"] = 0.0

    quote_volume = pd.to_numeric(
        out.get(
            "quote_volume",
            out.get("volume", pd.Series(np.nan, index=out.index)) * out.get("close", 1.0),
        ),
        errors="coerce",
    ).astype(float)
    liq_probs = calculate_ms_liq_probabilities(quote_volume)
    out = pd.concat([out, liq_probs], axis=1)
    out["low_liquidity_state"] = (out["ms_liq_state"] == 0.0).astype(float)

    # refill_lag_state: oi_delta negative (de-risking)
    if "oi_delta_1h" in out.columns:
        oi_delta = pd.to_numeric(out["oi_delta_1h"], errors="coerce").fillna(0.0)
        oi_probs = calculate_ms_oi_probabilities(oi_delta)
        out = pd.concat([out, oi_probs], axis=1)
        out["refill_lag_state"] = (oi_delta < 0).astype(float)
        out["deleveraging_state"] = (
            oi_delta < -out["oi_notional"].abs() * _CROWDING_OI_DELTA_PCT
            if "oi_notional" in out.columns
            else oi_delta < 0
        ).astype(float)
    else:
        oi_delta = pd.Series(0.0, index=out.index, dtype=float)
        out["ms_oi_state"] = np.nan
        out["prob_oi_decel"] = np.nan
        out["prob_oi_stable"] = np.nan
        out["prob_oi_accel"] = np.nan
        out["ms_oi_confidence"] = np.nan
        out["ms_oi_entropy"] = np.nan
        out["refill_lag_state"] = 0.0
        out["deleveraging_state"] = 0.0

    # aftershock_state: high vol + spread elevated
    out["aftershock_state"] = (
        (out["high_vol_regime"] > 0) & (out["spread_elevated_state"] > 0)
    ).astype(float)

    # compression_state_flag: low vol + low spread
    out["compression_state_flag"] = (
        (out["low_vol_regime"] > 0) & (out["spread_elevated_state"] == 0)
    ).astype(float)

    # crowding_state: OI high + funding positive
    if "oi_notional" in out.columns:
        oi_high = out["oi_notional"] > out["oi_notional"].rolling(96, min_periods=1).quantile(0.75)
        out["crowding_state"] = (oi_high & (out["funding_rate_scaled"] > 0)).astype(float)
    else:
        out["crowding_state"] = 0.0

    # trend regimes: use canonical log returns when available, otherwise derive from close
    if "logret_1" in out.columns:
        logret_1 = pd.to_numeric(out["logret_1"], errors="coerce").astype(float)
    elif "close" in out.columns:
        close = pd.to_numeric(out["close"], errors="coerce").astype(float)
        logret_1 = np.log(close / close.shift(1))
        out["logret_1"] = logret_1
    else:
        logret_1 = pd.Series(np.nan, index=out.index, dtype=float)

    if logret_1.notna().any():
        rolling_ret = logret_1.rolling(96, min_periods=1).sum()
        vol = logret_1.rolling(96, min_periods=1).std() * np.sqrt(96)
        trend_probs = calculate_ms_trend_probabilities(rolling_ret, rv=vol)
        out = pd.concat([out, trend_probs], axis=1)
        out["bull_trend_regime"] = (out["ms_trend_state"] == 1.0).astype(float)
        out["bear_trend_regime"] = (out["ms_trend_state"] == 2.0).astype(float)
        out["chop_regime"] = (out["ms_trend_state"] == 0.0).astype(float)
    else:
        out["ms_trend_state"] = np.nan
        out["prob_trend_chop"] = np.nan
        out["prob_trend_bull"] = np.nan
        out["prob_trend_bear"] = np.nan
        out["ms_trend_confidence"] = np.nan
        out["ms_trend_entropy"] = np.nan
        out["bull_trend_regime"] = 0.0
        out["bear_trend_regime"] = 0.0
        out["chop_regime"] = 0.0

    # ms_liquidation_state: rolling liquidation pressure
    if "liquidation_notional" in out.columns:
        liq_q80 = out["liquidation_notional"].rolling(288, min_periods=1).quantile(0.80)
        out["ms_liquidation_state"] = (out["liquidation_notional"] > liq_q80).astype(float)
    else:
        out["ms_liquidation_state"] = 0.0

    # macro_regime: multi-month trend label based on price vs. 90-day SMA.
    # Identifies macro bear/bull cycles that ms_trend_state (30-day window) misses.
    # 0.0=flat, 1.0=bull (close > 90d SMA * 1.05), 2.0=bear (close < 90d SMA * 0.95)
    # Warmup: 30 days minimum (8640 bars); full stability after 90 days (25920 bars).
    if "close" in out.columns:
        _macro_close = pd.to_numeric(out["close"], errors="coerce")
        _sma_90d = _macro_close.rolling(window=25920, min_periods=8640).mean().shift(1)
        _dev_pct = (_macro_close / _sma_90d.replace(0.0, np.nan) - 1.0)
        _macro = pd.Series(0.0, index=out.index)
        _macro[_dev_pct > 0.05] = 1.0
        _macro[_dev_pct < -0.05] = 2.0
        _macro[_sma_90d.isna()] = np.nan
        out["macro_regime"] = _macro
    else:
        out["macro_regime"] = np.nan

    liquidation_active = pd.to_numeric(out["ms_liquidation_state"], errors="coerce").fillna(0.0) > 0
    recent_liquidation = (
        liquidation_active.astype(float).rolling(48, min_periods=1).max().shift(1).fillna(0.0) > 0
    )
    refill_active = pd.to_numeric(out["refill_lag_state"], errors="coerce").fillna(0.0) > 0
    low_liquidity = pd.to_numeric(out["low_liquidity_state"], errors="coerce").fillna(0.0) > 0

    out["forced_flow_phase"] = "none"
    out.loc[liquidation_active, "forced_flow_phase"] = "cascade"
    out.loc[recent_liquidation & ~liquidation_active, "forced_flow_phase"] = "cooldown"
    out.loc[refill_active & ~liquidation_active & ~recent_liquidation, "forced_flow_phase"] = "refill"

    out["liquidity_phase"] = "normal"
    out.loc[low_liquidity, "liquidity_phase"] = "thin"
    out.loc[liquidation_active, "liquidity_phase"] = "collapse"
    out.loc[refill_active & ~liquidation_active, "liquidity_phase"] = "refill"
    out.loc[recent_liquidation & ~low_liquidity & ~refill_active & ~liquidation_active, "liquidity_phase"] = "recovered"

    out["liquidity_regime"] = "refill"
    out.loc[low_liquidity, "liquidity_regime"] = "low"
    out.loc[~low_liquidity & ~refill_active, "liquidity_regime"] = "refill"

    out["oi_phase"] = "neutral"
    out.loc[oi_delta > 0, "oi_phase"] = "expansion"
    out.loc[oi_delta < 0, "oi_phase"] = "flush"

    close_delta = close.diff().fillna(0.0)
    out["price_oi_quadrant"] = "price_up_oi_up"
    out.loc[(close_delta >= 0) & (oi_delta < 0), "price_oi_quadrant"] = "price_up_oi_down"
    out.loc[(close_delta < 0) & (oi_delta >= 0), "price_oi_quadrant"] = "price_down_oi_up"
    out.loc[(close_delta < 0) & (oi_delta < 0), "price_oi_quadrant"] = "price_down_oi_down"

    out["funding_regime"] = "normalizing"
    out.loc[pd.to_numeric(out["crowding_state"], errors="coerce").fillna(0.0) > 0, "funding_regime"] = "crowded"

    # Deduplicate columns from repeated pd.concat operations
    out = out.loc[:, ~out.columns.duplicated(keep="last")]
    out["ms_context_state_code"] = encode_context_state_code(
        out["ms_vol_state"],
        out["ms_liq_state"],
        out["ms_oi_state"],
        out["ms_funding_state"],
        out["ms_trend_state"],
        out["ms_spread_state"],
    )

    return out


def build_market_context(bars: pd.DataFrame, funding: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Build market context features."""
    features = _normalize_utc_timestamp_column(bars, frame_name=f"{symbol}_bars")
    if not funding.empty and "funding_rate_scaled" in funding.columns:
        funding = _normalize_utc_timestamp_column(
            funding[["timestamp", "funding_rate_scaled"]],
            frame_name=f"{symbol}_funding",
        )
        features = features.merge(
            funding[["timestamp", "funding_rate_scaled"]], on="timestamp", how="left"
        )
    return _build_market_context(symbol, features)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build market context.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--market", default="perp")
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    run_id = args.run_id
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    tf = args.timeframe
    market = args.market


    data_root = get_data_root()

    log_handlers = build_stage_log_handlers(args.log_path)
    logging.basicConfig(
        level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s"
    )

    manifest = start_manifest(
        f"build_market_context_{tf}" + ("_spot" if market == "spot" else ""),
        run_id,
        vars(args),
        [],
        [],
    )
    stats: dict[str, object] = {"symbols": {}}
    outputs: list[dict[str, object]] = []

    try:
        for symbol in symbols:
            feature_dataset = feature_dataset_dir_name()
            feat_paths = [
                run_scoped_lake_path(
                    data_root, run_id, "features", market, symbol, tf, feature_dataset
                ),
                data_root / "lake" / "features" / market / symbol / tf / feature_dataset,
            ]
            feat_dir = choose_partition_dir(feat_paths)
            if not feat_dir:
                logging.warning(f"No {feature_dataset} found for {symbol} {tf}")
                continue
            feat_files = list_parquet_files(feat_dir)
            features = read_parquet(feat_files)
            features = _filter_time_window(features, start=args.start, end=args.end)
            if features.empty:
                logging.warning(
                    "No %s remain for %s %s after start/end filtering",
                    feature_dataset,
                    symbol,
                    tf,
                )
                continue

            result = _build_market_context(symbol, features)

            if not result.empty:
                result["timestamp"] = pd.to_datetime(result["timestamp"], utc=True)
                result["symbol"] = symbol

                # Write to lake
                out_root = run_scoped_lake_path(
                    data_root, run_id, "features", market, symbol, tf, "market_context"
                )
                shared_ctx_root = (
                    data_root / "lake" / "features" / market / symbol / tf / "market_context"
                )
                for (year, month), group in result.groupby(
                    [result["timestamp"].dt.year, result["timestamp"].dt.month]
                ):
                    year, month = int(year), int(month)
                    out_dir = out_root / f"year={year}" / f"month={month:02d}"
                    out_path = out_dir / f"market_context_{symbol}_{year}-{month:02d}.parquet"
                    shared_path = (
                        shared_ctx_root / f"year={year}" / f"month={month:02d}"
                        / f"market_context_{symbol}_{year}-{month:02d}.parquet"
                    )

                    # Cache key based on the feature files that produced this month's data
                    _month_feat_files = [
                        f for f in feat_files
                        if f"year={year}" in str(f) and f"month={month:02d}" in str(f)
                    ]
                    _cache_key = lake_cache_key(
                        symbol, market, tf, year, month,
                        _month_feat_files,
                        start=str(args.start),
                        end=str(args.end),
                    )
                    actual_path = _written_path(write_parquet(group, out_path), out_path)
                    logging.info(
                        f"Wrote market context for {symbol} {year}-{month:02d} to {actual_path}"
                    )
                    outputs.append(
                        {
                            "path": str(actual_path),
                            "rows": len(group),
                            "start_ts": group["timestamp"].min().isoformat(),
                            "end_ts": group["timestamp"].max().isoformat(),
                        }
                    )
                    # Populate shared cache for future runs
                    if actual_path.exists() and not shared_path.exists() and _cache_key:
                        ensure_dir(shared_path.parent)
                        shutil.copy2(actual_path, shared_path)
                        write_cache_key(shared_path, _cache_key)

                report_path = _context_quality_report_path(
                    data_root,
                    run_id=run_id,
                    market=market,
                    symbol=symbol,
                    timeframe=tf,
                )
                quality_payload = {
                    "schema_version": "context_quality_report_v1",
                    "run_id": run_id,
                    "market": market,
                    "symbol": symbol,
                    "timeframe": tf,
                    "quality": summarize_context_quality(result),
                }
                _write_context_quality_report(report_path, quality_payload)
                outputs.append({"path": str(report_path), "rows": 1})
                stats["symbols"][symbol] = {
                    "rows": len(result),
                    "context_quality_report_path": str(report_path),
                    "context_quality_summary": quality_payload["quality"],
                }

        if not outputs:
            raise RuntimeError("build_market_context produced no market context artifacts")
        manifest["outputs"] = outputs
        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as e:
        logging.exception("Market context building failed")
        finalize_manifest(manifest, "failed", error=str(e), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
