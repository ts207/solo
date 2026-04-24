from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from project.core.config import get_data_root
from project.events.config import compose_event_config
from project.events.detectors.registry import get_detector, load_all_detectors
from project.io.utils import write_parquet
from project.research._family_event_utils import (
    load_features,
    merge_event_csv,
    safe_severity_quantiles,
)
from project.research.analyzers import run_analyzer_suite
from project.research.analyzers.base import AnalyzerResult
from project.specs.manifest import finalize_manifest, start_manifest

_LOG = logging.getLogger(__name__)


def _save_analyzer_results(results: dict, out_dir: Path, event_type: str) -> None:
    for name, res in results.items():
        summary_path = out_dir / f"{event_type.lower()}_{name}_summary.json"
        summary_path.write_text(json.dumps(res.summary, indent=2), encoding="utf-8")
        for table_name, df in res.tables.items():
            if not df.empty:
                write_parquet(df, out_dir / f"{event_type.lower()}_{table_name}.parquet")


def _coerce_override_value(raw: str) -> Any:
    token = str(raw).strip()
    lowered = token.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        if any(ch in token for ch in (".", "e", "E")):
            return float(token)
        return int(token)
    except ValueError:
        return token


def _parse_detector_overrides(tokens: List[str]) -> Dict[str, Any]:
    overrides: Dict[str, Any] = {}
    idx = 0
    while idx < len(tokens):
        token = str(tokens[idx]).strip()
        if not token.startswith("--"):
            idx += 1
            continue
        key = token[2:].replace("-", "_")
        next_idx = idx + 1
        if next_idx >= len(tokens) or str(tokens[next_idx]).startswith("--"):
            overrides[key] = True
            idx += 1
            continue
        overrides[key] = _coerce_override_value(tokens[next_idx])
        idx += 2
    return overrides


def _symbol_market_frame(features: pd.DataFrame) -> pd.DataFrame | None:
    if features.empty or "close" not in features.columns:
        return None
    return (
        features[["timestamp", "close"]]
        .drop_duplicates()
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


def _combine_analyzer_results(
    *,
    per_symbol_events: Dict[str, pd.DataFrame],
    market_by_symbol: Dict[str, pd.DataFrame],
) -> dict[str, AnalyzerResult]:
    aggregated: dict[str, dict[str, Any]] = {}
    for symbol, events_df in per_symbol_events.items():
        if events_df.empty:
            continue
        market = market_by_symbol.get(symbol)
        symbol_results = run_analyzer_suite(events_df, market=market)
        for analyzer_name, result in symbol_results.items():
            payload = aggregated.setdefault(
                analyzer_name,
                {"summary_by_symbol": {}, "tables": {}},
            )
            payload["summary_by_symbol"][symbol] = result.summary
            for table_name, table in result.tables.items():
                if table.empty:
                    continue
                out_table = table.copy()
                if "symbol" not in out_table.columns:
                    out_table["symbol"] = symbol
                payload["tables"].setdefault(table_name, []).append(out_table)

    combined: dict[str, AnalyzerResult] = {}
    for analyzer_name, payload in aggregated.items():
        tables = {
            table_name: pd.concat(frames, ignore_index=True)
            for table_name, frames in payload["tables"].items()
        }
        summary_by_symbol = payload["summary_by_symbol"]
        combined[analyzer_name] = AnalyzerResult(
            name=analyzer_name,
            summary={
                "n_symbols": len(summary_by_symbol),
                "symbols": sorted(summary_by_symbol.keys()),
                "by_symbol": summary_by_symbol,
                "n_events": int(sum(len(df) for df in per_symbol_events.values())),
            },
            tables=tables,
        )
    return combined


def _load_basis_features(run_id: str, symbol: str, timeframe: str, data_root: Path | None = None) -> pd.DataFrame:
    perp = load_features(run_id, symbol, timeframe=timeframe, market="perp", data_root=data_root)
    if perp.empty:
        return pd.DataFrame()

    # Use spot_close from perp features if available, otherwise fall back to spot features
    if "spot_close" in perp.columns and perp["spot_close"].notna().any():
        # Rename columns for detector compatibility
        result = perp.copy()
        result["close_perp"] = result["close"]
        result["close_spot"] = result["spot_close"]
        return result

    # Fallback: try loading spot features
    spot = load_features(run_id, symbol, timeframe=timeframe, market="spot", data_root=data_root)
    if spot.empty:
        return pd.DataFrame()

    # Merge for basis detection
    merged = pd.merge(
        perp[["timestamp", "close"]].rename(columns={"close": "close_perp"}),
        spot[["timestamp", "close"]].rename(columns={"close": "close_spot"}),
        on="timestamp",
        how="inner",
    ).dropna()

    # Merge back other perp features if needed
    merged = pd.merge(merged, perp.drop(columns=["close"]), on="timestamp", how="left")
    return merged


def _load_detector_input(
    *,
    detector: Any,
    event_type: str,
    run_id: str,
    symbol: str,
    timeframe: str,
    data_root: Path | None = None,
) -> pd.DataFrame:
    data_root = data_root or get_data_root()

    # Check if detector needs basis features (perp + spot)
    needs_basis = False
    basis_cols = {"close_perp", "close_spot", "basis_bps"}

    if event_type in (
        "BASIS_DISLOC",
        "BASIS_SNAPBACK",
        "CROSS_VENUE_DESYNC",
        "CROSS_VENUE_CATCHUP",
        "FND_DISLOC",
        "SPOT_PERP_BASIS_SHOCK",
    ):
        needs_basis = True
    elif detector:
        # Check required columns
        req = getattr(detector, "required_columns", ())
        if any(col in basis_cols for col in req):
            needs_basis = True

        # Check if it's a sequence/composite detector that might need basis features
        if not needs_basis and hasattr(detector, "_ensure_detectors"):
            try:
                detector._ensure_detectors()
                anchor = getattr(detector, "_anchor_detector", None)
                trigger = getattr(detector, "_trigger_detector", None)
                if anchor and any(col in getattr(anchor, "required_columns", ()) for col in basis_cols):
                    needs_basis = True
                elif trigger and any(col in getattr(trigger, "required_columns", ()) for col in basis_cols):
                    needs_basis = True
            except Exception as exc:
                _LOG.warning(
                    "Failed detector preflight while inferring basis feature requirements for %s: %s",
                    event_type,
                    exc,
                )

    if needs_basis:
        return _load_basis_features(run_id, symbol, timeframe, data_root=data_root)
    return load_features(run_id=run_id, symbol=symbol, timeframe=timeframe, data_root=data_root)


def main(argv: List[str] | None = None) -> int:
    DATA_ROOT = get_data_root()
    parser = argparse.ArgumentParser(description="Universal parameterized event analyzer")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--event_type", required=True)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    parser.add_argument("--data_root", default=None, help="Override data root directory")
    args, unknown = parser.parse_known_args(argv)

    # Allow overriding data root for custom artifact locations
    data_root_override = Path(args.data_root) if args.data_root else None

    if args.log_path:
        logging.basicConfig(filename=args.log_path, level=logging.INFO)

    event_type = str(args.event_type).strip().upper()
    try:
        cfg = compose_event_config(event_type)
    except Exception as e:
        _LOG.error("Failed to compose config for %s: %s", event_type, e)
        return 1

    out_dir = (
        Path(args.out_dir)
        if args.out_dir
        else DATA_ROOT / "reports" / cfg.reports_dir / args.run_id
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / cfg.events_file

    load_all_detectors()
    detector = get_detector(event_type)
    if not detector:
        _LOG.error("No detector found for event_type: %s", event_type)
        return 1

    # Ensure detector instance knows its own event type (important for polymorphic detectors)
    detector.event_type = event_type

    stage_name = os.getenv(
        "BACKTEST_STAGE_INSTANCE_ID", f"analyze_events__{event_type}_{args.timeframe}"
    )
    manifest = start_manifest(stage_name, args.run_id, vars(args), [], [])

    try:
        events_parts = []
        per_symbol_events: Dict[str, pd.DataFrame] = {}
        market_by_symbol: Dict[str, pd.DataFrame] = {}
        symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
        detector_params = dict(cfg.parameters)
        detector_params.update(_parse_detector_overrides(unknown))

        for symbol in symbols:
            features = _load_detector_input(
                detector=detector,
                event_type=event_type,
                run_id=str(args.run_id),
                symbol=symbol,
                timeframe=str(args.timeframe),
                data_root=data_root_override,
            )

            if features.empty:
                _LOG.warning("No features loaded for %s on %s", event_type, symbol)
                continue

            part = detector.detect(features, symbol=symbol, **detector_params)

            if not part.empty:
                if "event_type" not in part.columns:
                    part["event_type"] = event_type
                if "symbol" not in part.columns:
                    part["symbol"] = symbol

                # Add severity buckets if not present
                if "severity_bucket" not in part.columns:
                    if "severity" in part.columns:
                        part["severity_bucket"] = safe_severity_quantiles(part, col="severity")
                    else:
                        part["severity_bucket"] = "base"

                # Merge original feature columns for downstream conditioning
                if "eval_bar_ts" in part.columns and "timestamp" in features.columns:
                    feature_cols = [
                        c for c in features.columns if c not in part.columns or c == "timestamp"
                    ]
                    part = part.merge(
                        features[feature_cols],
                        left_on="eval_bar_ts",
                        right_on="timestamp",
                        how="left",
                        suffixes=("", "_feat"),
                    )
                events_parts.append(part)
                per_symbol_events[symbol] = part.copy()
                market = _symbol_market_frame(features)
                if market is not None:
                    market_by_symbol[symbol] = market

        new_df = pd.concat(events_parts, ignore_index=True) if events_parts else pd.DataFrame()
        final_df = merge_event_csv(out_path, event_type=event_type, new_df=new_df)

        row_count = (
            int(len(final_df[final_df["event_type"].astype(str) == event_type]))
            if not final_df.empty and "event_type" in final_df.columns
            else 0
        )

        # Run analyzer suite on aggregated events with proper continuous market bars
        # Use the loaded features (not sparse event frame) as market for correct horizon calculations
        if per_symbol_events:
            results = _combine_analyzer_results(
                per_symbol_events=per_symbol_events,
                market_by_symbol=market_by_symbol,
            )
            _save_analyzer_results(results, out_dir, event_type)

        summary = {
            "run_id": str(args.run_id),
            "event_type": event_type,
            "rows": row_count,
            "events_file": str(out_path),
        }

        (out_dir / f"{event_type.lower()}_summary.json").write_text(
            json.dumps(summary, indent=2), encoding="utf-8"
        )
        print(f"Wrote {row_count} rows for {event_type} to {out_path}")

        manifest["outputs"] = [{"path": str(out_path), "rows": row_count}]
        finalize_manifest(manifest, "success", stats={"event_count": row_count})
        return 0
    except Exception as exc:
        _LOG.exception("Universal analyzer failed for %s", event_type)
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
