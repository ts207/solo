"""Nightly live cost model fitting (T2.6).

Reads OMS audit logs and fits per-venue/symbol slippage statistics:
  - slippage_mean_bps: rolling mean of realised slippage
  - slippage_std_bps: rolling std of realised slippage
  - hour-of-day buckets
  - regime buckets (if regime_label column present)

Writes spec/cost_model.live.yaml (auto-generated, read-only).
Research uses static cost_model.yaml; live runtime uses the live overlay.

Usage:
    python project/scripts/fit_cost_model.py \
        --audit-log data/live/oms_audit.parquet \
        --output spec/cost_model.live.yaml
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

_LOG = logging.getLogger(__name__)

_DEFAULT_ROLLING_WINDOW = 500
_DEFAULT_MIN_TRADES = 20


def _load_audit_log(path: Path):
    """Load OMS audit log from parquet or jsonl."""
    import pandas as pd

    if not path.exists():
        raise FileNotFoundError(f"Audit log not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    elif suffix in (".json", ".jsonl", ".ndjson"):
        rows = []
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return pd.DataFrame(rows)
    else:
        raise ValueError(f"Unsupported audit log format: {suffix}")


def _extract_fills(audit_df):
    """Extract fill-level slippage from the audit log. Returns a DataFrame."""
    import pandas as pd

    if "event_type" in audit_df.columns:
        mask = audit_df["event_type"].astype(str).str.contains(
            "fill|execution|trade", case=False, na=False
        )
        fill_events = audit_df[mask]
    else:
        fill_events = audit_df

    required = {"symbol", "venue", "expected_price", "fill_price", "side"}
    if not required.issubset(fill_events.columns):
        _LOG.warning("Audit log missing columns for slippage fit: %s", required - set(fill_events.columns))
        return pd.DataFrame()

    keep_cols = [c for c in (list(required) + ["timestamp", "regime_label", "quantity"])
                 if c in fill_events.columns]
    df = fill_events[keep_cols].copy()

    df["expected_price"] = pd.to_numeric(df["expected_price"], errors="coerce")
    df["fill_price"] = pd.to_numeric(df["fill_price"], errors="coerce")
    df = df.dropna(subset=["expected_price", "fill_price"])
    df = df[df["expected_price"] > 0]

    sign = df["side"].map({"buy": 1, "sell": -1}).fillna(1.0)
    df["slippage_bps"] = (
        sign * (df["fill_price"] - df["expected_price"]) / df["expected_price"] * 1e4
    )

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df["hour_of_day"] = df["timestamp"].dt.hour

    return df


def _fit_stats(slippage_series) -> dict:
    """Fit mean, std, p95 from a slippage series."""
    import numpy as np
    import pandas as pd
    s = pd.Series(pd.to_numeric(slippage_series, errors="coerce")).dropna()
    if len(s) < _DEFAULT_MIN_TRADES:
        return {}
    return {
        "mean_bps": round(float(s.mean()), 4),
        "std_bps": round(float(s.std(ddof=1)), 4),
        "p95_bps": round(float(np.percentile(s, 95)), 4),
        "n_trades": int(len(s)),
    }


def fit_cost_model(
    fills_df,
    *,
    rolling_window: int = _DEFAULT_ROLLING_WINDOW,
) -> dict:
    """Fit cost model from fills DataFrame."""
    output: dict = {
        "_generated_at": datetime.now(UTC).isoformat(),
        "_source": "fit_cost_model.py",
        "venues": {},
    }

    if fills_df.empty:
        _LOG.warning("No fill data available; cost model will be empty")
        return output

    for (venue, symbol), group in fills_df.groupby(["venue", "symbol"]):
        recent = group.tail(rolling_window)
        stats = _fit_stats(recent["slippage_bps"])
        if not stats:
            continue

        venue_str = str(venue)
        symbol_str = str(symbol)
        if venue_str not in output["venues"]:
            output["venues"][venue_str] = {}

        entry: dict = {**stats}

        # Hour-of-day buckets
        if "hour_of_day" in recent.columns:
            hour_stats: dict = {}
            for hour, hgroup in recent.groupby("hour_of_day"):
                hs = _fit_stats(hgroup["slippage_bps"])
                if hs:
                    hour_stats[int(hour)] = hs
            if hour_stats:
                entry["hour_buckets"] = hour_stats

        # Regime buckets
        if "regime_label" in recent.columns:
            regime_stats: dict = {}
            for regime, rgroup in recent.groupby("regime_label"):
                rs = _fit_stats(rgroup["slippage_bps"])
                if rs:
                    regime_stats[str(regime)] = rs
            if regime_stats:
                entry["regime_buckets"] = regime_stats

        output["venues"][venue_str][symbol_str] = entry

    return output


def _write_yaml(data: dict, path: Path) -> None:
    try:
        import yaml
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=True)
    except ImportError:
        import json
        path.with_suffix(".json").parent.mkdir(parents=True, exist_ok=True)
        with open(path.with_suffix(".json"), "w") as f:
            json.dump(data, f, indent=2)


def _compute_drift_report(fitted: dict, static_path: Path | None) -> dict:
    """Compare fitted live costs against static research costs."""
    if static_path is None or not static_path.exists():
        return {}
    try:
        import yaml
        static = yaml.safe_load(static_path.read_text()) or {}
    except Exception:
        return {}

    static_cost = float(static.get("cost_bps", static.get("round_trip_cost_bps", 0.0)))
    if static_cost <= 0.0:
        return {}

    drift: dict = {}
    for venue, symbols in fitted.get("venues", {}).items():
        for symbol, stats in symbols.items():
            live_mean = float(stats.get("mean_bps", 0.0))
            ratio = live_mean / static_cost if static_cost else 0.0
            drift[f"{venue}/{symbol}"] = {
                "live_mean_bps": live_mean,
                "static_cost_bps": static_cost,
                "drift_ratio": round(ratio, 4),
                "alert": ratio > 1.5,
            }
    return drift


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fit live cost model from OMS audit log")
    parser.add_argument("--audit-log", required=True, help="Path to OMS audit log (parquet or jsonl)")
    parser.add_argument(
        "--output",
        default="spec/cost_model.live.yaml",
        help="Output path for live cost model YAML",
    )
    parser.add_argument(
        "--static-cost-model",
        default="spec/cost_model.yaml",
        help="Path to static research cost model for drift comparison",
    )
    parser.add_argument(
        "--drift-report",
        default="data/reports/dashboard/cost_drift.json",
        help="Path to write cost drift report",
    )
    parser.add_argument(
        "--rolling-window",
        type=int,
        default=_DEFAULT_ROLLING_WINDOW,
        help="Number of most recent trades to use for rolling fit",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    audit_path = Path(args.audit_log)
    _LOG.info("Loading audit log from %s", audit_path)
    try:
        audit_df = _load_audit_log(audit_path)
    except FileNotFoundError as exc:
        _LOG.error("%s", exc)
        return 1

    fills_df = _extract_fills(audit_df)
    _LOG.info("Extracted %d fill records", len(fills_df))

    fitted = fit_cost_model(fills_df, rolling_window=args.rolling_window)
    output_path = Path(args.output)
    _write_yaml(fitted, output_path)
    _LOG.info("Wrote live cost model to %s", output_path)

    drift = _compute_drift_report(fitted, Path(args.static_cost_model))
    if drift:
        drift_path = Path(args.drift_report)
        drift_path.parent.mkdir(parents=True, exist_ok=True)
        import json
        with open(drift_path, "w") as f:
            json.dump(drift, f, indent=2)
        _LOG.info("Wrote cost drift report to %s", drift_path)
        alerts = [k for k, v in drift.items() if v.get("alert")]
        if alerts:
            _LOG.warning("Cost drift alert (ratio > 1.5): %s", alerts)

    return 0


if __name__ == "__main__":
    sys.exit(main())
