from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from project.io.parquet_compat import patch_pandas_parquet_fallback
from project.tests.events.fixtures.deployable_core_historical_exchange_replay import (
    SLICE_PATH,
)

DEFAULT_SOURCE = (
    "data/lake/runs/liquidation_std_gate_sho_20260416T102301Z_043299a9a9/"
    "features/perp/BTCUSDT/5m/market_context/year=2024/month=01/"
    "market_context_BTCUSDT_2024-01.parquet"
)

PINNED_COLUMNS = (
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "symbol",
    "source",
    "taker_base_volume",
    "funding_event_ts",
    "funding_rate_scaled",
    "spread_bps",
    "imbalance",
    "depth_usd",
    "bid_depth_usd",
    "ask_depth_usd",
    "micro_depth_depletion",
    "basis_spot_coverage",
    "spot_close",
    "oi_notional",
    "liquidation_notional",
    "liquidation_count",
    "rv_96",
    "range_96",
    "range_med_2880",
    "oi_delta_1h",
    "ms_imbalance_24",
    "ms_funding_state",
    "ms_funding_confidence",
    "ms_funding_entropy",
    "ms_vol_state",
    "ms_vol_confidence",
    "ms_vol_entropy",
    "ms_spread_state",
    "ms_spread_confidence",
    "ms_spread_entropy",
    "ms_oi_state",
    "ms_oi_confidence",
    "ms_oi_entropy",
)


def _load_source(path: Path) -> pd.DataFrame:
    patch_pandas_parquet_fallback()
    if path.suffix.lower() == ".csv":
        frame = pd.read_csv(path)
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
        return frame
    return pd.read_parquet(path)


def materialize_fixture(
    *,
    source: Path,
    out: Path,
    start: str,
    end: str,
) -> Path:
    frame = _load_source(source).copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    sliced = frame[frame["timestamp"].between(start_ts, end_ts, inclusive="both")].copy()
    if sliced.empty:
        raise ValueError(f"No rows in requested interval {start}..{end}: {source}")

    missing = [column for column in PINNED_COLUMNS if column not in sliced.columns]
    if missing:
        raise ValueError(f"Source missing required pinned columns: {missing}")
    sliced = sliced.loc[:, list(PINNED_COLUMNS)].copy()
    sliced["close_perp"] = sliced["close"]
    sliced["close_spot"] = pd.to_numeric(sliced["spot_close"], errors="coerce")
    sliced = sliced.sort_values("timestamp").reset_index(drop=True)

    out.parent.mkdir(parents=True, exist_ok=True)
    sliced.to_csv(out, index=False, lineterminator="\n")
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Materialize the checked-in historical exchange replay CSV fixture "
            "from a local feature artifact."
        )
    )
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--out", default=str(SLICE_PATH))
    parser.add_argument("--start", default="2024-01-01T00:00:00+00:00")
    parser.add_argument("--end", default="2024-01-03T13:00:00+00:00")
    args = parser.parse_args()

    out = materialize_fixture(
        source=Path(args.source),
        out=Path(args.out),
        start=args.start,
        end=args.end,
    )
    print(f"Materialized historical exchange replay fixture: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
