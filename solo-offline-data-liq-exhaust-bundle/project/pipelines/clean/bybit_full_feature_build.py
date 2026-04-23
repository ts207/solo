"""
Builds the canonical Bybit 5m feature set from raw partitions.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from project.core.config import get_data_root
from project.io.utils import ensure_dir, list_parquet_files, read_parquet, write_parquet
from project.features.bybit_derivatives import build_bybit_derivatives_feature_set
from project.specs.manifest import finalize_manifest, start_manifest

_LOG = logging.getLogger(__name__)


def main() -> int:
    data_root = get_data_root()
    parser = argparse.ArgumentParser(description="Build canonical Bybit 5m feature set")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--out_root", default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    symbol = args.symbol.upper()
    bybit_root = data_root / "lake" / "raw" / "bybit" / "perp" / symbol

    manifest = start_manifest(
        "bybit_full_feature_build", args.run_id, vars(args), [], []
    )

    try:
        _LOG.info(f"Building features for {symbol}...")

        def _read_dir(p: Path) -> pd.DataFrame:
            files = list_parquet_files(p)
            if not files:
                return pd.DataFrame()
            return read_parquet(files)

        ohlcv = _read_dir(bybit_root / "ohlcv_5m")
        mark = _read_dir(bybit_root / "mark_price_5m")
        index = _read_dir(bybit_root / "index_price_5m")
        funding = _read_dir(bybit_root / "funding")
        oi = _read_dir(bybit_root / "open_interest")

        ticker_path = bybit_root / "tickers_5m"
        ticker = _read_dir(ticker_path) if ticker_path.exists() else None

        if ohlcv.empty:
            _LOG.error(f"No OHLCV data found for {symbol}")
            finalize_manifest(manifest, "failed", error=f"No OHLCV data for {symbol}")
            return 1

        feature_frame = build_bybit_derivatives_feature_set(
            ohlcv_5m=ohlcv,
            mark_price_5m=mark,
            index_price_5m=index,
            funding_df=funding,
            oi_df=oi,
            ticker_df=ticker,
        )

        out_root = (
            Path(args.out_root)
            if args.out_root
            else data_root / "lake" / "cleaned" / "bybit" / "perp" / symbol
        )
        ensure_dir(out_root)

        out_path = out_root / f"features_5m_{symbol}.parquet"
        write_parquet(feature_frame, out_path)

        _LOG.info(f"Canonical feature set written to {out_path} ({len(feature_frame)} rows)")
        manifest["outputs"] = [{"path": str(out_path), "rows": len(feature_frame), "storage": "parquet"}]
        finalize_manifest(manifest, "success", stats={"rows": len(feature_frame), "symbol": symbol})
        return 0
    except Exception as e:
        _LOG.exception(f"Feature build failed: {e}")
        finalize_manifest(manifest, "failed", error=str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
