from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd

from project.core.config import get_data_root
from project.eval.efficiency_tests import build_efficiency_report
from project.io.utils import ensure_dir, list_parquet_files, read_parquet, write_parquet
from project.specs.manifest import finalize_manifest, start_manifest

_LOG = logging.getLogger(__name__)
_DEFAULT_OUT_PATH = Path("lake") / "reports" / "market_health" / "efficiency_v1.parquet"


def _candidate_input_roots(data_root: Path, symbol: str, timeframe: str) -> list[Path]:
    return [
        data_root / "lake" / "raw" / "binance" / "perp" / symbol / f"ohlcv_{timeframe}",
        data_root / "lake" / "perp" / symbol / f"ohlcv_{timeframe}",
    ]


def _load_bars_from_path(path: Path) -> pd.DataFrame:
    candidate = Path(path)
    if not candidate.exists():
        return pd.DataFrame()
    if candidate.is_dir():
        files = list_parquet_files(candidate)
        if not files:
            return pd.DataFrame()
        return read_parquet(files)
    return read_parquet(candidate)


def _load_symbol_bars(
    *,
    data_root: Path,
    symbol: str,
    timeframe: str,
    bars_override: pd.DataFrame | None = None,
    bars_path: Path | None = None,
) -> pd.DataFrame:
    if bars_override is not None:
        df = bars_override
        if df.empty:
            return df
        if "symbol" in df.columns:
            filtered = df[df["symbol"].astype(str).str.upper() == symbol]
            if not filtered.empty:
                return filtered.reset_index(drop=True)
        return df.reset_index(drop=True)

    if bars_path is not None:
        df = _load_bars_from_path(bars_path)
        if df.empty:
            return df
        if "symbol" in df.columns:
            filtered = df[df["symbol"].astype(str).str.upper() == symbol]
            if not filtered.empty:
                return filtered.reset_index(drop=True)
        return df.reset_index(drop=True)

    for root in _candidate_input_roots(data_root, symbol, timeframe):
        df = _load_bars_from_path(root)
        if not df.empty:
            return df.reset_index(drop=True)
    return pd.DataFrame()


def build_market_efficiency_report_frame(
    bars: pd.DataFrame,
    *,
    lag: int = 2,
    default_symbol: str = "UNKNOWN",
    timeframe: str = "1m",
) -> pd.DataFrame:
    if bars.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "timeframe",
                "observations",
                "variance_ratio",
                "hurst_exponent",
                "return_autocorr",
                "data_start",
                "data_end",
            ]
        )
    if "close" not in bars.columns:
        raise ValueError("Market-efficiency report requires a 'close' column.")

    frame = bars.copy()
    if "symbol" not in frame.columns:
        frame["symbol"] = default_symbol
    if "timestamp" in frame.columns:
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
        frame = frame.sort_values(["symbol", "timestamp"], kind="stable")

    rows: list[dict[str, object]] = []
    for symbol, group in frame.groupby("symbol", dropna=False, sort=True):
        closes = pd.to_numeric(group["close"], errors="coerce")
        returns = closes.pct_change()
        metrics = build_efficiency_report(returns, lag=lag)
        rows.append(
            {
                "symbol": str(symbol or default_symbol),
                "timeframe": timeframe,
                "observations": int(metrics["observations"]),
                "variance_ratio": float(metrics["variance_ratio"]),
                "hurst_exponent": float(metrics["hurst_exponent"]),
                "return_autocorr": float(metrics["return_autocorr"]),
                "data_start": group["timestamp"].min() if "timestamp" in group.columns else pd.NaT,
                "data_end": group["timestamp"].max() if "timestamp" in group.columns else pd.NaT,
            }
        )
    return pd.DataFrame(rows).sort_values("symbol").reset_index(drop=True)


def _resolve_output_path(data_root: Path, out_path: str | None) -> Path:
    if out_path:
        return Path(out_path)
    return data_root / _DEFAULT_OUT_PATH


def _parse_symbols(symbols: str) -> list[str]:
    return [token.strip().upper() for token in str(symbols).split(",") if token.strip()]


def run_market_efficiency_report(
    *,
    run_id: str,
    symbols: Iterable[str],
    timeframe: str = "1m",
    lag: int = 2,
    bars_path: str | None = None,
    out_path: str | None = None,
) -> Path:
    data_root = get_data_root()
    symbol_list = [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]
    bars_override = Path(bars_path) if bars_path else None
    override_frame = _load_bars_from_path(bars_override) if bars_override is not None else None
    if (
        override_frame is not None
        and not override_frame.empty
        and "symbol" not in override_frame.columns
        and len(symbol_list) > 1
    ):
        raise ValueError(
            "bars_path override without a 'symbol' column can only be used with a single symbol."
        )
    report_frames: list[pd.DataFrame] = []
    input_paths: list[dict[str, str]] = []

    for symbol in symbol_list:
        loaded = _load_symbol_bars(
            data_root=data_root,
            symbol=symbol,
            timeframe=timeframe,
            bars_override=override_frame,
            bars_path=bars_override,
        )
        if loaded.empty:
            _LOG.warning("No bars found for %s %s", symbol, timeframe)
            continue
        report_frames.append(
            build_market_efficiency_report_frame(
                loaded,
                lag=lag,
                default_symbol=symbol,
                timeframe=timeframe,
            )
        )
        input_paths.append(
            {
                "path": str(bars_override)
                if bars_override is not None
                else str(_candidate_input_roots(data_root, symbol, timeframe)[0]),
            }
        )

    report = (
        pd.concat(report_frames, ignore_index=True)
        if report_frames
        else build_market_efficiency_report_frame(pd.DataFrame(), timeframe=timeframe)
    )

    target = _resolve_output_path(data_root, out_path)
    ensure_dir(target.parent)
    manifest = start_manifest(
        "build_market_efficiency_report",
        run_id,
        {
            "symbols": symbol_list,
            "timeframe": timeframe,
            "lag": lag,
            "bars_path": bars_path,
            "out_path": str(target),
        },
        input_paths,
        [{"path": str(target)}],
    )

    try:
        write_parquet(report, target)
        stats = {
            "rows": len(report),
            "symbols": int(report["symbol"].nunique()) if not report.empty else 0,
        }
        finalize_manifest(
            manifest,
            "success",
            stats=stats,
        )
    except Exception:
        finalize_manifest(manifest, "failed", stats={"rows": 0})
        raise
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build market-efficiency report artifact.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--lag", type=int, default=2)
    parser.add_argument("--bars_path", default=None)
    parser.add_argument("--out_path", default=None)
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_market_efficiency_report(
        run_id=args.run_id,
        symbols=_parse_symbols(args.symbols),
        timeframe=str(args.timeframe).strip(),
        lag=int(args.lag),
        bars_path=args.bars_path,
        out_path=args.out_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
