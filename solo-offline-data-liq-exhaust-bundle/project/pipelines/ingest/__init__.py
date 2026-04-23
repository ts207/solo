"""Ingest-stage pipeline entrypoints."""

from __future__ import annotations

from importlib import import_module

__all__ = [
    "build_universe_snapshots",
    "ingest_binance_spot_ohlcv_1m",
    "ingest_binance_spot_ohlcv_5m",
    "ingest_binance_um_book_ticker",
    "ingest_binance_um_funding",
    "ingest_binance_um_liquidation_snapshot",
    "ingest_binance_um_mark_price_1m",
    "ingest_binance_um_mark_price_5m",
    "ingest_binance_um_ohlcv",
    "ingest_binance_um_open_interest_hist",
    "run_slice1_data_layer",
]


def __getattr__(name: str):
    if name in __all__:
        return import_module(f"{__name__}.{name}")
    raise AttributeError(name)
