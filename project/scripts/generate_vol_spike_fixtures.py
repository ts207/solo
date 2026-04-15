#!/usr/bin/env python3
"""Generate fixture event registries for VOL_SPIKE benchmark slices."""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd

from project import PROJECT_ROOT
from project.core.config import get_data_root

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATA_ROOT = get_data_root()
FIXTURES_DIR = DATA_ROOT / "reports" / "benchmarks" / "fixtures"

VOL_SPIKE_SOURCES = [
    DATA_ROOT / "reports" / "volatility_transition" / "batch4_vol" / "vol_spike_edge_events.parquet",
    DATA_ROOT / "reports" / "volatility_transition" / "vol_spike_regime" / "vol_spike_edge_events.parquet",
]


def _parse_date(s: str) -> pd.Timestamp:
    return pd.Timestamp(s, tz="UTC")


def _load_vol_spike_events() -> pd.DataFrame:
    """Load all VOL_SPIKE events from sources."""
    frames = []
    for src in VOL_SPIKE_SOURCES:
        if src.exists():
            df = pd.read_parquet(src)
            frames.append(df)
            log.info(f"Loaded {len(df)} VOL_SPIKE events from {src}")
    
    if not frames:
        return pd.DataFrame()
    
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["timestamp", "symbol"])
    return combined


def generate_fixture(
    slice_id: str,
    start: str,
    end: str,
    events_df: pd.DataFrame,
) -> bool:
    """Generate a fixture for a slice."""
    start_dt = _parse_date(start)
    end_dt = _parse_date(end)
    
    # Filter events to date range and symbol
    events_df["timestamp"] = pd.to_datetime(events_df["timestamp"], utc=True, errors="coerce")
    mask = (events_df["timestamp"] >= start_dt) & (events_df["timestamp"] <= end_dt)
    filtered = events_df[mask].copy()
    
    if filtered.empty:
        log.warning(f"No VOL_SPIKE events found for {slice_id} ({start} to {end})")
        return False
    
    # Format for fixture
    fixture = pd.DataFrame()
    fixture["timestamp"] = filtered["timestamp"]
    fixture["symbol"] = filtered.get("symbol", "BTCUSDT")
    fixture["event_type"] = "VOL_SPIKE"
    fixture["event_score"] = filtered.get("event_score", 1.0)
    fixture["signal_column"] = "vol_spike_event"
    fixture["sign"] = filtered.get("sign", 1)
    fixture["detector_name"] = "VOL_SPIKE"
    
    output_path = FIXTURES_DIR / f"{slice_id}_event_registry.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fixture.to_parquet(output_path, index=False)
    
    log.info(f"Wrote {len(fixture)} events to {output_path}")
    return True


def main():
    # Define new slices
    slices = [
        ("vol_spike_2022_05", "2022-05-01", "2022-06-01"),
        ("vol_spike_2024_07", "2024-07-01", "2024-08-01"),
        ("vol_spike_2023_11", "2023-11-01", "2023-12-01"),
    ]
    
    # Load all VOL_SPIKE events once
    log.info("Loading VOL_SPIKE events...")
    events_df = _load_vol_spike_events()
    
    if events_df.empty:
        log.error("No VOL_SPIKE events found")
        return 1
    
    log.info(f"Total VOL_SPIKE events loaded: {len(events_df)}")
    
    # Generate fixtures for each slice
    success = True
    for slice_id, start, end in slices:
        log.info(f"Generating fixture for {slice_id}...")
        if not generate_fixture(slice_id, start, end, events_df):
            success = False
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())