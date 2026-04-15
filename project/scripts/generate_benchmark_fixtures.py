#!/usr/bin/env python3
"""Generate fixture event registries for benchmark slices from existing event data."""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
import yaml

from project import PROJECT_ROOT
from project.core.config import get_data_root

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATA_ROOT = get_data_root()
FIXTURES_DIR = DATA_ROOT / "reports" / "benchmarks" / "fixtures"

EVENT_SOURCES = {
    "VOL_SPIKE": [
        DATA_ROOT / "reports" / "volatility_transition" / "batch4_vol" / "vol_spike_edge_events.parquet",
        DATA_ROOT / "reports" / "volatility_transition" / "vol_spike_regime" / "vol_spike_edge_events.parquet",
    ],
    "VOL_SHOCK": [
        DATA_ROOT / "reports" / "volatility_transition" / "batch4_vol" / "vol_spike_edge_events.parquet",
    ],
    "FUNDING_PERSISTENCE_TRIGGER": [
        DATA_ROOT / "reports" / "funding_events" / "fpt_regime" / "funding_persistence_trigger_edge_events.parquet",
        DATA_ROOT / "reports" / "funding_events" / "batch2b_funding" / "funding_persistence_trigger_edge_events.parquet",
    ],
    "TREND_DECELERATION": [
        DATA_ROOT / "reports" / "trend_structure" / "batch5_trend" / "trend_deceleration_edge_events.parquet",
    ],
    "PULLBACK_PIVOT": [
        DATA_ROOT / "reports" / "trend_structure" / "batch5_trend" / "pullback_pivot_edge_events.parquet",
    ],
}


def _parse_date(s: str) -> pd.Timestamp:
    import pandas as pd
    return pd.Timestamp(s, tz="UTC")


def _load_events_for_type(event_type: str, start: str, end: str) -> pd.DataFrame:
    """Load events from existing sources."""
    sources = EVENT_SOURCES.get(event_type, [])
    frames = []
    start_dt = _parse_date(start)
    end_dt = _parse_date(end)
    
    for src in sources:
        if src.exists():
            df = pd.read_parquet(src)
            if not df.empty and "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
                mask = (df["timestamp"] >= start_dt) & (df["timestamp"] <= end_dt)
                filtered = df[mask].copy()
                if not filtered.empty:
                    frames.append(filtered)
                    log.info(f"Loaded {len(filtered)} {event_type} events from {src}")
    
    if not frames:
        log.warning(f"No events found for {event_type}")
        return pd.DataFrame()
    
    return pd.concat(frames, ignore_index=True)


def _format_fixture_events(events: pd.DataFrame, event_type: str) -> pd.DataFrame:
    """Format events for fixture format."""
    if events.empty:
        return pd.DataFrame(columns=["timestamp", "symbol", "event_type", "event_score", "signal_column", "sign"])
    
    result = pd.DataFrame()
    result["timestamp"] = events["timestamp"]
    result["symbol"] = events.get("symbol", "BTCUSDT")
    result["event_type"] = event_type
    result["event_score"] = events.get("event_score", 1.0)
    result["signal_column"] = f"{event_type.lower()}_event"
    result["sign"] = events.get("sign", 0)
    
    if "detector_name" not in result.columns:
        result["detector_name"] = event_type
    
    return result


def generate_fixture(
    slice_id: str,
    symbols: List[str],
    start: str,
    end: str,
    event_types: List[str],
    output_path: Path,
) -> bool:
    """Generate a fixture event registry for a benchmark slice."""
    log.info(f"Generating fixture for {slice_id}: events={event_types}, symbols={symbols}, {start} to {end}")
    
    all_events = []
    for event_type in event_types:
        events = _load_events_for_type(event_type, start, end)
        for symbol in symbols:
            sym_events = events[events["symbol"] == symbol.upper()].copy() if "symbol" in events.columns else events.copy()
            if not sym_events.empty:
                formatted = _format_fixture_events(sym_events, event_type)
                all_events.append(formatted)
    
    if all_events:
        fixture = pd.concat(all_events, ignore_index=True)
    else:
        fixture = pd.DataFrame(columns=["timestamp", "symbol", "event_type", "event_score", "signal_column", "sign"])
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fixture.to_parquet(output_path, index=False)
    log.info(f"Wrote {len(fixture)} events to {output_path}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Generate benchmark fixtures from existing events")
    parser.add_argument("--preset", type=str, default="core_v1", help="Preset name")
    parser.add_argument("--output_dir", type=str, default=None, help="Output directory")
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir) if args.output_dir else FIXTURES_DIR
    
    preset_path = PROJECT_ROOT / "configs" / "benchmarks" / "discovery" / f"{args.preset}.yaml"
    if not preset_path.exists():
        log.error(f"Preset not found: {preset_path}")
        return 1
    
    with open(preset_path) as f:
        preset = yaml.safe_load(f)
    
    slice_files = preset.get("slices", [])
    
    for slice_file in slice_files:
        slice_path = PROJECT_ROOT / "configs" / "benchmarks" / "discovery" / slice_file
        if not slice_path.exists():
            log.error(f"Slice not found: {slice_path}")
            continue
        
        with open(slice_path) as f:
            slice_cfg = yaml.safe_load(f)
        
        slice_id = slice_cfg["id"]
        symbols = slice_cfg["symbols"]
        start = slice_cfg["start"]
        end = slice_cfg["end"]
        
        search_spec = slice_cfg.get("search_spec", {})
        triggers = search_spec.get("triggers", {})
        event_types = triggers.get("events", [])
        
        if not event_types:
            log.warning(f"No events specified for {slice_id}")
            continue
        
        output_path = output_dir / f"{slice_id}_event_registry.parquet"
        generate_fixture(slice_id, symbols, start, end, event_types, output_path)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())