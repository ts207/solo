#!/usr/bin/env python3
"""Generate fixture event registries for benchmark slices from existing event data."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import yaml

from project import PROJECT_ROOT
from project.research.benchmarks.fixture_materialization import (
    FIXTURES_DIR,
    materialize_benchmark_fixture,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


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
        row_count = materialize_benchmark_fixture(
            slice_id=slice_id,
            symbols=symbols,
            start=start,
            end=end,
            event_types=event_types,
            output_path=output_path,
        )
        log.info("Wrote %d events to %s", row_count, output_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
