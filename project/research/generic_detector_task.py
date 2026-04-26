from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

from project.io.utils import ensure_dir

_LOG = logging.getLogger(__name__)


def run_generic_detector_task(
    run_id: str,
    event_type: str,
    symbols: list[str],
    timeframe: str = "5m",
    params: dict[str, Any] | None = None,
    out_dir: Path | None = None,
) -> int:
    """Thin argument adapter around the canonical analyze_events entrypoint."""
    from project.core.config import get_data_root
    from project.research.analyze_events import main as analyze_events_main

    data_root = get_data_root()
    if out_dir is None:
        out_dir = data_root / "reports" / event_type.lower() / run_id
    ensure_dir(out_dir)
    argv = [
        "--run_id",
        run_id,
        "--symbols",
        ",".join(symbols),
        "--event_type",
        event_type,
        "--timeframe",
        timeframe,
        "--out_dir",
        str(out_dir),
    ]
    for key, value in (params or {}).items():
        flag = f"--{str(key).replace('_', '-')}"
        argv.append(flag)
        if isinstance(value, bool):
            if value:
                argv[-1] = flag
            else:
                argv.pop()
            continue
        argv.append(str(value))
    return int(analyze_events_main(argv))


def run_task(run_id: str, args_list: list[str]) -> int:
    """Entry point for in-process DAG execution."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--event_type", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--timeframe", default="5m")
    # ... other params ...
    args, unknown = parser.parse_known_args(args_list)

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    params: dict[str, Any] = {}
    idx = 0
    while idx < len(unknown):
        token = str(unknown[idx]).strip()
        if not token.startswith("--"):
            idx += 1
            continue
        key = token[2:].replace("-", "_")
        next_idx = idx + 1
        if next_idx >= len(unknown) or str(unknown[next_idx]).startswith("--"):
            params[key] = True
            idx += 1
            continue
        params[key] = unknown[next_idx]
        idx += 2

    return run_generic_detector_task(
        run_id=run_id,
        event_type=args.event_type,
        symbols=symbols,
        timeframe=args.timeframe,
        params=params,
    )
