from __future__ import annotations

import argparse
from pathlib import Path

from project import PROJECT_ROOT
from project.scripts.run_golden_synthetic_discovery import run_golden_synthetic_discovery


def _default_config_path() -> Path:
    return PROJECT_ROOT / "configs" / "golden_synthetic_discovery_fast.yaml"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the fast synthetic detector and pipeline certification workflow."
    )
    parser.add_argument("--root", default=None, help="Output root for generated artifacts.")
    parser.add_argument(
        "--config",
        default=str(_default_config_path()),
        help="Fast synthetic certification config YAML path.",
    )
    parser.add_argument("--run_id", default=None)
    parser.add_argument("--symbols", default=None)
    parser.add_argument("--start_date", default=None)
    parser.add_argument("--end_date", default=None)
    parser.add_argument("--search_spec", default=None)
    parser.add_argument("--search_min_n", type=int, default=None)
    parser.add_argument("--search_budget", type=int, default=None)
    parser.add_argument("--events", nargs="+", default=None)
    parser.add_argument("--templates", nargs="+", default=None)
    parser.add_argument("--horizons", nargs="+", default=None)
    parser.add_argument("--directions", nargs="+", default=None)
    parser.add_argument("--contexts", nargs="+", default=None)
    parser.add_argument("--entry_lags", nargs="+", type=int, default=None)
    args = parser.parse_args(argv)

    root = (
        Path(args.root)
        if args.root
        else (PROJECT_ROOT.parent / "artifacts" / "golden_synthetic_discovery_fast")
    )
    overrides = {
        "run_id": args.run_id,
        "symbols": args.symbols,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "search_spec": args.search_spec,
        "search_min_n": args.search_min_n,
        "search_budget": args.search_budget,
        "events": args.events,
        "templates": args.templates,
        "horizons": args.horizons,
        "directions": args.directions,
        "contexts": args.contexts,
        "entry_lags": args.entry_lags,
    }
    run_golden_synthetic_discovery(
        root=root,
        config_path=Path(args.config),
        overrides=overrides,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
