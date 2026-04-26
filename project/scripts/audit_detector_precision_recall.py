"""
CLI audit script: measure precision/recall for all registered event detectors
across all synthetic run_ids.

Usage:
  python -m project.scripts.audit_detector_precision_recall
  python -m project.scripts.audit_detector_precision_recall --run_id synthetic_2021_bull
  python -m project.scripts.audit_detector_precision_recall --event_type VOL_SPIKE
  python -m project.scripts.audit_detector_precision_recall --out_dir /tmp/my_audit
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

from project.core.config import get_data_root
from project.events.detectors.registry import (
    get_detector,
    list_registered_event_types,
    load_all_detectors,
)
from project.scripts.detector_audit_module import (
    KNOWN_RUN_IDS,
    SYNTHETIC_LIVE_ONLY_EVENT_TYPES,
    build_symbol_df,
    load_manifest,
    load_truth_segments,
    measure_detector,
)


def _select_event_types(
    all_event_types: list[str],
    *,
    requested_event_type: str | None,
    include_live_only_synthetic: bool,
) -> tuple[list[str], list[str]]:
    if requested_event_type:
        selected = [et for et in all_event_types if et == requested_event_type.upper()]
        return selected, []
    skipped = []
    selected = []
    for event_type in all_event_types:
        if not include_live_only_synthetic and event_type in SYNTHETIC_LIVE_ONLY_EVENT_TYPES:
            skipped.append(event_type)
            continue
        selected.append(event_type)
    return selected, skipped


def _print_table(all_metrics: list, *, skipped_event_types: list[str] | None = None) -> None:
    """Print a human-readable classification table to stdout."""
    by_class: dict = defaultdict(list)
    for m in all_metrics:
        by_class[m["classification"]].append(m)

    order = ["broken", "noisy", "silent", "error", "stable", "uncovered"]
    header = f"{'EVENT_TYPE':<40} {'SYMBOL':<10} {'RUN_ID':<35} {'CLASS':<10} {'PREC':>6} {'REC':>6} {'EVENTS':>7} {'RATE/1K':>8}"
    print("\n" + "=" * len(header))
    print(header)
    print("=" * len(header))

    for cls in order:
        rows = by_class.get(cls, [])
        if not rows:
            continue
        print(f"\n--- {cls.upper()} ({len(rows)}) ---")
        for m in sorted(rows, key=lambda x: x["event_type"]):
            prec = f"{m['precision']:.3f}" if m["classification"] != "error" else "  err"
            rec_val = m.get("recall")
            rec = f"{rec_val:.3f}" if rec_val is not None else "  N/A"
            print(
                f"{m['event_type']:<40} {m['symbol']:<10} {m['run_id']:<35} "
                f"{m['classification']:<10} {prec:>6} {rec:>6} "
                f"{m['total_events']:>7} {m['event_rate_per_1k']:>8.1f}"
            )

    print("\n" + "=" * len(header))
    total = len(all_metrics)
    stable = len(by_class.get("stable", []))
    broken = (
        len(by_class.get("broken", []))
        + len(by_class.get("noisy", []))
        + len(by_class.get("silent", []))
    )
    print(
        f"TOTAL: {total}  STABLE: {stable}  NEED WORK: {broken}  ERROR: {len(by_class.get('error', []))}  UNCOVERED: {len(by_class.get('uncovered', []))}"
    )
    if skipped_event_types:
        print(f"SKIPPED LIVE-ONLY SYNTHETIC EVENTS: {', '.join(sorted(skipped_event_types))}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Audit detector precision/recall across synthetic datasets."
    )
    parser.add_argument(
        "--run_id", default=None, help="Run a single run_id (e.g. synthetic_2021_bull)"
    )
    parser.add_argument("--event_type", default=None, help="Audit only this event type")
    parser.add_argument(
        "--include_live_only_synthetic",
        type=int,
        default=0,
        help="Include detectors that are currently treated as live-data diagnostics rather than synthetic calibration targets.",
    )
    parser.add_argument("--out_dir", default=None, help="Output directory for JSON report")
    args = parser.parse_args(argv)

    data_root = get_data_root()

    load_all_detectors()
    all_event_types = list_registered_event_types()
    skipped_event_types: list[str] = []
    all_event_types, skipped_event_types = _select_event_types(
        all_event_types,
        requested_event_type=args.event_type,
        include_live_only_synthetic=bool(args.include_live_only_synthetic),
    )
    if args.event_type and not all_event_types:
        print(f"ERROR: event_type {args.event_type!r} not registered.")
        return 1

    run_ids = [args.run_id] if args.run_id else sorted(KNOWN_RUN_IDS)
    # Validate requested run_id exists on disk
    for run_id in run_ids:
        manifest_path = data_root / "synthetic" / run_id / "synthetic_generation_manifest.json"
        if not manifest_path.exists():
            print(f"ERROR: manifest not found for run_id {run_id!r}: {manifest_path}")
            return 1

    all_metrics = []

    for run_id in run_ids:
        print(f"\nAuditing {run_id} ...")
        manifest = load_manifest(data_root, run_id)
        segments = load_truth_segments(data_root, run_id)

        for symbol_entry in manifest["symbols"]:
            symbol = symbol_entry["symbol"]
            print(f"  Building DataFrame for {symbol} ...")
            df = build_symbol_df(symbol_entry)

            for event_type in all_event_types:
                detector = get_detector(event_type)
                if detector is None:
                    continue
                metrics = measure_detector(detector, df, symbol, segments, run_id)
                all_metrics.append(metrics.to_dict())

    # --- Output ---
    _print_table(all_metrics, skipped_event_types=skipped_event_types)

    timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    if args.out_dir:
        out_dir = Path(args.out_dir)
    else:
        out_dir = data_root / "artifacts" / "detector_audit" / timestamp

    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "metrics.json"
    report_path.write_text(json.dumps(all_metrics, indent=2), encoding="utf-8")
    print(f"\nReport saved to: {report_path}")

    # Non-zero exit if any detector is broken/noisy/silent
    needs_work = [
        m for m in all_metrics if m["classification"] in ("broken", "noisy", "silent", "error")
    ]
    return 1 if needs_work else 0


if __name__ == "__main__":
    raise SystemExit(main())
