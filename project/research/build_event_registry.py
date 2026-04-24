from __future__ import annotations

import argparse
import json
import logging
import sys
import traceback
from pathlib import Path
from typing import Dict, List

import pandas as pd

from project.core.config import get_data_root
from project.core.logging_utils import build_stage_log_handlers
from project.events.arbitration import arbitrate_events
from project.events.registry import (
    EVENT_REGISTRY_SPECS,
    assert_event_specs_available,
    build_event_flags,
    collect_registry_events,
    load_registry_events,
    load_registry_flags,
    merge_event_flags_for_selected_event_types,
    merge_registry_events,
    registry_contract_check,
    write_event_registry_artifacts,
)
from project.events.scoring import score_event_frame
from project.schemas.data_contracts import EventRegistrySchema
from project.specs.manifest import finalize_manifest, start_manifest


def _parse_symbols(symbols_csv: str) -> List[str]:
    symbols = [s.strip().upper() for s in str(symbols_csv).split(",") if s.strip()]
    return list(dict.fromkeys(symbols))


def main() -> int:
    DATA_ROOT = get_data_root()
    parser = argparse.ArgumentParser(
        description="Build canonical event registry artifacts from phase1 outputs"
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument(
        "--event_type", default="all", choices=["all", *sorted(EVENT_REGISTRY_SPECS.keys())]
    )
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    # Configure logging
    log_handlers = build_stage_log_handlers(args.log_path)
    logging.basicConfig(
        level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s"
    )

    try:
        assert_event_specs_available()

        symbols = _parse_symbols(args.symbols)
        selected_event_types = (
            sorted(EVENT_REGISTRY_SPECS.keys())
            if args.event_type == "all"
            else [str(args.event_type)]
        )

        params = {
            "run_id": args.run_id,
            "symbols": symbols,
            "event_type": args.event_type,
            "timeframe": str(args.timeframe),
        }
        inputs: List[Dict[str, object]] = []
        outputs: List[Dict[str, object]] = []
        manifest = start_manifest("build_event_registry", args.run_id, params, inputs, outputs)

        for event_type in selected_event_types:
            spec = EVENT_REGISTRY_SPECS[event_type]
            # Try both relative paths
            src = DATA_ROOT / "reports" / spec.reports_dir / args.run_id / spec.events_file
            if not src.exists():
                src = (
                    DATA_ROOT
                    / "data"
                    / "reports"
                    / spec.reports_dir
                    / args.run_id
                    / spec.events_file
                )

            logging.info(f"Adding input source for {event_type}: {src} (exists={src.exists()})")
            inputs.append({"path": str(src), "rows": None, "start_ts": None, "end_ts": None})

        logging.info(f"Collecting events for: {selected_event_types}")
        incoming_events = collect_registry_events(
            data_root=DATA_ROOT,
            run_id=args.run_id,
            event_types=selected_event_types,
        )
        logging.info(f"Collected {len(incoming_events)} incoming events")
        if not incoming_events.empty:
            logging.info(f"Incoming event types: {incoming_events['event_type'].unique()}")

        logging.info(f"Loading existing events for run {args.run_id}")
        existing_events = load_registry_events(data_root=DATA_ROOT, run_id=args.run_id)
        logging.info(f"Loaded {len(existing_events)} existing events")

        logging.info("Merging events")
        events = merge_registry_events(
            existing=existing_events,
            incoming=incoming_events,
            selected_event_types=selected_event_types,
        )
        logging.info(f"Merged total events: {len(events)}")

        if not events.empty:
            logging.info("Scoring event quality")
            events = score_event_frame(events)

            logging.info("Applying event arbitration")
            arb_result = arbitrate_events(events)
            events = arb_result.events
            if not arb_result.composite_events.empty:
                logging.info(f"Adding {len(arb_result.composite_events)} composite events")
                from project.events.event_normalizer import normalize_registry_events_frame

                # Propagate run_id to composite events if missing
                if "run_id" in events.columns and not events["run_id"].empty:
                    run_id = str(events["run_id"].iloc[0])
                    arb_result.composite_events["run_id"] = run_id

                events = pd.concat([events, arb_result.composite_events], ignore_index=True)
                # Re-normalize to ensure all columns are present and typed correctly
                events = normalize_registry_events_frame(events)
            if not arb_result.suppressed.empty:
                logging.info(f"Suppressed {len(arb_result.suppressed)} events via arbitration")

        if args.event_type == "all":
            logging.info("Building event flags for all event types")
            flags = build_event_flags(
                events=events,
                symbols=symbols,
                data_root=DATA_ROOT,
                run_id=args.run_id,
                timeframe=str(args.timeframe),
            )
        else:
            logging.info(f"Building event flags for selected types: {selected_event_types}")
            selected_events = events[
                events["event_type"].astype(str).isin(selected_event_types)
            ].copy()
            selected_flags = build_event_flags(
                events=selected_events,
                symbols=symbols,
                data_root=DATA_ROOT,
                run_id=args.run_id,
                timeframe=str(args.timeframe),
            )
            logging.info("Loading existing registry flags")
            existing_flags = load_registry_flags(data_root=DATA_ROOT, run_id=args.run_id)
            logging.info("Merging event flags")
            flags = merge_event_flags_for_selected_event_types(
                existing_flags=existing_flags,
                recomputed_flags=selected_flags,
                selected_event_types=selected_event_types,
            )

        logging.info("Running registry contract checks")
        for symbol in symbols:
            registry_contract_check(events, flags, symbol)

        if not events.empty:
            logging.info("Converting timestamps to int64")
            # Ensure timestamp itself is int64
            events["timestamp"] = (
                pd.to_datetime(events["timestamp"], utc=True, errors="coerce").astype("int64")
                // 10**6
            )
            for _ts_col in (
                "phenom_enter_ts",
                "eval_bar_ts",
                "enter_ts",
                "detected_ts",
                "signal_ts",
                "exit_ts",
            ):
                if _ts_col not in events.columns or events[_ts_col].isna().all():
                    events[_ts_col] = events["timestamp"]
                else:
                    ts_ser = pd.to_datetime(events[_ts_col], utc=True, errors="coerce")
                    # Fallback to main timestamp where values are NaT
                    ts_ser = ts_ser.fillna(pd.to_datetime(events["timestamp"], unit="ms", utc=True))
                    events[_ts_col] = ts_ser.astype("int64") // 10**6

            logging.info("Validating events against EventRegistrySchema")
            EventRegistrySchema.validate(events)

        logging.info("Writing event registry artifacts")
        paths = write_event_registry_artifacts(
            data_root=DATA_ROOT,
            run_id=args.run_id,
            events=events,
            event_flags=flags,
        )

        per_family_counts: Dict[str, int] = {
            event_type: 0 for event_type in sorted(EVENT_REGISTRY_SPECS.keys())
        }
        if not events.empty:
            for event_type, count in (
                events.groupby("event_type", sort=True).size().to_dict().items()
            ):
                per_family_counts[str(event_type)] = int(count)

        incoming_per_family_counts: Dict[str, int] = {
            event_type: 0 for event_type in selected_event_types
        }
        if not incoming_events.empty:
            for event_type, count in (
                incoming_events.groupby("event_type", sort=True).size().to_dict().items()
            ):
                incoming_per_family_counts[str(event_type)] = int(count)

        summary = {
            "run_id": args.run_id,
            "selected_event_types": selected_event_types,
            "incoming_event_rows": int(len(incoming_events)),
            "event_rows": int(len(events)),
            "event_flag_rows": int(len(flags)),
            "incoming_per_family_counts": incoming_per_family_counts,
            "per_family_counts": per_family_counts,
            **paths,
        }
        summary_path = Path(paths["registry_root"]) / "registry_manifest.json"
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

        outputs.append(
            {
                "path": str(paths["events_path"]),
                "rows": int(len(events)),
                "start_ts": None,
                "end_ts": None,
            }
        )
        outputs.append(
            {
                "path": str(paths["event_flags_path"]),
                "rows": int(len(flags)),
                "start_ts": None,
                "end_ts": None,
            }
        )
        outputs.append({"path": str(summary_path), "rows": 1, "start_ts": None, "end_ts": None})

        finalize_manifest(
            manifest,
            "success",
            stats={
                "incoming_event_rows": int(len(incoming_events)),
                "event_rows": int(len(events)),
                "event_flag_rows": int(len(flags)),
                "selected_event_family_count": int(len(selected_event_types)),
                "event_family_count": int(
                    sum(1 for value in per_family_counts.values() if int(value) > 0)
                ),
                "per_family_counts": per_family_counts,
            },
        )
        logging.info("Build event registry completed successfully")
        return 0
    except Exception as exc:
        logging.error(f"Build event registry failed: {exc}")
        logging.error(traceback.format_exc())
        if "manifest" in locals():
            finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
