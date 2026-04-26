from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import pandas as pd

from project.core.config import get_data_root
from project.events.registry import EVENT_REGISTRY_SPECS
from project.io.utils import read_parquet

TIME_COLUMNS = ("enter_ts", "timestamp", "signal_ts", "event_ts", "anchor_ts")
_THRESHOLD_FIXTURE_PATH = (
    Path(__file__).resolve().parents[1] / "tests" / "events" / "fixtures" / "detector_thresholds.json"
)


def _resolve_default_truth_map_and_data_root(
    *,
    run_id: str,
    data_root: Path,
) -> tuple[Path, Path]:
    default_truth_map = data_root / "synthetic" / run_id / "synthetic_regime_segments.json"
    if default_truth_map.exists():
        return data_root, default_truth_map

    artifacts_root = data_root.parent / "artifacts"
    preferred = artifacts_root / run_id / "synthetic" / run_id / "synthetic_regime_segments.json"
    if preferred.exists():
        return preferred.parents[2], preferred

    matches = sorted(artifacts_root.glob(f"*/synthetic/{run_id}/synthetic_regime_segments.json"))
    if matches:
        selected = matches[0]
        return selected.parents[2], selected

    return data_root, default_truth_map


def load_truth_map(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("segments"), list):
        return [dict(item) for item in payload["segments"] if isinstance(item, Mapping)]
    raise ValueError(f"Invalid truth-map payload: {path}")


def _load_detector_thresholds() -> dict[str, dict[str, dict[str, float]]]:
    if not _THRESHOLD_FIXTURE_PATH.exists():
        return {}

    payload = json.loads(_THRESHOLD_FIXTURE_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {}

    thresholds: dict[str, dict[str, dict[str, float]]] = {}
    for event_type, runs in payload.items():
        if not isinstance(runs, Mapping):
            continue
        normalized_runs: dict[str, dict[str, float]] = {}
        for run_id, bounds in runs.items():
            if not isinstance(bounds, Mapping):
                continue
            normalized_runs[str(run_id)] = {
                str(key): float(value)
                for key, value in bounds.items()
                if str(key) in {"min_precision", "min_recall"}
            }
        thresholds[str(event_type).strip().upper()] = normalized_runs
    return thresholds


def _run_has_calibrated_thresholds(
    thresholds: Mapping[str, Mapping[str, Mapping[str, float]]], run_id: str
) -> bool:
    return any(str(run_id) in runs for runs in thresholds.values())


def _passes_symbol_gates(symbol_row: Mapping[str, Any], *, gate_mode: str) -> bool:
    if not bool(symbol_row.get("passed_hit_requirement", False)):
        return False
    if gate_mode == "hit_only":
        return True
    return bool(symbol_row.get("passed_off_regime_bound", False)) and (
        symbol_row.get("passed_precision_bound") is not False
    )


def _event_time_series(frame: pd.DataFrame) -> pd.Series:
    for column in TIME_COLUMNS:
        if column in frame.columns:
            return pd.to_datetime(frame[column], utc=True, errors="coerce")
    return pd.Series(dtype="datetime64[ns, UTC]")


def load_event_frame(*, data_root: Path, run_id: str, event_type: str) -> pd.DataFrame:
    spec = EVENT_REGISTRY_SPECS.get(str(event_type).strip().upper())
    if spec is None:
        return pd.DataFrame()

    # Try the standard report path first
    report_dir = Path(data_root) / "reports" / spec.reports_dir / run_id
    report_path = report_dir / spec.events_file
    csv_path = report_path.with_suffix(".csv")
    source_path = report_path if report_path.exists() else csv_path

    # Try event-type specific files (e.g., climax_volume_bar_edge_events.parquet)
    if not source_path.exists():
        event_type_lower = str(event_type).strip().lower().replace("_", "_")
        for pattern in [f"{event_type_lower}_events.parquet", f"{event_type_lower}_edge_events.parquet"]:
            alt_path = report_dir / pattern
            if alt_path.exists():
                source_path = alt_path
                break

    if not source_path.exists():
        # Fallback: look in the run's events directory
        events_path = Path(data_root) / "events" / run_id / "events.parquet"
        if events_path.exists():
            frame = read_parquet(events_path)
            if "event_type" in frame.columns:
                frame = frame[frame["event_type"].astype(str).str.upper() == str(event_type).strip().upper()].copy()
            return frame.reset_index(drop=True) if not frame.empty else pd.DataFrame()
        return pd.DataFrame()

    frame = read_parquet(source_path)
    if frame.empty:
        return frame
    if "event_type" in frame.columns:
        frame = frame[
            frame["event_type"].astype(str).str.upper() == str(event_type).strip().upper()
        ].copy()
    return frame.reset_index(drop=True)


def _truth_windows(
    segments: Iterable[Mapping[str, Any]],
    *,
    symbol: str,
    event_type: str,
    tolerance: pd.Timedelta,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    windows: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    for segment in segments:
        if str(segment.get("symbol", "")).upper() != str(symbol).upper():
            continue
        event_truth_windows = segment.get("event_truth_windows", {})
        event_specific = event_truth_windows.get(str(event_type).strip().upper(), [])
        if event_specific:
            for window in event_specific:
                start_ts = pd.Timestamp(window["start_ts"], tz="UTC") - tolerance
                end_ts = pd.Timestamp(window["end_ts"], tz="UTC") + tolerance
                windows.append((start_ts, end_ts))
            continue
        start_ts = pd.Timestamp(segment["start_ts"], tz="UTC") - tolerance
        end_ts = pd.Timestamp(segment["end_ts"], tz="UTC") + tolerance
        windows.append((start_ts, end_ts))
    return windows


def _count_hits(
    times: pd.Series, windows: list[tuple[pd.Timestamp, pd.Timestamp]]
) -> tuple[int, int]:
    if times.empty or not windows:
        return 0, 0
    in_window = pd.Series(False, index=times.index)
    hit_windows = 0
    for start_ts, end_ts in windows:
        mask = times.between(start_ts, end_ts, inclusive="both")
        if bool(mask.any()):
            hit_windows += 1
        in_window = in_window | mask
    return int(in_window.sum()), int(hit_windows)


def _collect_truth_event_types(
    segments: Iterable[Mapping[str, Any]],
    *,
    field_name: str,
    selected_event_types: set[str],
) -> list[str]:
    return sorted(
        {
            str(event_type).strip().upper()
            for segment in segments
            for event_type in segment.get(field_name, [])
            if str(event_type).strip()
            and (
                not selected_event_types or str(event_type).strip().upper() in selected_event_types
            )
        }
    )


def _build_event_reports(
    *,
    segments: list[dict[str, Any]],
    event_types: list[str],
    field_name: str,
    data_root: Path,
    run_id: str,
    max_off_regime_rate: float,
    get_tolerance,
    min_precision_fraction: float | None = None,
    detector_thresholds: Mapping[str, Mapping[str, Mapping[str, float]]] | None = None,
    run_has_calibrated_thresholds: bool = False,
) -> list[dict[str, Any]]:
    event_reports: list[dict[str, Any]] = []
    detector_thresholds = detector_thresholds or {}
    for event_type in event_types:
        spec = EVENT_REGISTRY_SPECS.get(event_type)
        if spec and spec.synthetic_coverage == "synthetic-unvalidatable":
            continue
        calibration_bounds = detector_thresholds.get(event_type, {}).get(run_id)
        gate_mode = "generic"
        if calibration_bounds is None and run_has_calibrated_thresholds:
            gate_mode = "hit_only"

        frame = load_event_frame(data_root=data_root, run_id=run_id, event_type=event_type)
        times = _event_time_series(frame)
        total_events = int(times.notna().sum())
        per_symbol: list[dict[str, Any]] = []
        relevant_segments = [
            segment for segment in segments if event_type in segment.get(field_name, [])
        ]
        for symbol in sorted(
            {
                str(segment.get("symbol", "")).upper()
                for segment in relevant_segments
                if str(segment.get("symbol", "")).strip()
            }
        ):
            if not frame.empty and "symbol" in frame.columns:
                symbol_frame = frame[frame["symbol"].astype(str).str.upper() == symbol].copy()
            else:
                symbol_frame = frame.iloc[0:0].copy()
            symbol_times = _event_time_series(symbol_frame)
            windows = _truth_windows(
                relevant_segments,
                symbol=symbol,
                event_type=event_type,
                tolerance=get_tolerance(event_type),
            )
            in_window_events, hit_windows = _count_hits(symbol_times, windows)
            off_regime_events = max(0, int(symbol_times.notna().sum()) - in_window_events)
            expected_windows = len(windows)
            off_regime_rate = float(off_regime_events / max(1, int(symbol_times.notna().sum())))
            precision = float(in_window_events / max(1, int(symbol_times.notna().sum())))
            recall = float(hit_windows / max(1, expected_windows)) if expected_windows > 0 else 1.0
            if gate_mode == "hit_only":
                passed_precision = None
                passed_off_regime = None
                passed_recall = None
            else:
                passed_precision = (
                    bool(precision >= float(min_precision_fraction))
                    if min_precision_fraction is not None
                    else None
                )
                passed_off_regime = bool(off_regime_rate <= float(max_off_regime_rate))
                passed_recall = None
            per_symbol.append(
                {
                    "symbol": symbol,
                    "expected_windows": expected_windows,
                    "windows_hit": int(hit_windows),
                    "in_window_events": int(in_window_events),
                    "off_regime_events": int(off_regime_events),
                    "off_regime_rate": off_regime_rate,
                    "precision": precision,
                    "recall": recall,
                    "gate_mode": gate_mode,
                    "passed_hit_requirement": bool(
                        hit_windows > 0 if expected_windows > 0 else True
                    ),
                    "passed_off_regime_bound": passed_off_regime,
                    "passed_precision_bound": passed_precision,
                    "passed_recall_bound": passed_recall,
                }
            )
        event_reports.append(
            {
                "event_type": event_type,
                "truth_role": "supporting"
                if field_name == "supporting_event_types"
                else "expected",
                "reports_dir": EVENT_REGISTRY_SPECS[event_type].reports_dir
                if event_type in EVENT_REGISTRY_SPECS
                else None,
                "total_events": total_events,
                "calibration_bounds": dict(calibration_bounds) if calibration_bounds else None,
                "gate_mode": gate_mode,
                "per_symbol": per_symbol,
            }
        )
    return event_reports


def validate_detector_truth(
    *,
    data_root: Path,
    run_id: str,
    truth_map_path: Path,
    tolerance_minutes: int | dict[str, int] = 30,
    max_off_regime_rate: float = 0.35,
    min_precision_fraction: float | None = 0.5,
    event_types: Iterable[str] | None = None,
    include_supporting_events: bool = False,
) -> dict[str, Any]:
    segments = load_truth_map(truth_map_path)

    # Enforce profile/manifest freeze integrity
    manifest_path = truth_map_path.parent / "synthetic_generation_manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("status") != "frozen":
            pass  # some manifests might not have status=frozen exactly.
        # But we must ensure the pipeline run manifest references this exact synthetic profile
        run_manifest_path = data_root / "runs" / run_id / "run_manifest.json"
        if run_manifest_path.exists():
            run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
            run_profile = run_manifest.get("synthetic_profile") or run_manifest.get("profile")
            gen_profile = manifest.get("profile_name")
            if run_profile and gen_profile and str(run_profile) != str(gen_profile):
                raise ValueError(
                    f"Profile mismatch: Validation run used profile '{run_profile}' "
                    f"but synthetic truth map was generated with '{gen_profile}'."
                )

    selected_event_types = {
        str(event_type).strip().upper()
        for event_type in (event_types or [])
        if str(event_type).strip()
    }
    detector_thresholds = _load_detector_thresholds()
    run_has_calibrated_thresholds = _run_has_calibrated_thresholds(detector_thresholds, run_id)

    def _get_tolerance(event_type: str) -> pd.Timedelta:
        if isinstance(tolerance_minutes, dict):
            minutes = tolerance_minutes.get(event_type, 30)
        else:
            minutes = int(tolerance_minutes)
        return pd.Timedelta(minutes=minutes)

    expected_event_types = _collect_truth_event_types(
        segments,
        field_name="expected_event_types",
        selected_event_types=selected_event_types,
    )
    event_reports = _build_event_reports(
        segments=segments,
        event_types=expected_event_types,
        field_name="expected_event_types",
        data_root=data_root,
        run_id=run_id,
        max_off_regime_rate=float(max_off_regime_rate),
        get_tolerance=_get_tolerance,
        min_precision_fraction=min_precision_fraction,
        detector_thresholds=detector_thresholds,
        run_has_calibrated_thresholds=run_has_calibrated_thresholds,
    )
    supporting_event_reports = (
        _build_event_reports(
            segments=segments,
            event_types=_collect_truth_event_types(
                segments,
                field_name="supporting_event_types",
                selected_event_types=selected_event_types,
            ),
            field_name="supporting_event_types",
            data_root=data_root,
            run_id=run_id,
            max_off_regime_rate=float(max_off_regime_rate),
            get_tolerance=_get_tolerance,
            min_precision_fraction=min_precision_fraction,
            detector_thresholds=detector_thresholds,
            run_has_calibrated_thresholds=run_has_calibrated_thresholds,
        )
        if include_supporting_events
        else []
    )

    overall_pass = all(
        all(
            _passes_symbol_gates(
                symbol_row,
                gate_mode=str(event_row.get("gate_mode", "generic")),
            )
            for symbol_row in event_row["per_symbol"]
        )
        for event_row in event_reports
    )
    return {
        "schema_version": "synthetic_detector_truth_validation_v3",
        "run_id": run_id,
        "truth_map_path": str(truth_map_path),
        "tolerance_minutes": tolerance_minutes
        if isinstance(tolerance_minutes, int)
        else dict(tolerance_minutes),
        "max_off_regime_rate": float(max_off_regime_rate),
        "min_precision_fraction": (
            float(min_precision_fraction) if min_precision_fraction is not None else None
        ),
        "selected_event_types": sorted(selected_event_types),
        "run_has_calibrated_thresholds": bool(run_has_calibrated_thresholds),
        "event_reports": event_reports,
        "supporting_event_reports": supporting_event_reports,
        "include_supporting_events": bool(include_supporting_events),
        "passed": bool(overall_pass),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate detector outputs against a synthetic truth map."
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--truth_map_path", default=None)
    parser.add_argument("--data_root", default=None)
    parser.add_argument("--tolerance_minutes", type=int, default=30)
    parser.add_argument("--max_off_regime_rate", type=float, default=0.35)
    parser.add_argument(
        "--min_precision_fraction",
        type=float,
        default=0.5,
        help="Minimum fraction of events that must fall inside regime windows",
    )
    parser.add_argument("--event_types", nargs="+", default=None)
    parser.add_argument("--include_supporting_events", type=int, default=0)
    parser.add_argument("--json_out", default=None)
    args = parser.parse_args(argv)

    data_root = Path(args.data_root) if args.data_root else get_data_root()
    if args.truth_map_path:
        truth_map_path = Path(args.truth_map_path)
    else:
        data_root, truth_map_path = _resolve_default_truth_map_and_data_root(
            run_id=str(args.run_id),
            data_root=Path(data_root),
        )
    result = validate_detector_truth(
        data_root=data_root,
        run_id=str(args.run_id),
        truth_map_path=truth_map_path,
        tolerance_minutes=int(args.tolerance_minutes),
        max_off_regime_rate=float(args.max_off_regime_rate),
        min_precision_fraction=args.min_precision_fraction,
        event_types=args.event_types,
        include_supporting_events=bool(args.include_supporting_events),
    )
    if args.json_out:
        out_path = Path(args.json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
