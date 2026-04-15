"""
Per-detector precision/recall regression tests.

Tests are parameterized from project/tests/events/fixtures/detector_thresholds.json.
The fixture is seeded after the audit (plan Task 5).

If the fixture file is empty or an entry is missing, the test is SKIPPED (not
failed), allowing incremental population during the fix phase.

Mark: pytest.mark.slow — run with `pytest -m slow` or include in full suite.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Dict

import pytest

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "detector_thresholds.json"


def _load_thresholds() -> Dict[str, Any]:
    if not FIXTURE_PATH.exists():
        return {}
    raw = FIXTURE_PATH.read_text(encoding="utf-8").strip()
    if not raw or raw == "{}":
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def _test_params():
    thresholds = _load_thresholds()
    params = []
    for event_type, run_map in thresholds.items():
        for run_id, bounds in run_map.items():
            params.append(pytest.param(event_type, run_id, bounds, id=f"{event_type}/{run_id}"))
    return params


@pytest.mark.slow
@pytest.mark.parametrize("event_type,run_id,bounds", _test_params())
def test_detector_precision_recall(event_type: str, run_id: str, bounds: Dict[str, float]) -> None:
    """
    Assert that a detector meets minimum precision and recall on a specific run_id.

    Averages metrics across all symbols in the run. Skips if the detector is
    uncovered (no truth windows) for all symbols.
    """
    from project.core.config import get_data_root
    from project.events.detectors.registry import get_detector, load_all_detectors
    from project.scripts.detector_audit_module import (
        build_symbol_df,
        load_manifest,
        load_truth_segments,
        measure_detector,
    )

    load_all_detectors()
    detector = get_detector(event_type)
    if detector is None:
        pytest.skip(f"Detector {event_type!r} not registered — fixture may be stale")

    data_root = get_data_root()
    manifest_path = data_root / "synthetic" / run_id / "synthetic_generation_manifest.json"
    if not manifest_path.exists():
        pytest.skip(f"Synthetic run {run_id!r} not found at {manifest_path}")

    manifest = load_manifest(data_root, run_id)
    segments = load_truth_segments(data_root, run_id)

    precision_vals = []
    recall_vals = []
    error_msgs = []

    for symbol_entry in manifest["symbols"]:
        df = build_symbol_df(symbol_entry)
        metrics = measure_detector(detector, df, symbol_entry["symbol"], segments, run_id)

        if metrics.classification == "error":
            error_msgs.append(f"{symbol_entry['symbol']}: {metrics.error}")
            continue
        if metrics.classification == "uncovered":
            continue

        precision_vals.append(metrics.precision)
        if not math.isnan(metrics.recall):
            recall_vals.append(metrics.recall)

    if error_msgs:
        pytest.fail(f"{event_type}/{run_id} detection errors:\n" + "\n".join(error_msgs))

    if not precision_vals:
        pytest.skip(f"{event_type}/{run_id}: all symbols uncovered — no truth windows")

    avg_precision = sum(precision_vals) / len(precision_vals)
    avg_recall = sum(recall_vals) / len(recall_vals) if recall_vals else float("nan")

    min_precision = float(bounds.get("min_precision", 0.50))
    min_recall = float(bounds.get("min_recall", 0.30))

    assert avg_precision >= min_precision, (
        f"{event_type}/{run_id}: avg precision {avg_precision:.3f} < required {min_precision:.3f}"
    )
    if not math.isnan(avg_recall):
        assert avg_recall >= min_recall, (
            f"{event_type}/{run_id}: avg recall {avg_recall:.3f} < required {min_recall:.3f}"
        )
