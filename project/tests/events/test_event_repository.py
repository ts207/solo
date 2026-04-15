from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from project.events.event_repository import _read_phase1_events, merge_registry_events
from project.events.event_specs import EventRegistrySpec


def test_read_phase1_events_logs_read_failures(monkeypatch, tmp_path, caplog):
    spec = EventRegistrySpec(
        event_type="TEST_EVENT",
        reports_dir="reports_dir",
        events_file="events.parquet",
        signal_column="test_event",
    )
    path = tmp_path / "reports" / spec.reports_dir / "run_1"
    path.mkdir(parents=True, exist_ok=True)
    file_path = path / spec.events_file
    file_path.write_text("broken", encoding="utf-8")

    def _boom(_path: Path):
        raise RuntimeError("parquet read exploded")

    monkeypatch.setattr(pd, "read_parquet", _boom)

    with caplog.at_level(logging.WARNING):
        out = _read_phase1_events(tmp_path, "run_1", spec)

    assert out.empty
    assert "Failed to read phase1 events" in caplog.text


def test_merge_registry_events_handles_all_na_columns_without_warning(caplog):
    existing = pd.DataFrame(
        [
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "symbol": "BTCUSDT",
                "event_type": "A",
                "event_id": "a1",
                "unused": pd.NA,
            }
        ]
    )
    incoming = pd.DataFrame(
        [
            {
                "timestamp": "2026-01-01T00:05:00Z",
                "symbol": "BTCUSDT",
                "event_type": "B",
                "event_id": "b1",
                "unused": pd.NA,
            }
        ]
    )

    with caplog.at_level(logging.WARNING):
        out = merge_registry_events(existing=existing, incoming=incoming, selected_event_types=None)

    assert len(out) == 1
    assert caplog.records == []
