from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pandas as pd

import project.research.phase2 as phase2


def test_prepare_events_dataframe_attaches_diagnostics(monkeypatch, tmp_path):
    ts = pd.date_range("2026-01-01", periods=30, freq="12h", tz="UTC")
    monkeypatch.setattr(
        phase2,
        "load_registry_episode_anchors",
        lambda **_kwargs: pd.DataFrame(
            {
                "symbol": ["BTCUSDT"] * len(ts),
                "enter_ts": ts,
                "split_label": ["train"] * len(ts),
            }
        ),
    )

    out = phase2.prepare_events_dataframe(
        data_root=tmp_path,
        run_id="diag_run",
        event_type="VOL_SHOCK",
        symbols=["BTCUSDT"],
        event_registry_specs={
            "VOL_SHOCK": SimpleNamespace(reports_dir="vol", events_file="events.parquet")
        },
        horizons=["15m"],
        entry_lag_bars=1,
        fam_config={},
        logger=logging.getLogger("phase2-diag"),
    )

    diag = out.attrs.get("phase2_prepare_diagnostics", {})
    assert diag["run_id"] == "diag_run"
    assert diag["event_type"] == "VOL_SHOCK"
    assert diag["raw_event_count"] == len(ts)
    assert diag["canonical_episode_count"] == len(out)
    assert diag["resplit_attempted"] is True
    assert diag["returned_empty_due_to_holdout"] is False
    assert diag["split_counts"]["validation"] > 0
    assert diag["split_counts"]["test"] > 0
