from __future__ import annotations

import logging
from types import SimpleNamespace

import pandas as pd
import pytest

import project.research.phase2 as svc


def test_prepare_events_dataframe_fails_closed_on_invalid_split_order(monkeypatch, tmp_path):
    monkeypatch.setattr(
        svc,
        "load_registry_episode_anchors",
        lambda **_kwargs: pd.DataFrame(
            {
                "symbol": ["BTCUSDT", "BTCUSDT"],
                "enter_ts": pd.to_datetime(
                    ["2026-01-01T00:10:00Z", "2026-01-01T00:05:00Z"],
                    utc=True,
                ),
                "split_label": ["train", "validation"],
            }
        ),
    )

    with pytest.raises(ValueError, match="Holdout integrity sentinel failed"):
        svc.prepare_events_dataframe(
            data_root=tmp_path,
            run_id="run_holdout",
            event_type="VOL_SHOCK",
            symbols=["BTCUSDT"],
            event_registry_specs={
                "VOL_SHOCK": SimpleNamespace(reports_dir="vol", events_file="events.csv")
            },
            horizons=["5m"],
            entry_lag_bars=1,
            fam_config={},
            logger=logging.getLogger("holdout-test"),
        )


def test_prepare_events_dataframe_resplits_when_validation_or_test_missing(monkeypatch, tmp_path):
    ts = pd.date_range("2026-01-01", periods=30, freq="12h", tz="UTC")
    monkeypatch.setattr(
        svc,
        "load_registry_episode_anchors",
        lambda **_kwargs: pd.DataFrame(
            {
                "symbol": ["BTCUSDT"] * len(ts),
                "enter_ts": ts,
                # Degenerate incoming labels should trigger safeguard + deterministic resplit.
                "split_label": ["train"] * len(ts),
            }
        ),
    )

    out = svc.prepare_events_dataframe(
        data_root=tmp_path,
        run_id="run_holdout",
        event_type="VOL_SHOCK",
        symbols=["BTCUSDT"],
        event_registry_specs={
            "VOL_SHOCK": SimpleNamespace(reports_dir="vol", events_file="events.csv")
        },
        horizons=["15m"],
        entry_lag_bars=1,
        fam_config={},
        logger=logging.getLogger("holdout-test"),
    )

    assert not out.empty
    counts = out["split_label"].astype(str).value_counts().to_dict()
    assert int(counts.get("validation", 0)) > 0
    assert int(counts.get("test", 0)) > 0


def test_prepare_events_dataframe_fail_closes_when_resplit_still_has_no_oos(monkeypatch, tmp_path):
    ts = pd.date_range("2026-01-01", periods=4, freq="5min", tz="UTC")
    monkeypatch.setattr(
        svc,
        "load_registry_episode_anchors",
        lambda **_kwargs: pd.DataFrame(
            {
                "symbol": ["BTCUSDT"] * len(ts),
                "enter_ts": ts,
                # This forces safeguard and then resplit; <5 rows keeps all-train.
                "split_label": ["train"] * len(ts),
            }
        ),
    )

    out = svc.prepare_events_dataframe(
        data_root=tmp_path,
        run_id="run_holdout",
        event_type="VOL_SHOCK",
        symbols=["BTCUSDT"],
        event_registry_specs={
            "VOL_SHOCK": SimpleNamespace(reports_dir="vol", events_file="events.csv")
        },
        horizons=["5m"],
        entry_lag_bars=1,
        fam_config={},
        logger=logging.getLogger("holdout-test"),
    )

    assert out.empty
