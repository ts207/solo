# project/tests/test_run_event_quality_analysis.py
import pandas as pd
import numpy as np
import json
import pytest
from pathlib import Path
from project.research.run_event_quality_analysis import run_event_quality_analysis


def _make_features(n_bars: int = 400) -> pd.DataFrame:
    np.random.seed(1)
    dates = pd.date_range("2023-01-01", periods=n_bars, freq="5min")
    df = pd.DataFrame(
        {
            "timestamp": dates,
            "close": 100.0 + np.cumsum(np.random.normal(0, 0.05, n_bars)),
        }
    )
    df["event_vol_spike"] = [i % 15 == 0 for i in range(n_bars)]
    df["event_band_break"] = [i % 15 == 0 for i in range(n_bars)]  # co-fires with vol_spike
    df["event_rare"] = [i % 200 == 0 for i in range(n_bars)]  # below min_n
    return df


def test_run_event_quality_analysis_creates_files(tmp_path):
    df = _make_features()
    output_dir = tmp_path / "event_quality"
    result = run_event_quality_analysis(df, output_dir=output_dir, min_n=10)
    assert (output_dir / "firing_rates.csv").exists()
    assert (output_dir / "cooccurrence.csv").exists()
    assert (output_dir / "information_gain.csv").exists()
    assert (output_dir / "event_return_lead_lag.csv").exists()
    assert (output_dir / "event_event_lead_lag.csv").exists()
    assert (output_dir / "summary.json").exists()


def test_run_event_quality_analysis_summary_structure(tmp_path):
    df = _make_features()
    output_dir = tmp_path / "event_quality"
    run_event_quality_analysis(df, output_dir=output_dir, min_n=10)
    with open(output_dir / "summary.json") as f:
        summary = json.load(f)
    assert "below_min_n_events" in summary
    assert "top_redundancy_pairs" in summary
    assert "top_ig_events" in summary
    assert "bottom_ig_events" in summary


def test_run_event_quality_analysis_detects_sparse_event(tmp_path):
    df = _make_features()
    output_dir = tmp_path / "event_quality"
    run_event_quality_analysis(df, output_dir=output_dir, min_n=10)
    with open(output_dir / "summary.json") as f:
        summary = json.load(f)
    sparse = [e["event_id"] for e in summary["below_min_n_events"]]
    assert "rare" in sparse
