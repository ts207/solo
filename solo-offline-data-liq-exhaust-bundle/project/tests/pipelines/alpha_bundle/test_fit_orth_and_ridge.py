from __future__ import annotations

import json
import sys

import numpy as np
import pandas as pd

from project.pipelines.alpha_bundle import fit_orth_and_ridge as fit_script


def _mock_training_frames(n_rows: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    ts = pd.date_range("2024-01-01", periods=n_rows, freq="5min", tz="UTC")
    signal = np.linspace(-1.0, 1.0, n_rows)
    signals = pd.DataFrame(
        {
            "ts_event": ts,
            "symbol": ["BTCUSDT"] * n_rows,
            "signal_a": signal,
        }
    )
    labels = pd.DataFrame(
        {
            "ts_event": ts,
            "symbol": ["BTCUSDT"] * n_rows,
            "y": 0.25 * signal + 0.01,
        }
    )
    return signals, labels


def test_fit_orth_and_ridge_lambda_grid_path_succeeds(monkeypatch, tmp_path):
    signals, labels = _mock_training_frames(360)
    payloads = {
        "signals.parquet": signals,
        "labels.parquet": labels,
    }

    monkeypatch.setattr(fit_script, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(
        fit_script,
        "read_parquet",
        lambda paths: payloads[paths[0].name].copy(),
    )
    monkeypatch.setattr(
        fit_script, "start_manifest", lambda *args, **kwargs: {"stage": "alpha_fit_orth_ridge"}
    )
    monkeypatch.setattr(fit_script, "finalize_manifest", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "fit_orth_and_ridge.py",
            "--run_id",
            "ridge_cv",
            "--signals_path",
            "signals.parquet",
            "--label_path",
            "labels.parquet",
            "--signal_cols",
            "signal_a",
            "--lambda_grid",
            "0.01,0.1",
            "--cv_blocks",
            "4",
        ],
    )

    rc = fit_script.main()

    assert rc == 0
    out_path = next((tmp_path / "model_registry").glob("CombModelRidge_*.json"))
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["cv_stats"]["selection_reason"] == "grid_search"
    assert payload["cv_stats"]["splits"] == 2
    assert payload["lambda"] in {0.01, 0.1}


def test_fit_orth_and_ridge_lambda_grid_falls_back_when_no_valid_splits(monkeypatch, tmp_path):
    signals, labels = _mock_training_frames(140)
    payloads = {
        "signals.parquet": signals,
        "labels.parquet": labels,
    }

    monkeypatch.setattr(fit_script, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(
        fit_script,
        "read_parquet",
        lambda paths: payloads[paths[0].name].copy(),
    )
    monkeypatch.setattr(
        fit_script, "start_manifest", lambda *args, **kwargs: {"stage": "alpha_fit_orth_ridge"}
    )
    monkeypatch.setattr(fit_script, "finalize_manifest", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "fit_orth_and_ridge.py",
            "--run_id",
            "ridge_cv_small",
            "--signals_path",
            "signals.parquet",
            "--label_path",
            "labels.parquet",
            "--signal_cols",
            "signal_a",
            "--lambda_",
            "0.25",
            "--lambda_grid",
            "0.01,0.1",
            "--cv_blocks",
            "4",
        ],
    )

    rc = fit_script.main()

    assert rc == 0
    out_path = next((tmp_path / "model_registry").glob("CombModelRidge_*.json"))
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["cv_stats"]["selection_reason"] == "fallback_no_valid_splits"
    assert payload["cv_stats"]["splits"] == 0
    assert payload["lambda"] == 0.25
