from __future__ import annotations

import builtins
import importlib
import sys

import numpy as np
import pandas as pd
import pytest


def test_scoring_import_succeeds_without_sklearn(monkeypatch):
    original_import = builtins.__import__

    def _guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name.startswith("sklearn"):
            raise ModuleNotFoundError("No module named 'sklearn'")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)
    sys.modules.pop("project.events.scoring", None)
    sys.modules.pop("project.events.scoring.confidence", None)

    scoring = importlib.import_module("project.events.scoring")

    assert callable(scoring.score_event_frame)
    assert "event_tradeability_score" in scoring.EventScoreColumns


def test_confidence_training_raises_clear_dependency_error_without_sklearn(monkeypatch):
    original_import = builtins.__import__

    def _guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name.startswith("sklearn"):
            raise ModuleNotFoundError("No module named 'sklearn'")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)
    sys.modules.pop("project.events.scoring", None)
    sys.modules.pop("project.events.scoring.confidence", None)

    scoring = importlib.import_module("project.events.scoring")
    frame = pd.DataFrame(
        {
            "target": [0, 1],
            "timestamp": pd.date_range("2024-01-01", periods=2, freq="5min", tz="UTC"),
        }
    )

    with pytest.raises(ModuleNotFoundError, match="scikit-learn is required"):
        scoring.train_event_confidence_model(
            frame,
            candidate_feature_columns=["timestamp"],
            min_train_rows=1,
        )


def test_confidence_training_populates_train_and_test_summaries(monkeypatch):
    confidence = importlib.import_module("project.events.scoring.confidence")

    class DummyPipeline:
        def fit(self, frame, y):
            return self

        def predict_proba(self, frame):
            probs = np.linspace(0.2, 0.8, num=len(frame), dtype=float)
            return np.column_stack([1.0 - probs, probs])

    class DummyCalibratedClassifierCV:
        def __init__(self, *, estimator, method, cv):
            self.estimator = estimator

        def fit(self, frame, y):
            return self

        def predict_proba(self, frame):
            return self.estimator.predict_proba(frame)

    monkeypatch.setattr(
        confidence,
        "_load_sklearn_objects",
        lambda: {"CalibratedClassifierCV": DummyCalibratedClassifierCV},
    )
    monkeypatch.setattr(confidence, "_build_pipeline", lambda **kwargs: DummyPipeline())
    monkeypatch.setattr(
        confidence,
        "_metrics",
        lambda y_true, y_prob: {"n": float(len(list(y_true))), "mean_prob": float(np.mean(list(y_prob)))},
    )

    frame = pd.DataFrame(
        {
            "target": [0, 1, 1],
            "split_label": ["train", "train", "test"],
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="5min", tz="UTC"),
        }
    )

    model = confidence.train_event_confidence_model(
        frame,
        candidate_feature_columns=["timestamp"],
        min_train_rows=1,
    )

    assert model.training_summary["train"]["n"] == 2.0
    assert model.training_summary["test"]["n"] == 1.0
