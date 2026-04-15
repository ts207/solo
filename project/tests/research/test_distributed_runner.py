from __future__ import annotations

import pandas as pd

from project.core.column_registry import ColumnRegistry
from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.events.event_specs import EVENT_REGISTRY_SPECS
from project.research.search.distributed_runner import _slice_chunk_features


def test_slice_chunk_features_preserves_evaluator_and_trigger_columns(monkeypatch):
    monkeypatch.setattr(
        "project.research.search.distributed_runner.load_context_state_map",
        lambda: {("vol_regime", "high"): "high_vol_regime"},
    )

    spec = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="24b",
        template_id="continuation",
        context={"vol_regime": "high"},
        feature_condition=TriggerSpec.feature_predicate("rv_pct", ">", 0.8),
    )

    event_spec = EVENT_REGISTRY_SPECS["VOL_SHOCK"]
    event_cols = ColumnRegistry.event_cols("VOL_SHOCK", signal_col=event_spec.signal_column)
    direction_cols = ColumnRegistry.event_direction_cols("VOL_SHOCK")

    features = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z"]),
            "close": [100.0],
            "split_label": ["train"],
            "symbol": ["BTCUSDT"],
            "high_vol_regime": [1.0],
            "trending_state": [1.0],
            "chop_state": [0.0],
            "prob_spread_tight": [0.9],
            "prob_spread_wide": [0.1],
            "ms_vol_confidence": [0.9],
            "ms_vol_entropy": [0.1],
            "rv_pct": [0.95],
            event_cols[0]: [True],
            direction_cols[0]: [1.0],
            "unused_column": [123],
        }
    )

    sliced = _slice_chunk_features([spec], features)

    assert "close" in sliced.columns
    assert "timestamp" in sliced.columns
    assert "split_label" in sliced.columns
    assert event_cols[0] in sliced.columns
    assert direction_cols[0] in sliced.columns
    assert "high_vol_regime" in sliced.columns
    assert "trending_state" in sliced.columns
    assert "chop_state" in sliced.columns
    assert "prob_spread_tight" in sliced.columns
    assert "prob_spread_wide" in sliced.columns
    assert "ms_vol_confidence" in sliced.columns
    assert "ms_vol_entropy" in sliced.columns
    assert "rv_pct" in sliced.columns
    assert "unused_column" not in sliced.columns
