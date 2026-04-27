import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import MagicMock, patch
from project.validate.forward_confirm import oos_frozen_thesis_replay_v1
from project.domain.hypotheses import HypothesisSpec

@pytest.fixture
def mock_thesis():
    thesis = MagicMock(spec=HypothesisSpec)
    thesis.context = {"symbol": "BTCUSDT", "timeframe": "5m"}
    thesis.hypothesis_id = "test_thesis"
    return thesis

@pytest.fixture
def synthetic_features():
    dates = pd.date_range("2025-01-01", "2025-01-02", freq="5min")
    df = pd.DataFrame(index=dates)
    df["close"] = np.random.randn(len(dates)).cumsum() + 100
    df["open"] = df["close"].shift(1)
    df["high"] = df["close"] + 0.1
    df["low"] = df["close"] - 0.1
    df["volume"] = 1000
    # Add dummy event flag column if needed by evaluator
    # (Actually evaluate_hypothesis_batch will detect events based on TriggerSpec in HypothesisSpec)
    return df

@patch("project.validate.forward_confirm.prepare_search_features_for_symbol")
@patch("project.validate.forward_confirm.evaluate_hypothesis_batch")
def test_oos_frozen_thesis_replay_v1_success(mock_eval, mock_prepare, mock_thesis, synthetic_features):
    mock_prepare.return_value = synthetic_features
    
    # Mock metrics result
    mock_metrics = pd.DataFrame([{
        "n": 10,
        "mean_return_net_bps": 5.0,
        "t_stat_net": 2.5,
        "hit_rate": 0.6,
        "mae_mean_bps": 10.0,
        "mfe_mean_bps": 15.0
    }])
    mock_eval.return_value = mock_metrics
    
    res = oos_frozen_thesis_replay_v1(
        run_id="test_run",
        thesis=mock_thesis,
        start="2025-01-01",
        end="2025-01-02",
        data_root=Path("/tmp/data")
    )
    
    assert res["event_count"] == 10
    assert res["mean_return_net_bps"] == 5.0
    assert res["t_stat_net"] == 2.5
    assert res["hit_rate"] == 0.6

@patch("project.validate.forward_confirm.prepare_search_features_for_symbol")
def test_oos_frozen_thesis_replay_v1_empty_features(mock_prepare, mock_thesis):
    mock_prepare.return_value = pd.DataFrame()
    
    res = oos_frozen_thesis_replay_v1(
        run_id="test_run",
        thesis=mock_thesis,
        start="2025-01-01",
        end="2025-01-02",
        data_root=Path("/tmp/data")
    )
    
    assert res["event_count"] == 0
    assert res["status"] == "fail"
    assert res["reason"] == "no_oos_features_loaded"
