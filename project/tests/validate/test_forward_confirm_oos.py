import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import MagicMock, patch
from project.validate.forward_confirm import oos_frozen_thesis_replay_v1, _load_frozen_thesis
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

@patch("project.validate.forward_confirm.load_normalized_operator_proposal")
@patch("project.validate.forward_confirm.Path.exists")
@patch("project.domain.hypotheses.TriggerSpec.validate", return_value=None)
def test_load_frozen_thesis_from_proposal(mock_trigger_val, mock_exists, mock_load):
    mock_exists.return_value = True
    
    mock_proposal = MagicMock()
    mock_proposal.hypothesis.anchor.type = "event"
    mock_proposal.hypothesis.anchor.event_id = "test_event"
    mock_proposal.hypothesis.direction = "long"
    mock_proposal.hypothesis.horizon_bars = 24
    mock_proposal.hypothesis.template.id = "test_template"
    mock_proposal.hypothesis.filters.contexts = {"symbol": "ETHUSDT"}
    mock_proposal.hypothesis.sampling_policy.entry_lag_bars = 1
    
    mock_load.return_value = mock_proposal
    
    thesis = _load_frozen_thesis(run_id="run1", proposal_path=Path("prop.yaml"))
    
    assert isinstance(thesis, HypothesisSpec)
    assert thesis.trigger.trigger_type == "event"
    assert thesis.direction == "long"
    assert thesis.horizon == "24"
    assert thesis.context == {"symbol": "ETHUSDT"}

@patch("project.validate.forward_confirm._load_frozen_thesis")
def test_build_forward_confirmation_payload_fail_closed(mock_load):
    from project.validate.forward_confirm import build_forward_confirmation_payload
    mock_load.side_effect = ValueError("No frozen thesis found")
    
    with pytest.raises(RuntimeError, match="forward-confirm snapshot mode is disabled"):
        build_forward_confirmation_payload(
            run_id="run1",
            window="2025-01-01/2025-01-02",
        )
