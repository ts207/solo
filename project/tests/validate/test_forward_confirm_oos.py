import pytest
import json
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import MagicMock, patch
from project.validate.forward_confirm import oos_frozen_thesis_replay_v1, _load_frozen_thesis, build_forward_confirmation_payload
from project.domain.hypotheses import HypothesisSpec

@pytest.fixture
def mock_thesis():
    # Use real HypothesisSpec but mock validation
    with patch("project.domain.hypotheses.TriggerSpec.validate", return_value=None):
        from project.domain.hypotheses import TriggerSpec
        trigger = TriggerSpec(trigger_type="EVENT", event_id="TEST_EVENT")
        object.__setattr__(trigger, "_enable_validation", False)
        thesis = HypothesisSpec(
            trigger=trigger,
            direction="long",
            horizon="24",
            template_id="test_template",
            context={"symbol": "BTCUSDT", "timeframe": "5m"}
        )
        object.__setattr__(thesis, "_enable_validation", False)
        return thesis

@pytest.fixture
def synthetic_features():
    dates = pd.date_range("2025-01-01", "2025-01-02", freq="5min", tz="UTC")
    df = pd.DataFrame(index=range(len(dates)))
    df["timestamp"] = dates
    df["close"] = np.random.randn(len(dates)).cumsum() + 100
    df["open"] = df["close"].shift(1)
    df["high"] = df["close"] + 0.1
    df["low"] = df["close"] - 0.1
    df["volume"] = 1000
    return df

@patch("project.validate.forward_confirm.prepare_search_features_for_symbol")
@patch("project.validate.forward_confirm.EvaluationContext")
@patch("project.validate.forward_confirm.expected_cost_per_trade_bps")
def test_oos_frozen_thesis_replay_v1_success(mock_cost, mock_context_cls, mock_prepare, mock_thesis, synthetic_features):
    mock_prepare.return_value = synthetic_features

    mock_context = MagicMock()
    # Mock some triggers
    mask = pd.Series(False, index=synthetic_features.index)
    mask.iloc[:10] = True
    mock_context.event_mask.return_value = (mask, None)
    mock_context.forward_returns.return_value = pd.Series([0.001] * len(synthetic_features))
    mock_context.weights = pd.Series([1.0] * len(synthetic_features))
    mock_context_cls.return_value = mock_context

    mock_cost.return_value = pd.Series([0.0] * len(synthetic_features))

    res = oos_frozen_thesis_replay_v1(
        run_id="test_run",
        thesis=mock_thesis,
        start="2025-01-01T00:00:00Z",
        end="2025-01-01T23:59:59Z",
        data_root=Path("/tmp/data")
    )

    assert res["event_count"] > 0
    assert "mean_return_net_bps" in res
    assert "t_stat_net" in res

    # Verify end=end was passed (Patch 1)
    args, kwargs = mock_prepare.call_args
    assert kwargs["end"] == "2025-01-01T23:59:59Z"
    assert kwargs["run_id"] == "test_run__forward_confirm_oos"
    assert kwargs["expected_event_ids"] == ["TEST_EVENT"]


@patch("project.validate.forward_confirm.prepare_search_features_for_symbol")
@patch("project.validate.forward_confirm.EvaluationContext")
@patch("project.validate.forward_confirm.expected_cost_per_trade_bps")
def test_oos_replay_strips_identity_context_before_filtering(
    mock_cost, mock_context_cls, mock_prepare, synthetic_features
):
    mock_prepare.return_value = synthetic_features

    from project.domain.hypotheses import TriggerSpec

    with patch("project.domain.hypotheses.TriggerSpec.validate", return_value=None):
        trigger = TriggerSpec(trigger_type="EVENT", event_id="TEST_EVENT")
        object.__setattr__(trigger, "_enable_validation", False)
        thesis = HypothesisSpec(
            trigger=trigger,
            direction="long",
            horizon="24",
            template_id="test_template",
            context={
                "carry_state": "funding_neg",
                "symbol": "BTCUSDT",
                "timeframe": "5m",
            },
        )
        object.__setattr__(thesis, "_enable_validation", False)

    mock_context = MagicMock()
    mask = pd.Series(False, index=synthetic_features.index)
    mask.iloc[:10] = True
    mock_context.event_mask.return_value = (mask, None)
    mock_context.forward_returns.return_value = pd.Series([0.001] * len(synthetic_features))
    mock_context.weights = pd.Series([1.0] * len(synthetic_features))
    mock_context_cls.return_value = mock_context
    mock_cost.return_value = pd.Series([0.0] * len(synthetic_features))

    oos_frozen_thesis_replay_v1(
        run_id="test_run",
        thesis=thesis,
        start="2025-01-01T00:00:00Z",
        end="2025-01-01T23:59:59Z",
        data_root=Path("/tmp/data"),
    )

    filtered_thesis = mock_context.event_mask.call_args.args[0]
    assert filtered_thesis.context == {"carry_state": "funding_neg"}


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
    mock_proposal.start = "2022-01-01"
    mock_proposal.end = "2024-12-31"
    mock_proposal.symbols = ["ETHUSDT"]
    mock_proposal.timeframe = "5m"
    mock_proposal.hypothesis.anchor.type = "event"
    mock_proposal.hypothesis.anchor.event_id = "test_event"
    mock_proposal.hypothesis.direction = "long"
    mock_proposal.hypothesis.horizon_bars = 24
    mock_proposal.hypothesis.template.id = "test_template"
    mock_proposal.hypothesis.filters.contexts = {"carry_state": ["funding_neg"]}
    mock_proposal.hypothesis.sampling_policy.entry_lag_bars = 1

    mock_load.return_value = mock_proposal

    thesis, r_start, r_end = _load_frozen_thesis(run_id="run1", proposal_path=Path("prop.yaml"))

    assert isinstance(thesis, HypothesisSpec)
    assert thesis.trigger.event_id == "TEST_EVENT"
    assert thesis.context == {
        "carry_state": "funding_neg",
        "symbol": "ETHUSDT",
        "timeframe": "5m",
    }
    assert r_start == "2022-01-01"

@patch("project.validate.forward_confirm._load_frozen_thesis")
def test_build_forward_confirmation_payload_overlap_fails(mock_load, mock_thesis):
    mock_load.return_value = (mock_thesis, "2022-01-01", "2024-12-31")

    with pytest.raises(ValueError, match="overlaps research window"):
        build_forward_confirmation_payload(
            run_id="run1",
            window="2024-01-01/2025-01-01",
        )

@patch("project.validate.forward_confirm._load_frozen_thesis")
def test_build_forward_confirmation_payload_unknown_research_fails(mock_load, mock_thesis):
    # Patch 2: Fail closed on unknown research window
    mock_load.return_value = (mock_thesis, None, None)

    with pytest.raises(ValueError, match="requires research_start and research_end"):
        build_forward_confirmation_payload(
            run_id="run1",
            window="2025-01-01/2025-06-30",
        )


@patch("project.validate.forward_confirm.oos_frozen_thesis_replay_v1")
@patch("project.validate.forward_confirm._load_frozen_thesis")
def test_build_forward_confirmation_payload_serializes_thesis_id(
    mock_load, mock_replay, mock_thesis
):
    mock_load.return_value = (mock_thesis, "2022-01-01", "2024-12-31")
    mock_replay.return_value = {"event_count": 1, "trade_count": 1}

    payload = build_forward_confirmation_payload(
        run_id="run1",
        window="2025-01-01/2025-06-30",
    )

    assert isinstance(payload["source"]["thesis_id"], str)
    json.dumps(payload)


def test_forward_confirm_loader_does_not_rank_candidates():
    import inspect
    import project.validate.forward_confirm as fc

    src = inspect.getsource(fc._load_frozen_thesis)
    forbidden = ["sort_values", "idxmax", "nlargest", "rank_score"]
    for token in forbidden:
        assert token not in src, f"Forbidden token '{token}' found in _load_frozen_thesis"

@patch("project.validate.forward_confirm.read_json")
@patch("project.validate.forward_confirm.Path.exists")
def test_load_frozen_thesis_ambiguous_promoted_fails(mock_exists, mock_read_json):
    mock_exists.return_value = True
    mock_read_json.return_value = {
        "theses": [
            {"lineage": {"candidate_id": "c1"}},
            {"lineage": {"candidate_id": "c2"}}
        ]
    }

    with pytest.raises(ValueError, match="Ambiguous promoted run"):
        _load_frozen_thesis(run_id="run1")

@patch("project.validate.forward_confirm.prepare_search_features_for_symbol")
@patch("project.validate.forward_confirm.EvaluationContext")
@patch("project.validate.forward_confirm.expected_cost_per_trade_bps")
def test_oos_frozen_thesis_replay_v1_horizon_filtering(mock_cost, mock_context_cls, mock_prepare, mock_thesis, synthetic_features):
    # Patch 3: Event near oos_end with exit_ts > oos_end is dropped
    mock_prepare.return_value = synthetic_features

    mock_context = MagicMock()
    # Trigger at the last bar
    mask = pd.Series(False, index=synthetic_features.index)
    mask.iloc[-1] = True
    mock_context.event_mask.return_value = (mask, None)
    mock_context.forward_returns.return_value = pd.Series([0.001] * len(synthetic_features))
    mock_context.weights = pd.Series([1.0] * len(synthetic_features))
    mock_context_cls.return_value = mock_context

    mock_cost.return_value = pd.Series([0.0] * len(synthetic_features))

    # Window ends exactly at the last bar's timestamp, so signal_ts == end but exit_ts > end
    end_ts = synthetic_features["timestamp"].iloc[-1].isoformat()

    res = oos_frozen_thesis_replay_v1(
        run_id="test_run",
        thesis=mock_thesis,
        start="2025-01-01T00:00:00Z",
        end=end_ts,
        data_root=Path("/tmp/data")
    )

    # Should fail with no events after filtering
    assert res["status"] == "fail"
    assert res["reason"] == "all_events_filtered_by_oos_boundary"

def test_to_utc_ts_handling():
    # Patch 4: tz-aware window strings do not crash
    from project.validate.forward_confirm import _to_utc_ts
    ts1 = _to_utc_ts("2025-01-01")
    assert ts1.tzinfo is not None

    ts2 = _to_utc_ts("2025-01-01T00:00:00Z")
    assert ts2.tzinfo is not None
    assert ts1 == ts2

@patch("project.validate.forward_confirm.Path.exists")
def test_load_frozen_thesis_missing_proposal_fails(mock_exists):
    # Patch 5: explicit missing --proposal path fails
    mock_exists.return_value = False
    with pytest.raises(FileNotFoundError, match="proposal not found"):
        _load_frozen_thesis(run_id="run1", proposal_path=Path("missing.yaml"))

@patch("project.validate.forward_confirm.read_json")
@patch("project.validate.forward_confirm.Path.exists")
def test_load_frozen_thesis_malformed_promoted_fails(mock_exists, mock_read_json):
    # Patch 6: promoted thesis schema validation failure fails closed
    mock_exists.return_value = True
    mock_read_json.return_value = {
        "theses": [{"invalid": "schema"}]
    }
    with pytest.raises(ValueError, match="invalid promoted thesis schema"):
        _load_frozen_thesis(run_id="run1")
