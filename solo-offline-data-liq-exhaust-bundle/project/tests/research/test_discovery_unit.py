import pandas as pd
import pytest
from project.research.discovery import (
    _synthesize_registry_candidates,
    infer_event_direction_sign,
    resolve_registry_direction_policy,
)


def test_synthesize_registry_candidates_basic():
    events_df = pd.DataFrame(
        {
            "timestamp": [1, 2, 3],
            "return_24": [0.01, 0.02, 0.03],
        }
    )

    # Default behavior: continuation and mean_reversion for 1 horizon
    df = _synthesize_registry_candidates(
        run_id="r0",
        symbol="BTCUSDT",
        event_type="VOL_SPIKE",
        events_df=events_df,
        horizon_bars=24,
        entry_lag_bars=1,
    )

    assert len(df) == 2
    assert set(df["rule_template"]) == {"continuation", "mean_reversion"}
    assert all(df["horizon"] == "24b")


def test_synthesize_registry_candidates_expanded():
    events_df = pd.DataFrame(
        {
            "timestamp": [1, 2, 3],
            "return_12": [0.01, 0.02, 0.03],
            "return_24": [0.01, 0.02, 0.03],
        }
    )

    # Explicit subsets
    df = _synthesize_registry_candidates(
        run_id="r0",
        symbol="BTCUSDT",
        event_type="VOL_SPIKE",
        events_df=events_df,
        horizon_bars=24,
        entry_lag_bars=1,
        templates=("continuation",),
        horizons=("1h", "2h"),
        directions=("long", "short"),
        entry_lags=(1, 2),
    )

    # 1 template * 2 horizons * 2 directions * 2 lags = 8
    assert len(df) == 8
    assert all(df["rule_template"] == "continuation")
    assert set(df["horizon"]) == {"1h", "2h"}
    assert set(df["direction"]) == {1.0, -1.0}
    assert set(df["entry_lag_bars"]) == {1, 2}


def test_synthesize_registry_candidates_search_budget():
    events_df = pd.DataFrame(
        {
            "timestamp": [1, 2, 3],
            "return_24": [0.01, 0.02, 0.03],
        }
    )

    df = _synthesize_registry_candidates(
        run_id="r0",
        symbol="BTCUSDT",
        event_type="VOL_SPIKE",
        events_df=events_df,
        horizon_bars=24,
        entry_lag_bars=1,
        templates=("continuation", "mean_reversion", "vol_expansion"),
        horizons=("1h", "2h"),
        search_budget=2,
    )

    assert len(df) == 2


def test_synthesize_registry_candidates_respects_operator_side_policy():
    events_df = pd.DataFrame(
        {
            "timestamp": [1, 2, 3],
            "return_24": [0.01, 0.02, 0.03],
        }
    )

    df = _synthesize_registry_candidates(
        run_id="r0",
        symbol="BTCUSDT",
        event_type="VOL_SPIKE",
        events_df=events_df,
        horizon_bars=24,
        entry_lag_bars=1,
        templates=("continuation", "mean_reversion", "reversal_or_squeeze"),
        directions=("long",),
    ).set_index("rule_template")

    assert float(df.loc["continuation", "direction"]) == 1.0
    assert float(df.loc["reversal_or_squeeze", "direction"]) == 1.0
    assert float(df.loc["mean_reversion", "direction"]) == -1.0


def test_infer_event_direction_sign_uses_text_direction_tokens():
    events_df = pd.DataFrame(
        {
            "timestamp": [1, 2, 3],
            "direction": ["up", "up", "down"],
        }
    )

    assert infer_event_direction_sign(events_df, event_type="VOL_SPIKE", default=0.0) == 1.0


def test_synthesize_registry_candidates_skips_unresolved_zero_direction():
    events_df = pd.DataFrame(
        {
            "timestamp": [1, 2, 3],
            "direction": ["non_directional", "non_directional", "non_directional"],
            "return_24": [0.0, 0.0, 0.0],
        }
    )

    df = _synthesize_registry_candidates(
        run_id="r0",
        symbol="BTCUSDT",
        event_type="ZSCORE_STRETCH",
        events_df=events_df,
        horizon_bars=24,
        entry_lag_bars=1,
    )

    assert df.empty


def test_resolve_registry_direction_policy_reports_non_directional_skip():
    events_df = pd.DataFrame(
        {
            "timestamp": [1, 2, 3],
            "direction": ["non_directional", "non_directional", "non_directional"],
        }
    )

    out = resolve_registry_direction_policy(events_df, event_type="ZSCORE_STRETCH", default=0.0)

    assert out["resolved"] is False
    assert out["policy"] == "non_directional_skip"
    assert out["source"] == "unresolved"
