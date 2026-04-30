from __future__ import annotations

import pandas as pd
import pytest

from project.research.event_lift import (
    EventLiftGateError,
    EventLiftRequest,
    classify_event_lift,
    run_event_lift,
)


def _write_scorecard(data_root, *, decision: str) -> None:
    out = data_root / "reports" / "regime_baselines"
    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "schema_version": "regime_scorecard_v1",
                "source_run_ids": ["baseline_run"],
                "matrix_id": "core_v1",
                "regime_id": "vol_regime=high+carry_state=funding_neg",
                "candidate_baseline_count": 12,
                "stable_positive_count": 0,
                "year_conditional_count": 0,
                "unstable_count": 0,
                "negative_count": 12 if decision == "reject_directional" else 0,
                "insufficient_support_count": 0,
                "best_symbol": "BTCUSDT",
                "best_direction": "long",
                "best_horizon_bars": 24,
                "best_mean_net_bps": 1.0,
                "best_t_stat_net": 2.0,
                "best_max_year_pnl_share": 0.4,
                "best_effective_n": 100,
                "classification": "negative" if decision == "reject_directional" else "stable_positive",
                "decision": decision,
                "next_action": "run_event_lift_for_best_tuple",
            }
        ]
    ).to_parquet(out / "regime_scorecard.parquet", index=False)


def _write_market_context(data_root, *, run_id: str = "source_run", n: int = 80) -> None:
    out = (
        data_root
        / "lake"
        / "runs"
        / run_id
        / "features"
        / "perp"
        / "BTCUSDT"
        / "5m"
        / "market_context"
        / "year=2022"
        / "month=01"
    )
    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2022-01-01", periods=n, freq="5min", tz="UTC"),
            "close": [100.0 + i for i in range(n)],
            "symbol": ["BTCUSDT"] * n,
            "vol_regime": ["high"] * n,
            "carry_state": ["funding_neg"] * n,
            "ms_trend_state": [1.0] * n,
            "spread_bps": [1.0] * n,
        }
    ).to_parquet(out / "market_context_BTCUSDT_2022-01.parquet", index=False)


def _write_events(data_root) -> None:
    out = data_root / "reports" / "funding_events" / "event_run"
    out.mkdir(parents=True, exist_ok=True)
    timestamps = pd.date_range("2022-01-01 00:10", periods=8, freq="25min", tz="UTC")
    pd.DataFrame(
        {
            "timestamp": timestamps,
            "event_type": ["FUNDING_EXTREME_ONSET"] * len(timestamps),
            "symbol": ["BTCUSDT"] * len(timestamps),
            "funding_extreme_onset_event": [1] * len(timestamps),
        }
    ).to_parquet(out / "funding_episode_events.parquet", index=False)


def _request(tmp_path, *, audit: bool = False) -> EventLiftRequest:
    return EventLiftRequest(
        run_id="event_lift_test",
        mechanism_id="funding_squeeze",
        regime_id="vol_regime=high+carry_state=funding_neg",
        event_id="FUNDING_EXTREME_ONSET",
        symbol="BTCUSDT",
        direction="long",
        horizon_bars=4,
        data_root=tmp_path,
        source_run_id="source_run",
        event_source_run_id="event_run",
        allow_nonviable_regime_audit=audit,
    )


def _stats(mean: float, *, effective_n: int = 30) -> dict:
    return {
        "n": effective_n,
        "effective_n": effective_n,
        "mean_net_bps": mean,
        "mean_net_bps_2x_cost": mean - 1.0,
    }


def test_event_lift_rejects_non_allow_scorecard_regime(tmp_path):
    _write_scorecard(tmp_path, decision="reject_directional")

    with pytest.raises(EventLiftGateError, match="scorecard decision=reject_directional"):
        run_event_lift(_request(tmp_path))


def test_event_lift_allows_audit_mode_but_marks_non_promotable(tmp_path):
    _write_scorecard(tmp_path, decision="reject_directional")
    _write_market_context(tmp_path)
    _write_events(tmp_path)

    result = run_event_lift(_request(tmp_path, audit=True))

    assert result["scorecard_decision"] == "reject_directional"
    assert result["audit_only"] is True
    assert result["promotion_eligible"] is False
    assert result["classification"] == "audit_only"
    assert result["decision"] == "audit_only"


def test_event_lift_classifies_regime_proxy():
    controls = {
        "event_plus_regime": _stats(5.0),
        "regime_only_matched_non_event": _stats(6.0),
        "event_only": _stats(4.0),
        "unconditional_all": _stats(0.0),
        "opposite_direction": _stats(-2.0),
        "entry_lags": [dict(_stats(5.0), lag_bars=0), dict(_stats(3.0), lag_bars=2)],
    }

    assert classify_event_lift(
        controls=controls,
        max_year_pnl_share=0.4,
        mean_net_bps_2x_cost=4.0,
    ) == (
        "regime_proxy",
        "park",
        "event_plus_regime does not beat matched regime-only control",
    )


def test_event_lift_classifies_direction_invalid():
    controls = {
        "event_plus_regime": _stats(5.0),
        "regime_only_matched_non_event": _stats(1.0),
        "event_only": _stats(1.0),
        "unconditional_all": _stats(0.0),
        "opposite_direction": _stats(2.0),
        "entry_lags": [dict(_stats(5.0), lag_bars=0), dict(_stats(3.0), lag_bars=2)],
    }

    classification, decision, reason = classify_event_lift(
        controls=controls,
        max_year_pnl_share=0.4,
        mean_net_bps_2x_cost=4.0,
    )

    assert classification == "direction_invalid"
    assert decision == "kill"
    assert reason == "opposite_direction is positive"


def test_event_lift_classifies_timing_unstable():
    controls = {
        "event_plus_regime": _stats(5.0),
        "regime_only_matched_non_event": _stats(1.0),
        "event_only": _stats(1.0),
        "unconditional_all": _stats(0.0),
        "opposite_direction": _stats(-2.0),
        "entry_lags": [
            dict(_stats(5.0), lag_bars=0),
            dict(_stats(4.0), lag_bars=1),
            dict(_stats(7.0), lag_bars=2),
            dict(_stats(3.0), lag_bars=3),
        ],
    }

    classification, decision, reason = classify_event_lift(
        controls=controls,
        max_year_pnl_share=0.4,
        mean_net_bps_2x_cost=4.0,
    )

    assert classification == "timing_unstable"
    assert decision == "park"
    assert reason == "entry_lag_2_or_3 beats base materially"
