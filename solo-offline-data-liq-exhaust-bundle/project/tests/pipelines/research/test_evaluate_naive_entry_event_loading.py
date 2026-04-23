from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

import project.research.evaluate_naive_entry as evaluate_naive_entry


def test_load_phase1_events_uses_registry_spec_paths_and_subtype_filter(monkeypatch, tmp_path):
    monkeypatch.setattr(evaluate_naive_entry, "DATA_ROOT", tmp_path, raising=False)
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))

    run_id = "r_eval"
    events_path = (
        tmp_path / "reports" / "funding_events" / run_id / "funding_episode_events.parquet"
    )
    events_path.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "event_type": "FUNDING_EXTREME_ONSET",
                "symbol": "BTCUSDT",
                "enter_ts": "2026-01-01T00:00:00Z",
            },
            {
                "event_type": "FUNDING_PERSISTENCE_TRIGGER",
                "symbol": "BTCUSDT",
                "enter_ts": "2026-01-01T00:05:00Z",
            },
        ]
    ).to_parquet(events_path, index=False)

    out = evaluate_naive_entry._load_phase1_events(
        run_id=run_id, event_type="FUNDING_PERSISTENCE_TRIGGER"
    )
    assert not out.empty
    assert set(out["event_type"].astype(str).unique()) == {"FUNDING_PERSISTENCE_TRIGGER"}


def test_load_phase2_candidates_collects_search_engine_bridge_pass_rows(monkeypatch, tmp_path):
    monkeypatch.setattr(evaluate_naive_entry, "DATA_ROOT", tmp_path, raising=False)
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))

    run_id = "r_eval_phase2"
    search_dir = tmp_path / "reports" / "phase2" / run_id / "search_engine"
    search_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "candidate_id": "search_1",
                "event_type": "STATE_CHOP_STATE",
                "hypothesis_id": "hyp_1",
                "gate_bridge_tradable": True,
            },
            {
                "candidate_id": "search_2",
                "event_type": "STATE_CHOP_STATE",
                "hypothesis_id": "hyp_2",
                "gate_bridge_tradable": False,
            },
        ]
    ).to_parquet(search_dir / "phase2_candidates.parquet", index=False)

    out = evaluate_naive_entry._load_phase2_candidates(run_id)

    assert list(out["candidate_id"]) == ["search_1"]
    assert set(out["event_type"].astype(str).unique()) == {"STATE_CHOP_STATE"}


def test_main_evaluates_bridge_pass_phase2_candidates(monkeypatch, tmp_path):
    monkeypatch.setattr(evaluate_naive_entry, "DATA_ROOT", tmp_path, raising=False)
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(tmp_path))

    run_id = "r_eval_main"
    search_dir = tmp_path / "reports" / "phase2" / run_id / "search_engine"
    search_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "search_1",
                "hypothesis_id": "hyp_1",
                "event_type": "STATE_CHOP_STATE",
                "gate_bridge_tradable": True,
            }
        ]
    ).to_parquet(search_dir / "phase2_candidates.parquet", index=False)

    captured = {}

    def fake_finalize(manifest, status, stats=None, error=None):
        captured["status"] = status
        captured["stats"] = stats or {}

    monkeypatch.setattr(
        evaluate_naive_entry,
        "start_manifest",
        lambda *args, **kwargs: {"stage": "evaluate_naive_entry"},
    )
    monkeypatch.setattr(evaluate_naive_entry, "finalize_manifest", fake_finalize)
    monkeypatch.setattr(
        evaluate_naive_entry,
        "_load_phase1_events",
        lambda run_id, event_type: pd.DataFrame(
            [
                {"event_type": event_type, "symbol": "BTCUSDT", "forward_return_h": 0.01},
                {"event_type": event_type, "symbol": "BTCUSDT", "forward_return_h": 0.02},
            ]
        ),
    )
    monkeypatch.setattr(
        evaluate_naive_entry,
        "write_parquet",
        lambda df, path: captured.update({"rows": len(df), "out_path": path, "df": df.copy()}),
    )
    monkeypatch.setattr(
        evaluate_naive_entry,
        "parser",
        None,
        raising=False,
    )
    monkeypatch.setattr(
        evaluate_naive_entry.argparse.ArgumentParser,
        "parse_args",
        lambda self: SimpleNamespace(
            run_id=run_id,
            symbols="BTCUSDT",
            min_trades=20,
            min_expectancy_after_cost=0.0,
            max_drawdown=1.0,
            retail_profile="capital_constrained",
            out_dir=None,
            log_path=None,
        ),
    )

    rc = evaluate_naive_entry.main()

    assert rc == 0
    assert captured["status"] == "success"
    assert captured["rows"] == 1
    assert captured["stats"]["evaluated_hypotheses"] == 1
    assert float(captured["df"]["naive_expectancy"].iloc[0]) == 0.015


def test_build_regime_events_supports_state_and_transition_candidates(monkeypatch):
    features = pd.DataFrame(
        [
            {
                "timestamp": "2025-01-01T00:00:00Z",
                "symbol": "BTCUSDT",
                "close": 100.0,
                "chop_regime": 0.0,
                "bull_trend_regime": 1.0,
                "bear_trend_regime": 0.0,
            },
            {
                "timestamp": "2025-01-01T00:05:00Z",
                "symbol": "BTCUSDT",
                "close": 101.0,
                "chop_regime": 1.0,
                "bull_trend_regime": 0.0,
                "bear_trend_regime": 0.0,
            },
            {
                "timestamp": "2025-01-01T00:10:00Z",
                "symbol": "BTCUSDT",
                "close": 102.0,
                "chop_regime": 1.0,
                "bull_trend_regime": 0.0,
                "bear_trend_regime": 0.0,
            },
        ]
    )
    features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)

    monkeypatch.setattr(
        evaluate_naive_entry, "load_features", lambda *args, **kwargs: features.copy()
    )

    state_events = evaluate_naive_entry._build_regime_events(
        run_id="r1",
        symbol="BTCUSDT",
        event_type="STATE_CHOP_STATE",
        horizon="5m",
    )
    transition_events = evaluate_naive_entry._build_regime_events(
        run_id="r1",
        symbol="BTCUSDT",
        event_type="TRANSITION_TRENDING_STATE_CHOP_STATE",
        horizon="5m",
    )

    assert len(state_events) == 1
    assert state_events["event_type"].iloc[0] == "STATE_CHOP_STATE"
    assert round(float(state_events["forward_return_h"].iloc[0]), 6) == round(
        (102.0 / 101.0) - 1.0, 6
    )

    assert len(transition_events) == 1
    assert transition_events["event_type"].iloc[0] == "TRANSITION_TRENDING_STATE_CHOP_STATE"
