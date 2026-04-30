from __future__ import annotations

import json

import pandas as pd

from project.scripts.run_event_lift import main


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


def _argv(tmp_path, *, audit: bool = False) -> list[str]:
    args = [
        "--run-id",
        "event_lift_test",
        "--mechanism-id",
        "funding_squeeze",
        "--regime-id",
        "vol_regime=high+carry_state=funding_neg",
        "--event-id",
        "FUNDING_EXTREME_ONSET",
        "--symbol",
        "BTCUSDT",
        "--direction",
        "long",
        "--horizon-bars",
        "4",
        "--data-root",
        str(tmp_path),
        "--source-run-id",
        "source_run",
        "--event-source-run-id",
        "event_run",
    ]
    if audit:
        args.append("--allow-nonviable-regime-audit")
    return args


def test_run_event_lift_cli_fails_closed_for_reject_directional_scorecard(tmp_path, capsys):
    _write_scorecard(tmp_path, decision="reject_directional")

    rc = main(_argv(tmp_path))

    assert rc == 1
    assert capsys.readouterr().out.strip() == (
        "fail: regime_id=vol_regime=high+carry_state=funding_neg is not eligible for event lift; "
        "scorecard decision=reject_directional"
    )
    assert not (tmp_path / "reports" / "event_lift" / "event_lift_test").exists()


def test_run_event_lift_cli_writes_outputs_in_audit_mode(tmp_path):
    _write_scorecard(tmp_path, decision="reject_directional")
    _write_market_context(tmp_path)
    _write_events(tmp_path)

    rc = main(_argv(tmp_path, audit=True))

    assert rc == 0
    out_dir = tmp_path / "reports" / "event_lift" / "event_lift_test"
    for name in ["event_lift.json", "event_lift.parquet", "event_lift.md"]:
        assert (out_dir / name).exists()

    payload = json.loads((out_dir / "event_lift.json").read_text(encoding="utf-8"))
    row = payload["rows"][0]
    assert row["schema_version"] == "event_lift_v1"
    assert row["audit_only"] is True
    assert row["promotion_eligible"] is False
    assert row["classification"] == "audit_only"
    assert row["decision"] == "audit_only"
