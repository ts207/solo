from __future__ import annotations

import importlib
import json
import sys

import pandas as pd


def test_run_determinism_replay_stage_emits_digest(monkeypatch, tmp_path):
    run_id = "replay_stage_run"
    data_root = tmp_path / "data"
    runtime_dir = data_root / "runs" / run_id / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    ticks = pd.DataFrame(
        [
            {
                "tick_time": 1,
                "lane_id": "alpha_5s",
                "role": "alpha",
                "instrument_id": "BTCUSDT",
                "venue_id": "binance",
                "event_id": "e1",
                "source_seq": 1,
            },
            {
                "tick_time": 2,
                "lane_id": "alpha_5s",
                "role": "alpha",
                "instrument_id": "BTCUSDT",
                "venue_id": "binance",
                "event_id": "e2",
                "source_seq": 2,
            },
        ]
    )
    ticks.to_parquet(runtime_dir / "causal_ticks.parquet", index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))
    stage = importlib.import_module("project.pipelines.runtime.run_determinism_replay_checks")
    stage = importlib.reload(stage)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_determinism_replay_checks.py", "--run_id", run_id],
    )
    rc = stage.main()
    assert rc == 0
    payload = json.loads((runtime_dir / "determinism_replay.json").read_text(encoding="utf-8"))
    assert payload["status"] == "pass"
    assert str(payload["replay_digest"]).startswith("blake2b_256:")
