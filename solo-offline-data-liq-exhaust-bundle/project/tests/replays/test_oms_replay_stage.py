from __future__ import annotations

import importlib
import json
import sys

import pandas as pd


def test_run_oms_replay_stage_emits_report(monkeypatch, tmp_path):
    run_id = "oms_replay_stage_run"
    data_root = tmp_path / "data"
    runtime_dir = data_root / "runs" / run_id / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    normalized = pd.DataFrame(
        [
            {
                "event_id": "e1",
                "event_type": "oms_submit",
                "order_id": "o1",
                "lane_id": "exec_1s",
                "source_id": "oms:BTCUSDT",
                "source_seq": 1,
                "event_time_us": 1,
                "recv_time_us": 2,
                "instrument_id": "BTCUSDT",
                "venue_id": "binance",
                "role": "execution",
                "provenance": "execution",
            },
            {
                "event_id": "e2",
                "event_type": "oms_ack",
                "order_id": "o1",
                "lane_id": "exec_1s",
                "source_id": "oms:BTCUSDT",
                "source_seq": 2,
                "event_time_us": 3,
                "recv_time_us": 4,
                "instrument_id": "BTCUSDT",
                "venue_id": "binance",
                "role": "execution",
                "provenance": "execution",
            },
        ]
    )
    normalized.to_parquet(runtime_dir / "normalized_events.parquet", index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))
    stage = importlib.import_module("project.pipelines.runtime.run_oms_replay_validation")
    stage = importlib.reload(stage)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_oms_replay_validation.py", "--run_id", run_id],
    )
    rc = stage.main()
    assert rc == 0
    payload = json.loads((runtime_dir / "oms_replay_validation.json").read_text(encoding="utf-8"))
    assert payload["status"] == "pass"
    assert int(payload["violation_count"]) == 0
    assert str(payload["replay_digest"]).startswith("blake2b_256:")


def test_run_oms_replay_stage_fail_on_violations(monkeypatch, tmp_path):
    run_id = "oms_replay_stage_fail"
    data_root = tmp_path / "data"
    runtime_dir = data_root / "runs" / run_id / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    normalized = pd.DataFrame(
        [
            {
                "event_id": "e1",
                "event_type": "oms_fill",
                "order_id": "o1",
                "lane_id": "exec_1s",
                "source_id": "oms:BTCUSDT",
                "source_seq": 1,
                "event_time_us": 1,
                "recv_time_us": 2,
                "instrument_id": "BTCUSDT",
                "venue_id": "binance",
                "role": "execution",
                "provenance": "execution",
            }
        ]
    )
    normalized.to_parquet(runtime_dir / "normalized_events.parquet", index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))
    stage = importlib.import_module("project.pipelines.runtime.run_oms_replay_validation")
    stage = importlib.reload(stage)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_oms_replay_validation.py", "--run_id", run_id, "--fail_on_violations", "1"],
    )
    rc = stage.main()
    assert rc == 1
