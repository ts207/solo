from __future__ import annotations

import pandas as pd

from project.tests.conftest import PROJECT_ROOT

from project.pipelines.pipeline_audit import run_runtime_postflight_audit


def test_runtime_postflight_surfaces_determinism_and_oms_fields(tmp_path):
    data_root = tmp_path / "data"
    repo_root = tmp_path / "repo"
    runtime_root = repo_root / "spec" / "runtime"
    runtime_root.mkdir(parents=True)
    (runtime_root / "lanes.yaml").write_text(
        "schema_version: 1\n"
        "tick_time_unit: us\n"
        "lanes:\n"
        "  - lane_id: alpha_5s\n"
        "    cadence_us: 5000000\n"
        "    watermark:\n"
        "      policy: bounded_out_of_orderness\n"
        "      max_lateness_us: 1000000\n"
        "      idle_source_policy: stall\n"
        "      idle_timeout_us: 1000000\n"
        "    processing_time_gate:\n"
        "      require_recv_time_leq_decision_time: true\n"
        "    inputs:\n"
        "      normalized_event_types: [order_submit, order_fill]\n",
        encoding="utf-8",
    )
    (runtime_root / "firewall.yaml").write_text(
        "schema_version: 1\nroles:\n  alpha: {allowed_provenance: [market, execution]}\n  events: {allowed_provenance: [market]}\n  execution: {allowed_provenance: [execution]}\n",
        encoding="utf-8",
    )
    (runtime_root / "hashing.yaml").write_text(
        "schema_version: 1\nconfig_version: v1\nmodel_version: v1\nalgorithm: blake2b_256\nrecord_sort_keys: [source_seq, event_id]\ncanonicalization: {ensure_ascii: true}\n",
        encoding="utf-8",
    )
    events_dir = data_root / "events" / "run1"
    events_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "event_id": "1",
                "event_type": "order_submit",
                "timestamp": "2024-01-01T00:00:00Z",
                "detected_ts": "2024-01-01T00:00:00Z",
                "symbol": "BTCUSDT",
                "source_seq": 1,
                "order_id": "o1",
                "provenance": "execution",
            },
            {
                "event_id": "2",
                "event_type": "order_fill",
                "timestamp": "2024-01-01T00:00:01Z",
                "detected_ts": "2024-01-01T00:00:01Z",
                "symbol": "BTCUSDT",
                "source_seq": 2,
                "order_id": "o1",
                "provenance": "execution",
            },
        ]
    ).to_csv(events_dir / "events.csv", index=False)

    out = run_runtime_postflight_audit(
        run_id="run1", data_root=data_root, repo_root=repo_root, determinism_replay_checks=True
    )
    assert "determinism_status" in out
    assert "replay_digest" in out
    assert "oms_replay_status" in out
    assert "oms_replay_digest" in out
