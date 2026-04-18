import json

import pandas as pd

from project import PROJECT_ROOT
from project.research.benchmarks import discovery_benchmark
from project.research.benchmarks.benchmark_modes import get_mode


def test_benchmark_runner_immutability():
    ledger_config_path = PROJECT_ROOT.parent / "project/configs/discovery_ledger.yaml"
    scoring_config_path = PROJECT_ROOT.parent / "project/configs/discovery_scoring_v2.yaml"

    def get_bytes(p):
        return p.read_bytes() if p.exists() else b""

    before_ledger = get_bytes(ledger_config_path)
    before_scoring = get_bytes(scoring_config_path)

    base_search = {"symbol": "BTC", "cases": []}
    base_scoring = {"v2_scoring": {"enabled": True}}
    base_ledger = {"enabled": False}

    mode_d = get_mode("D")
    assert mode_d is not None
    resolved = discovery_benchmark._resolved_benchmark_mode_config(
        base_search, base_scoring, base_ledger, mode_d
    )

    assert resolved["mode_id"] == "D"
    assert resolved["search"]["mode"] == "hierarchical"
    assert resolved["ledger"]["enabled"] is False
    assert base_ledger["enabled"] is False

    assert get_bytes(ledger_config_path) == before_ledger
    assert get_bytes(scoring_config_path) == before_scoring


def test_benchmark_output_persistence(tmp_path):
    import json

    from project.research.benchmarks import discovery_benchmark

    base_search = {"search": "flat"}
    base_scoring = {"v2": True}
    base_ledger = {"enabled": False}

    out_dir = tmp_path / "ledger"
    out_dir.mkdir()

    mode_d = get_mode("D")
    assert mode_d is not None
    resolved = discovery_benchmark._resolved_benchmark_mode_config(
        base_search, base_scoring, base_ledger, mode_d
    )

    with open(out_dir / "resolved_mode_config.json", "w") as f:
        json.dump(resolved, f, indent=2)

    assert (out_dir / "resolved_mode_config.json").exists()
    saved = json.loads((out_dir / "resolved_mode_config.json").read_text())
    assert saved["mode_id"] == "D"
    assert saved["ledger"]["enabled"] is False


def test_benchmark_mode_registry_is_single_path():
    from project.research.benchmarks import discovery_benchmark

    base_search = {"search": "flat"}
    base_scoring = {"v2": True}
    base_ledger = {"enabled": False}

    mode_d = get_mode("D")
    assert mode_d is not None
    assert get_mode("A") is None
    assert get_mode("F") is None

    resolved = discovery_benchmark._resolved_benchmark_mode_config(
        base_search, base_scoring, base_ledger, mode_d
    )

    assert resolved["mode_id"] == "D"
    assert resolved["search"]["mode"] == "hierarchical"
    assert resolved["scoring_v2"]["enable_discovery_v2_scoring"] is True
    assert resolved["ledger"]["enabled"] is False


def test_run_benchmark_job_materializes_missing_fixture_registry(tmp_path, monkeypatch):
    mode_d = get_mode("D")
    assert mode_d is not None

    fixture_path = tmp_path / "fixtures" / "missing_fixture.parquet"
    data_root = tmp_path / "data"
    out_dir = tmp_path / "out"

    observed = {}

    def fake_materialize(**kwargs):
        observed["materialize_kwargs"] = kwargs
        kwargs["output_path"].parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
                {
                    "timestamp": pd.Timestamp("2024-07-05T00:00:00Z"),
                    "symbol": "BTCUSDT",
                    "event_type": "VOL_SPIKE",
                    "event_score": 1.0,
                    "signal_column": "vol_spike_event",
                    "sign": 1,
                }
            ]
        ).to_parquet(kwargs["output_path"], index=False)
        return 1

    def fake_phase2_run(**kwargs):
        observed["event_registry_override"] = kwargs.get("event_registry_override")
        observed["min_t_stat"] = kwargs.get("min_t_stat")
        observed["min_n"] = kwargs.get("min_n")
        observed["gate_profile"] = kwargs.get("gate_profile")
        observed["discovery_profile"] = kwargs.get("discovery_profile")
        phase2_dir = data_root / "reports" / "phase2" / kwargs["run_id"]
        phase2_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_parquet(phase2_dir / "phase2_candidates.parquet", index=False)

    monkeypatch.setattr(
        discovery_benchmark,
        "materialize_benchmark_fixture",
        fake_materialize,
    )
    monkeypatch.setattr(
        discovery_benchmark.phase2_search_engine,
        "run",
        fake_phase2_run,
    )

    result = discovery_benchmark.run_benchmark_job(
        run_id="fixture_materialization_smoke",
        symbols="BTCUSDT",
        timeframe="5m",
        start="2024-07-01",
        end="2024-08-01",
        search_spec={"triggers": {"events": ["VOL_SPIKE"]}},
        mode=mode_d,
        data_root=data_root,
        out_dir=out_dir,
        event_source="fixture",
        fixture_event_registry=str(fixture_path),
        phase2_overrides={
            "min_t_stat": 1.5,
            "min_n": 24,
            "gate_profile": "discovery",
            "discovery_profile": "standard",
        },
    )

    assert observed["materialize_kwargs"]["event_types"] == ["VOL_SPIKE"]
    assert observed["event_registry_override"] == str(fixture_path)
    assert observed["min_t_stat"] == 1.5
    assert observed["min_n"] == 24
    assert observed["gate_profile"] == "discovery"
    assert observed["discovery_profile"] == "standard"
    assert result["status"] == "success"


def test_run_benchmark_job_keeps_empty_candidate_frame_as_zero_with_diagnostics_context(
    tmp_path, monkeypatch
):
    mode_d = get_mode("D")
    assert mode_d is not None

    data_root = tmp_path / "data"
    out_dir = tmp_path / "out"
    phase2_dir = data_root / "reports" / "phase2" / "diag_run"
    phase2_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame().to_parquet(phase2_dir / "phase2_candidates.parquet", index=False)
    (phase2_dir / "phase2_diagnostics.json").write_text(
        json.dumps(
            {
                "feasible_hypotheses": 14,
                "metrics_rows": 12,
                "valid_metrics_rows": 9,
                "bridge_candidates_rows": 0,
                "gate_funnel": {"generated": 14},
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(discovery_benchmark.phase2_search_engine, "run", lambda **_: None)

    result = discovery_benchmark.run_benchmark_job(
        run_id="diag_run",
        symbols="BTCUSDT",
        timeframe="5m",
        start="2024-01-01",
        end="2024-01-02",
        search_spec={"triggers": {"events": ["VOL_SPIKE"]}},
        mode=mode_d,
        data_root=data_root,
        out_dir=out_dir,
    )

    assert result["status"] == "success"
    assert result["artifact_paths"]["phase2_diagnostics"].endswith("phase2_diagnostics.json")
    assert result["candidate_count"] == 0
    assert result["benchmark_metrics"]["candidate_count"] == 0
    assert result["benchmark_metrics"]["candidate_count_basis"] == "phase2_candidates_parquet"
    assert result["benchmark_metrics"]["emergence"] is False
    assert result["benchmark_metrics"]["phase2_diagnostics"]["candidate_count"] == 14
    assert (
        result["benchmark_metrics"]["phase2_diagnostics"]["candidate_count_basis"]
        == "phase2_diagnostics_fallback"
    )
    assert result["benchmark_metrics"]["phase2_diagnostics"]["bridge_candidate_count"] == 0
