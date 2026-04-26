from __future__ import annotations

import importlib.util
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from project.tests.conftest import PROJECT_ROOT


def _load_runner_module():
    script_path = PROJECT_ROOT / "scripts" / "run_benchmark_matrix.py"
    spec = importlib.util.spec_from_file_location("run_benchmark_matrix", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_benchmark_matrix_dry_run_writes_manifest(tmp_path, monkeypatch):
    module = _load_runner_module()
    data_root = tmp_path / "data"
    out_dir = data_root / "reports" / "perf_matrix"
    matrix_path = tmp_path / "matrix.yaml"
    matrix_path.write_text(
        "version: 1\n"
        "matrix_id: unit_matrix\n"
        "defaults:\n"
        "  mode: research\n"
        "  flags:\n"
        "    run_hypothesis_generator: 0\n"
        "runs:\n"
        "  - run_id: unit_run_1\n"
        "    symbols: BTCUSDT\n"
        "    start: 2024-01-01\n"
        "    end: 2024-01-02\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(module, "DATA_ROOT", data_root)
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_benchmark_matrix.py",
            "--matrix",
            str(matrix_path),
            "--out_dir",
            str(out_dir),
        ],
    )
    rc = module.main()
    assert rc == 0

    manifest_path = out_dir / "matrix_manifest.json"
    assert manifest_path.exists()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["matrix_id"] == "unit_matrix"
    assert payload["execute"] is False
    assert payload["results"][0]["status"] == "dry_run"
    assert "--run_id" in payload["results"][0]["command"]
    assert "unit_run_1" in payload["results"][0]["command"]
    summary_path = out_dir / "benchmark_summary.json"
    review_path = out_dir / "benchmark_review.json"
    assert summary_path.exists()
    assert review_path.exists()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["matrix_id"] == "unit_matrix"
    assert summary["status_counts"]["dry_run"] == 1


def test_benchmark_matrix_defaults_to_canonical_benchmark_root(tmp_path, monkeypatch):
    module = _load_runner_module()
    data_root = tmp_path / "data"
    matrix_path = tmp_path / "matrix_default.yaml"
    matrix_path.write_text(
        "version: 1\n"
        "matrix_id: canonical_matrix\n"
        "runs:\n"
        "  - run_id: canonical_run_1\n"
        "    symbols: BTCUSDT\n"
        "    start: 2024-01-01\n"
        "    end: 2024-01-02\n",
        encoding="utf-8",
    )

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)

    monkeypatch.setattr(module, "DATA_ROOT", data_root)
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))
    monkeypatch.setattr(module, "datetime", FixedDatetime)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_benchmark_matrix.py",
            "--matrix",
            str(matrix_path),
        ],
    )
    rc = module.main()
    assert rc == 0

    expected_dir = data_root / "reports" / "benchmarks" / "canonical_matrix_20260101_000000"
    assert expected_dir.exists()
    assert (expected_dir / "matrix_manifest.json").exists()


def test_benchmark_matrix_execute_records_success(tmp_path, monkeypatch):
    module = _load_runner_module()
    data_root = tmp_path / "data"
    out_dir = data_root / "reports" / "perf_matrix_exec"
    matrix_path = tmp_path / "matrix_exec.yaml"
    matrix_path.write_text(
        "version: 1\n"
        "matrix_id: exec_matrix\n"
        "runs:\n"
        "  - run_id: exec_run_1\n"
        "    symbols: BTCUSDT\n"
        "    start: 2024-01-01\n"
        "    end: 2024-01-02\n",
        encoding="utf-8",
    )

    fake_run_all = tmp_path / "fake_run_all.py"
    fake_run_all.write_text("raise SystemExit(0)\n", encoding="utf-8")

    monkeypatch.setattr(module, "DATA_ROOT", data_root)
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))

    def fake_matrix_report(**kwargs):
        out_path = Path(kwargs["out_dir"]) / "research_run_matrix_summary.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps({"baseline_run_id": kwargs["baseline_run_id"]}),
            encoding="utf-8",
        )
        (out_path.parent / "research_run_matrix_summary.md").write_text(
            "# Research Run Matrix Summary\n",
            encoding="utf-8",
        )
        return out_path

    monkeypatch.setattr(module, "write_run_matrix_summary_report", fake_matrix_report)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_benchmark_matrix.py",
            "--matrix",
            str(matrix_path),
            "--run_all",
            str(fake_run_all),
            "--python",
            sys.executable,
            "--execute",
            "1",
            "--out_dir",
            str(out_dir),
        ],
    )
    rc = module.main()
    assert rc == 0

    payload = json.loads((out_dir / "matrix_manifest.json").read_text(encoding="utf-8"))
    assert payload["execute"] is True
    assert payload["results"][0]["status"] == "success"
    assert payload["results"][0]["returncode"] == 0
    summary = json.loads((out_dir / "benchmark_summary.json").read_text(encoding="utf-8"))
    assert summary["status_counts"]["success"] == 1
    review = json.loads((out_dir / "benchmark_review.json").read_text(encoding="utf-8"))
    assert review["schema_version"] == "benchmark_review_v1"
    assert review["slices"][0]["benchmark_status"] == "coverage_limited"
    assert payload["research_run_matrix_summary_json"].endswith(
        "research_run_matrix_summary.json"
    )
    assert (out_dir / "research_run_matrix_summary.json").exists()
    assert (out_dir / "research_run_matrix_summary.md").exists()
    assert (out_dir / "canonical_path_report.json").exists()
    assert (out_dir / "canonical_path_report.md").exists()


def test_canonical_path_report_uses_artifact_candidate_counts():
    module = _load_runner_module()

    report = module.build_canonical_path_report(
        matrix_id="unit",
        results=[
            {
                "run_id": "slice_D",
                "slice_id": "slice",
                "mode_id": "D",
                "status": "success",
                "benchmark_metrics": {
                    "candidate_count": 0,
                    "candidate_count_basis": "phase2_candidates_parquet",
                    "phase2_diagnostics": {
                        "candidate_count": 14,
                        "candidate_count_basis": "phase2_diagnostics_fallback",
                    },
                    "top10": {"promotion_density": 0.0},
                },
            },
        ],
    )

    row = report["slices"][0]
    assert row["mode_id"] == "D"
    assert row["candidate_count"] == 0
    assert row["candidate_count_basis"] == "phase2_candidates_parquet"
    assert row["has_phase2_diagnostics"] is True
    assert row["verdict"] == "canonical_no_final_candidates"
    assert report["summary"]["noncanonical_mode_slices"] == 0


def test_benchmark_matrix_execute_emits_post_run_reports(tmp_path, monkeypatch):
    module = _load_runner_module()
    data_root = tmp_path / "data"
    out_dir = data_root / "reports" / "perf_matrix_reports"
    matrix_path = tmp_path / "matrix_reports.yaml"
    matrix_path.write_text(
        "version: 1\n"
        "matrix_id: report_matrix\n"
        "runs:\n"
        "  - run_id: report_run_1\n"
        "    symbols: BTCUSDT\n"
        "    start: 2024-01-01\n"
        "    end: 2024-01-02\n"
        "    timeframe: 5m\n"
        "    fixture_event_registry: tmp/fixture_events.parquet\n"
        "    post_reports:\n"
        "      live_foundation:\n"
        "        enabled: true\n"
        "        config: spec/benchmarks/btc_live_foundation.yaml\n"
        "      context_comparison:\n"
        "        enabled: true\n"
        "        search_space_path: spec/search/search_benchmark_fnd_disloc.yaml\n",
        encoding="utf-8",
    )

    fake_run_all = tmp_path / "fake_run_all.py"
    fake_run_all.write_text("raise SystemExit(0)\n", encoding="utf-8")

    monkeypatch.setattr(module, "DATA_ROOT", data_root)
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))

    def fake_matrix_report(**kwargs):
        out_path = Path(kwargs["out_dir"]) / "research_run_matrix_summary.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps({"baseline_run_id": kwargs["baseline_run_id"]}),
            encoding="utf-8",
        )
        (out_path.parent / "research_run_matrix_summary.md").write_text(
            "# Research Run Matrix Summary\n",
            encoding="utf-8",
        )
        return out_path

    def fake_live_report(**kwargs):
        path = (
            data_root
            / "reports"
            / "live_foundation"
            / kwargs["run_id"]
            / "perp"
            / kwargs["symbol"]
            / kwargs["timeframe"]
            / "live_data_foundation_report.json"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"schema_version": "live_data_foundation_report_v1"}), encoding="utf-8"
        )
        return path

    def fake_context_payload(**kwargs):
        assert kwargs["event_registry_override"] == "tmp/fixture_events.parquet"
        return {
            "schema_version": "context_mode_comparison_v1",
            "run_id": kwargs["run_id"],
            "symbols": kwargs["symbols"],
            "timeframe": kwargs["timeframe"],
            "hard_label": {"evaluated_rows": 4, "selected": {"hypothesis_id": "h1", "valid": True}},
            "confidence_aware": {
                "evaluated_rows": 4,
                "selected": {"hypothesis_id": "h1", "valid": True},
            },
            "selection_changed": False,
            "selection_outcome_changed": False,
            "delta": {"n": -5.0},
        }

    def fake_context_report(*, out_path, comparison):
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(comparison), encoding="utf-8")
        return out_path

    monkeypatch.setattr(module, "write_live_data_foundation_report", fake_live_report)
    monkeypatch.setattr(module, "build_context_mode_comparison_payload", fake_context_payload)
    monkeypatch.setattr(module, "write_context_mode_comparison_report", fake_context_report)
    monkeypatch.setattr(module, "write_run_matrix_summary_report", fake_matrix_report)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_benchmark_matrix.py",
            "--matrix",
            str(matrix_path),
            "--run_all",
            str(fake_run_all),
            "--python",
            sys.executable,
            "--execute",
            "1",
            "--out_dir",
            str(out_dir),
        ],
    )

    rc = module.main()
    assert rc == 0

    payload = json.loads((out_dir / "matrix_manifest.json").read_text(encoding="utf-8"))
    generated = payload["results"][0]["generated_reports"]
    assert "live_foundation" in generated
    assert "context_mode_comparison" in generated

    summary = json.loads((out_dir / "benchmark_summary.json").read_text(encoding="utf-8"))
    assert "generated_reports" in summary["slices"][0]
    assert "live_foundation" in summary["slices"][0]["generated_reports"]
    review = json.loads((out_dir / "benchmark_review.json").read_text(encoding="utf-8"))
    assert review["slices"][0]["benchmark_status"] == "informative"
    assert review["slices"][0]["context_comparison_present"] is True
    assert payload["research_run_matrix_summary_json"].endswith(
        "research_run_matrix_summary.json"
    )


def test_benchmark_matrix_execute_failed_runs_still_emit_matrix_summary(tmp_path, monkeypatch):
    module = _load_runner_module()
    data_root = tmp_path / "data"
    out_dir = data_root / "reports" / "perf_matrix_failed"
    matrix_path = tmp_path / "matrix_failed.yaml"
    matrix_path.write_text(
        "version: 1\n"
        "matrix_id: failed_matrix\n"
        "runs:\n"
        "  - run_id: failed_run_1\n"
        "    symbols: BTCUSDT\n"
        "    start: 2024-01-01\n"
        "    end: 2024-01-02\n",
        encoding="utf-8",
    )

    fake_run_all = tmp_path / "fake_run_all_failed.py"
    fake_run_all.write_text("raise SystemExit(1)\n", encoding="utf-8")

    monkeypatch.setattr(module, "DATA_ROOT", data_root)
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))

    def fake_matrix_report(**kwargs):
        out_path = Path(kwargs["out_dir"]) / "research_run_matrix_summary.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps({"baseline_run_id": kwargs["baseline_run_id"]}),
            encoding="utf-8",
        )
        (out_path.parent / "research_run_matrix_summary.md").write_text(
            "# Research Run Matrix Summary\n",
            encoding="utf-8",
        )
        return out_path

    monkeypatch.setattr(module, "write_run_matrix_summary_report", fake_matrix_report)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_benchmark_matrix.py",
            "--matrix",
            str(matrix_path),
            "--run_all",
            str(fake_run_all),
            "--python",
            sys.executable,
            "--execute",
            "1",
            "--fail_fast",
            "0",
            "--out_dir",
            str(out_dir),
        ],
    )
    rc = module.main()
    assert rc == 1

    payload = json.loads((out_dir / "matrix_manifest.json").read_text(encoding="utf-8"))
    assert payload["results"][0]["status"] == "failed"
    assert payload["research_run_matrix_summary_json"].endswith(
        "research_run_matrix_summary.json"
    )
    assert payload["certification_passed"] is False

    cert = json.loads((out_dir / "benchmark_certification.json").read_text(encoding="utf-8"))
    assert cert["passed"] is False
    assert any(issue["type"] == "execution_failures" for issue in cert["issues"])
