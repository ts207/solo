from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.io.utils import ensure_dir, write_parquet
from project.scripts.run_golden_synthetic_discovery import run_golden_synthetic_discovery


class _Completed:
    def __init__(self, returncode: int = 0):
        self.returncode = returncode
        self.stdout = ""
        self.stderr = ""


def test_golden_synthetic_discovery_workflow_writes_summary(tmp_path: Path, monkeypatch) -> None:
    def _fake_runner(*, data_root: Path, argv: list[str]):
        run_id = argv[argv.index("--run_id") + 1]
        out_dir = data_root / "reports" / "phase2" / run_id
        ensure_dir(out_dir)
        write_parquet(
            pd.DataFrame([{"event_type": "CROSS_VENUE_DESYNC", "candidate_id": "cand-1"}]),
            out_dir / "phase2_candidates.parquet",
        )
        (out_dir / "phase2_diagnostics.json").write_text(
            json.dumps(
                {
                    "discovery_profile": "synthetic",
                    "hypotheses_generated": 12,
                    "bridge_candidates_rows": 1,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        return _Completed(0)

    monkeypatch.setattr(
        "project.scripts.run_golden_synthetic_discovery.validate_detector_truth",
        lambda **kwargs: {"passed": True, "event_reports": [{"event_type": "CROSS_VENUE_DESYNC"}]},
    )

    payload = run_golden_synthetic_discovery(
        root=tmp_path,
        config_path=Path("project/configs/golden_synthetic_discovery.yaml"),
        pipeline_runner=_fake_runner,
    )
    summary_path = tmp_path / "reliability" / "golden_synthetic_discovery_summary.json"

    assert payload["workflow_id"] == "golden_synthetic_discovery_v1"
    assert summary_path.exists()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["truth_validation"]["passed"] is True
    assert summary["candidate_summary"]["candidate_rows"] == 1
    assert summary["search_engine_diagnostics"]["discovery_profile"] == "synthetic"


def test_golden_synthetic_discovery_applies_narrowing_overrides(
    tmp_path: Path, monkeypatch
) -> None:
    captured: dict[str, object] = {}

    def _fake_runner(*, data_root: Path, argv: list[str]):
        captured["argv"] = list(argv)
        run_id = argv[argv.index("--run_id") + 1]
        out_dir = data_root / "reports" / "phase2" / run_id
        ensure_dir(out_dir)
        write_parquet(pd.DataFrame(), out_dir / "phase2_candidates.parquet")
        (out_dir / "phase2_diagnostics.json").write_text(
            json.dumps({"discovery_profile": "synthetic", "search_budget": 32}, indent=2),
            encoding="utf-8",
        )
        return _Completed(0)

    monkeypatch.setattr(
        "project.scripts.run_golden_synthetic_discovery.validate_detector_truth",
        lambda **kwargs: {"passed": True, "event_reports": [{"event_type": "FND_DISLOC"}]},
    )

    config_path = tmp_path / "fast.yaml"
    config_path.write_text(
        "\n".join(
            [
                "workflow_id: fast_test",
                "run_id: fast_run",
                "symbols: BTCUSDT",
                "start_date: 2026-01-01",
                "end_date: 2026-01-14",
                "events: [FND_DISLOC]",
                "templates: [continuation]",
                "entry_lags: [1, 2]",
                "search_budget: 32",
                "required_outputs:",
                "  - synthetic/{run_id}/synthetic_generation_manifest.json",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    payload = run_golden_synthetic_discovery(
        root=tmp_path,
        config_path=config_path,
        pipeline_runner=_fake_runner,
        overrides={"search_min_n": 4},
    )

    argv = captured["argv"]
    assert "--events" in argv
    assert "FND_DISLOC" in argv
    assert "--templates" in argv
    assert "continuation" in argv
    assert "--entry_lags" in argv
    assert "--search_budget" in argv
    assert "32" in argv
    assert "--search_min_n" in argv
    assert "4" in argv
    assert payload["selection"]["events"] == ["FND_DISLOC"]
    assert payload["selection"]["search_budget"] == 32
    assert payload["required_outputs"] == ["synthetic/fast_run/synthetic_generation_manifest.json"]
