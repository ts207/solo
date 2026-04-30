from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.research import candidate_autopsy
from project.scripts import run_candidate_autopsy


def test_run_candidate_autopsy_script_writes_reports(monkeypatch, tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    run_id = "run"
    candidate_id = "cand"
    trace_dir = data_root / "reports" / "candidate_traces" / run_id
    trace_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "run_id": run_id,
                "candidate_id": candidate_id,
                "symbol": "BTCUSDT",
                "event_id": "PRICE_DOWN_OI_DOWN",
                "template_id": "mean_reversion",
                "context_key": "VOL_REGIME",
                "context_value": "HIGH",
                "direction": "long",
                "horizon_bars": 24,
                "event_ts": "2022-01-01T00:00:00Z",
                "entry_ts": "2022-01-01T00:05:00Z",
                "exit_ts": "2022-01-01T02:05:00Z",
                "entry_price": 100.0,
                "exit_price": 101.0,
                "gross_return_bps": 100.0,
                "cost_bps": 2.0,
                "net_return_bps": 98.0,
                "context_pass": True,
                "entry_lag_bars": 1,
                "source_artifact": "unit",
            }
        ]
    ).to_parquet(trace_dir / "cand_traces.parquet", index=False)
    (trace_dir / "cand_traces.json").write_text(
        json.dumps({"row_count": 1}),
        encoding="utf-8",
    )
    for report_name, payload in {
        "regime": {
            "status": "fail",
            "classification": "year_conditional",
            "concentration": {"max_pnl_share": 0.6393, "max_pnl_year": 2022},
        },
        "specificity": {"status": "review", "classification": "insufficient_trace_data"},
    }.items():
        report_dir = data_root / "reports" / report_name / run_id
        report_dir.mkdir(parents=True)
        suffix = "year_split" if report_name == "regime" else "specificity"
        (report_dir / f"cand_{suffix}.json").write_text(json.dumps(payload), encoding="utf-8")
    reproduction = data_root / "reports" / "reproduction" / run_id
    reproduction.mkdir(parents=True)
    (reproduction / "governed_reproduction.json").write_text(
        json.dumps({"status": "pass"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        candidate_autopsy,
        "build_discover_doctor_report",
        lambda **_kwargs: {"status": "validate_ready"},
    )

    exit_code = run_candidate_autopsy.main(
        [
            "--run-id",
            run_id,
            "--candidate-id",
            candidate_id,
            "--data-root",
            str(data_root),
        ]
    )

    base = data_root / "reports" / "autopsy" / run_id
    assert exit_code == 0
    assert (base / "cand_autopsy.json").exists()
    assert (base / "cand_autopsy.md").exists()
    payload = json.loads((base / "cand_autopsy.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == "candidate_autopsy_v1"
    assert payload["decision"] == "park"
