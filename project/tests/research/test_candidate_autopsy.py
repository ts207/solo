from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.research import candidate_autopsy


def _write_inputs(data_root: Path, run_id: str, candidate_id: str) -> None:
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
    ).to_parquet(trace_dir / f"{candidate_id}_traces.parquet", index=False)
    (trace_dir / f"{candidate_id}_traces.json").write_text(
        json.dumps({"row_count": 1, "status": "extracted"}),
        encoding="utf-8",
    )

    regime_dir = data_root / "reports" / "regime" / run_id
    regime_dir.mkdir(parents=True)
    (regime_dir / f"{candidate_id}_year_split.json").write_text(
        json.dumps(
            {
                "status": "fail",
                "classification": "year_conditional",
                "concentration": {"max_pnl_share": 0.6393, "max_pnl_year": 2022},
            }
        ),
        encoding="utf-8",
    )

    specificity_dir = data_root / "reports" / "specificity" / run_id
    specificity_dir.mkdir(parents=True)
    (specificity_dir / f"{candidate_id}_specificity.json").write_text(
        json.dumps(
            {
                "status": "fail",
                "classification": "context_proxy",
                "reason": "base does not beat context-only control",
            }
        ),
        encoding="utf-8",
    )

    reproduction_dir = data_root / "reports" / "reproduction" / run_id
    reproduction_dir.mkdir(parents=True)
    (reproduction_dir / "governed_reproduction.json").write_text(
        json.dumps({"status": "pass"}),
        encoding="utf-8",
    )

    results_dir = data_root / "reports" / "results"
    results_dir.mkdir(parents=True)
    (results_dir / "results_index.json").write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "run_id": run_id,
                        "candidate_id": candidate_id,
                        "mechanism_id": "forced_flow_reversal",
                        "event_id": "PRICE_DOWN_OI_DOWN",
                        "template_id": "mean_reversion",
                        "direction": "long",
                        "horizon_bars": 24,
                        "symbol": "",
                        "decision": "park",
                        "decision_reason": "context_proxy_and_year_pnl_concentration_2022",
                        "nearby_attempt_count": 13,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )


def test_candidate_autopsy_builds_park_decision(monkeypatch, tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    run_id = "run"
    candidate_id = "cand"
    _write_inputs(data_root, run_id, candidate_id)

    def fake_doctor(**_kwargs):
        return {"status": "validate_ready"}

    monkeypatch.setattr(candidate_autopsy, "build_discover_doctor_report", fake_doctor)

    report = candidate_autopsy.build_candidate_autopsy(
        run_id=run_id,
        candidate_id=candidate_id,
        data_root=data_root,
    )

    assert report["schema_version"] == "candidate_autopsy_v1"
    assert report["mechanism_id"] == "forced_flow_reversal"
    assert report["event_id"] == "PRICE_DOWN_OI_DOWN"
    assert report["template_id"] == "mean_reversion"
    assert report["decision"] == "park"
    assert report["primary_failure_reason"] == "context_proxy_and_year_pnl_concentration_2022"
    assert report["evidence"]["discover_doctor_status"] == "validate_ready"
    assert report["evidence"]["governed_reproduction_status"] == "pass"
    assert report["evidence"]["nearby_attempt_count"] == 13
    assert report["evidence"]["trace_rows"] == 1
    assert report["evidence"]["trace_mean_net_bps"] == 98.0
    assert report["evidence"]["year_split_status"] == "fail"
    assert report["evidence"]["specificity_status"] == "fail"
    assert report["evidence_class"] == "parked_candidate"
    assert "drop_2022_after_result" in report["forbidden_rescue_actions"]
    assert "define ex-ante crisis/high-vol regime thesis" in report["conditions_to_reopen"]


def test_candidate_autopsy_builds_kill_decision_for_negative_reproduction(
    monkeypatch, tmp_path: Path
) -> None:
    data_root = tmp_path / "data"
    run_id = "run"
    candidate_id = "hyp_oi"
    phase2 = data_root / "reports" / "phase2" / run_id
    phase2.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "candidate_id": candidate_id,
                "event_type": "OI_FLUSH",
                "rule_template": "exhaustion_reversal",
                "direction": "long",
                "horizon": "24b",
                "symbol": "BTCUSDT",
            }
        ]
    ).to_parquet(phase2 / "phase2_candidates.parquet", index=False)
    reproduction_dir = data_root / "reports" / "reproduction" / run_id
    reproduction_dir.mkdir(parents=True)
    (reproduction_dir / "governed_reproduction.json").write_text(
        json.dumps(
            {
                "status": "fail",
                "decision": "kill",
                "reason": "current governed reproduction failed one or more falsification checks",
            }
        ),
        encoding="utf-8",
    )
    results_dir = data_root / "reports" / "results"
    results_dir.mkdir(parents=True)
    (results_dir / "results_index.json").write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "run_id": run_id,
                        "candidate_id": candidate_id,
                        "mechanism_id": "forced_flow_reversal",
                        "event_id": "OI_FLUSH",
                        "template_id": "exhaustion_reversal",
                        "direction": "long",
                        "horizon_bars": 24,
                        "decision": "kill",
                        "decision_reason": "governed_reproduction_negative_t_stat",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(candidate_autopsy, "build_discover_doctor_report", lambda **_kwargs: {"status": "validate_ready"})

    report = candidate_autopsy.build_candidate_autopsy(
        run_id=run_id,
        candidate_id=candidate_id,
        data_root=data_root,
    )

    assert report["decision"] == "kill"
    assert report["evidence_class"] == "killed_candidate"
    assert report["primary_failure_reason"] == "governed_reproduction_negative_t_stat"
    assert report["conditions_to_reopen"] == [
        "detector/materialization bug found",
        "new data source changes OI_FLUSH definition",
    ]
    assert "validate despite negative reproduction" in report["forbidden_rescue_actions"]


def test_run_candidate_autopsy_writes_json_and_markdown(monkeypatch, tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    run_id = "run"
    candidate_id = "cand"
    _write_inputs(data_root, run_id, candidate_id)
    monkeypatch.setattr(
        candidate_autopsy,
        "build_discover_doctor_report",
        lambda **_kwargs: {"status": "validate_ready"},
    )

    report = candidate_autopsy.run_candidate_autopsy(
        run_id=run_id,
        candidate_id=candidate_id,
        data_root=data_root,
    )

    base = data_root / "reports" / "autopsy" / run_id
    payload = json.loads((base / "cand_autopsy.json").read_text(encoding="utf-8"))
    markdown = (base / "cand_autopsy.md").read_text(encoding="utf-8")
    assert payload["decision"] == report["decision"]
    assert "## Evidence" in markdown
    assert "`park`" in markdown
