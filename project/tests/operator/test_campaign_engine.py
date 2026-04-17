from pathlib import Path

import pandas as pd
import pytest
import yaml

import project.operator.campaign_engine as campaign_engine
from project.core.exceptions import DataIntegrityError
from project.research.knowledge.memory import ensure_memory_store, read_memory_table


def test_run_campaign_executes_multiple_cycles(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    proposal_path = tmp_path / "initial.yaml"
    proposal_path.write_text(
        yaml.safe_dump(
            {
                "program_id": "btc_campaign",
                "start": "2021-01-01",
                "end": "2021-12-31",
                "symbols": ["BTCUSDT"],
                "timeframe": "5m",
                "hypothesis": {
                    "anchor": {"type": "event", "event_id": "VOL_SHOCK"},
                    "template": {"id": "mean_reversion"},
                    "direction": "short",
                    "horizon_bars": 12,
                    "entry_lag_bars": 1,
                },
                "search_spec": {},
            }
        ),
        encoding="utf-8",
    )
    campaign_spec = tmp_path / "campaign.yaml"
    campaign_spec.write_text(
        yaml.safe_dump(
            {
                "campaign_id": "camp1",
                "initial_proposal": str(proposal_path),
                "max_cycles": 3,
                "stop_conditions": {"max_fail_streak": 2},
            }
        ),
        encoding="utf-8",
    )

    ensure_memory_store("btc_campaign", data_root=data_root)
    run_counter = {"value": 0}

    def fake_issue_proposal(proposal_path, registry_root, data_root=None, run_id=None, plan_only=True, dry_run=False, check=False):
        run_counter["value"] += 1
        run_id = f"run_{run_counter['value']}"
        proposals = read_memory_table("btc_campaign", "proposals", data_root=data_root)
        proposals = pd.concat(
            [
                proposals,
                pd.DataFrame([
                    {
                        "proposal_id": f"proposal::{run_id}",
                        "program_id": "btc_campaign",
                        "run_id": run_id,
                        "proposal_path": str(proposal_path),
                    }
                ]),
            ],
            ignore_index=True,
        )
        from project.research.knowledge.memory import write_memory_table
        write_memory_table("btc_campaign", "proposals", proposals, data_root=data_root)
        return {"run_id": run_id, "program_id": "btc_campaign"}

    def fake_outputs(run_id, program_id=None, data_root=None):
        idx = int(run_id.split("_")[-1])
        metric = 1.8 if idx == 1 else 0.1
        diagnosis = "low_sample_power" if idx == 1 else "no_effect"
        return {
            "run_id": run_id,
            "program_id": "btc_campaign",
            "terminal_status": "completed",
            "verdict": "KEEP_RESEARCH",
            "candidate_count": 1 if idx == 1 else 0,
            "promoted_count": 0,
            "top_candidate": {"metric_value": metric, "label": "VOL_SHOCK / mean_reversion / short / 12"},
            "negative_result_diagnostics": {"diagnosis": diagnosis},
        }

    monkeypatch.setattr(campaign_engine, "issue_proposal", fake_issue_proposal)
    monkeypatch.setattr(campaign_engine, "write_operator_outputs_for_run", fake_outputs)

    report = campaign_engine.run_campaign(campaign_spec_path=campaign_spec, data_root=data_root, plan_only=False)
    assert report["executed_cycles"] == 2
    assert report["stop_reason"] == "decision_stop"
    assert Path(report["report_path"]).exists()


def test_load_latest_cycle_report_raises_on_malformed_json(tmp_path):
    paths = campaign_engine.CampaignPaths(
        root=tmp_path,
        proposals_dir=tmp_path / "proposals",
        reports_dir=tmp_path / "reports",
    )
    paths.reports_dir.mkdir(parents=True)
    (paths.reports_dir / "campaign_report.json").write_text("{", encoding="utf-8")

    with pytest.raises(DataIntegrityError, match="Failed to read campaign report"):
        campaign_engine._load_latest_cycle_report(paths)
