import pytest
import pandas as pd
import json
import sys
import yaml
from pathlib import Path
from types import SimpleNamespace
from project.research.campaign_controller import (
    CampaignController,
    CampaignConfig,
    CampaignSummary,
    CampaignMemoryIntegrityError,
)
from project.research.knowledge.memory import (
    ensure_memory_store,
    read_memory_table,
    write_memory_table,
)


@pytest.fixture
def test_env(tmp_path):
    reg_dir = tmp_path / "registries"
    reg_dir.mkdir()

    (reg_dir / "events.yaml").write_text(
        yaml.dump(
            {
                "events": {
                    "E1": {"enabled": True, "family": "F1", "instrument_classes": ["crypto"]},
                    "E2": {"enabled": True, "family": "F1", "instrument_classes": ["crypto"]},
                    "E3": {"enabled": True, "family": "F2", "instrument_classes": ["crypto"]},
                    "E4": {"enabled": True, "family": "F2", "instrument_classes": ["crypto"]},
                    "VOL_SHOCK": {
                        "enabled": True,
                        "family": "VOL",
                        "instrument_classes": ["crypto"],
                    },
                    "ZSCORE_STRETCH": {
                        "enabled": True,
                        "family": "STAT",
                        "instrument_classes": ["crypto"],
                    },
                }
            }
        )
    )
    (reg_dir / "templates.yaml").write_text(
        yaml.dump(
            {
                "templates": {
                    "continuation": {"enabled": True, "supports_trigger_types": ["EVENT"]},
                    "mean_reversion": {"enabled": True, "supports_trigger_types": ["EVENT"]},
                }
            }
        )
    )
    (reg_dir / "contexts.yaml").write_text(
        yaml.dump(
            {
                "context_dimensions": {
                    "vol_regime": {"allowed_values": ["low", "high"]},
                    "carry_state": {"allowed_values": ["positive", "negative", "neutral"]},
                    "session": {"allowed_values": ["open", "close"]},
                }
            }
        )
    )
    (reg_dir / "search_limits.yaml").write_text(
        yaml.dump({"limits": {"max_events_per_run": 10, "max_templates_per_run": 10}})
    )
    (reg_dir / "states.yaml").write_text(yaml.dump({"states": {}}))
    (reg_dir / "features.yaml").write_text(yaml.dump({"features": {}}))
    (reg_dir / "detectors.yaml").write_text(yaml.dump({"detector_ownership": {}}))

    data_root = tmp_path / "data"
    data_root.mkdir()

    config = CampaignConfig(program_id="test_campaign", max_runs=2)
    return CampaignController(config, data_root, reg_dir)


def test_campaign_request_generation(test_env):
    controller = test_env
    req = controller._propose_next_request()
    assert req is not None
    assert req["program_id"] == "test_campaign"
    assert len(req["trigger_space"]["events"]["include"]) > 0


def test_frontier_tracking(test_env, tmp_path):
    controller = test_env
    # Mock a ledger where E1 is tested but E2 is not (F1 is partial)
    # E3 and E4 are untested (F2 is not yet started)
    ledger_data = [
        {
            "hypothesis_id": "h1",
            "trigger_payload": json.dumps({"event_id": "E1"}),
            "eval_status": "evaluated",
            "expectancy": 0.1,
            "run_id": "run1",
        }
    ]
    pd.DataFrame(ledger_data).to_parquet(controller.ledger_path)

    summary = controller._update_campaign_stats()
    assert summary.total_runs == 1

    frontier = json.loads(controller.frontier_path.read_text())
    assert "E1" not in frontier["untested_events"]
    assert "E2" in frontier["untested_events"]
    assert "E3" in frontier["untested_events"]

    # F1 is partially explored (E1 tested, E2 not)
    assert "F1" in frontier["partially_explored_families"]
    # F2 is NOT partially explored because tested count is 0
    assert "F2" not in frontier["partially_explored_families"]


def test_campaign_request_skips_events_seen_in_json_trigger_payload(test_env):
    controller = test_env
    pd.DataFrame(
        [
            {
                "hypothesis_id": "h1",
                "trigger_payload": json.dumps({"event_id": "E1"}),
                "eval_status": "evaluated",
                "expectancy": 0.1,
                "run_id": "run1",
            }
        ]
    ).to_parquet(controller.ledger_path)

    req = controller._propose_next_request()

    assert req is not None
    assert "E1" not in req["trigger_space"]["events"]["include"]


def test_execute_pipeline_invokes_run_all(monkeypatch, test_env, tmp_path):
    controller = test_env
    captured = {}

    def _fake_run(cmd, check, cwd):
        captured["cmd"] = list(cmd)
        captured["check"] = check
        captured["cwd"] = cwd

    monkeypatch.setattr("project.research.campaign_controller.subprocess.run", _fake_run)

    config_path = tmp_path / "experiment.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "program_id": "test_campaign",
                "instrument_scope": {
                    "symbols": ["BTCUSDT"],
                    "start": "2022-01-01",
                    "end": "2023-12-31",
                    "timeframe": "5m",
                },
            }
        ),
        encoding="utf-8",
    )
    controller._execute_pipeline(config_path, "campaign_run_1")

    assert captured["cmd"][:3] == [sys.executable, "-m", "project.pipelines.run_all"]
    assert captured["check"] is True
    assert "--symbols" in captured["cmd"]
    assert "BTCUSDT" in captured["cmd"]
    assert "--start" in captured["cmd"]
    assert "2022-01-01" in captured["cmd"]
    assert "--end" in captured["cmd"]
    assert "2023-12-31" in captured["cmd"]
    assert "--timeframes" in captured["cmd"]
    assert "5m" in captured["cmd"]


def test_run_campaign_persists_proposals_and_evidence_ledger(monkeypatch, test_env):
    controller = test_env
    emitted = {"count": 0}
    request = controller._build_proposal(
        events=["VOL_SHOCK"],
        templates=["mean_reversion"],
        horizons=[12],
        description="test frontier request",
        promotion_enabled=False,
        date_scope=("2022-01-01", "2023-12-31"),
    )

    def _fake_next_request():
        if emitted["count"] == 0:
            emitted["count"] += 1
            return request
        return None

    monkeypatch.setattr(controller, "_propose_next_request", _fake_next_request)
    monkeypatch.setattr(
        "project.research.campaign_controller.build_experiment_plan",
        lambda *args, **kwargs: SimpleNamespace(
            program_id="test_campaign",
            estimated_hypothesis_count=1,
            required_detectors=["VolShockRelaxationDetector"],
            required_features=[],
            required_states=[],
        ),
    )

    def _fake_execute(config_path, run_id, *, command=None):
        run_dir = controller.data_root / "runs" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "run_manifest.json").write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "program_id": "test_campaign",
                    "status": "success",
                    "terminal_status": "completed",
                    "mechanical_outcome": "success",
                    "planned_stages": [],
                }
            ),
            encoding="utf-8",
        )
        phase2_dir = controller.data_root / "reports" / "phase2" / run_id
        phase2_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([{"run_id": run_id}]).iloc[0:0].to_parquet(
            phase2_dir / "phase2_candidates.parquet",
            index=False,
        )

    monkeypatch.setattr(controller, "_execute_pipeline", _fake_execute)
    monkeypatch.setattr(
        controller,
        "_update_campaign_stats",
        lambda: CampaignSummary(program_id="test_campaign", total_generated=1),
    )
    monkeypatch.setattr(controller, "_should_halt", lambda summary: False)

    controller.run_campaign()

    proposals = read_memory_table("test_campaign", "proposals", data_root=controller.data_root)
    evidence = read_memory_table("test_campaign", "evidence_ledger", data_root=controller.data_root)

    assert len(proposals) == 1
    assert proposals.iloc[0]["status"] == "executed"
    assert len(evidence) == 1
    assert evidence.iloc[0]["run_id"].startswith("run_1_")


def test_step_repair_uses_event_from_failure_detail(test_env):
    controller = test_env
    proposal = controller._step_repair(
        {
            "latest_reflection": {},
            "superseded_stages": set(),
            "next_actions": {
                "repair": [
                    {
                        "stage": "build_event_registry",
                        "failure_detail": "detector truth broke for VOL_SHOCK under audit",
                        "proposed_scope": {"stage": "build_event_registry"},
                    }
                ]
            },
        }
    )

    assert proposal is not None
    assert proposal["trigger_space"]["events"]["include"] == ["VOL_SHOCK"]


def test_step_repair_uses_stage_default_before_global_fallback(test_env):
    controller = test_env
    proposal = controller._step_repair(
        {
            "latest_reflection": {},
            "superseded_stages": set(),
            "next_actions": {
                "repair": [
                    {
                        "stage": "build_event_registry",
                        "failure_detail": "",
                        "proposed_scope": {"stage": "build_event_registry"},
                    }
                ]
            },
        }
    )

    assert proposal is not None
    assert proposal["trigger_space"]["events"]["include"] == ["VOL_SHOCK"]


def test_propose_next_request_honors_repair_focus_with_actionable_queue(test_env):
    controller = test_env
    paths = ensure_memory_store("test_campaign", data_root=controller.data_root)
    paths.belief_state.write_text(
        json.dumps(
            {
                "current_focus": "repair_pipeline",
                "avoid_regions": [],
                "promising_regions": [],
                "open_repairs": [
                    {
                        "stage": "build_event_registry",
                        "failure_class": "mechanical",
                        "failure_detail": "detector truth broke for VOL_SHOCK",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    paths.next_actions.write_text(
        json.dumps(
            {
                "repair": [
                    {
                        "reason": "mechanical failure detected",
                        "priority": "high",
                        "failure_detail": "detector truth broke for VOL_SHOCK",
                        "proposed_scope": {
                            "stage": "build_event_registry",
                            "failure_class": "mechanical",
                            "failure_detail": "detector truth broke for VOL_SHOCK",
                        },
                    }
                ],
                "exploit": [],
                "explore_adjacent": [],
                "hold": [],
            }
        ),
        encoding="utf-8",
    )

    proposal = controller._propose_next_request()

    assert proposal is not None
    assert proposal["trigger_space"]["events"]["include"] == ["VOL_SHOCK"]
    assert proposal["promotion"]["enabled"] is False


def test_propose_next_request_fails_closed_when_repair_focus_has_no_action(test_env):
    controller = test_env
    paths = ensure_memory_store("test_campaign", data_root=controller.data_root)
    paths.belief_state.write_text(
        json.dumps(
            {
                "current_focus": "repair_pipeline",
                "avoid_regions": [],
                "promising_regions": [],
                "open_repairs": [],
            }
        ),
        encoding="utf-8",
    )
    paths.next_actions.write_text(
        json.dumps({"repair": [], "exploit": [], "explore_adjacent": [], "hold": []}),
        encoding="utf-8",
    )

    with pytest.raises(CampaignMemoryIntegrityError, match="current_focus=repair_pipeline"):
        controller._propose_next_request()


def test_context_for_proposal_uses_registry_dimensions(test_env):
    controller = test_env
    assert controller._context_for_proposal() == {
        "vol_regime": ["low", "high"],
        "carry_state": ["funding_pos", "funding_neg"],
    }


def test_step_explore_adjacent_skips_exact_tested_scope_and_preserves_scope_fields(test_env):
    controller = test_env
    controller.config.enable_context_conditioning = False
    write_memory_table(
        "test_campaign",
        "tested_regions",
        pd.DataFrame(
            [
                {
                    "trigger_type": "EVENT",
                    "event_type": "VOL_SHOCK",
                    "template_id": "mean_reversion",
                    "direction": "long",
                    "horizon": "24b",
                    "entry_lag": 1,
                    "context_json": "{}",
                }
            ]
        ),
        data_root=controller.data_root,
    )

    proposal = controller._step_explore_adjacent(
        {
            "next_actions": {
                "explore_adjacent": [
                    {
                        "proposed_scope": {
                            "trigger_type": "EVENT",
                            "event_type": "VOL_SHOCK",
                            "template_id": "mean_reversion",
                            "direction": "long",
                            "horizon": "24b",
                            "entry_lag": 1,
                        }
                    },
                    {
                        "proposed_scope": {
                            "trigger_type": "EVENT",
                            "event_type": "VOL_SHOCK",
                            "template_id": "mean_reversion",
                            "direction": "short",
                            "horizon": "24b",
                            "entry_lag": 2,
                        }
                    },
                ]
            }
        }
    )

    assert proposal is not None
    assert proposal["trigger_space"]["events"]["include"] == ["VOL_SHOCK"]
    assert proposal["templates"]["include"] == ["mean_reversion"]
    assert proposal["evaluation"]["horizons_bars"] == [24]
    assert proposal["evaluation"]["directions"] == ["short"]
    assert proposal["evaluation"]["entry_lags"] == [2]


def test_read_memory_fails_closed_on_corrupted_json(tmp_path):
    reg_dir = tmp_path / "registries"
    reg_dir.mkdir()
    for name, payload in {
        "events.yaml": {"events": {}},
        "templates.yaml": {"templates": {}},
        "contexts.yaml": {"context_dimensions": {}},
        "search_limits.yaml": {"limits": {}},
        "states.yaml": {"states": {}},
        "features.yaml": {"features": {}},
        "detectors.yaml": {"detector_ownership": {}},
    }.items():
        (reg_dir / name).write_text(yaml.dump(payload), encoding="utf-8")

    data_root = tmp_path / "data"
    paths = ensure_memory_store("test_campaign", data_root=data_root)
    paths.belief_state.write_text("{not-json", encoding="utf-8")

    controller = CampaignController(CampaignConfig(program_id="test_campaign"), data_root, reg_dir)

    with pytest.raises(CampaignMemoryIntegrityError, match="belief_state.json"):
        controller._read_memory()
