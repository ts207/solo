import json

import pandas as pd

from project.apps.chatgpt.handlers import get_operator_dashboard, render_operator_summary


def test_get_operator_dashboard_reads_memory_and_runs(tmp_path) -> None:
    program_id = "PROG_TEST"
    memory_root = tmp_path / "artifacts" / "experiments" / program_id / "memory"
    run_root = tmp_path / "runs" / "run_demo_001"
    phase2_root = tmp_path / "reports" / "phase2" / "run_demo_001"
    memory_root.mkdir(parents=True)
    run_root.mkdir(parents=True)
    phase2_root.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "run_id": "run_demo_001",
                "status": "executed",
                "issued_at": "2026-04-01T15:00:00+00:00",
                "objective_name": "retail_profitability",
                "promotion_profile": "research",
                "symbols": "BTCUSDT,ETHUSDT",
                "experiment_type": "discovery",
                "allowed_change_field": "trigger_key",
                "baseline_run_id": "",
                "decision": "exploit",
                "mutation_type": "refine",
                "plan_only": False,
                "dry_run": False,
                "returncode": 0,
                "proposal_path": "/tmp/proposal.yaml",
            }
        ]
    ).to_csv(memory_root / "proposals.csv", index=False)

    pd.DataFrame(
        [
            {
                "created_at": "2026-04-01T15:10:00+00:00",
                "run_id": "run_demo_001",
                "run_status": "success",
                "market_findings": "Liquidity shock edge survives costs.",
                "system_findings": "No mechanical issues detected.",
                "recommended_next_action": "Scale exploit lane.",
                "recommended_next_experiment": "Stress adjacent thresholds.",
                "confidence": 0.78,
            }
        ]
    ).to_csv(memory_root / "reflections.csv", index=False)

    pd.DataFrame(
        [
            {
                "updated_at": "2026-04-01T15:20:00+00:00",
                "run_id": "run_demo_001",
                "verdict": "keep_research",
                "recommended_next_action": "run follow-up sweep",
                "recommended_next_experiment": "trigger_key ±1",
                "terminal_status": "success",
                "promoted_count": 2,
                "candidate_count": 9,
                "negative_diagnosis": "",
            }
        ]
    ).to_csv(memory_root / "evidence_ledger.csv", index=False)

    (memory_root / "belief_state.json").write_text(
        json.dumps(
            {
                "current_focus": "repair funding drift",
                "promising_regions": ["BTCUSDT::liq_shock"],
                "avoid_regions": ["ETHUSDT::depth_collapse"],
                "open_repairs": ["late manifest gap"],
                "last_reflection_run_id": "run_demo_001",
            }
        ),
        encoding="utf-8",
    )
    (memory_root / "next_actions.json").write_text(
        json.dumps(
            {
                "repair": ["repair manifest lineage"],
                "exploit": ["clone BTC liq shock thesis"],
                "explore_adjacent": ["expand ETH horizon"],
                "hold": [],
            }
        ),
        encoding="utf-8",
    )
    (run_root / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "run_demo_001",
                "program_id": program_id,
                "status": "success",
                "checklist_decision": "KEEP_RESEARCH",
                "objective_name": "retail_profitability",
                "promotion_profile": "research",
                "experiment_type": "discovery",
                "start": "2022-01-01",
                "end": "2023-12-31",
                "finished_at": "2026-04-01T15:30:00+00:00",
                "planned_stage_count": 44,
                "completed_stage_count": 44,
                "artifact_count": 181,
                "candidate_count": 9,
                "promoted_count": 2,
                "normalized_symbols": ["BTCUSDT", "ETHUSDT"],
                "normalized_timeframes": ["5m"],
                "effective_behavior": {"runs_search_engine": True},
                "planned_stage_instances": ["phase2_search_engine", "promote_candidates"],
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "candidate_id": "BTCUSDT::cand_demo",
                "event_type": "LIQUIDITY_GAP_PRINT",
                "template_verb": "continuation",
                "direction": "long",
                "horizon": "24b",
                "symbol": "BTCUSDT",
                "n_events": 32,
                "mean_return_bps": 43.8033,
                "after_cost_expectancy_per_trade": 0.00418033,
                "t_stat": 3.9191,
                "q_value": 0.0023,
                "bridge_eval_status": "tradable",
                "gate_bridge_tradable": True,
            }
        ]
    ).to_csv(phase2_root / "phase2_candidates.csv", index=False)
    (phase2_root / "phase2_diagnostics.json").write_text(
        json.dumps(
            {
                "bridge_candidates_rows": 1,
                "rejection_reason_counts": {},
                "gate_funnel": {
                    "generated": 1,
                    "phase2_candidates_written": 1,
                    "phase2_final": 1,
                },
            }
        ),
        encoding="utf-8",
    )

    payload = get_operator_dashboard(program_id=program_id, data_root=str(tmp_path), limit=5)

    assert payload["layout"] == "dashboard"
    assert payload["active_program_id"] == program_id
    assert payload["summary"]["recent_proposals"] == 1
    assert payload["summary"]["bridge_tradable"] == 1
    assert payload["recent_proposals"][0]["run_id"] == "run_demo_001"
    assert payload["recent_runs"][0]["run_id"] == "run_demo_001"
    assert payload["selected_run"]["run_id"] == "run_demo_001"
    assert payload["selected_run"]["candidate_snapshot"]["pipeline_status"] == "promoted"
    assert payload["memory"]["belief_state"]["current_focus"] == "repair funding drift"
    assert payload["memory"]["next_actions"]["repair"] == ["repair manifest lineage"]
    assert payload["candidate_board"][0]["best_candidate"]["label"] == "LIQUIDITY_GAP_PRINT / continuation / long / 24b"


def test_render_operator_summary_passes_dashboard_payload_through() -> None:
    dashboard = {
        "layout": "dashboard",
        "title": "Edge Operator",
        "summary": {"known_programs": 1},
    }

    rendered = render_operator_summary(dashboard=dashboard, source_tool="edge_get_operator_dashboard")

    assert rendered["layout"] == "dashboard"
    assert rendered["widget"] == "operator_dashboard"
    assert rendered["source_tool"] == "edge_get_operator_dashboard"


def test_get_operator_dashboard_coerces_string_limit(tmp_path) -> None:
    program_id = "PROG_LIMIT"
    memory_root = tmp_path / "artifacts" / "experiments" / program_id / "memory"
    memory_root.mkdir(parents=True)

    pd.DataFrame(
        [
            {"run_id": "run_1", "status": "success", "issued_at": "2026-04-01T10:00:00+00:00"},
            {"run_id": "run_2", "status": "success", "issued_at": "2026-04-01T11:00:00+00:00"},
        ]
    ).to_csv(memory_root / "proposals.csv", index=False)

    payload = get_operator_dashboard(program_id=program_id, data_root=str(tmp_path), limit="1")

    assert payload["summary"]["recent_proposals"] == 2
    assert len(payload["recent_proposals"]) == 1


def test_get_operator_dashboard_surfaces_invalid_run_manifest(tmp_path) -> None:
    run_root = tmp_path / "runs" / "broken_run"
    run_root.mkdir(parents=True)
    (run_root / "run_manifest.json").write_text("{", encoding="utf-8")

    payload = get_operator_dashboard(data_root=str(tmp_path), limit=5)

    assert any(run["status"] == "invalid_manifest" for run in payload["recent_runs"])


def test_get_operator_dashboard_program_filter_scans_beyond_default_recent_cap(tmp_path) -> None:
    target_program = "PROG_TARGET"
    other_program = "PROG_OTHER"
    runs_root = tmp_path / "runs"
    runs_root.mkdir(parents=True)

    for i in range(101):
        run_id = f"run_other_{i:03d}"
        run_root = runs_root / run_id
        run_root.mkdir(parents=True)
        (run_root / "run_manifest.json").write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "program_id": other_program,
                    "status": "success",
                    "finished_at": f"2026-04-01T00:{i % 60:02d}:00+00:00",
                    "normalized_symbols": ["BTCUSDT"],
                }
            ),
            encoding="utf-8",
        )

    target_run_root = runs_root / "run_target_001"
    target_run_root.mkdir(parents=True)
    (target_run_root / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "run_target_001",
                "program_id": target_program,
                "status": "success",
                "finished_at": "2026-04-02T00:00:00+00:00",
                "normalized_symbols": ["ETHUSDT"],
            }
        ),
        encoding="utf-8",
    )

    payload = get_operator_dashboard(program_id=target_program, data_root=str(tmp_path), limit=5)

    assert payload["active_program_id"] == target_program
    assert payload["recent_runs"][0]["run_id"] == "run_target_001"


def test_render_operator_summary_normalizes_json_and_loose_shapes() -> None:
    rendered = render_operator_summary(
        title="Edge Operator",
        summary='["memory available"]',
        sections='{"Memory":"Loaded","Runs":["run_1","run_2"]}',
        source_tool="edge_get_operator_dashboard",
    )

    assert rendered["summary"] == {"items": ["memory available"]}
    assert rendered["sections"] == [
        {"heading": "Memory", "body": "Loaded"},
        {"heading": "Runs", "body": "['run_1', 'run_2']"},
    ]


def test_render_operator_summary_accepts_stringified_dashboard_payload() -> None:
    rendered = render_operator_summary(
        dashboard=json.dumps(
            {
                "layout": "dashboard",
                "title": "Edge Operator",
                "summary": ["known program"],
                "sections": {"Memory": "Loaded"},
            }
        ),
        source_tool="edge_get_operator_dashboard",
    )

    assert rendered["layout"] == "dashboard"
    assert rendered["summary"] == {"items": ["known program"]}
    assert rendered["sections"] == [{"heading": "Memory", "body": "Loaded"}]


def test_get_memory_summary_returns_belief_and_proposals(tmp_path) -> None:
    program_id = "MEM_TEST"
    memory_root = tmp_path / "artifacts" / "experiments" / program_id / "memory"
    memory_root.mkdir(parents=True)

    (memory_root / "belief_state.json").write_text(
        json.dumps({"current_focus": "test focus", "available": True}),
        encoding="utf-8",
    )
    pd.DataFrame([{"run_id": "run_1", "status": "success"}]).to_csv(memory_root / "proposals.csv", index=False)

    from project.apps.chatgpt.handlers import get_memory_summary

    res = get_memory_summary(program_id=program_id, data_root=str(tmp_path), limit=5)

    assert res["program_id"] == program_id
    assert res["available"] is True
    assert res["memory"]["belief_state"]["current_focus"] == "test focus"
    assert len(res["recent_proposals"]) == 1
    assert res["widget"] == "operator_dashboard"


def test_compare_runs_clams_to_six_runs(monkeypatch) -> None:
    captured_run_ids = []

    def fake_build_report(run_ids, **kwargs):
        captured_run_ids.extend(run_ids)
        return {"report": "done"}

    monkeypatch.setattr("project.apps.chatgpt.handlers.build_time_slice_report", fake_build_report)

    from project.apps.chatgpt.handlers import compare_runs

    run_ids = [f"run_{i}" for i in range(10)]
    compare_runs(run_ids=run_ids)

    assert len(captured_run_ids) == 6
    assert captured_run_ids[-1] == "run_5"
