from __future__ import annotations

import pandas as pd

from project.research.knowledge.memory import ensure_memory_store, write_memory_table
from project.research.knowledge.query import (
    query_adjacent_regions,
    query_agent_knobs,
    query_memory_rows,
    query_static_rows,
)


def test_query_surfaces_return_machine_readable_rows(tmp_path, monkeypatch):
    data_root = tmp_path
    static_root = data_root / "knowledge" / "static"
    static_root.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "entity_id": "event::BASIS_DISLOC",
                "entity_type": "event",
                "name": "BASIS_DISLOC",
                "title": "BASIS_DISLOC",
                "family": "STATISTICAL_DISLOCATION",
                "enabled": True,
                "source_path": "events.yaml",
                "description": "",
                "attributes_json": "{}",
            },
            {
                "entity_id": "template::continuation",
                "entity_type": "template",
                "name": "continuation",
                "title": "continuation",
                "family": "",
                "enabled": True,
                "source_path": "templates.yaml",
                "description": "",
                "attributes_json": "{}",
            },
        ]
    ).to_parquet(static_root / "entities.parquet", index=False)
    pd.DataFrame(
        [
            {
                "relation_id": "r1",
                "from_entity_id": "event::BASIS_DISLOC",
                "relation_type": "compatible_with_template",
                "to_entity_id": "template::continuation",
                "source_path": "templates.yaml",
                "attributes_json": "{}",
            }
        ]
    ).to_parquet(static_root / "relations.parquet", index=False)
    pd.DataFrame(
        [
            {
                "knob_id": "k1",
                "scope": "agent",
                "group": "campaign_memory",
                "name": "campaign_memory_promising_top_k",
                "cli_flag": "--campaign_memory_promising_top_k",
                "value_type": "int",
                "default_value_json": "5",
                "choices_json": "[]",
                "description": "Top K promising regions.",
                "source_module": "project.pipelines.pipeline_planning",
                "agent_level": "core",
                "mutability": "proposal_settable",
                "risk": "low",
            },
            {
                "knob_id": "k2",
                "scope": "agent",
                "group": "promotion_policy_resolution",
                "name": "deploy.enforce_placebo_controls",
                "cli_flag": "",
                "value_type": "bool",
                "default_value_json": "true",
                "choices_json": "[]",
                "description": "Deploy promotion requires the placebo-control bundle to pass.",
                "source_module": "project.research.services.promotion_service",
                "agent_level": "advanced",
                "mutability": "inspect_only",
                "risk": "high",
            },
        ]
    ).to_parquet(static_root / "agent_knobs.parquet", index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))
    ensure_memory_store("btc_campaign", data_root=data_root)
    write_memory_table(
        "btc_campaign",
        "tested_regions",
        pd.DataFrame(
            [
                {
                    "region_key": "k1",
                    "program_id": "btc_campaign",
                    "run_id": "r1",
                    "hypothesis_id": "h1",
                    "candidate_id": "c1",
                    "symbol_scope": "BTCUSDT",
                    "event_type": "BASIS_DISLOC",
                    "trigger_type": "EVENT",
                    "template_id": "continuation",
                    "direction": "long",
                    "horizon": "15m",
                    "entry_lag": 0,
                    "context_hash": "ctx1",
                    "context_json": '{"liquidity_regime":"low"}',
                    "eval_status": "rejected",
                    "train_n_obs": 100,
                    "validation_n_obs": 40,
                    "test_n_obs": 20,
                    "q_value": 0.04,
                    "mean_return_bps": 3.2,
                    "after_cost_expectancy": 2.1,
                    "stressed_after_cost_expectancy": 1.2,
                    "robustness_score": 0.7,
                    "gate_bridge_tradable": True,
                    "gate_promo_statistical": True,
                    "gate_promo_retail_net_expectancy": False,
                    "mechanical_status": "ok",
                    "primary_fail_gate": "gate_promo_retail_net_expectancy",
                    "warning_count": 0,
                    "updated_at": "2026-03-11T00:00:00+00:00",
                }
            ]
        ),
        data_root=data_root,
    )
    write_memory_table(
        "btc_campaign",
        "failures",
        pd.DataFrame(
            [
                {
                    "run_id": "r1",
                    "program_id": "btc_campaign",
                    "stage": "promote_candidates",
                    "failure_class": "gate_reject",
                    "failure_detail": "candidate rejected",
                    "artifact_path": "/tmp/promote_candidates.json",
                    "is_mechanical": False,
                    "is_repeated": False,
                    "superseded_by_run_id": "",
                }
            ]
        ),
        data_root=data_root,
    )

    static_payload = query_static_rows(data_root=data_root, event="BASIS_DISLOC")
    memory_payload = query_memory_rows(
        data_root=data_root, program_id="btc_campaign", event_type="BASIS_DISLOC"
    )
    adjacent_payload = query_adjacent_regions(
        data_root=data_root,
        program_id="btc_campaign",
        event_type="BASIS_DISLOC",
        template="continuation",
    )
    knobs_payload = query_agent_knobs(data_root=data_root, group="campaign_memory")
    advanced_knobs_payload = query_agent_knobs(
        data_root=data_root,
        include_advanced=True,
        group="promotion_policy_resolution",
        mutability="any",
    )

    assert static_payload["entities"][0]["name"] == "BASIS_DISLOC"
    assert static_payload["relations"][0]["relation_type"] == "compatible_with_template"
    assert knobs_payload["knobs"][0]["name"] == "campaign_memory_promising_top_k"
    assert knobs_payload["knobs"][0]["agent_level"] == "core"
    assert memory_payload["tested_regions"][0]["event_type"] == "BASIS_DISLOC"
    assert memory_payload["failures"][0]["failure_class"] == "gate_reject"
    assert adjacent_payload["adjacent_regions"][0]["template_id"] == "continuation"
    assert advanced_knobs_payload["knobs"][0]["name"] == "deploy.enforce_placebo_controls"


def test_query_static_rows_returns_empty_payload_when_static_tables_are_absent(tmp_path, monkeypatch):
    data_root = tmp_path
    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))

    payload = query_static_rows(data_root=data_root, event="VOL_SHOCK")

    assert payload == {"entities": [], "relations": []}
