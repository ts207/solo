from __future__ import annotations

import json

import yaml

from project.research.helpers import writer
from project.strategy.dsl.schema import (
    Blueprint,
    EntrySpec,
    EvaluationSpec,
    ExitSpec,
    LineageSpec,
    SizingSpec,
    SymbolScopeSpec,
)


def _make_blueprint(*, bp_id: str, candidate_id: str) -> Blueprint:
    return Blueprint(
        id=bp_id,
        run_id="run_test",
        event_type="VOL_SHOCK",
        candidate_id=candidate_id,
        symbol_scope=SymbolScopeSpec(
            mode="single_symbol",
            symbols=["BTCUSDT"],
            candidate_symbol="BTCUSDT",
        ),
        direction="long",
        entry=EntrySpec(
            triggers=["entry_trigger"],
            conditions=[],
            confirmations=[],
            delay_bars=0,
            cooldown_bars=0,
        ),
        exit=ExitSpec(
            time_stop_bars=5,
            invalidation={"metric": "ret_1m", "operator": "<", "value": -1.0},
            stop_type="percent",
            stop_value=0.01,
            target_type="percent",
            target_value=0.02,
        ),
        sizing=SizingSpec(
            mode="fixed_risk",
            risk_per_trade=0.01,
            max_gross_leverage=1.0,
        ),
        overlays=[],
        evaluation=EvaluationSpec(
            min_trades=10,
            cost_model={"fees_bps": 2.0, "slippage_bps": 4.0, "funding_included": False},
            robustness_flags={
                "oos_required": True,
                "multiplicity_required": True,
                "regime_stability_required": True,
            },
        ),
        lineage=LineageSpec(
            source_path="source.csv",
            compiler_version="strategy_dsl_v1",
            generated_at_utc="1970-01-01T00:00:00Z",
        ),
    )


def test_apply_retail_constraints_with_pydantic_blueprints():
    bp = _make_blueprint(bp_id="bp_1", candidate_id="c_1")

    updated = writer.apply_retail_constraints(
        [bp],
        {
            "retail_profile": "balanced_growth",
            "retail_min_net_expectancy_bps": 2.0,
        },
    )

    assert len(updated) == 1
    assert updated[0].lineage.constraints["retail_profile"] == "balanced_growth"
    assert updated[0].lineage.constraints["retail_min_net_expectancy_bps"] == 2.0
    assert bp.lineage.constraints == {}


def test_sort_cap_and_write_artifacts(tmp_path):
    bp1 = _make_blueprint(bp_id="bp_1", candidate_id="c_1")
    bp2 = _make_blueprint(bp_id="bp_2", candidate_id="c_2")

    sorted_blueprints = writer.sort_blueprints_for_write(
        [bp1, bp2],
        [
            {
                "candidate_id": "c_1",
                "after_cost_expectancy": 0.0010,
                "robustness_score": 0.6,
                "n_events": 100,
            },
            {
                "candidate_id": "c_2",
                "after_cost_expectancy": 0.0020,
                "robustness_score": 0.7,
                "n_events": 200,
            },
        ],
    )
    assert [bp.candidate_id for bp in sorted_blueprints] == ["c_2", "c_1"]

    capped, dropped_ids = writer.apply_portfolio_cap(sorted_blueprints, 1)
    assert [bp.candidate_id for bp in capped] == ["c_2"]
    assert dropped_ids == ["bp_1"]

    out_jsonl = tmp_path / "blueprints.jsonl"
    out_yaml = tmp_path / "blueprints.yaml"
    writer.write_blueprint_artifacts(
        blueprints=capped,
        out_jsonl=out_jsonl,
        out_yaml=out_yaml,
    )

    jsonl_rows = [
        json.loads(line)
        for line in out_jsonl.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    yaml_rows = yaml.safe_load(out_yaml.read_text(encoding="utf-8"))

    assert len(jsonl_rows) == 1
    assert jsonl_rows[0]["candidate_id"] == "c_2"
    assert isinstance(yaml_rows, list)
    assert yaml_rows[0]["candidate_id"] == "c_2"
