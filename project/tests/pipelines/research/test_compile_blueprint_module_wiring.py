from __future__ import annotations

import json
import sys

import pandas as pd

from project.research import compile_strategy_blueprints as compiler
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


def test_blueprint_family_cluster_key_is_stable_and_family_scoped():
    bp_a = _make_blueprint(bp_id="bp_1", candidate_id="c_1")
    bp_b = _make_blueprint(bp_id="bp_2", candidate_id="c_2")
    bp_c = _make_blueprint(bp_id="bp_3", candidate_id="c_3")

    bp_a = bp_a.model_copy(update={"lineage": bp_a.lineage.model_copy(update={"template_verb": "mean_reversion"})})
    bp_b = bp_b.model_copy(update={"lineage": bp_b.lineage.model_copy(update={"template_verb": "mean_reversion"})})
    bp_c = bp_c.model_copy(update={"lineage": bp_c.lineage.model_copy(update={"template_verb": "breakout"})})

    key_a = compiler._stable_blueprint_family_cluster_key(bp_a)
    key_b = compiler._stable_blueprint_family_cluster_key(bp_b)
    key_c = compiler._stable_blueprint_family_cluster_key(bp_c)

    assert key_a == key_b
    assert key_a != key_c


def test_choose_event_rows_delegates_to_selection_module(monkeypatch):
    captured: dict[str, object] = {}
    sentinel_rows = [{"candidate_id": "c1"}]
    sentinel_diag = {"reason": "sentinel"}
    sentinel_df = pd.DataFrame([{"candidate_id": "c1"}])

    def _fake_choose_event_rows(**kwargs):
        captured.update(kwargs)
        return sentinel_rows, sentinel_diag, sentinel_df

    monkeypatch.setattr(compiler, "_selection_choose_event_rows", _fake_choose_event_rows)

    rows, diag, selection_df = compiler._choose_event_rows(
        run_id="run_test",
        event_type="VOL_SHOCK",
        edge_rows=[],
        phase2_df=pd.DataFrame(),
        max_per_event=1,
        allow_fallback_blueprints=True,
        strict_cost_fields=True,
        min_events=25,
    )

    assert rows == sentinel_rows
    assert diag == sentinel_diag
    assert selection_df is sentinel_df
    assert captured["data_root"] == compiler.DATA_ROOT
    assert captured["candidate_id_fn"] is compiler._candidate_id
    assert captured["load_gates_spec_fn"] is compiler._load_gates_spec
    assert captured["passes_quality_floor_fn"] is compiler._passes_quality_floor
    assert captured["rank_key_fn"] is compiler._rank_key
    assert captured["passes_fallback_gate_fn"] is compiler._passes_fallback_gate
    assert captured["as_bool_fn"] is compiler._as_bool
    assert captured["safe_float_fn"] is compiler._safe_float


def test_annotate_blueprints_external_validation_supports_pydantic_models(monkeypatch):
    bp = _make_blueprint(bp_id="bp_1", candidate_id="c_1")
    monkeypatch.setattr(
        compiler,
        "_load_external_validation_strategy_metrics",
        lambda run_id: ({}, "sha256:ignored", "unused_source"),
    )

    annotated, stats = compiler._annotate_blueprints_with_external_validation_evidence(
        blueprints=[bp],
        run_id="run_test",
        evidence_hash="sha256:test_hash",
    )

    assert len(annotated) == 1
    assert annotated[0].lineage.wf_status == "pass"
    assert annotated[0].lineage.wf_evidence_hash == "sha256:test_hash"
    assert bp.lineage.wf_evidence_hash == ""
    assert stats["wf_evidence_used"] is False


def test_main_compilation_loop_accepts_record_dicts(monkeypatch, tmp_path):
    data_root = tmp_path
    run_id = "compile_loop_dicts"
    promo_dir = data_root / "reports" / "promotions" / run_id
    promo_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "status": "PROMOTED",
            }
        ]
    ).to_parquet(promo_dir / "promoted_candidates.parquet", index=False)

    monkeypatch.setattr(compiler, "DATA_ROOT", data_root)
    monkeypatch.setattr(compiler, "_checklist_decision", lambda _run_id: "PROMOTE")
    monkeypatch.setattr(compiler, "_load_run_mode", lambda _run_id: "research")
    monkeypatch.setattr(compiler, "ontology_spec_hash", lambda _root: "sha256:test")
    monkeypatch.setattr(compiler, "_load_operator_registry", lambda: {})
    monkeypatch.setattr(compiler, "start_manifest", lambda *args, **kwargs: {})
    monkeypatch.setattr(compiler, "finalize_manifest", lambda *args, **kwargs: None)

    def _fake_compile_blueprint(**kwargs):
        row = kwargs["merged_row"]
        return _make_blueprint(bp_id="bp_1", candidate_id=str(row["candidate_id"])), 0

    monkeypatch.setattr(compiler, "compile_blueprint", _fake_compile_blueprint)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compile_strategy_blueprints.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--ignore_checklist",
            "1",
        ],
    )

    rc = compiler.main()
    assert rc == 0
    out_path = data_root / "reports" / "strategy_blueprints" / run_id / "blueprints.jsonl"
    assert out_path.exists()
    payload = json.loads(out_path.read_text(encoding="utf-8").splitlines()[0])
    assert payload["candidate_id"] == "cand_1"
