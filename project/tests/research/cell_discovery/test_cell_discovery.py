from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pandas as pd
import yaml

from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.io.utils import read_parquet, write_parquet
from project.research.cell_discovery import cells_service
from project.research.cell_discovery.compiler import compile_cells
from project.research.cell_discovery.data_feasibility import verify_data_contract
from project.research.cell_discovery.models import (
    DataFeasibilityResult,
    DiscoveryRegistry,
    EventAtom,
    HorizonSet,
    RankingPolicy,
)
from project.research.cell_discovery.paths import paths_for_run
from project.research.cell_discovery.redundancy import build_redundancy_clusters
from project.research.cell_discovery.registry import load_registry
from project.research.cell_discovery.scoreboard import build_scoreboard
from project.research.cell_discovery.thesis_assembly import assemble_theses


def _forward_metrics(value: float, *, t_stat: float = 2.0) -> dict[str, float]:
    return {
        "fold_valid_count": 3.0,
        "fold_fail_ratio": 0.0,
        "forward_n": 30.0,
        "fold_median_after_cost_expectancy": value,
        "fold_median_t_stat": t_stat,
    }


def _matching_lineage_row(
    lineage: pd.DataFrame,
    context_cell: str,
    *,
    reference: pd.Series | None = None,
) -> pd.Series:
    mask = lineage["source_context_cell"].astype(str).eq(context_cell)
    if reference is not None:
        for column in ("source_event_atom", "direction", "horizon", "template"):
            if column in lineage.columns and column in reference:
                mask &= lineage[column].astype(str).eq(str(reference[column]))
    return lineage[mask].iloc[0]


def _write_scoreboard_inputs(rows: pd.DataFrame, paths) -> None:
    write_parquet(rows, paths.candidate_universe_path)
    fold_rows: list[dict[str, object]] = []
    for _, row in rows.iterrows():
        valid_raw = row.get("fold_valid_count", 0)
        valid_count = 0 if pd.isna(valid_raw) else int(float(valid_raw or 0))
        if valid_count <= 0 or pd.isna(row.get("fold_median_after_cost_expectancy", pd.NA)):
            continue
        forward_n = int(float(row.get("forward_n", valid_count * 10) or valid_count * 10))
        per_fold_n = max(3, forward_n // valid_count)
        for fold_id in range(1, valid_count + 1):
            fold_rows.append(
                {
                    "fold_id": fold_id,
                    "valid": True,
                    "n": per_fold_n,
                    "after_cost_expectancy_bps": float(
                        row["fold_median_after_cost_expectancy"]
                    ),
                    "t_stat": float(row.get("fold_median_t_stat", 2.0) or 2.0),
                    "hypothesis_id": row["hypothesis_id"],
                    "trigger_key": "event:UNIT",
                }
            )
    write_parquet(pd.DataFrame(fold_rows), paths.run_dir / "phase2_candidate_fold_metrics.parquet")


def test_default_discovery_registry_loads_bounded_surface() -> None:
    registry = load_registry("spec/discovery")

    assert [atom.atom_id for atom in registry.event_atoms] == [
        "vol_shock_core",
        "funding_extreme_core",
        "funding_persistence_core",
    ]
    assert {cell.executability_class for cell in registry.context_cells} == {
        "runtime",
        "supportive_only",
    }
    assert registry.ranking_policy.max_search_hypotheses == 1000
    assert registry.ranking_policy.min_forward_valid_folds == 3
    assert registry.ranking_policy.min_forward_support == 30
    assert registry.ranking_policy.min_forward_support_fraction == 0.10
    assert registry.ranking_policy.min_contrast_lift_bps == 5.0
    assert [rule.rule_type for rule in registry.contrast_rules] == [
        "in_bucket_vs_unconditional"
    ]


def test_cell_compiler_writes_search_spec_and_lineage(tmp_path: Path) -> None:
    registry = load_registry("spec/discovery")
    compiled = compile_cells(
        registry=registry,
        run_id="UNIT_CELL_COMPILE",
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
        start="2025-01-01",
        end="2025-02-01",
    )

    assert compiled.search_spec_path.exists()
    assert compiled.experiment_path.exists()
    assert compiled.lineage_path.exists()
    assert compiled.estimated_hypothesis_count == 108

    lineage = read_parquet([compiled.lineage_path])
    assert {
        "hypothesis_id",
        "source_discovery_mode",
        "source_cell_id",
        "source_event_atom",
        "source_context_cell",
        "source_discovery_spec_version",
        "runtime_executable",
        "thesis_eligible",
    }.issubset(set(lineage.columns))
    assert set(lineage["source_discovery_mode"]) == {"edge_cells"}
    assert set(lineage["symbol"]) == {"BTCUSDT"}
    assert lineage["source_cell_id"].is_unique


def test_compiler_lineage_is_atom_specific_for_mixed_authored_surface(tmp_path: Path) -> None:
    registry = DiscoveryRegistry(
        event_atoms=(
            EventAtom(
                atom_id="vol_long_mr",
                event_family="volatility",
                event_type="VOL_SHOCK",
                directions=("long",),
                templates=("mean_reversion",),
                horizons=("12b",),
                search_role="context",
                promotion_role="ineligible",
                runtime_role="observer",
            ),
            EventAtom(
                atom_id="funding_short_cont",
                event_family="funding",
                event_type="FUNDING_EXTREME_ONSET",
                directions=("short",),
                templates=("continuation",),
                horizons=("24b",),
                search_role="primary_trigger",
                promotion_role="eligible",
                runtime_role="trade_trigger",
            ),
        ),
        context_cells=(),
        horizons=HorizonSet(horizons=("12b", "24b")),
        ranking_policy=RankingPolicy(max_search_hypotheses=10),
    )

    compiled = compile_cells(
        registry=registry,
        run_id="UNIT_CELL_MIXED_AUTHORED",
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
        start="2025-01-01",
        end="2025-02-01",
    )

    lineage = read_parquet([compiled.lineage_path])
    assert len(lineage) == 2
    assert set(lineage["template"]) == {"mean_reversion", "continuation"}
    assert set(lineage["direction"]) == {"long", "short"}
    assert lineage["source_cell_id"].is_unique


def test_phase2_edge_cells_filters_unauthorized_broad_search_hypotheses() -> None:
    from project.research.phase2_search_engine import _filter_edge_cell_authorized_hypotheses

    authorized = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="12b",
        template_id="mean_reversion",
        entry_lag=1,
    )
    unauthorized = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="short",
        horizon="24b",
        template_id="continuation",
        entry_lag=1,
    )
    lineage = pd.DataFrame({"hypothesis_id": [authorized.hypothesis_id()]})

    filtered = _filter_edge_cell_authorized_hypotheses(
        [authorized, unauthorized],
        lineage,
    )

    assert [spec.hypothesis_id() for spec in filtered] == [authorized.hypothesis_id()]


def test_phase2_edge_cells_fails_closed_without_lineage() -> None:
    from project.research.phase2_search_engine import _filter_edge_cell_authorized_hypotheses

    hypothesis = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="12b",
        template_id="mean_reversion",
        entry_lag=1,
    )

    assert _filter_edge_cell_authorized_hypotheses([hypothesis], pd.DataFrame()) == []
    assert _filter_edge_cell_authorized_hypotheses(
        [hypothesis],
        pd.DataFrame({"symbol": ["BTCUSDT"]}),
    ) == []


def test_phase2_edge_cells_filters_authorization_by_symbol() -> None:
    from project.research.phase2_search_engine import _filter_edge_cell_authorized_hypotheses

    hypothesis = HypothesisSpec(
        trigger=TriggerSpec.event("VOL_SHOCK"),
        direction="long",
        horizon="12b",
        template_id="mean_reversion",
        entry_lag=1,
    )
    lineage = pd.DataFrame(
        {
            "hypothesis_id": [hypothesis.hypothesis_id()],
            "symbol": ["BTCUSDT"],
        }
    )

    assert _filter_edge_cell_authorized_hypotheses(
        [hypothesis],
        lineage,
        symbol="BTCUSDT",
    ) == [hypothesis]
    assert _filter_edge_cell_authorized_hypotheses(
        [hypothesis],
        lineage,
        symbol="ETHUSDT",
    ) == []


def test_scoreboard_requires_forward_support_and_contrast(tmp_path: Path) -> None:
    registry = load_registry("spec/discovery/expanded_v2")
    run_id = "UNIT_CELL_SCOREBOARD"
    compiled = compile_cells(
        registry=registry,
        run_id=run_id,
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
        start="2025-01-01",
        end="2025-02-01",
    )
    paths = paths_for_run(data_root=tmp_path, run_id=run_id)
    lineage = read_parquet([compiled.lineage_path])
    unconditional = lineage[lineage["source_context_cell"] == "unconditional"].iloc[0]
    executable = _matching_lineage_row(lineage, "high_vol", reference=unconditional)
    low_support = _matching_lineage_row(lineage, "low_vol", reference=unconditional)

    rows = pd.DataFrame(
        [
            {
                "hypothesis_id": unconditional["hypothesis_id"],
                "symbol": "BTCUSDT",
                "n_events": 80,
                "mean_return_bps": 3.0,
                "cost_adjusted_return_bps": 3.0,
                "median_return_bps": 2.0,
                "hit_rate": 0.55,
                "t_stat": 2.0,
                "p_value": 0.02,
                "q_value": 0.04,
                "robustness_score": 0.6,
                **_forward_metrics(3.0),
            },
            {
                "hypothesis_id": executable["hypothesis_id"],
                "symbol": "BTCUSDT",
                "n_events": 90,
                "mean_return_bps": 12.0,
                "cost_adjusted_return_bps": 11.0,
                "median_return_bps": 8.0,
                "hit_rate": 0.62,
                "t_stat": 3.0,
                "p_value": 0.01,
                "q_value": 0.02,
                "robustness_score": 0.8,
                **_forward_metrics(11.0, t_stat=3.0),
            },
            {
                "hypothesis_id": low_support["hypothesis_id"],
                "symbol": "BTCUSDT",
                "n_events": 5,
                "mean_return_bps": 20.0,
                "cost_adjusted_return_bps": 19.0,
                "median_return_bps": 10.0,
                "hit_rate": 0.70,
                "t_stat": 4.0,
                "p_value": 0.01,
                "q_value": 0.02,
                "robustness_score": 0.9,
                **_forward_metrics(19.0, t_stat=4.0),
            },
        ]
    )
    _write_scoreboard_inputs(rows, paths)

    summary = build_scoreboard(registry=registry, run_id=run_id, data_root=tmp_path)

    assert summary["scoreboard_rows"] == 3
    scoreboard = read_parquet([paths.scoreboard_path])
    top = scoreboard.iloc[0]
    assert top["context_cell"] == "high_vol"
    assert top["status"] == "rankable_runtime_executable"
    assert top["contrast_lift_bps"] == 8.0
    assert top["rank_score"] > 0
    rejected = scoreboard[scoreboard["context_cell"] == "low_vol"].iloc[0]
    assert rejected["status"] == "rejected_low_support"


def test_scoreboard_blocks_in_sample_only_rows_without_forward_evidence(tmp_path: Path) -> None:
    registry = load_registry("spec/discovery/expanded_v2")
    run_id = "UNIT_CELL_SCOREBOARD_NO_FORWARD"
    compiled = compile_cells(
        registry=registry,
        run_id=run_id,
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
    )
    paths = paths_for_run(data_root=tmp_path, run_id=run_id)
    lineage = read_parquet([compiled.lineage_path])
    unconditional = lineage[lineage["source_context_cell"] == "unconditional"].iloc[0]
    executable = _matching_lineage_row(lineage, "high_vol", reference=unconditional)

    _write_scoreboard_inputs(
        pd.DataFrame(
            [
                {
                    "hypothesis_id": unconditional["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 80,
                    "mean_return_bps": 2.0,
                    "cost_adjusted_return_bps": 2.0,
                    "t_stat": 2.0,
                    "robustness_score": 0.6,
                    **_forward_metrics(2.0),
                },
                {
                    "hypothesis_id": executable["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 90,
                    "mean_return_bps": 100.0,
                    "cost_adjusted_return_bps": 99.0,
                    "t_stat": 12.0,
                    "robustness_score": 0.9,
                },
            ]
        ),
        paths,
    )

    build_scoreboard(registry=registry, run_id=run_id, data_root=tmp_path)
    scoreboard = read_parquet([paths.scoreboard_path])
    blocked = scoreboard[scoreboard["context_cell"] == "high_vol"].iloc[0]

    assert blocked["status"] == "blocked_missing_forward_window"
    assert float(blocked["rank_score"]) == 0.0


def test_scoreboard_rejects_negative_forward_evidence_as_instability(tmp_path: Path) -> None:
    registry = load_registry("spec/discovery/expanded_v2")
    run_id = "UNIT_CELL_SCOREBOARD_NEG_FORWARD"
    compiled = compile_cells(
        registry=registry,
        run_id=run_id,
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
    )
    paths = paths_for_run(data_root=tmp_path, run_id=run_id)
    lineage = read_parquet([compiled.lineage_path])
    unconditional = lineage[lineage["source_context_cell"] == "unconditional"].iloc[0]
    executable = _matching_lineage_row(lineage, "high_vol", reference=unconditional)

    _write_scoreboard_inputs(
        pd.DataFrame(
            [
                {
                    "hypothesis_id": unconditional["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 80,
                    "mean_return_bps": 2.0,
                    "cost_adjusted_return_bps": 2.0,
                    "t_stat": 2.0,
                    "robustness_score": 0.6,
                    **_forward_metrics(2.0),
                },
                {
                    "hypothesis_id": executable["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 90,
                    "mean_return_bps": 50.0,
                    "cost_adjusted_return_bps": 49.0,
                    "t_stat": 8.0,
                    "robustness_score": 0.9,
                    **_forward_metrics(-4.0, t_stat=-1.0),
                },
            ]
        ),
        paths,
    )

    build_scoreboard(registry=registry, run_id=run_id, data_root=tmp_path)
    scoreboard = read_parquet([paths.scoreboard_path])
    rejected = scoreboard[scoreboard["context_cell"] == "high_vol"].iloc[0]

    assert rejected["status"] == "rejected_instability"
    assert float(rejected["rank_score"]) == 0.0


def test_scoreboard_rejects_too_few_forward_folds(tmp_path: Path) -> None:
    registry = load_registry("spec/discovery/expanded_v2")
    run_id = "UNIT_CELL_SCOREBOARD_FEW_FORWARD_FOLDS"
    compiled = compile_cells(
        registry=registry,
        run_id=run_id,
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
    )
    paths = paths_for_run(data_root=tmp_path, run_id=run_id)
    lineage = read_parquet([compiled.lineage_path])
    unconditional = lineage[lineage["source_context_cell"] == "unconditional"].iloc[0]
    executable = lineage[
        (lineage["source_context_cell"] == "bullish_trend")
        & (lineage["source_event_atom"] == unconditional["source_event_atom"])
        & (lineage["direction"] == unconditional["direction"])
        & (lineage["horizon"] == unconditional["horizon"])
        & (lineage["template"] == unconditional["template"])
    ].iloc[0]

    _write_scoreboard_inputs(
        pd.DataFrame(
            [
                {
                    "hypothesis_id": unconditional["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 120,
                    "mean_return_bps": 2.0,
                    "cost_adjusted_return_bps": 2.0,
                    "t_stat": 2.0,
                    "robustness_score": 0.6,
                    **_forward_metrics(2.0),
                },
                {
                    "hypothesis_id": executable["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 120,
                    "mean_return_bps": 25.0,
                    "cost_adjusted_return_bps": 25.0,
                    "t_stat": 4.0,
                    "robustness_score": 0.9,
                    "fold_valid_count": 1.0,
                    "fold_fail_ratio": 0.0,
                    "forward_n": 80.0,
                    "fold_median_after_cost_expectancy": 300.0,
                    "fold_median_t_stat": 10.0,
                },
            ]
        ),
        paths,
    )
    write_parquet(
        pd.DataFrame(
            [
                {
                    "hypothesis_id": executable["hypothesis_id"],
                    "valid": True,
                    "n": 6,
                    "after_cost_expectancy_bps": 300.0,
                    "t_stat": 10.0,
                }
            ]
        ),
        paths.run_dir / "phase2_candidate_fold_metrics.parquet",
    )

    build_scoreboard(registry=registry, run_id=run_id, data_root=tmp_path)
    scoreboard = read_parquet([paths.scoreboard_path])
    rejected = scoreboard[scoreboard["context_cell"] == "bullish_trend"].iloc[0]

    assert rejected["status"] == "rejected_insufficient_forward_folds"
    assert float(rejected["rank_score"]) == 0.0


def test_scoreboard_rejects_too_little_forward_support(tmp_path: Path) -> None:
    registry = load_registry("spec/discovery/expanded_v2")
    run_id = "UNIT_CELL_SCOREBOARD_LOW_FORWARD_SUPPORT"
    compiled = compile_cells(
        registry=registry,
        run_id=run_id,
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
    )
    paths = paths_for_run(data_root=tmp_path, run_id=run_id)
    lineage = read_parquet([compiled.lineage_path])
    unconditional = lineage[lineage["source_context_cell"] == "unconditional"].iloc[0]
    executable = lineage[
        (lineage["source_context_cell"] == "bullish_trend")
        & (lineage["source_event_atom"] == unconditional["source_event_atom"])
        & (lineage["direction"] == unconditional["direction"])
        & (lineage["horizon"] == unconditional["horizon"])
        & (lineage["template"] == unconditional["template"])
    ].iloc[0]

    _write_scoreboard_inputs(
        pd.DataFrame(
            [
                {
                    "hypothesis_id": unconditional["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 120,
                    "mean_return_bps": 2.0,
                    "cost_adjusted_return_bps": 2.0,
                    "t_stat": 2.0,
                    "robustness_score": 0.6,
                    **_forward_metrics(2.0),
                },
                {
                    "hypothesis_id": executable["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 120,
                    "mean_return_bps": 25.0,
                    "cost_adjusted_return_bps": 25.0,
                    "t_stat": 4.0,
                    "robustness_score": 0.9,
                    "fold_valid_count": 3.0,
                    "fold_fail_ratio": 0.0,
                    "forward_n": 5.0,
                    "fold_median_after_cost_expectancy": 40.0,
                    "fold_median_t_stat": 3.0,
                },
            ]
        ),
        paths,
    )
    write_parquet(
        pd.DataFrame(
            [
                {
                    "hypothesis_id": executable["hypothesis_id"],
                    "valid": True,
                    "n": 6,
                    "after_cost_expectancy_bps": 30.0,
                    "t_stat": 3.0,
                },
                {
                    "hypothesis_id": executable["hypothesis_id"],
                    "valid": True,
                    "n": 7,
                    "after_cost_expectancy_bps": 40.0,
                    "t_stat": 3.0,
                },
                {
                    "hypothesis_id": executable["hypothesis_id"],
                    "valid": True,
                    "n": 7,
                    "after_cost_expectancy_bps": 50.0,
                    "t_stat": 3.0,
                },
            ]
        ),
        paths.run_dir / "phase2_candidate_fold_metrics.parquet",
    )

    build_scoreboard(registry=registry, run_id=run_id, data_root=tmp_path)
    scoreboard = read_parquet([paths.scoreboard_path])
    rejected = scoreboard[scoreboard["context_cell"] == "bullish_trend"].iloc[0]

    assert rejected["status"] == "rejected_insufficient_forward_support"
    assert float(rejected["forward_n"]) == 5.0
    assert float(rejected["rank_score"]) == 0.0


def test_scoreboard_rejects_missing_contrast_complement(tmp_path: Path) -> None:
    registry = load_registry("spec/discovery/expanded_v2")
    run_id = "UNIT_CELL_SCOREBOARD_NO_COMPLEMENT"
    compiled = compile_cells(
        registry=registry,
        run_id=run_id,
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
    )
    paths = paths_for_run(data_root=tmp_path, run_id=run_id)
    lineage = read_parquet([compiled.lineage_path])
    executable = lineage[lineage["source_context_cell"] == "high_vol"].iloc[0]

    _write_scoreboard_inputs(
        pd.DataFrame(
            [
                {
                    "hypothesis_id": executable["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 90,
                    "mean_return_bps": 20.0,
                    "cost_adjusted_return_bps": 19.0,
                    "t_stat": 4.0,
                    "robustness_score": 0.9,
                    **_forward_metrics(19.0, t_stat=4.0),
                },
            ]
        ),
        paths,
    )

    build_scoreboard(registry=registry, run_id=run_id, data_root=tmp_path)
    scoreboard = read_parquet([paths.scoreboard_path])
    contrast = read_parquet([paths.contrast_path])

    rejected = scoreboard[scoreboard["context_cell"] == "high_vol"].iloc[0]
    contrast_row = contrast[contrast["source_cell_id"] == executable["source_cell_id"]].iloc[0]
    assert rejected["status"] == "rejected_no_contrast"
    assert contrast_row["contrast_blocked_reason"] == "missing_complement"
    assert float(rejected["rank_score"]) == 0.0


def test_scoreboard_enforces_configured_min_contrast_lift(tmp_path: Path) -> None:
    base_registry = load_registry("spec/discovery/expanded_v2")
    registry = replace(
        base_registry,
        ranking_policy=replace(base_registry.ranking_policy, min_contrast_lift_bps=5.0),
    )
    run_id = "UNIT_CELL_SCOREBOARD_MIN_LIFT"
    compiled = compile_cells(
        registry=registry,
        run_id=run_id,
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
    )
    paths = paths_for_run(data_root=tmp_path, run_id=run_id)
    lineage = read_parquet([compiled.lineage_path])
    unconditional = lineage[lineage["source_context_cell"] == "unconditional"].iloc[0]
    executable = _matching_lineage_row(lineage, "high_vol", reference=unconditional)

    _write_scoreboard_inputs(
        pd.DataFrame(
            [
                {
                    "hypothesis_id": unconditional["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 80,
                    "mean_return_bps": 10.0,
                    "cost_adjusted_return_bps": 10.0,
                    "t_stat": 3.0,
                    "robustness_score": 0.8,
                    **_forward_metrics(10.0, t_stat=3.0),
                },
                {
                    "hypothesis_id": executable["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 90,
                    "mean_return_bps": 13.0,
                    "cost_adjusted_return_bps": 13.0,
                    "t_stat": 4.0,
                    "robustness_score": 0.9,
                    **_forward_metrics(13.0, t_stat=4.0),
                },
            ]
        ),
        paths,
    )

    build_scoreboard(registry=registry, run_id=run_id, data_root=tmp_path)
    scoreboard = read_parquet([paths.scoreboard_path])
    contrast = read_parquet([paths.contrast_path])

    rejected = scoreboard[scoreboard["context_cell"] == "high_vol"].iloc[0]
    contrast_row = contrast[contrast["source_cell_id"] == executable["source_cell_id"]].iloc[0]
    assert float(rejected["contrast_lift_bps"]) == 3.0
    assert rejected["status"] == "rejected_no_contrast"
    assert contrast_row["contrast_blocked_reason"] == "insufficient_contrast_lift"
    assert float(rejected["rank_score"]) == 0.0


def test_scoreboard_filters_unauthorized_phase2_rows(tmp_path: Path) -> None:
    registry = load_registry("spec/discovery/expanded_v2")
    run_id = "UNIT_CELL_SCOREBOARD_AUTHZ"
    compiled = compile_cells(
        registry=registry,
        run_id=run_id,
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
    )
    paths = paths_for_run(data_root=tmp_path, run_id=run_id)
    lineage = read_parquet([compiled.lineage_path])
    unconditional = lineage[lineage["source_context_cell"] == "unconditional"].iloc[0]
    executable = _matching_lineage_row(lineage, "high_vol", reference=unconditional)

    rows = pd.DataFrame(
        [
            {
                "hypothesis_id": unconditional["hypothesis_id"],
                "symbol": "BTCUSDT",
                "n_events": 80,
                "mean_return_bps": 2.0,
                "cost_adjusted_return_bps": 2.0,
                "t_stat": 2.0,
                "robustness_score": 0.6,
                **_forward_metrics(2.0),
            },
            {
                "hypothesis_id": executable["hypothesis_id"],
                "symbol": "BTCUSDT",
                "n_events": 90,
                "mean_return_bps": 8.0,
                "cost_adjusted_return_bps": 7.0,
                "t_stat": 3.0,
                "robustness_score": 0.8,
                **_forward_metrics(7.0, t_stat=3.0),
            },
            {
                "hypothesis_id": "hyp_unauthorized",
                "symbol": "BTCUSDT",
                "n_events": 500,
                "mean_return_bps": 1000.0,
                "cost_adjusted_return_bps": 1000.0,
                "t_stat": 20.0,
                "robustness_score": 1.0,
                **_forward_metrics(1000.0, t_stat=20.0),
            },
        ]
    )
    _write_scoreboard_inputs(rows, paths)

    summary = build_scoreboard(registry=registry, run_id=run_id, data_root=tmp_path)
    scoreboard = read_parquet([paths.scoreboard_path])

    assert summary["unauthorized_rows_filtered"] == 1
    assert "hyp_unauthorized" not in set(scoreboard.get("hypothesis_id", pd.Series(dtype=str)))
    assert int(summary["scoreboard_rows"]) == 2


def test_scoreboard_filters_symbol_pruned_phase2_rows(tmp_path: Path) -> None:
    registry = load_registry("spec/discovery")
    run_id = "UNIT_CELL_SCOREBOARD_SYMBOL_AUTHZ"
    compiled = compile_cells(
        registry=registry,
        run_id=run_id,
        data_root=tmp_path,
        symbols=["BTCUSDT", "ETHUSDT"],
        timeframe="5m",
        cell_feasibility=[
            {
                "event_atom_id": "vol_shock_core",
                "context_cell": "unconditional",
                "symbol": "BTCUSDT",
                "status": "pass",
                "blocked_reasons": [],
            },
            {
                "event_atom_id": "vol_shock_core",
                "context_cell": "unconditional",
                "symbol": "ETHUSDT",
                "status": "block",
                "blocked_reasons": ["blocked_missing_data"],
            },
        ],
    )
    paths = paths_for_run(data_root=tmp_path, run_id=run_id)
    lineage = read_parquet([compiled.lineage_path])
    authorized = lineage.iloc[0]

    _write_scoreboard_inputs(
        pd.DataFrame(
            [
                {
                    "hypothesis_id": authorized["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 80,
                    "mean_return_bps": 2.0,
                    "cost_adjusted_return_bps": 2.0,
                    "t_stat": 2.0,
                    "robustness_score": 0.6,
                    **_forward_metrics(2.0),
                },
                {
                    "hypothesis_id": authorized["hypothesis_id"],
                    "symbol": "ETHUSDT",
                    "n_events": 80,
                    "mean_return_bps": 50.0,
                    "cost_adjusted_return_bps": 50.0,
                    "t_stat": 8.0,
                    "robustness_score": 0.9,
                    **_forward_metrics(50.0, t_stat=8.0),
                },
            ]
        ),
        paths,
    )

    summary = build_scoreboard(registry=registry, run_id=run_id, data_root=tmp_path)
    raw = read_parquet([paths.raw_cells_path])

    assert summary["unauthorized_rows_filtered"] == 1
    assert set(raw["symbol"]) == {"BTCUSDT"}


def test_data_feasibility_blocks_missing_forward_window(monkeypatch, tmp_path: Path) -> None:
    registry = load_registry("spec/discovery")

    def _fake_feature_surface(**kwargs):
        symbols = [str(symbol).upper() for symbol in kwargs["symbols"]]
        event_types = [str(event).upper() for event in kwargs["event_types"]]
        return {
            "status": "pass",
            "symbols": {
                symbol: {
                    "status": "pass",
                    "detectors": {
                        event: {
                            "status": "pass",
                            "required_columns": ["close"],
                            "blocking_columns": [],
                            "degraded_columns": [],
                        }
                        for event in event_types
                    },
                }
                for symbol in symbols
            },
            "detectors": {event: {"status": "pass"} for event in event_types},
        }

    def _fake_contract(**_kwargs):
        return {"keys": {"high_vol_regime", "low_vol_regime", "carry_state"}}

    monkeypatch.setattr(
        "project.research.cell_discovery.data_feasibility.analyze_feature_surface_viability",
        _fake_feature_surface,
    )
    monkeypatch.setattr(
        "project.research.cell_discovery.data_feasibility.load_symbol_joined_condition_contract",
        _fake_contract,
    )
    result = verify_data_contract(
        registry=registry,
        run_id="UNIT_CELL_DATA_BLOCK",
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
        start="2025-01-01",
        end="2025-01-01T01:00:00Z",
    )

    assert result.status == "block"
    assert "blocked_missing_forward_window" in result.payload["blocked_reasons"]
    assert result.payload["cell_status_counts"]["block"] == len(result.payload["cell_feasibility"])
    assert result.report_path.exists()


def test_data_feasibility_reports_partial_cell_blocks(monkeypatch, tmp_path: Path) -> None:
    registry = load_registry("spec/discovery")

    def _fake_feature_surface(**kwargs):
        symbols = [str(symbol).upper() for symbol in kwargs["symbols"]]
        return {
            "status": "warn",
            "symbols": {
                symbol: {
                    "status": "warn",
                    "detectors": {
                        "VOL_SHOCK": {
                            "status": "pass",
                            "required_columns": ["close"],
                            "blocking_columns": [],
                            "degraded_columns": [],
                        },
                        "FUNDING_EXTREME_ONSET": {
                            "status": "block",
                            "required_columns": ["funding_rate_bps"],
                            "blocking_columns": ["funding_rate_bps"],
                            "degraded_columns": [],
                        },
                    },
                }
                for symbol in symbols
            },
            "detectors": {
                "VOL_SHOCK": {"status": "pass"},
                "FUNDING_EXTREME_ONSET": {"status": "block"},
            },
        }

    def _fake_contract(**_kwargs):
        return {"keys": {"vol_regime", "high_vol_regime", "low_vol_regime", "carry_state"}}

    monkeypatch.setattr(
        "project.research.cell_discovery.data_feasibility.analyze_feature_surface_viability",
        _fake_feature_surface,
    )
    monkeypatch.setattr(
        "project.research.cell_discovery.data_feasibility.load_symbol_joined_condition_contract",
        _fake_contract,
    )

    result = verify_data_contract(
        registry=registry,
        run_id="UNIT_CELL_DATA_PARTIAL",
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
        start="2025-01-01",
        end="2025-02-01",
    )

    pass_rows = [
        row
        for row in result.payload["cell_feasibility"]
        if row["event_type"] == "VOL_SHOCK" and row["status"] == "pass"
    ]
    blocked_rows = [
        row
        for row in result.payload["cell_feasibility"]
        if row["event_type"] == "FUNDING_EXTREME_ONSET" and row["status"] == "block"
    ]
    assert result.status == "warn"
    assert pass_rows
    assert blocked_rows
    assert result.payload["blocked_by_reason"]["blocked_missing_data"] == len(blocked_rows)
    assert "blocked_missing_data" in result.payload["blocked_reasons"]


def test_compiler_reduces_lineage_from_partial_cell_feasibility(tmp_path: Path) -> None:
    registry = load_registry("spec/discovery")
    matrix = [
        {
            "event_atom_id": atom.atom_id,
            "context_cell": context_cell,
            "status": "pass" if atom.atom_id == "vol_shock_core" else "block",
            "blocked_reasons": (
                [] if atom.atom_id == "vol_shock_core" else ["blocked_missing_data"]
            ),
        }
        for atom in registry.event_atoms
        for context_cell in ("unconditional", "bullish_trend", "positive_funding")
    ]

    compiled = compile_cells(
        registry=registry,
        run_id="UNIT_CELL_COMPILE_PARTIAL",
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
        start="2025-01-01",
        end="2025-02-01",
        cell_feasibility=matrix,
    )

    lineage = read_parquet([compiled.lineage_path])
    skipped = json.loads(compiled.skipped_cells_path.read_text())

    assert compiled.estimated_hypothesis_count == 36
    assert compiled.cell_count == 3
    assert compiled.family_counts == {"volatility": 3}
    assert compiled.skipped_cell_count == 6
    assert set(lineage["source_event_atom"]) == {"vol_shock_core"}
    assert skipped["skipped_cell_count"] == 6
    assert {row["event_atom_id"] for row in skipped["skipped_cells"]} == {
        "funding_extreme_core",
        "funding_persistence_core",
    }


def test_compiler_prunes_cell_feasibility_by_symbol(tmp_path: Path) -> None:
    registry = load_registry("spec/discovery")
    matrix = [
        {
            "event_atom_id": "vol_shock_core",
            "context_cell": "unconditional",
            "symbol": "BTCUSDT",
            "status": "pass",
            "blocked_reasons": [],
        },
        {
            "event_atom_id": "vol_shock_core",
            "context_cell": "unconditional",
            "symbol": "ETHUSDT",
            "status": "block",
            "blocked_reasons": ["blocked_missing_data"],
        },
    ]

    compiled = compile_cells(
        registry=registry,
        run_id="UNIT_CELL_COMPILE_SYMBOL_PARTIAL",
        data_root=tmp_path,
        symbols=["BTCUSDT", "ETHUSDT"],
        timeframe="5m",
        start="2025-01-01",
        end="2025-02-01",
        cell_feasibility=matrix,
    )

    lineage = read_parquet([compiled.lineage_path])
    skipped = json.loads(compiled.skipped_cells_path.read_text())

    assert compiled.estimated_hypothesis_count == 12
    assert compiled.cell_count == 1
    assert compiled.skipped_cell_count == 1
    assert set(lineage["symbol"]) == {"BTCUSDT"}
    assert set(lineage["source_event_atom"]) == {"vol_shock_core"}
    assert set(lineage["source_context_cell"]) == {"unconditional"}
    assert skipped["skipped_cells"][0]["symbol"] == "ETHUSDT"


def test_plan_cells_compiles_reduced_surface_for_partial_data_block(
    monkeypatch,
    tmp_path: Path,
) -> None:
    registry = load_registry("spec/discovery")
    matrix = [
        {
            "event_atom_id": atom.atom_id,
            "context_cell": context_cell,
            "status": "pass" if atom.atom_id == "vol_shock_core" else "block",
            "blocked_reasons": (
                [] if atom.atom_id == "vol_shock_core" else ["blocked_missing_data"]
            ),
        }
        for atom in registry.event_atoms
        for context_cell in ("unconditional", "bullish_trend", "positive_funding")
    ]
    report_path = paths_for_run(data_root=tmp_path, run_id="UNIT_CELL_PLAN_PARTIAL").data_contract_path

    def _fake_verify_data_contract(**_kwargs):
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("{}")
        return DataFeasibilityResult(
            status="warn",
            report_path=report_path,
            payload={
                "cell_feasibility": matrix,
                "cell_status_counts": {"pass": 3, "warn": 0, "unknown": 0, "block": 6},
                "blocked_reasons": ["blocked_missing_data"],
            },
        )

    monkeypatch.setattr(cells_service, "verify_data_contract", _fake_verify_data_contract)

    result = cells_service.plan_cells(
        run_id="UNIT_CELL_PLAN_PARTIAL",
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
        start="2025-01-01",
        end="2025-02-01",
    )

    lineage = read_parquet([Path(result["lineage_path"])])

    assert result["exit_code"] == 0
    assert result["status"] == "planned"
    assert result["data_status"] == "warn"
    assert result["estimated_hypothesis_count"] == 36
    assert result["skipped_cell_count"] == 6
    assert result["family_counts"] == {"volatility": 3}
    assert set(lineage["source_event_atom"]) == {"vol_shock_core"}


def test_summarize_cells_reports_plan_only_skipped_cells(
    monkeypatch,
    tmp_path: Path,
) -> None:
    registry = load_registry("spec/discovery")
    matrix = [
        {
            "event_atom_id": atom.atom_id,
            "context_cell": context_cell,
            "status": "pass" if atom.atom_id == "vol_shock_core" else "block",
            "blocked_reasons": (
                [] if atom.atom_id == "vol_shock_core" else ["blocked_missing_data"]
            ),
        }
        for atom in registry.event_atoms
        for context_cell in ("unconditional", "bullish_trend", "positive_funding")
    ]
    report_path = paths_for_run(data_root=tmp_path, run_id="UNIT_CELL_SUMMARY_PLAN").data_contract_path

    def _fake_verify_data_contract(**_kwargs):
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("{}")
        return DataFeasibilityResult(
            status="warn",
            report_path=report_path,
            payload={
                "cell_feasibility": matrix,
                "cell_status_counts": {"pass": 3, "warn": 0, "unknown": 0, "block": 6},
                "blocked_reasons": ["blocked_missing_data"],
            },
        )

    monkeypatch.setattr(cells_service, "verify_data_contract", _fake_verify_data_contract)

    cells_service.plan_cells(
        run_id="UNIT_CELL_SUMMARY_PLAN",
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
        start="2025-01-01",
        end="2025-02-01",
    )
    summary = cells_service.summarize_cells(
        run_id="UNIT_CELL_SUMMARY_PLAN",
        data_root=tmp_path,
    )

    assert summary["exit_code"] == 0
    assert summary["status"] == "ok"
    assert summary["skipped_cell_count"] == 6
    assert summary["skipped_by_reason"] == {"blocked_missing_data": 6}
    assert summary["skipped_by_event_atom"] == {
        "funding_extreme_core": 3,
        "funding_persistence_core": 3,
    }
    assert len(summary["skipped_cells"]) == 6


def test_run_cells_blocks_when_feasibility_prunes_entire_surface(
    monkeypatch,
    tmp_path: Path,
) -> None:
    report_path = paths_for_run(data_root=tmp_path, run_id="UNIT_CELL_RUN_ZERO").data_contract_path

    def _fake_verify_data_contract(**_kwargs):
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("{}")
        return DataFeasibilityResult(
            status="block",
            report_path=report_path,
            payload={
                "cell_feasibility": [
                    {
                        "event_atom_id": "vol_shock_core",
                        "context_cell": "unconditional",
                        "status": "block",
                        "blocked_reasons": ["blocked_missing_forward_window"],
                    }
                ],
                "blocked_reasons": ["blocked_missing_forward_window"],
            },
        )

    monkeypatch.setattr(cells_service, "verify_data_contract", _fake_verify_data_contract)

    result = cells_service.run_cells(
        run_id="UNIT_CELL_RUN_ZERO",
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
        start="2025-01-01",
        end="2025-01-01T01:00:00Z",
    )

    lineage = read_parquet([Path(result["lineage_path"])])

    assert result["exit_code"] == 1
    assert result["status"] == "blocked_by_data"
    assert result["estimated_hypothesis_count"] == 0
    assert result["skipped_cell_count"] == 1
    assert lineage.empty


def test_run_cells_uses_compiled_surface_as_data_block_gate(
    monkeypatch,
    tmp_path: Path,
) -> None:
    report_path = paths_for_run(data_root=tmp_path, run_id="UNIT_CELL_RUN_PARTIAL").data_contract_path
    captured: dict[str, str] = {}

    def _fake_verify_data_contract(**_kwargs):
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("{}")
        return DataFeasibilityResult(
            status="block",
            report_path=report_path,
            payload={
                "cell_feasibility": [
                    {
                        "event_atom_id": "vol_shock_core",
                        "context_cell": "unconditional",
                        "status": "pass",
                        "blocked_reasons": [],
                    }
                ],
                "blocked_reasons": ["blocked_missing_data"],
            },
        )

    def _fake_run_phase2(**kwargs):
        captured["lineage_path"] = str(kwargs["lineage_path"])
        captured["discovery_mode"] = str(kwargs["discovery_mode"])
        return 0

    monkeypatch.setattr(cells_service, "verify_data_contract", _fake_verify_data_contract)
    monkeypatch.setattr(
        "project.research.phase2_search_engine.run",
        _fake_run_phase2,
    )
    monkeypatch.setattr(cells_service, "build_scoreboard", lambda **_kwargs: {"scoreboard_rows": 0})
    monkeypatch.setattr(
        cells_service,
        "build_redundancy_clusters",
        lambda **_kwargs: {"representative_rows": 0},
    )

    result = cells_service.run_cells(
        run_id="UNIT_CELL_RUN_PARTIAL",
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
        start="2025-01-01",
        end="2025-02-01",
    )

    lineage = read_parquet([Path(captured["lineage_path"])])

    assert result["exit_code"] == 0
    assert result["status"] == "executed"
    assert captured["discovery_mode"] == "edge_cells"
    assert set(lineage["source_event_atom"]) == {"vol_shock_core"}
    assert set(lineage["source_context_cell"]) == {"unconditional"}


def test_summarize_cells_merges_scoreboard_and_skipped_cells(tmp_path: Path) -> None:
    registry = load_registry("spec/discovery")
    run_id = "UNIT_CELL_SUMMARY_MERGE"
    matrix = [
        {
            "event_atom_id": "vol_shock_core",
            "context_cell": "unconditional",
            "status": "pass",
            "blocked_reasons": [],
        },
        {
            "event_atom_id": "funding_extreme_core",
            "context_cell": "unconditional",
            "status": "block",
            "blocked_reasons": ["blocked_missing_data"],
        },
    ]
    compiled = compile_cells(
        registry=registry,
        run_id=run_id,
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
        cell_feasibility=matrix,
    )
    paths = paths_for_run(data_root=tmp_path, run_id=run_id)
    lineage = read_parquet([compiled.lineage_path])
    unconditional = lineage.iloc[0]
    _write_scoreboard_inputs(
        pd.DataFrame(
            [
                {
                    "hypothesis_id": unconditional["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 80,
                    "mean_return_bps": 2.0,
                    "cost_adjusted_return_bps": 2.0,
                    "t_stat": 2.0,
                    "robustness_score": 0.6,
                    **_forward_metrics(2.0),
                },
            ]
        ),
        paths,
    )

    build_scoreboard(registry=registry, run_id=run_id, data_root=tmp_path)
    summary = cells_service.summarize_cells(run_id=run_id, data_root=tmp_path)

    assert summary["scoreboard_rows"] == 1
    assert summary["skipped_cell_count"] == 1
    assert summary["skipped_by_reason"] == {"blocked_missing_data": 1}
    assert summary["skipped_cells"][0]["event_atom_id"] == "funding_extreme_core"


def test_redundancy_and_thesis_assembly_use_representatives_only(tmp_path: Path) -> None:
    registry = load_registry("spec/discovery/expanded_v2")
    run_id = "UNIT_CELL_ASSEMBLY"
    compiled = compile_cells(
        registry=registry,
        run_id=run_id,
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
        start="2025-01-01",
        end="2025-02-01",
    )
    paths = paths_for_run(data_root=tmp_path, run_id=run_id)
    lineage = read_parquet([compiled.lineage_path])
    unconditional = lineage[lineage["source_context_cell"] == "unconditional"].iloc[0]
    executable = _matching_lineage_row(lineage, "high_vol", reference=unconditional)

    _write_scoreboard_inputs(
        pd.DataFrame(
            [
                {
                    "hypothesis_id": unconditional["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 80,
                    "mean_return_bps": 3.0,
                    "cost_adjusted_return_bps": 3.0,
                    "t_stat": 2.0,
                    "robustness_score": 0.6,
                    **_forward_metrics(3.0),
                },
                {
                    "hypothesis_id": executable["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 90,
                    "mean_return_bps": 12.0,
                    "cost_adjusted_return_bps": 11.0,
                    "t_stat": 3.0,
                    "robustness_score": 0.8,
                    **_forward_metrics(11.0, t_stat=3.0),
                },
            ]
        ),
        paths,
    )
    build_scoreboard(registry=registry, run_id=run_id, data_root=tmp_path)

    clusters = build_redundancy_clusters(run_id=run_id, data_root=tmp_path)
    report = assemble_theses(run_id=run_id, data_root=tmp_path)

    assert clusters["representative_rows"] == 1
    assert paths.clusters_path.exists()
    assert paths.cluster_representatives_path.exists()
    assert report["generated_count"] == 1
    proposal = yaml.safe_load(Path(report["generated"][0]["proposal_path"]).read_text())
    assert proposal["start"] == "2025-01-01"
    assert proposal["end"] == "2025-02-01"
    assert proposal["search_spec"]["path"] == str(compiled.search_spec_path)
    assert proposal["hypothesis"]["filters"]["contexts"] == {"vol_regime": ["high"]}
    assert proposal["artifacts"]["context_translation"] == "runtime_executable"
    assert proposal["artifacts"]["context_routing"][0]["condition_name"] == "vol_regime_high"
    assert proposal["artifacts"]["context_routing"][0]["routing_source"] == "runtime"


def test_redundancy_merges_behaviorally_similar_pnl_traces(tmp_path: Path) -> None:
    registry = load_registry("spec/discovery/expanded_v2")
    run_id = "UNIT_CELL_PNL_REDUNDANCY"
    compiled = compile_cells(
        registry=registry,
        run_id=run_id,
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
        start="2025-01-01",
        end="2025-02-01",
    )
    paths = paths_for_run(data_root=tmp_path, run_id=run_id)
    lineage = read_parquet([compiled.lineage_path])
    unconditional = lineage[lineage["source_context_cell"] == "unconditional"].iloc[0]
    high_vol = _matching_lineage_row(lineage, "high_vol", reference=unconditional)
    low_vol = _matching_lineage_row(lineage, "low_vol", reference=unconditional)

    _write_scoreboard_inputs(
        pd.DataFrame(
            [
                {
                    "hypothesis_id": unconditional["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 90,
                    "mean_return_bps": 2.0,
                    "cost_adjusted_return_bps": 2.0,
                    "t_stat": 2.0,
                    "robustness_score": 0.6,
                    **_forward_metrics(2.0),
                },
                {
                    "hypothesis_id": high_vol["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 90,
                    "mean_return_bps": 12.0,
                    "cost_adjusted_return_bps": 12.0,
                    "t_stat": 3.0,
                    "robustness_score": 0.8,
                    **_forward_metrics(12.0, t_stat=3.0),
                },
                {
                    "hypothesis_id": low_vol["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 90,
                    "mean_return_bps": 10.0,
                    "cost_adjusted_return_bps": 10.0,
                    "t_stat": 3.0,
                    "robustness_score": 0.8,
                    **_forward_metrics(10.0, t_stat=3.0),
                },
            ]
        ),
        paths,
    )
    build_scoreboard(registry=registry, run_id=run_id, data_root=tmp_path)
    write_parquet(
        pd.DataFrame(
            [
                {"signal_ts": "2025-01-01T00:00:00Z", "cell_id": high_vol["source_cell_id"], "pnl_bps": 1.0},
                {"signal_ts": "2025-01-01T00:05:00Z", "cell_id": high_vol["source_cell_id"], "pnl_bps": 2.0},
                {"signal_ts": "2025-01-01T00:10:00Z", "cell_id": high_vol["source_cell_id"], "pnl_bps": 3.0},
                {"signal_ts": "2025-01-01T00:00:00Z", "cell_id": low_vol["source_cell_id"], "pnl_bps": 1.1},
                {"signal_ts": "2025-01-01T00:05:00Z", "cell_id": low_vol["source_cell_id"], "pnl_bps": 2.1},
                {"signal_ts": "2025-01-01T00:10:00Z", "cell_id": low_vol["source_cell_id"], "pnl_bps": 3.1},
            ]
        ),
        paths.pnl_traces_path,
    )

    build_redundancy_clusters(run_id=run_id, data_root=tmp_path)
    clusters = read_parquet([paths.clusters_path])
    representatives = read_parquet([paths.cluster_representatives_path])
    pair = clusters[clusters["cell_id"].isin([high_vol["source_cell_id"], low_vol["source_cell_id"]])]

    assert len(pair) == 2
    assert pair["redundancy_cluster_id"].nunique() == 1
    assert set(pair["cluster_basis"]) == {"pnl_similarity+structural"}
    assert pair["max_pnl_similarity"].min() >= 0.85
    assert len(
        representatives[
            representatives["cell_id"].isin([high_vol["source_cell_id"], low_vol["source_cell_id"]])
        ]
    ) == 1


def test_thesis_assembly_downgrades_mapped_supportive_only_representative(
    tmp_path: Path,
) -> None:
    registry = load_registry("spec/discovery")
    run_id = "UNIT_CELL_SUPPORTIVE_REJECT"
    compiled = compile_cells(
        registry=registry,
        run_id=run_id,
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
        start="2025-03-01",
        end="2025-04-01",
    )
    paths = paths_for_run(data_root=tmp_path, run_id=run_id)
    lineage = read_parquet([compiled.lineage_path])
    unconditional = lineage[lineage["source_context_cell"] == "unconditional"].iloc[0]
    supportive = lineage[lineage["source_context_cell"] == "positive_funding"].iloc[0]

    _write_scoreboard_inputs(
        pd.DataFrame(
            [
                {
                    "hypothesis_id": unconditional["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 80,
                    "mean_return_bps": 2.0,
                    "cost_adjusted_return_bps": 2.0,
                    "t_stat": 2.0,
                    "robustness_score": 0.6,
                    **_forward_metrics(2.0),
                },
                {
                    "hypothesis_id": supportive["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 90,
                    "mean_return_bps": 20.0,
                    "cost_adjusted_return_bps": 19.0,
                    "t_stat": 4.0,
                    "robustness_score": 0.9,
                    **_forward_metrics(19.0, t_stat=4.0),
                },
            ]
        ),
        paths,
    )
    build_scoreboard(registry=registry, run_id=run_id, data_root=tmp_path)
    build_redundancy_clusters(run_id=run_id, data_root=tmp_path)

    report = assemble_theses(run_id=run_id, data_root=tmp_path)

    assert report["generated_count"] == 1
    assert report["rejected_count"] == 0
    assert report["generated"][0]["disposition"] == "supportive_only_context_downgraded"
    proposal = yaml.safe_load(Path(report["generated"][0]["proposal_path"]).read_text())
    assert proposal["start"] == "2025-03-01"
    assert proposal["end"] == "2025-04-01"
    assert proposal["search_spec"]["path"] == str(compiled.search_spec_path)
    assert proposal["hypothesis"]["filters"]["contexts"] == {}
    assert proposal["artifacts"]["context_translation"] == "supportive_only_context_downgraded"
    assert proposal["artifacts"]["context_routing"][0]["condition_name"] == (
        "carry_state_funding_pos"
    )
    assert proposal["artifacts"]["supportive_context"]["canonical_regime"] == (
        "BASIS_FUNDING_DISLOCATION"
    )


def test_thesis_assembly_rejects_unmapped_supportive_only_representative(
    tmp_path: Path,
) -> None:
    registry = load_registry("spec/discovery")
    run_id = "UNIT_CELL_SUPPORTIVE_UNMAPPED_REJECT"
    compiled = compile_cells(
        registry=registry,
        run_id=run_id,
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
        start="2025-05-01",
        end="2025-06-01",
    )
    paths = paths_for_run(data_root=tmp_path, run_id=run_id)
    lineage = read_parquet([compiled.lineage_path])
    unconditional = lineage[lineage["source_context_cell"] == "unconditional"].iloc[0]
    supportive = lineage[lineage["source_context_cell"] == "positive_funding"].iloc[0]

    _write_scoreboard_inputs(
        pd.DataFrame(
            [
                {
                    "hypothesis_id": unconditional["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 80,
                    "mean_return_bps": 2.0,
                    "cost_adjusted_return_bps": 2.0,
                    "t_stat": 2.0,
                    "robustness_score": 0.6,
                    **_forward_metrics(2.0),
                },
                {
                    "hypothesis_id": supportive["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 90,
                    "mean_return_bps": 20.0,
                    "cost_adjusted_return_bps": 19.0,
                    "t_stat": 4.0,
                    "robustness_score": 0.9,
                    **_forward_metrics(19.0, t_stat=4.0),
                },
            ]
        ),
        paths,
    )
    build_scoreboard(registry=registry, run_id=run_id, data_root=tmp_path)
    build_redundancy_clusters(run_id=run_id, data_root=tmp_path)
    representatives = read_parquet([paths.cluster_representatives_path])
    representatives["supportive_context_json"] = ""
    write_parquet(representatives, paths.cluster_representatives_path)

    report = assemble_theses(run_id=run_id, data_root=tmp_path)

    assert report["generated_count"] == 0
    assert report["rejected_count"] == 1
    assert report["rejected"][0]["reason"] == "supportive_only_context_unmapped"


def test_thesis_assembly_rejects_missing_source_scope(tmp_path: Path) -> None:
    registry = load_registry("spec/discovery/expanded_v2")
    run_id = "UNIT_CELL_MISSING_SCOPE_REJECT"
    compiled = compile_cells(
        registry=registry,
        run_id=run_id,
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
    )
    paths = paths_for_run(data_root=tmp_path, run_id=run_id)
    lineage = read_parquet([compiled.lineage_path])
    unconditional = lineage[lineage["source_context_cell"] == "unconditional"].iloc[0]
    executable = _matching_lineage_row(lineage, "high_vol", reference=unconditional)
    _write_scoreboard_inputs(
        pd.DataFrame(
            [
                {
                    "hypothesis_id": unconditional["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 80,
                    "mean_return_bps": 2.0,
                    "cost_adjusted_return_bps": 2.0,
                    "t_stat": 2.0,
                    "robustness_score": 0.6,
                    **_forward_metrics(2.0),
                },
                {
                    "hypothesis_id": executable["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 90,
                    "mean_return_bps": 12.0,
                    "cost_adjusted_return_bps": 11.0,
                    "t_stat": 3.0,
                    "robustness_score": 0.8,
                    **_forward_metrics(11.0, t_stat=3.0),
                },
            ]
        ),
        paths,
    )
    build_scoreboard(registry=registry, run_id=run_id, data_root=tmp_path)
    build_redundancy_clusters(run_id=run_id, data_root=tmp_path)

    report = assemble_theses(run_id=run_id, data_root=tmp_path)

    assert report["generated_count"] == 0
    assert report["rejected_count"] == 1
    assert report["rejected"][0]["reason"] == "invalid_proposal: missing_source_scope"


def test_thesis_assembly_rejects_runtime_context_blocked_by_condition_routing(
    monkeypatch,
    tmp_path: Path,
) -> None:
    registry = load_registry("spec/discovery/expanded_v2")
    run_id = "UNIT_CELL_RUNTIME_ROUTING_REJECT"
    compiled = compile_cells(
        registry=registry,
        run_id=run_id,
        data_root=tmp_path,
        symbols=["BTCUSDT"],
        timeframe="5m",
        start="2025-07-01",
        end="2025-08-01",
    )
    paths = paths_for_run(data_root=tmp_path, run_id=run_id)
    lineage = read_parquet([compiled.lineage_path])
    unconditional = lineage[lineage["source_context_cell"] == "unconditional"].iloc[0]
    executable = _matching_lineage_row(lineage, "high_vol", reference=unconditional)
    _write_scoreboard_inputs(
        pd.DataFrame(
            [
                {
                    "hypothesis_id": unconditional["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 80,
                    "mean_return_bps": 2.0,
                    "cost_adjusted_return_bps": 2.0,
                    "t_stat": 2.0,
                    "robustness_score": 0.6,
                    **_forward_metrics(2.0),
                },
                {
                    "hypothesis_id": executable["hypothesis_id"],
                    "symbol": "BTCUSDT",
                    "n_events": 90,
                    "mean_return_bps": 12.0,
                    "cost_adjusted_return_bps": 11.0,
                    "t_stat": 3.0,
                    "robustness_score": 0.8,
                    **_forward_metrics(11.0, t_stat=3.0),
                },
            ]
        ),
        paths,
    )
    build_scoreboard(registry=registry, run_id=run_id, data_root=tmp_path)
    build_redundancy_clusters(run_id=run_id, data_root=tmp_path)
    monkeypatch.setattr(
        "project.research.cell_discovery.thesis_assembly.condition_routing",
        lambda *_args, **_kwargs: ("__BLOCKED__", "blocked"),
    )

    report = assemble_theses(run_id=run_id, data_root=tmp_path)

    assert report["generated_count"] == 0
    assert report["rejected_count"] == 1
    assert report["rejected"][0]["reason"] == "runtime_context_not_executable"
