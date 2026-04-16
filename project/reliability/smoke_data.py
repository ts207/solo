from __future__ import annotations

import hashlib
import json
import os
import platform
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List


import numpy as np
import pandas as pd

from project.core.config import get_data_root
from project.core.feature_schema import feature_dataset_dir_name
from project.engine.runner import run_engine
from project.io.utils import ensure_dir, write_parquet
from project.research.cli.promotion_cli import run_promotion_cli
from project.research.candidate_schema import ensure_candidate_schema
from project.research.discovery import _synthesize_registry_candidates
from project.research.services.candidate_discovery_service import (
    _apply_validation_multiple_testing,
    _split_and_score_candidates,
)
from project.research.regime_routing import annotate_regime_metadata
from project.research.services.reporting_service import write_candidate_reports
from project.research.validation import assign_test_families, apply_multiple_testing
from project.research.validation.evidence_bundle import bundle_to_flat_record
from project.specs.ontology import ontology_spec_hash
from project.reliability.manifest_checks import summarize_manifest_environment
from project.reliability.schemas import SMOKE_DATASET_VERSION


SMOKE_RUN_ID = "smoke_run"
SMOKE_SYMBOLS = ("BTCUSDT", "ETHUSDT")
SMOKE_EVENT_TYPE = "VOL_SHOCK"
SMOKE_HORIZON_BARS = 24
SMOKE_BAR_PERIODS = 288
SMOKE_EVENT_PERIODS = 96


@dataclass(frozen=True)
class SmokeDatasetInfo:
    root: Path
    run_id: str
    symbols: tuple[str, ...]
    seed: int
    start_ts: pd.Timestamp
    end_ts: pd.Timestamp


def _rng(seed: int) -> np.random.Generator:
    return np.random.default_rng(int(seed))


def _stable_symbol_seed(symbol: str) -> int:
    digest = hashlib.sha256(str(symbol).encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big")


def _write_df(df: pd.DataFrame, path: Path) -> Path:
    actual, _ = write_parquet(df, path)
    return Path(actual)


def build_smoke_bars(
    symbol: str, *, periods: int = SMOKE_BAR_PERIODS, freq: str = "5min", seed: int = 0
) -> pd.DataFrame:
    rng = _rng(seed + (_stable_symbol_seed(symbol) % 1000))
    ts = pd.date_range("2024-01-01", periods=periods, freq=freq, tz="UTC")
    base = 100.0 if symbol == "BTCUSDT" else 60.0
    trend = np.linspace(0.0, 3.0 if symbol == "BTCUSDT" else 0.4, periods)
    noise = rng.normal(0.0, 0.15 if symbol == "BTCUSDT" else 0.25, periods).cumsum() / 10.0
    close = base + trend + noise
    open_ = np.concatenate([[close[0]], close[:-1]])
    high = np.maximum(open_, close) + 0.2
    low = np.minimum(open_, close) - 0.2
    volume = np.linspace(1000.0, 1200.0, periods) + (50 if symbol == "BTCUSDT" else -25)
    quote_volume = volume * close
    return pd.DataFrame(
        {
            "timestamp": ts,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "quote_volume": quote_volume,
            "funding_rate_scaled": 0.0001 if symbol == "BTCUSDT" else -0.0001,
            "direction_score": np.where(symbol == "BTCUSDT", 0.65, -0.05),
        }
    )


def build_smoke_events(
    symbol: str, *, periods: int = SMOKE_EVENT_PERIODS, freq: str = "15min", seed: int = 0
) -> pd.DataFrame:
    rng = _rng(seed + (_stable_symbol_seed(symbol) % 1000))
    ts = pd.date_range("2024-01-01", periods=periods, freq=freq, tz="UTC")
    strong = symbol == "BTCUSDT"
    base_ret = 0.0015 if strong else -0.0002
    noise = rng.normal(0.0, 0.00035 if strong else 0.0015, periods)
    returns = base_ret + noise
    return pd.DataFrame(
        {
            "enter_ts": ts,
            "timestamp": ts,
            "symbol": symbol,
            "event_type": SMOKE_EVENT_TYPE,
            f"return_{SMOKE_HORIZON_BARS}": returns,
        }
    )


def build_smoke_dataset(
    output_root: Path, *, seed: int = 20260101, storage_mode: str = "auto"
) -> SmokeDatasetInfo:
    root = Path(output_root)
    ensure_dir(root)
    if storage_mode == "csv-fallback":
        os.environ["BACKTEST_FORCE_CSV_FALLBACK"] = "1"
    elif storage_mode == "auto":
        os.environ.pop("BACKTEST_FORCE_CSV_FALLBACK", None)
    os.environ["BACKTEST_DATA_ROOT"] = str(root)

    start_ts = pd.Timestamp("2024-01-01T00:00:00Z")
    end_ts = pd.Timestamp("2024-01-02T00:00:00Z")
    for symbol in SMOKE_SYMBOLS:
        bars = build_smoke_bars(symbol, seed=seed)
        feature_dir = (
            root / "lake" / "features" / "perp" / symbol / "5m" / feature_dataset_dir_name()
        )
        bar_dir = root / "lake" / "cleaned" / "perp" / symbol / "bars_5m"
        ensure_dir(feature_dir)
        ensure_dir(bar_dir)
        _write_df(
            bars[
                [
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "quote_volume",
                    "funding_rate_scaled",
                    "direction_score",
                ]
            ],
            feature_dir / "slice.parquet",
        )
        _write_df(
            bars[["timestamp", "open", "high", "low", "close", "volume", "quote_volume"]],
            bar_dir / "slice.parquet",
        )
    universe = pd.DataFrame(
        {
            "symbol": list(SMOKE_SYMBOLS),
            "listing_start": [pd.Timestamp("2023-01-01T00:00:00Z")] * len(SMOKE_SYMBOLS),
            "listing_end": [pd.Timestamp("2025-12-31T00:00:00Z")] * len(SMOKE_SYMBOLS),
        }
    )
    ensure_dir(root / "lake" / "metadata" / "universe_snapshots")
    _write_df(universe, root / "lake" / "metadata" / "universe_snapshots" / "univ.parquet")
    ensure_dir(root / "runs" / SMOKE_RUN_ID)
    run_manifest = {
        "run_id": SMOKE_RUN_ID,
        "run_mode": "confirmatory",
        "discovery_profile": "smoke",
        "confirmatory_rerun_run_id": "",
        "candidate_origin_run_id": SMOKE_RUN_ID,
        "program_id": "smoke_program",
        "smoke_dataset_version": SMOKE_DATASET_VERSION,
    }
    (root / "runs" / SMOKE_RUN_ID / "run_manifest.json").write_text(
        json.dumps(run_manifest, indent=2, sort_keys=True), encoding="utf-8"
    )
    return SmokeDatasetInfo(
        root=root,
        run_id=SMOKE_RUN_ID,
        symbols=SMOKE_SYMBOLS,
        seed=int(seed),
        start_ts=start_ts,
        end_ts=end_ts,
    )


def run_engine_smoke(dataset: SmokeDatasetInfo) -> Dict[str, Any]:
    from project.strategy.dsl.schema import (
        Blueprint,
        SymbolScopeSpec,
        EntrySpec,
        ExitSpec,
        SizingSpec,
        LineageSpec,
        EvaluationSpec,
    )

    bp = Blueprint(
        id="smoke_dsl",
        run_id=dataset.run_id,
        event_type="mock_event",
        candidate_id="smoke_candidate",
        direction="long",
        symbol_scope=SymbolScopeSpec(
            mode="single_symbol", symbols=["BTCUSDT"], candidate_symbol="BTCUSDT"
        ),
        entry=EntrySpec(
            triggers=["event_detected"],
            conditions=[],
            confirmations=[],
            delay_bars=0,
            cooldown_bars=0,
            condition_logic="all",
            condition_nodes=[],
            arm_bars=0,
            reentry_lockout_bars=0,
        ),
        exit=ExitSpec(
            time_stop_bars=5,
            invalidation={"metric": "smoke", "operator": "==", "value": 0.0},
            stop_type="percent",
            stop_value=0.05,
            target_type="percent",
            target_value=0.05,
            trailing_stop_type="none",
            trailing_stop_value=0.0,
            break_even_r=0.0,
        ),
        sizing=SizingSpec(
            mode="fixed_risk",
            risk_per_trade=0.01,
            target_vol=None,
            max_gross_leverage=1.0,
            max_position_scale=1.0,
            portfolio_risk_budget=1.0,
            symbol_risk_budget=1.0,
        ),
        overlays=[],
        evaluation=EvaluationSpec(
            min_trades=0,
            cost_model={"fees_bps": 5.0, "slippage_bps": 0.0, "funding_included": True},
            robustness_flags={
                "oos_required": False,
                "multiplicity_required": False,
                "regime_stability_required": False,
            },
        ),
        lineage=LineageSpec(
            source_path="smoke", compiler_version="smoke", generated_at_utc="smoke"
        ),
    )
    result = run_engine(
        data_root=dataset.root,
        run_id=dataset.run_id,
        symbols=["BTCUSDT"],
        strategies=["dsl_interpreter_v1__smoke_dsl"],
        params_by_strategy={"dsl_interpreter_v1__smoke_dsl": {"dsl_blueprint": bp.model_dump()}},
        params={
            "max_portfolio_gross": 1.0,
            "max_strategy_gross": 1.0,
            "max_symbol_gross": 1.0,
            "max_new_exposure_per_bar": 2.0,
        },
        cost_bps=5.0,
        start_ts=dataset.start_ts,
        end_ts=dataset.end_ts,
    )
    return result


def run_research_smoke(dataset: SmokeDatasetInfo) -> Dict[str, Any]:
    symbol_candidates: Dict[str, pd.DataFrame] = {}
    frames: List[pd.DataFrame] = []
    for symbol in dataset.symbols:
        events = build_smoke_events(symbol, seed=dataset.seed)
        features = build_smoke_bars(symbol, seed=dataset.seed)
        candidates = _synthesize_registry_candidates(
            run_id=dataset.run_id,
            symbol=symbol,
            event_type=SMOKE_EVENT_TYPE,
            events_df=events,
            horizon_bars=SMOKE_HORIZON_BARS,
            entry_lag_bars=0,
        )
        candidates["hypothesis_id"] = [f"hyp_smoke_{symbol.lower()}_{idx}" for idx in range(len(candidates))]
        candidates["run_id"] = dataset.run_id
        candidates["split_scheme_id"] = "smoke_tvt"
        scored = _split_and_score_candidates(
            candidates,
            events,
            features_df=features,
            horizon_bars=SMOKE_HORIZON_BARS,
            split_scheme_id="smoke_tvt",
            purge_bars=1,
            embargo_bars=1,
            bar_duration_minutes=5,
        )
        symbol_candidates[symbol] = scored
        frames.append(scored)
    combined = pd.concat(frames, ignore_index=True)
    combined = _apply_validation_multiple_testing(combined)
    combined = annotate_regime_metadata(combined)
    symbol_candidates = {str(symbol): frame.copy() for symbol, frame in combined.groupby("symbol")}
    out_dir = dataset.root / "reports" / "phase2" / dataset.run_id
    write_candidate_reports(
        out_dir=out_dir,
        combined_candidates=combined,
        symbol_candidates=symbol_candidates,
        diagnostics={
            "run_id": dataset.run_id,
            "smoke_dataset_version": SMOKE_DATASET_VERSION,
            "combined_candidate_rows": int(len(combined)),
        },
    )
    return {
        "combined_candidates": combined,
        "symbol_candidates": symbol_candidates,
        "output_dir": out_dir,
    }


def build_smoke_edge_candidates(
    research_result: Dict[str, Any], dataset: SmokeDatasetInfo
) -> pd.DataFrame:
    combined = research_result["combined_candidates"].copy()
    rows = []
    current_hash = ontology_spec_hash(Path.cwd())
    for i, rec in enumerate(combined.to_dict(orient="records")):
        symbol = str(rec["symbol"])
        candidate_id = str(rec["candidate_id"])
        promoted_like = symbol == "BTCUSDT" and "continuation" in str(rec.get("rule_template", ""))
        rows.append(
            {
                "candidate_id": candidate_id,
                "hypothesis_id": rec.get("hypothesis_id", ""),
                "family_id": rec.get("family_id", "SMOKE_FAMILY"),
                "run_id": dataset.run_id,
                "symbol": symbol,
                "event_type": rec.get("event_type", SMOKE_EVENT_TYPE),
                "event": rec.get("event_type", SMOKE_EVENT_TYPE),
                "template_verb": rec.get("rule_template", "continuation"),
                "horizon": rec.get("horizon", "24b"),
                "state_id": "SMOKE_STATE",
                "condition_label": "all",
                "effect_raw": float(rec.get("estimate", 0.0)),
                "effect_shrunk_state": float(rec.get("estimate", 0.0)),
                "p_value": float(rec.get("p_value_raw", 1.0)),
                "q_value": float(rec.get("p_value_adj", 1.0)),
                "q_value_by": float(rec.get("p_value_adj_by", rec.get("p_value_adj", 1.0))),
                "q_value_cluster": float(rec.get("p_value_adj_holm", rec.get("p_value_adj", 1.0))),
                "q_value_family": float(rec.get("p_value_adj", 1.0)),
                "is_discovery": bool(promoted_like),
                "n_events": int(rec.get("n_obs", 120 if promoted_like else 80)),
                "selection_score": float(rec.get("estimate_bps", 10.0 if promoted_like else -1.0)),
                "robustness_score": 0.85 if promoted_like else 0.20,
                "effective_sample_size": int(rec.get("n_obs", 120 if promoted_like else 80)),
                "gate_phase2_final": True,
                "fail_reasons": "" if promoted_like else "null_effect",
                "fail_gate_primary": "" if promoted_like else "gate_phase2_final",
                "fail_reason_primary": "" if promoted_like else "null_effect",
                "effective_lag_bars": 0,
                "gate_bridge_tradable": bool(promoted_like),
                "bridge_fail_gate_primary": "" if promoted_like else "bridge_null",
                "bridge_fail_reason_primary": "" if promoted_like else "bridge_null",
                "promotion_fail_gate_primary": "",
                "promotion_fail_reason_primary": "",
                "gate_promo_statistical": "pass" if promoted_like else "missing_evidence",
                "gate_promo_stability": "pass" if promoted_like else "missing_evidence",
                "gate_promo_cost_survival": "pass" if promoted_like else "missing_evidence",
                "gate_promo_negative_control": "pass" if promoted_like else "missing_evidence",
                "gate_promo_hypothesis_audit": "pass" if promoted_like else "missing_evidence",
                "plan_row_id": f"plan_{i}",
                "confirmatory_locked": True,
                "frozen_spec_hash": current_hash,
                "run_mode": "confirmatory",
                "baseline_expectancy_bps": 5.0,
                "bridge_validation_after_cost_bps": float(
                    rec.get("estimate_bps", 15.0 if promoted_like else -2.0)
                ),
                "bridge_validation_stressed_after_cost_bps": float(
                    rec.get("estimate_bps", 12.0 if promoted_like else -3.0)
                ),
                "net_expectancy_bps": float(
                    rec.get("estimate_bps", 12.0 if promoted_like else -3.0)
                ),
                "std_return": 0.01 if promoted_like else 0.03,
                "gate_stability": promoted_like,
                "val_t_stat": 2.6 if promoted_like else -0.3,
                "oos1_t_stat": 2.0 if promoted_like else -0.5,
                "test_t_stat": 1.9 if promoted_like else -0.2,
                "gate_after_cost_positive": promoted_like,
                "gate_after_cost_stressed_positive": promoted_like,
                "gate_bridge_after_cost_positive_validation": promoted_like,
                "gate_bridge_after_cost_stressed_positive_validation": promoted_like,
                "gate_delay_robustness": promoted_like,
                "validation_samples": 120 if promoted_like else 40,
                "test_samples": 60 if promoted_like else 20,
                "pass_shift_placebo": promoted_like,
                "pass_random_entry_placebo": promoted_like,
                "pass_direction_reversal_placebo": promoted_like,
                "event_is_descriptive": False,
                "event_is_trade_trigger": True,
                "gate_delayed_entry_stress": promoted_like,
                "gate_bridge_microstructure": promoted_like,
                "control_pass_rate": 0.0 if promoted_like else 0.5,
                "tob_coverage": 0.95 if promoted_like else 0.10,
                "mean_train_return": 0.010 if promoted_like else -0.003,
                "mean_validation_return": 0.012 if promoted_like else -0.001,
                "mean_test_return": 0.009 if promoted_like else -0.002,
                "regime_mean_map": {"low": 8.0, "high": 6.0}
                if promoted_like
                else {"low": -1.0, "high": 0.5},
                "regime_counts": {"low": 40, "high": 35},
                "symbol_expectancy_map": {
                    symbol: float(rec.get("estimate_bps", 15.0 if promoted_like else -2.0))
                },
                "cost_survival_ratio": 1.0 if promoted_like else 0.25,
                "effective_cost_bps": 3.0,
                "turnover_proxy_mean": 1.5,
                "gate_regime_stability": promoted_like,
                "gate_structural_break": promoted_like,
                "shrinkage_loso_stable": True,
                "shrinkage_borrowing_dominant": False,
                "structural_robustness_score": 0.9 if promoted_like else 0.2,
                "repeated_fold_consistency": 0.8 if promoted_like else 0.1,
                "robustness_panel_complete": promoted_like,
                "bridge_certified": promoted_like,
                "gate_bridge_low_capital_viability": promoted_like,
                "low_capital_viability_score": 0.8 if promoted_like else 0.1,
                "low_capital_reject_reason_codes": "" if promoted_like else "capacity,costs",
                "gate_promo_retail_viability": promoted_like,
                "gate_promo_low_capital_viability": promoted_like,
                "sign_consistency": 0.80 if promoted_like else 0.40,
                "stability_score": float(rec.get("robustness_score", 0.85 if promoted_like else 0.2)),
            }
        )
    out = ensure_candidate_schema(pd.DataFrame(rows))
    out["p_value_for_fdr"] = pd.to_numeric(out.get("p_value", np.nan), errors="coerce")
    return out


def materialize_smoke_promotion_inputs(
    dataset: SmokeDatasetInfo, research_result: Dict[str, Any]
) -> pd.DataFrame:
    candidates = build_smoke_edge_candidates(research_result, dataset)
    edge_dir = dataset.root / "reports" / "edge_candidates" / dataset.run_id
    ensure_dir(edge_dir)
    _write_df(candidates, edge_dir / "edge_candidates_normalized.parquet")
    phase2_dir = dataset.root / "reports" / "phase2" / dataset.run_id
    ensure_dir(phase2_dir)
    hypothesis_registry = pd.DataFrame(
        [
            {
                "hypothesis_id": str(row.get("hypothesis_id", "")).strip(),
                "plan_row_id": str(row.get("plan_row_id", "")).strip()
                or str(row.get("hypothesis_id", "")).strip(),
                "statuses": ["executed"],
                "executed": True,
            }
            for row in candidates.to_dict(orient="records")
            if str(row.get("hypothesis_id", "")).strip()
        ]
    )
    _write_df(hypothesis_registry, phase2_dir / "hypothesis_registry.parquet")
    neg_dir = dataset.root / "reports" / "negative_control" / dataset.run_id
    ensure_dir(neg_dir)
    (neg_dir / "negative_control_summary.json").write_text(
        json.dumps({"by_event": {SMOKE_EVENT_TYPE: {"pass_rate_after_bh": 0.0}}}, indent=2),
        encoding="utf-8",
    )
    materialize_smoke_validation_artifacts(dataset, candidates)
    return candidates


def materialize_smoke_validation_artifacts(
    dataset: SmokeDatasetInfo, candidates: pd.DataFrame
) -> None:
    """Write the minimal canonical validation artifacts required by execute_promotion.

    The smoke flow bypasses the real validate stage, so we synthesize a structurally
    valid ValidationBundle from the smoke edge-candidate table and write it to the
    expected validation directory.
    """
    from project.research.validation.contracts import (
        ValidatedCandidateRecord,
        ValidationBundle,
        ValidationDecision,
        ValidationMetrics,
    )
    from project.research.validation.result_writer import (
        write_validated_candidate_tables,
        write_validation_bundle,
    )

    val_dir = dataset.root / "reports" / "validation" / dataset.run_id
    ensure_dir(val_dir)

    validated: List[ValidatedCandidateRecord] = []
    rejected: List[ValidatedCandidateRecord] = []

    for row in candidates.to_dict(orient="records"):
        candidate_id = str(row.get("candidate_id", "")).strip()
        if not candidate_id:
            continue
        promoted_like = bool(row.get("is_discovery", False))
        status = "validated" if promoted_like else "rejected"
        decision = ValidationDecision(
            status=status,
            candidate_id=candidate_id,
            run_id=dataset.run_id,
            program_id="smoke_program",
            reason_codes=[] if promoted_like else ["failed_oos_validation"],
            summary="smoke synthetic validation",
        )
        metrics = ValidationMetrics(
            sample_count=int(row.get("n_events", 80)),
            effective_sample_size=float(row.get("effective_sample_size", 80)),
            expectancy=float(row.get("effect_raw", 0.0)),
            net_expectancy=float(row.get("net_expectancy_bps", 0.0)) / 10_000.0,
            hit_rate=0.55 if promoted_like else 0.45,
            p_value=float(row.get("p_value", 0.01 if promoted_like else 0.5)),
            q_value=float(row.get("q_value", 0.05 if promoted_like else 0.8)),
            stability_score=float(row.get("robustness_score", 0.85 if promoted_like else 0.2)),
            cost_sensitivity=float(row.get("cost_survival_ratio", 1.0 if promoted_like else 0.25)),
        )
        record = ValidatedCandidateRecord(
            candidate_id=candidate_id,
            decision=decision,
            metrics=metrics,
            anchor_summary=SMOKE_EVENT_TYPE,
            template_id=str(row.get("template_verb", "continuation")),
            direction="long",
            horizon_bars=SMOKE_HORIZON_BARS,
            validation_stage_version="v1",
        )
        if promoted_like:
            validated.append(record)
        else:
            rejected.append(record)

    created_at = datetime.now(timezone.utc).isoformat()
    bundle = ValidationBundle(
        run_id=dataset.run_id,
        created_at=created_at,
        program_id="smoke_program",
        validated_candidates=validated,
        rejected_candidates=rejected,
        inconclusive_candidates=[],
        summary_stats={
            "run_id": dataset.run_id,
            "validated_count": len(validated),
            "rejected_count": len(rejected),
            "smoke": True,
        },
        effect_stability_report={
            "run_id": dataset.run_id,
            "smoke": True,
            "stability_by_candidate": {},
        },
    )
    write_validation_bundle(bundle, base_dir=val_dir)
    write_validated_candidate_tables(bundle, base_dir=val_dir)


def run_promotion_smoke(
    dataset: SmokeDatasetInfo, research_result: Dict[str, Any]
) -> Dict[str, Any]:
    materialize_smoke_promotion_inputs(dataset, research_result)
    result = run_promotion_cli(
        [
            "--run_id",
            dataset.run_id,
            "--program_id",
            "smoke_program",
            "--objective_name",
            "retail_profitability",
            "--promotion_profile",
            "research",
            "--min_events",
            "40",
            "--min_tob_coverage",
            "0.60",
            "--allow_missing_negative_controls",
            "0",
            "--require_multiplicity_diagnostics",
            "1",
        ]
    )
    return {"service_result": result, "output_dir": result.output_dir}


def build_smoke_summary(*, dataset: SmokeDatasetInfo, storage_mode: str) -> Dict[str, Any]:
    config_hash = hashlib.sha256(
        json.dumps(
            {"seed": dataset.seed, "storage_mode": storage_mode, "symbols": list(dataset.symbols)},
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return summarize_manifest_environment(
        git_sha=os.getenv("GITHUB_SHA", "local"),
        python_version=sys.version.split()[0],
        storage_mode="csv-fallback"
        if os.getenv("BACKTEST_FORCE_CSV_FALLBACK", "0") in {"1", "true", "TRUE"}
        else "parquet",
        smoke_dataset_version=SMOKE_DATASET_VERSION,
        config_hash=config_hash,
    )
