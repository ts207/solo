from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

import numpy as np
import pandas as pd

from project import PROJECT_ROOT
from project.core.coercion import as_bool, safe_float, safe_int
from project.core.config import get_data_root
from project.core.logging_utils import build_stage_log_handlers
from project.core.timeframes import normalize_timeframe
from project.events.phase2 import PHASE2_EVENT_CHAIN as _CANONICAL_PHASE2_EVENT_CHAIN
from project.io.utils import ensure_dir, write_parquet
from project.research.services.pathing import bridge_event_out_dir
from project.specs.manifest import finalize_manifest, start_manifest
from project.specs.ontology import ontology_spec_hash

PHASE2_EVENT_CHAIN = list(_CANONICAL_PHASE2_EVENT_CHAIN)


def _is_missing_value(value: object) -> bool:
    return value is None or (isinstance(value, float) and not np.isfinite(value))


def _quiet_float(value: object, default: float) -> float:
    if _is_missing_value(value):
        return float(default)
    coerced = safe_float(value, default)
    return float(default if coerced is None else coerced)


def _quiet_int(value: object, default: int) -> int:
    if _is_missing_value(value):
        return int(default)
    coerced = safe_int(value, default)
    return int(default if coerced is None else coerced)


def _normalize_direction_value(value: object) -> str:
    if _is_missing_value(value):
        return ""
    if isinstance(value, (int, float, np.integer, np.floating)):
        numeric = float(value)
        if numeric > 0:
            return "long"
        if numeric < 0:
            return "short"
        return "flat"
    text = str(value).strip().lower()
    if not text:
        return ""
    if text in {"1", "+1", "1.0", "+1.0", "long", "buy", "up", "bull", "bullish"}:
        return "long"
    if text in {"-1", "-1.0", "short", "sell", "down", "bear", "bearish"}:
        return "short"
    if text in {"0", "0.0", "flat", "neutral", "both"}:
        return "flat"
    return text


def _parse_symbols_csv(symbols_csv: str) -> list[str]:
    symbols = [s.strip().upper() for s in str(symbols_csv).split(",") if s.strip()]
    ordered: list[str] = []
    seen = set()
    for symbol in symbols:
        if symbol not in seen:
            ordered.append(symbol)
            seen.add(symbol)
    return ordered


def _infer_symbol_tag(row: dict[str, object], run_symbols: Sequence[str]) -> str:
    symbol_value = str(row.get("symbol", "")).strip().upper()
    if symbol_value:
        return symbol_value
    condition = str(row.get("condition", "")).strip().lower()
    if condition.startswith("symbol_"):
        inferred = condition.removeprefix("symbol_").upper()
        if inferred:
            return inferred
    if len(run_symbols) == 1:
        return str(run_symbols[0]).upper()
    return "ALL"


def _candidate_type_from_action(action_name: str) -> str:
    action = str(action_name or "").strip().lower()
    if action == "entry_gate_skip" or action.startswith("risk_throttle_"):
        return "overlay"
    if action == "no_action" or action.startswith("delay_") or action == "reenable_at_half_life":
        return "standalone"
    return "standalone"


def _is_confirmatory_run_mode(run_mode: str) -> bool:
    return str(run_mode or "").strip().lower() in {
        "confirmatory",
        "production",
        "certification",
        "promotion",
        "deploy",
    }


def _load_latest_adjacent_survivorship_index(
    run_id: str,
) -> tuple[dict[tuple[str, str, str, str], dict[str, object]], str | None]:
    data_root = get_data_root()
    base = data_root / "reports" / "adjacent_survivorship"
    if not base.exists():
        return {}, None

    candidates = sorted(
        base.glob(f"*/vs_{run_id}/adjacent_survivorship.json"),
        key=lambda path: path.stat().st_mtime if path.exists() else 0.0,
        reverse=True,
    )
    for path in candidates:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        index: dict[tuple[str, str, str, str], dict[str, object]] = {}
        rows = payload.get("candidate_rows", [])
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            key = (
                str(row.get("symbol", "")),
                str(row.get("event_type", "")),
                str(row.get("direction", "")),
                str(row.get("horizon", "")),
            )
            index[key] = row
        return index, str(path)
    return {}, None


def _apply_adjacent_survivorship_annotations(
    df: pd.DataFrame, *, run_id: str
) -> tuple[pd.DataFrame, str | None]:
    if df.empty:
        return df.copy(), None
    required = ["candidate_symbol", "event_type", "direction", "horizon"]
    if not all(col in df.columns for col in required):
        out = df.copy()
        out["adjacent_window_survived"] = np.nan
        out["adjacent_window_target_run_id"] = np.nan
        out["adjacent_window_failure_reasons"] = np.nan
        out["adjacent_window_target_after_cost_expectancy_per_trade"] = np.nan
        return out, None

    adjacent_index, report_path = _load_latest_adjacent_survivorship_index(run_id)
    out = df.copy()
    survived: list[object] = []
    target_runs: list[object] = []
    fail_reasons: list[object] = []
    target_expectancy: list[object] = []

    report_target_run_id = None
    if report_path is not None:
        try:
            report_target_run_id = Path(report_path).parent.parent.name
        except Exception:
            report_target_run_id = None

    for _, row in out.iterrows():
        key = (
            str(row.get("candidate_symbol", "")),
            str(row.get("event_type", row.get("event", ""))),
            str(row.get("direction", "")),
            str(row.get("horizon", "")),
        )
        match = adjacent_index.get(key)
        if match is None:
            survived.append(np.nan)
            target_runs.append(np.nan)
            fail_reasons.append(np.nan)
            target_expectancy.append(np.nan)
            continue
        survived.append(bool(match.get("survived_adjacent_window", False)))
        target_runs.append(str(match.get("target_run_id") or report_target_run_id or ""))
        failure_tokens = match.get("failure_reasons", [])
        if isinstance(failure_tokens, list):
            fail_reasons.append(
                "|".join(str(token) for token in failure_tokens if str(token).strip())
            )
        else:
            fail_reasons.append(str(failure_tokens))
        target_expectancy.append(match.get("target_after_cost_expectancy_per_trade"))

    out["adjacent_window_survived"] = survived
    out["adjacent_window_target_run_id"] = target_runs
    out["adjacent_window_failure_reasons"] = fail_reasons
    out["adjacent_window_target_after_cost_expectancy_per_trade"] = target_expectancy
    return out, report_path


def _normalize_edge_candidates_df(
    df: pd.DataFrame,
    *,
    run_mode: str,
    is_confirmatory: bool,
    current_spec_hash: str,
) -> pd.DataFrame:
    out = df.copy()
    if not out.empty:
        out["confirmatory_locked"] = bool(is_confirmatory)
        out["frozen_spec_hash"] = current_spec_hash if is_confirmatory else np.nan
        out["run_mode"] = run_mode

    mandatory_columns = [
        "run_id",
        "candidate_symbol",
        "run_symbols",
        "event",
        "candidate_id",
        "status",
        "candidate_type",
        "overlay_base_candidate_id",
        "edge_score",
        "expected_return_proxy",
        "expectancy_per_trade",
        "after_cost_expectancy_per_trade",
        "stressed_after_cost_expectancy_per_trade",
        "selection_score_executed",
        "bridge_eval_status",
        "bridge_train_after_cost_bps",
        "bridge_validation_after_cost_bps",
        "bridge_validation_stressed_after_cost_bps",
        "bridge_validation_trades",
        "bridge_effective_cost_bps_per_trade",
        "bridge_gross_edge_bps_per_trade",
        "gate_bridge_has_trades_validation",
        "gate_bridge_after_cost_positive_validation",
        "gate_bridge_after_cost_stressed_positive_validation",
        "gate_bridge_edge_cost_ratio",
        "gate_bridge_turnover_controls",
        "gate_bridge_tradable",
        "gate_all_research",
        "cost_ratio",
        "turnover_proxy_mean",
        "avg_dynamic_cost_bps",
        "cost_config_digest",
        "after_cost_includes_funding_carry",
        "funding_carry_eval_coverage",
        "mean_funding_carry_bps",
        "execution_model_json",
        "variance",
        "stability_proxy",
        "robustness_score",
        "event_frequency",
        "capacity_proxy",
        "profit_density_score",
        "n_events",
        "source_path",
        "is_discovery",
        "phase2_quality_score",
        "phase2_quality_components",
        "compile_eligible_phase2_fallback",
        "promotion_track",
        "discovery_start",
        "discovery_end",
        "p_value",
        "q_value",
        "hypothesis_id",
        "train_n_obs",
        "validation_n_obs",
        "test_n_obs",
        "validation_samples",
        "test_samples",
        "sample_size",
        "confirmatory_locked",
        "frozen_spec_hash",
        "run_mode",
        "adjacent_window_survived",
        "adjacent_window_target_run_id",
        "adjacent_window_failure_reasons",
        "adjacent_window_target_after_cost_expectancy_per_trade",
        "effect_raw",
        "effect_shrunk_state",
        "shrinkage_factor",
        "shrinkage_loso_stable",
        "shrinkage_scope",
        "shrinkage_delta",
        "shrinkage_posterior_residual_z",
        "shrinkage_borrowing_dominant",
        "shrinkage_pooling_group_size",
        "p_value_shrunk",
    ]

    all_cols = list(mandatory_columns)
    for c in out.columns:
        if c not in all_cols:
            all_cols.append(c)

    for c in mandatory_columns:
        if c not in out.columns:
            out[c] = np.nan

    if "gate_bridge_tradable" in out.columns:
        out["gate_bridge_tradable"] = out["gate_bridge_tradable"].apply(
            lambda x: (
                "pass"
                if str(x).lower().strip() in ("1", "true", "t", "yes", "y", "on", "pass")
                else "fail"
            )
        )
    if "direction" in out.columns:
        out["direction"] = out["direction"].apply(_normalize_direction_value)

    out = out[all_cols].copy()
    if not out.empty:
        out["selection_score_executed"] = pd.to_numeric(
            out.get("selection_score_executed"), errors="coerce"
        ).fillna(0.0)
        out = out.sort_values(
            ["selection_score_executed", "profit_density_score", "edge_score", "stability_proxy"],
            ascending=[False, False, False, False],
        ).reset_index(drop=True)
    return out


def _phase2_row_to_candidate(
    run_id: str,
    event: str,
    row: dict[str, object],
    idx: int,
    source_path: Path,
    default_status: str,
    run_symbols: Sequence[str],
) -> dict[str, object]:
    # Lossless handoff: start with every field from the input row
    candidate = dict(row)

    # Ensure canonical identifiers and standard types
    candidate["run_id"] = str(run_id)
    candidate["event"] = str(event)
    candidate["candidate_id"] = str(row.get("candidate_id", f"{event}_{idx}"))
    candidate["status"] = str(row.get("status", default_status))
    candidate["source_path"] = str(source_path)
    candidate["direction"] = _normalize_direction_value(row.get("direction"))

    # Symbols
    candidate["run_symbols"] = list(run_symbols)
    candidate["candidate_symbol"] = _infer_symbol_tag(row=row, run_symbols=run_symbols)

    # Handle common statistical aliases if missing
    if "n_events" not in candidate:
        candidate["n_events"] = _quiet_int(row.get("sample_size", row.get("count", 0)), 0)
    if "sample_size" not in candidate:
        candidate["sample_size"] = candidate["n_events"]

    # Consistency for score fields often used in sorting or gating
    risk_reduction = max(0.0, -_quiet_float(row.get("delta_adverse_mean"), 0.0))
    opp_delta = _quiet_float(row.get("delta_opportunity_mean"), 0.0)

    if "edge_score" not in candidate:
        candidate["edge_score"] = _quiet_float(
            row.get("edge_score"), risk_reduction + max(0.0, opp_delta)
        )

    expectancy_source = row.get("after_cost_expectancy_per_trade")
    if _is_missing_value(expectancy_source):
        expectancy_source = row.get("expectancy_after_multiplicity")
    if _is_missing_value(expectancy_source):
        expectancy_source = row.get("expectancy_per_trade")
    if _is_missing_value(expectancy_source):
        expectancy_source = row.get("expectancy")

    if "expectancy_per_trade" not in candidate:
        candidate["expectancy_per_trade"] = _quiet_float(
            expectancy_source, _quiet_float(row.get("expected_return_proxy"), opp_delta)
        )

    if "after_cost_expectancy_per_trade" not in candidate:
        candidate["after_cost_expectancy_per_trade"] = _quiet_float(
            row.get("after_cost_expectancy_per_trade", row.get("expectancy")),
            candidate["expectancy_per_trade"],
        )
    if "stressed_after_cost_expectancy_per_trade" not in candidate:
        candidate["stressed_after_cost_expectancy_per_trade"] = _quiet_float(
            row.get("stressed_after_cost_expectancy_per_trade"),
            candidate["after_cost_expectancy_per_trade"],
        )

    after_cost_bps = _quiet_float(
        row.get("bridge_validation_after_cost_bps"),
        _quiet_float(candidate.get("after_cost_expectancy_per_trade"), np.nan) * 10000.0,
    )
    stressed_after_cost_bps = _quiet_float(
        row.get("bridge_validation_stressed_after_cost_bps"),
        _quiet_float(candidate.get("stressed_after_cost_expectancy_per_trade"), np.nan)
        * 10000.0,
    )
    if "bridge_validation_after_cost_bps" not in candidate and np.isfinite(after_cost_bps):
        candidate["bridge_validation_after_cost_bps"] = after_cost_bps
    if (
        "bridge_validation_stressed_after_cost_bps" not in candidate
        and np.isfinite(stressed_after_cost_bps)
    ):
        candidate["bridge_validation_stressed_after_cost_bps"] = stressed_after_cost_bps
    if "bridge_train_after_cost_bps" not in candidate and np.isfinite(after_cost_bps):
        candidate["bridge_train_after_cost_bps"] = after_cost_bps
    if "bridge_validation_trades" not in candidate:
        candidate["bridge_validation_trades"] = _quiet_int(
            row.get("validation_samples", row.get("validation_n_obs", row.get("n_events", 0))),
            0,
        )
    if "bridge_effective_cost_bps_per_trade" not in candidate:
        candidate["bridge_effective_cost_bps_per_trade"] = _quiet_float(
            row.get("expected_cost_bps_per_trade"), 0.0
        )
    if "bridge_gross_edge_bps_per_trade" not in candidate:
        candidate["bridge_gross_edge_bps_per_trade"] = _quiet_float(
            row.get("mean_return_gross_bps", row.get("mean_return_bps")), np.nan
        )

    if "std_return" not in candidate:
        mean_bps = _quiet_float(row.get("mean_return_bps"), np.nan)
        t_stat = abs(_quiet_float(row.get("t_stat"), np.nan))
        n_obs = max(1, _quiet_int(row.get("n", row.get("sample_size", 0)), 0))
        if np.isfinite(mean_bps) and np.isfinite(t_stat) and t_stat > 0.0:
            candidate["std_return"] = abs(mean_bps / 10000.0) * np.sqrt(float(n_obs)) / t_stat

    # Robustness / Stability
    gate_cols = [
        "gate_a_ci_separated",
        "gate_b_time_stable",
        "gate_c_regime_stable",
        "gate_d_friction_floor",
        "gate_f_exposure_guard",
        "gate_e_simplicity",
    ]
    gates_present = [g for g in gate_cols if g in row]
    if gates_present:
        stability_proxy = float(
            sum(1 for g in gates_present if as_bool(row.get(g))) / len(gates_present)
        )
    else:
        stability_proxy = _quiet_float(row.get("stability_proxy"), 0.0)

    candidate["stability_proxy"] = stability_proxy
    if "robustness_score" not in candidate:
        candidate["robustness_score"] = _quiet_float(row.get("robustness_score"), stability_proxy)

    return candidate


def _build_symbol_eval_lookup(event_dir: Path) -> dict[str, dict[str, object]]:
    path = event_dir / "phase2_symbol_evaluation.csv"
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}
    if df.empty:
        return {}

    grouped: dict[str, list[dict[str, object]]] = {}
    for _, row in df.iterrows():
        cid = str(row.get("candidate_id", "")).strip()
        if not cid:
            continue
        symbol = str(row.get("symbol", "ALL")).strip().upper() or "ALL"
        deployable = as_bool(row.get("deployable", False))
        ev = _quiet_float(row.get("ev"), 0.0)
        variance = _quiet_float(row.get("variance"), 0.0)
        sharpe_like = _quiet_float(row.get("sharpe_like"), 0.0)
        stability_score = _quiet_float(row.get("stability_score"), 0.0)
        capacity_proxy = _quiet_float(row.get("capacity_proxy"), 0.0)
        row_score = ev * max(0.0, sharpe_like) * max(0.0, stability_score)
        grouped.setdefault(cid, []).append(
            {
                "symbol": symbol,
                "deployable": deployable,
                "ev": ev,
                "variance": variance,
                "stability_score": stability_score,
                "capacity_proxy": capacity_proxy,
                "row_score": row_score,
            }
        )

    lookup: dict[str, dict[str, object]] = {}
    for cid, items in grouped.items():
        if not items:
            continue
        best = max(items, key=lambda item: float(item.get("row_score", -1e18)))
        symbol_scores = {
            str(item.get("symbol", "ALL")).strip().upper() or "ALL": _quiet_float(
                item.get("row_score"), 0.0
            )
            for item in items
        }
        positive_scores = [score for score in symbol_scores.values() if score > 0.0]
        similar_score_band = True
        if len(positive_scores) > 1:
            max_score = max(positive_scores)
            min_score = min(positive_scores)
            similar_score_band = bool(min_score >= (0.75 * max_score))
        deployable_symbols = [item for item in items if bool(item.get("deployable", False))]
        rollout_eligible = bool(len(deployable_symbols) > 1 and similar_score_band)
        lookup[cid] = {
            "candidate_symbol": str(best.get("symbol", "ALL")).strip().upper() or "ALL",
            "symbol": str(best.get("symbol", "ALL")).strip().upper() or "ALL",
            "symbol_scores": json.dumps(symbol_scores),
            "rollout_eligible": rollout_eligible,
            "expectancy_per_trade": _quiet_float(best.get("ev"), 0.0),
            "variance": _quiet_float(best.get("variance"), 0.0),
            "stability_proxy": _quiet_float(best.get("stability_score"), 0.0),
            "robustness_score": _quiet_float(best.get("stability_score"), 0.0),
            "capacity_proxy": _quiet_float(best.get("capacity_proxy"), 0.0),
            "profit_density_score": _quiet_float(best.get("row_score"), 0.0),
            "status": "PROMOTED" if bool(best.get("deployable", False)) else "DRAFT",
        }
    return lookup


def _build_bridge_eval_lookup(
    *, run_id: str, event_type: str, timeframe: str
) -> dict[str, dict[str, object]]:
    bridge_root = bridge_event_out_dir(
        data_root=get_data_root(),
        run_id=run_id,
        event_type=event_type,
        timeframe=timeframe,
    )
    if not bridge_root.exists():
        return {}

    lookup: dict[str, dict[str, object]] = {}
    for symbol_dir in sorted(path for path in bridge_root.iterdir() if path.is_dir()):
        bridge_path = symbol_dir / "bridge_evaluation.parquet"
        if not bridge_path.exists():
            continue
        try:
            frame = pd.read_parquet(bridge_path)
        except Exception:
            continue
        if frame.empty or "candidate_id" not in frame.columns:
            continue
        for _, row in frame.iterrows():
            candidate_id = str(row.get("candidate_id", "")).strip()
            if candidate_id:
                lookup[candidate_id] = row.to_dict()
    return lookup


def _run_research_chain(
    run_id: str,
    symbols: str,
) -> None:
    phase2_script_path = PROJECT_ROOT / "research" / "cli" / "candidate_discovery_cli.py"
    registry_script_path = PROJECT_ROOT / "research" / "build_event_registry.py"
    bridge_script_path = PROJECT_ROOT / "research" / "bridge_evaluate_phase2.py"
    for event_type, script, extra_args in PHASE2_EVENT_CHAIN:
        script_path = PROJECT_ROOT / "research" / script
        if not script_path.exists():
            logging.warning("Missing phase1 script (skipping): %s", script_path)
            continue

        cmd = [
            sys.executable,
            str(script_path),
            "--run_id",
            run_id,
            "--symbols",
            symbols,
            *extra_args,
        ]
        result = subprocess.run(cmd)
        if result.returncode != 0:
            logging.warning("Phase1 stage failed (non-blocking): %s", script)
            continue

        if not phase2_script_path.exists():
            logging.warning("Missing phase2 script (skipping): %s", phase2_script_path)
            continue
        if registry_script_path.exists():
            registry_cmd = [
                sys.executable,
                str(registry_script_path),
                "--run_id",
                run_id,
                "--symbols",
                symbols,
                "--event_type",
                event_type,
                "--timeframe",
                "5m",
            ]
            registry_result = subprocess.run(registry_cmd)
            if registry_result.returncode != 0:
                logging.warning("Event registry stage failed (non-blocking): %s", event_type)
                continue
        else:
            logging.warning("Missing event-registry script (skipping): %s", registry_script_path)
            continue

        phase2_cmd = [
            sys.executable,
            str(phase2_script_path),
            "--run_id",
            run_id,
            "--event_type",
            event_type,
            "--symbols",
            symbols,
            "--mode",
            "research",
        ]
        phase2_result = subprocess.run(phase2_cmd)
        if phase2_result.returncode != 0:
            logging.warning("Phase2 stage failed (non-blocking): %s", event_type)
            continue
        if bridge_script_path.exists():
            bridge_cmd = [
                sys.executable,
                str(bridge_script_path),
                "--run_id",
                run_id,
                "--event_type",
                event_type,
                "--symbols",
                symbols,
            ]
            bridge_result = subprocess.run(bridge_cmd)
            if bridge_result.returncode != 0:
                logging.warning("Bridge stage failed (non-blocking): %s", event_type)


def _collect_phase2_candidates(run_id: str, run_symbols: Sequence[str]) -> list[dict[str, object]]:
    DATA_ROOT = get_data_root()
    rows: list[dict[str, object]] = []
    phase2_root = DATA_ROOT / "reports" / "phase2" / run_id
    if not phase2_root.exists():
        return rows

    root_candidate_csv = phase2_root / "phase2_candidates.csv"
    root_candidate_parquet = phase2_root / "phase2_candidates.parquet"
    if root_candidate_csv.exists() or root_candidate_parquet.exists():
        try:
            root_df = (
                pd.read_csv(root_candidate_csv)
                if root_candidate_csv.exists()
                else pd.read_parquet(root_candidate_parquet)
            )
        except Exception:
            root_df = pd.DataFrame()
        if not root_df.empty:
            if "gate_all_research" in root_df.columns:
                root_df = root_df[root_df["gate_all_research"].map(as_bool)].copy()
            elif "gate_all" in root_df.columns:
                root_df = root_df[root_df["gate_all"].map(as_bool)].copy()
            if "gate_bridge_tradable" in root_df.columns:
                root_df = root_df[root_df["gate_bridge_tradable"].map(as_bool)].copy()
            if not root_df.empty:
                source_path = root_candidate_csv if root_candidate_csv.exists() else root_candidate_parquet
                for idx, row in root_df.iterrows():
                    row_payload = row.to_dict()
                    row_payload["status"] = (
                        str(row_payload.get("status", "PROMOTED_RESEARCH")).strip()
                        or "PROMOTED_RESEARCH"
                    )
                    event_name = (
                        str(
                            row_payload.get(
                                "event_type",
                                row_payload.get("event", row_payload.get("trigger_key", "phase2_root")),
                            )
                        ).strip()
                        or "phase2_root"
                    )
                    rows.append(
                        _phase2_row_to_candidate(
                            run_id=run_id,
                            event=event_name,
                            row=row_payload,
                            idx=idx,
                            source_path=source_path,
                            default_status="PROMOTED_RESEARCH",
                            run_symbols=run_symbols,
                        )
                    )

    for event_dir in sorted([p for p in phase2_root.iterdir() if p.is_dir()]):
        candidate_root = event_dir
        if (
            not (candidate_root / "phase2_candidates.csv").exists()
            and not (candidate_root / "phase2_candidates.parquet").exists()
        ):
            timeframe_roots = [
                child
                for child in sorted(event_dir.iterdir())
                if child.is_dir()
                and (
                    (child / "phase2_candidates.csv").exists()
                    or (child / "phase2_candidates.parquet").exists()
                )
            ]
        else:
            timeframe_roots = [candidate_root]
        for candidate_root in timeframe_roots:
            promoted_json = candidate_root / "promoted_candidates.json"
            candidate_csv = candidate_root / "phase2_candidates.csv"
            candidate_parquet = candidate_root / "phase2_candidates.parquet"
            symbol_eval_lookup = _build_symbol_eval_lookup(candidate_root)
            timeframe = normalize_timeframe(
                candidate_root.name if candidate_root != event_dir else "5m"
            )
            bridge_eval_lookup = _build_bridge_eval_lookup(
                run_id=run_id,
                event_type=event_dir.name,
                timeframe=timeframe,
            )
            event_rows: list[dict[str, object]] = []
            phase2_lookup: dict[str, dict[str, object]] = {}
            if candidate_csv.exists() or candidate_parquet.exists():
                try:
                    phase2_df = (
                        pd.read_csv(candidate_csv)
                        if candidate_csv.exists()
                        else pd.read_parquet(candidate_parquet)
                    )
                except Exception:
                    phase2_df = pd.DataFrame()
                if not phase2_df.empty:
                    for idx, payload in enumerate(phase2_df.to_dict(orient="records")):
                        cid = str(payload.get("candidate_id", "")).strip()
                        if not cid:
                            cond = str(payload.get("condition", "")).strip()
                            act = str(payload.get("action", "")).strip()
                            if cond and act:
                                cid = f"{cond}__{act}"
                                payload["candidate_id"] = cid
                        if cid:
                            phase2_lookup[cid] = payload

            if promoted_json.exists():
                payload = json.loads(promoted_json.read_text(encoding="utf-8"))
                promoted = payload.get("candidates", []) if isinstance(payload, dict) else []
                for idx, candidate in enumerate(promoted):
                    if not isinstance(candidate, dict):
                        continue
                    candidate_row = dict(candidate)
                    cid = str(candidate_row.get("candidate_id", "")).strip()
                    if not cid:
                        cond = str(candidate_row.get("condition", "")).strip()
                        act = str(candidate_row.get("action", "")).strip()
                        if cond and act:
                            cid = f"{cond}__{act}"
                            candidate_row["candidate_id"] = cid
                    if cid and cid in phase2_lookup:
                        merged = dict(phase2_lookup[cid])
                        merged.update(candidate_row)
                        candidate_row = merged
                    if ("gate_bridge_tradable" in candidate_row) and (
                        not as_bool(candidate_row.get("gate_bridge_tradable", False))
                    ):
                        continue
                    if cid and cid in symbol_eval_lookup:
                        candidate_row.update(symbol_eval_lookup[cid])
                    if cid and cid in bridge_eval_lookup:
                        candidate_row.update(bridge_eval_lookup[cid])
                    event_name = (
                        str(
                            candidate_row.get(
                                "event_type", candidate_row.get("event", event_dir.name)
                            )
                        ).strip()
                        or event_dir.name
                    )
                    event_rows.append(
                        _phase2_row_to_candidate(
                            run_id=run_id,
                            event=event_name,
                            row=candidate_row,
                            idx=idx,
                            source_path=promoted_json,
                            default_status="PROMOTED",
                            run_symbols=run_symbols,
                        )
                    )

            if not event_rows and (candidate_csv.exists() or candidate_parquet.exists()):
                df = (
                    pd.read_csv(candidate_csv)
                    if candidate_csv.exists()
                    else pd.read_parquet(candidate_parquet)
                )
                if not df.empty:
                    if "gate_all_research" in df.columns:
                        df = df[df["gate_all_research"].map(as_bool)].copy()
                    elif "gate_all" in df.columns:
                        df = df[df["gate_all"].map(as_bool)].copy()
                    if "gate_bridge_tradable" in df.columns:
                        df = df[df["gate_bridge_tradable"].map(as_bool)].copy()
                    if not df.empty:
                        for idx, row in df.iterrows():
                            row_payload = row.to_dict()
                            row_payload["status"] = (
                                str(row_payload.get("status", "PROMOTED_RESEARCH")).strip()
                                or "PROMOTED_RESEARCH"
                            )
                            cid = str(row_payload.get("candidate_id", "")).strip()
                            if not cid:
                                cond = str(row_payload.get("condition", "")).strip()
                                act = str(row_payload.get("action", "")).strip()
                                if cond and act:
                                    cid = f"{cond}__{act}"
                                    row_payload["candidate_id"] = cid
                            if cid and cid in symbol_eval_lookup:
                                row_payload.update(symbol_eval_lookup[cid])
                            if cid and cid in bridge_eval_lookup:
                                row_payload.update(bridge_eval_lookup[cid])
                            event_name = (
                                str(
                                    row_payload.get(
                                        "event_type", row_payload.get("event", event_dir.name)
                                    )
                                ).strip()
                                or event_dir.name
                            )
                            event_rows.append(
                                _phase2_row_to_candidate(
                                    run_id=run_id,
                                    event=event_name,
                                    row=row_payload,
                                    idx=idx,
                                    source_path=candidate_csv
                                    if candidate_csv.exists()
                                    else candidate_parquet,
                                    default_status="PROMOTED_RESEARCH",
                                    run_symbols=run_symbols,
                                )
                            )

            rows.extend(event_rows)
    return rows


def main() -> int:
    DATA_ROOT = get_data_root()
    parser = argparse.ArgumentParser(description="Expand and normalize edge candidate universe")
    parser.add_argument("--run_id", required=True)
    parser.add_argument(
        "--symbols", required=True, help="Comma-separated discovery symbols for this run"
    )
    parser.add_argument("--execute", type=int, default=0)
    parser.add_argument("--hypothesis_datasets", default="auto", help=argparse.SUPPRESS)
    parser.add_argument("--hypothesis_max_fused", type=int, default=24, help=argparse.SUPPRESS)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    log_handlers = build_stage_log_handlers(args.log_path)
    logging.basicConfig(
        level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s"
    )

    run_symbols = _parse_symbols_csv(args.symbols)
    if not run_symbols:
        print("--symbols must include at least one symbol", file=sys.stderr)
        return 1

    params = {
        "run_id": args.run_id,
        "symbols": run_symbols,
        "execute": int(args.execute),
        "hypothesis_datasets": str(args.hypothesis_datasets),
        "hypothesis_max_fused": int(args.hypothesis_max_fused),
    }
    inputs: list[dict[str, object]] = []
    outputs: list[dict[str, object]] = []
    manifest = start_manifest("export_edge_candidates", args.run_id, params, inputs, outputs)

    try:
        if int(args.execute):
            _run_research_chain(
                run_id=args.run_id,
                symbols=args.symbols,
            )

        rows = _collect_phase2_candidates(args.run_id, run_symbols=run_symbols)

        # S1/S2: Apply Hierarchical Shrinkage across the collected candidate universe
        from project.research.helpers.shrinkage import _apply_hierarchical_shrinkage
        from project.specs.manifest import load_run_manifest

        run_manifest = load_run_manifest(args.run_id)
        run_mode = str(run_manifest.get("run_mode", "exploratory")).strip().lower()
        is_confirmatory = _is_confirmatory_run_mode(run_mode)
        current_spec_hash = ontology_spec_hash(PROJECT_ROOT.parent)

        candidates_df = pd.DataFrame(rows)
        if not candidates_df.empty:
            # We need standard columns for shrinkage:
            # - canonical_family, canonical_event_type, template_verb, horizon, state_id, symbol
            # These are already in the candidate rows.
            shrunk_df = _apply_hierarchical_shrinkage(
                candidates_df,
                train_only_lambda=True,  # Enforce S1 requirement
                split_col="split_label",
                run_mode=run_mode,
            )
            # Merge back the shrunk columns
            # (Note: _apply_hierarchical_shrinkage returns a full DF, so we just use it)
            df = shrunk_df
        else:
            df = candidates_df

        if not df.empty:
            df["confirmatory_locked"] = bool(is_confirmatory)
            df["frozen_spec_hash"] = current_spec_hash if is_confirmatory else np.nan
            df["run_mode"] = run_mode
        df, adjacent_report_path = _apply_adjacent_survivorship_annotations(df, run_id=args.run_id)

        out_dir = DATA_ROOT / "reports" / "edge_candidates" / args.run_id
        ensure_dir(out_dir)
        out_csv = out_dir / "edge_candidates_normalized.parquet"
        out_json = out_dir / "edge_candidates_normalized.json"
        df = _normalize_edge_candidates_df(
            df,
            run_mode=run_mode,
            is_confirmatory=is_confirmatory,
            current_spec_hash=current_spec_hash,
        )
        write_parquet(df, out_csv)
        out_json.write_text(df.to_json(orient="records", indent=2), encoding="utf-8")

        outputs.append(
            {"path": str(out_csv), "rows": len(df), "start_ts": None, "end_ts": None}
        )
        outputs.append(
            {"path": str(out_json), "rows": len(df), "start_ts": None, "end_ts": None}
        )
        finalize_manifest(
            manifest,
            "success",
            stats={
                "candidate_count": len(df),
                "adjacent_survivorship_report_path": adjacent_report_path,
            },
        )
        return 0
    except Exception as exc:  # pragma: no cover
        logging.exception("Edge candidate export failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
