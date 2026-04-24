from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Mapping, Tuple

import numpy as np
import pandas as pd

from project.core.coercion import safe_float, safe_int
from project.core.config import get_data_root
from project.core.timeframes import normalize_timeframe
from project.io.utils import ensure_dir, write_parquet
from project.research.bridge_evaluation import (
    evaluate_bridge_performance,
)
from project.research.helpers.viability import (
    evaluate_low_capital_viability,
    evaluate_retail_constraints,
)
from project.research.services.pathing import bridge_event_out_dir, phase2_event_out_dir
from project.specs.manifest import finalize_manifest, start_manifest
from project.specs.objective import resolve_objective_profile_contract


def _filter_candidates_for_symbol(candidates: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if candidates.empty:
        return candidates.copy()
    symbol_name = str(symbol).strip().upper()
    if not symbol_name:
        return candidates.copy()
    if "symbol" not in candidates.columns:
        return candidates.copy()
    symbol_series = candidates["symbol"].astype(str).str.strip().str.upper()
    scoped = candidates.loc[symbol_series.isin({symbol_name, "ALL", ""})].copy()
    return scoped


def _bool_series(df: pd.DataFrame, column: str, default: bool = False) -> pd.Series:
    if column not in df.columns:
        return pd.Series(bool(default), index=df.index, dtype=bool)
    raw = df[column]
    truthy = {"1", "true", "t", "yes", "y", "on"}
    return raw.map(
        lambda x: (
            bool(x)
            if isinstance(x, bool)
            else (str(x).strip().lower() in truthy if x is not None else bool(default))
        )
    ).astype(bool)


def _load_candidates(path: Path | str) -> pd.DataFrame:
    src = Path(path)
    frame = pd.DataFrame()
    if src.suffix.lower() == ".parquet" and src.exists():
        try:
            frame = pd.read_parquet(src)
        except Exception:
            frame = pd.DataFrame()
    else:
        parquet_path = src.with_suffix(".parquet")
        if parquet_path.exists():
            try:
                frame = pd.read_parquet(parquet_path)
            except Exception:
                frame = pd.DataFrame()
        if frame.empty and src.exists():
            try:
                frame = pd.read_csv(src)
            except Exception:
                frame = pd.DataFrame()
    if frame.empty:
        return frame

    out = frame.copy()
    gate_bridge_columns = [col for col in out.columns if col.startswith("gate_bridge_")]
    for col in gate_bridge_columns:
        out[col] = False
    for col in (
        "bridge_eval_status",
        "bridge_fail_reasons",
        "bridge_fail_gate_primary",
        "bridge_fail_reason_primary",
    ):
        if col in out.columns:
            out[col] = ""

    stale_bridge_columns = [
        col for col in out.columns if ("bridge" in col.lower()) and (col not in gate_bridge_columns)
    ]
    if stale_bridge_columns:
        out = out.drop(columns=stale_bridge_columns, errors="ignore")
    return out


def _select_bridge_candidates(
    full_candidates: pd.DataFrame,
    mode: str,
    candidate_mask: str | None = None,
) -> pd.DataFrame:
    if full_candidates.empty:
        return full_candidates.copy()

    mask = str(candidate_mask or "auto").strip().lower()
    mode_name = str(mode).strip().lower()
    out = full_candidates.copy()
    gate_research = _bool_series(out, "gate_phase2_research", default=False)
    gate_final = _bool_series(out, "gate_phase2_final", default=False)
    is_discovery = _bool_series(out, "is_discovery", default=False)

    if mask == "all":
        selected = pd.Series(True, index=out.index, dtype=bool)
    elif mask == "final":
        selected = gate_final & is_discovery
    elif mask == "research":
        selected = gate_research
    elif mode_name == "production":
        selected = gate_final & is_discovery
    else:
        selected = gate_research
    return out.loc[selected].copy()


def _build_policy_variant_specs(
    *,
    low_capital_contract: Mapping[str, Any] | None = None,
    cooldown_bars: List[int] | None = None,
    include_one_trade_per_episode: bool = False,
) -> List[Dict[str, Any]]:
    contract = dict(low_capital_contract or {})
    delays = {
        max(0, safe_int(contract.get("entry_delay_bars_default"), 1)),
        max(0, safe_int(contract.get("entry_delay_bars_stress"), 1)),
    }
    cooldown_values = sorted({max(0, safe_int(value, 0)) for value in (cooldown_bars or [])})

    specs: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for delay in sorted(delays):
        base_flags = [False, True] if include_one_trade_per_episode else [False]
        cooldown_options = [0] + cooldown_values
        for one_trade in base_flags:
            for cooldown in cooldown_options:
                tokens = [f"delay_{delay}"]
                if one_trade:
                    tokens.append("one_trade_per_episode")
                if cooldown > 0:
                    tokens.append(f"cooldown_{cooldown}")
                variant_id = "__".join(tokens)
                if variant_id in seen:
                    continue
                seen.add(variant_id)
                specs.append(
                    {
                        "variant_id": variant_id,
                        "variant_delay_bars": int(delay),
                        "variant_one_trade_per_episode": bool(one_trade),
                        "variant_cooldown_bars": int(cooldown),
                    }
                )
    return specs


def _build_bridge_symbol_calibrations(
    *,
    metrics_df: pd.DataFrame,
    base_fee_bps: float,
    min_tob_coverage: float,
) -> Dict[str, Any]:
    if metrics_df.empty:
        return {}
    out: Dict[str, Any] = {}
    for symbol, group in metrics_df.groupby("symbol"):
        symbol_name = str(symbol).strip().upper()
        if not symbol_name:
            continue
        eff_cost = pd.to_numeric(
            group.get("bridge_effective_cost_bps_per_trade", pd.Series(dtype=float)),
            errors="coerce",
        ).dropna()
        if eff_cost.empty:
            continue
        calibrated_cost = float(eff_cost.mean())
        fee_bps = max(0.0, float(base_fee_bps))
        slip_bps = max(0.0, calibrated_cost - fee_bps)
        out[symbol_name] = {
            "symbol": symbol_name,
            "base_fee_bps": fee_bps,
            "base_slippage_bps": slip_bps,
            "calibrated_cost_bps": calibrated_cost,
            "min_tob_coverage": float(min_tob_coverage),
        }
    return out


def _bridge_summary_count_fields(
    *,
    n_candidates_in: int,
    n_candidates_tradable: int,
    n_candidates_tradable_without_microstructure: int,
    top_5_bridge_fail_reasons: Mapping[str, int] | None,
) -> Dict[str, Any]:
    candidates_in = int(n_candidates_in)
    tradable = int(n_candidates_tradable)
    tradable_wo_micro = int(n_candidates_tradable_without_microstructure)
    return {
        "n_candidates_in": candidates_in,
        "n_candidates_tradable": tradable,
        "n_candidates_tradable_without_microstructure": tradable_wo_micro,
        "microstructure_delta_tradable": max(0, tradable_wo_micro - tradable),
        "top_5_bridge_fail_reasons": dict(top_5_bridge_fail_reasons or {}),
        "candidate_count": candidates_in,
        "tradable_count": tradable,
    }


def _build_bridge_summary_payload(df_out: pd.DataFrame) -> Dict[str, Any]:
    if df_out.empty:
        summary = _bridge_summary_count_fields(
            n_candidates_in=0,
            n_candidates_tradable=0,
            n_candidates_tradable_without_microstructure=0,
            top_5_bridge_fail_reasons={},
        )
        summary.update(
            {
                "after_cost_non_positive_count": 0,
                "median_bridge_validation_after_cost_bps": 0.0,
                "median_bridge_effective_cost_bps_per_trade": 0.0,
                "uniform_negative_expectancy_count": 0,
                "primary_fail_gate_counts": {},
            }
        )
        return summary

    fail_reason_counts: Dict[str, int] = {}
    for reasons in df_out.get("bridge_fail_reasons", pd.Series("", index=df_out.index)).fillna(""):
        for reason in str(reasons).split(","):
            reason_name = reason.strip()
            if reason_name:
                fail_reason_counts[reason_name] = fail_reason_counts.get(reason_name, 0) + 1

    primary_fail_counts = (
        df_out.get("bridge_fail_gate_primary", pd.Series("", index=df_out.index))
        .fillna("")
        .astype(str)
        .str.strip()
    )
    primary_fail_counts = {
        gate: int(count)
        for gate, count in primary_fail_counts[primary_fail_counts != ""]
        .value_counts()
        .to_dict()
        .items()
    }

    tradable_wo_micro = int(
        _bool_series(df_out, "gate_bridge_tradable_without_microstructure", default=False).sum()
    )
    tradable = int(_bool_series(df_out, "gate_bridge_tradable", default=False).sum())
    summary = _bridge_summary_count_fields(
        n_candidates_in=int(len(df_out)),
        n_candidates_tradable=tradable,
        n_candidates_tradable_without_microstructure=tradable_wo_micro,
        top_5_bridge_fail_reasons=dict(
            sorted(fail_reason_counts.items(), key=lambda item: (-item[1], item[0]))[:5]
        ),
    )

    after_cost = pd.to_numeric(
        df_out.get("bridge_validation_after_cost_bps", pd.Series(dtype=float)),
        errors="coerce",
    )
    effective_cost = pd.to_numeric(
        df_out.get("bridge_effective_cost_bps_per_trade", pd.Series(dtype=float)),
        errors="coerce",
    )
    summary.update(
        {
            "after_cost_non_positive_count": int((after_cost.fillna(-np.inf) <= 0.0).sum()),
            "median_bridge_validation_after_cost_bps": float(after_cost.dropna().median())
            if after_cost.notna().any()
            else 0.0,
            "median_bridge_effective_cost_bps_per_trade": float(effective_cost.dropna().median())
            if effective_cost.notna().any()
            else 0.0,
            "uniform_negative_expectancy_count": int(
                (
                    after_cost.notna()
                    & np.isclose(after_cost, after_cost.iloc[0], atol=1e-12)
                    & (after_cost < 0.0)
                ).sum()
            )
            if not after_cost.empty and after_cost.notna().all()
            else 0,
            "primary_fail_gate_counts": primary_fail_counts,
        }
    )
    return summary


def _evaluate_policy_variants_for_candidate(row: pd.Series, **kwargs) -> List[Dict[str, Any]]:
    bridge_result = dict(kwargs.get("bridge_result") or {})
    policy_variants = list(kwargs.get("policy_variants") or [])
    min_validation_trades = safe_int(kwargs.get("min_validation_trades"), 0)
    stressed_cost_multiplier = safe_float(kwargs.get("stressed_cost_multiplier"), 1.0)
    edge_cost_k = safe_float(kwargs.get("edge_cost_k"), 0.0)
    require_retail_viability = bool(kwargs.get("require_retail_viability", False))
    enforce_low_capital_viability = bool(kwargs.get("enforce_low_capital_viability", False))
    low_capital_contract = dict(kwargs.get("low_capital_contract") or {})
    min_net_expectancy_bps = safe_float(kwargs.get("min_net_expectancy_bps"), 0.0)
    max_fee_plus_slippage_bps = kwargs.get("max_fee_plus_slippage_bps")
    max_daily_turnover_multiple = kwargs.get("max_daily_turnover_multiple")

    baseline_delay = safe_int(
        bridge_result.get("bridge_effective_lag_bars_used"),
        safe_int(row.get("effective_lag_bars"), 1),
    )
    base_trades = max(0, safe_int(bridge_result.get("bridge_validation_trades"), 0))
    base_after_cost = safe_float(bridge_result.get("bridge_validation_after_cost_bps"), np.nan)
    base_eff_cost = max(
        0.0,
        safe_float(bridge_result.get("bridge_effective_cost_bps_per_trade"), 0.0),
    )

    results: List[Dict[str, Any]] = []
    for spec in policy_variants:
        variant = dict(spec)
        delay = safe_int(variant.get("variant_delay_bars"), baseline_delay)
        one_trade = bool(variant.get("variant_one_trade_per_episode", False))
        cooldown = max(0, safe_int(variant.get("variant_cooldown_bars"), 0))

        delay_penalty = max(0, delay - baseline_delay) * base_eff_cost
        after_cost_bps = (
            base_after_cost - delay_penalty if np.isfinite(base_after_cost) else float(np.nan)
        )
        stressed_after_cost_bps = (
            after_cost_bps - ((stressed_cost_multiplier - 1.0) * base_eff_cost)
            if np.isfinite(after_cost_bps)
            else float(np.nan)
        )

        trade_factor = 1.0
        if one_trade:
            trade_factor *= 0.5
        if cooldown > 0:
            trade_factor *= 1.0 / (1.0 + (float(cooldown) / 10.0))
        validation_trades = int(np.floor(base_trades * trade_factor))

        retail_row = dict(row.to_dict())
        retail_row.update(
            {
                "bridge_validation_after_cost_bps": after_cost_bps,
                "bridge_effective_cost_bps_per_trade": base_eff_cost,
                "turnover_proxy_mean": safe_float(row.get("turnover_proxy_mean"), np.nan),
                "tob_coverage": safe_float(row.get("tob_coverage"), np.nan),
            }
        )
        retail_eval = evaluate_retail_constraints(
            retail_row,
            min_tob_coverage=0.0,
            min_net_expectancy_bps=min_net_expectancy_bps,
            max_fee_plus_slippage_bps=max_fee_plus_slippage_bps,
            max_daily_turnover_multiple=max_daily_turnover_multiple,
        )
        gate_retail = bool(retail_eval.get("gate_retail_viability", True))
        low_cap_eval = evaluate_low_capital_viability(
            retail_row,
            low_capital_contract=low_capital_contract,
            baseline_after_cost_bps=after_cost_bps,
            effective_cost_bps=base_eff_cost,
            turnover_proxy_mean=safe_float(row.get("turnover_proxy_mean"), np.nan),
        )
        gate_low_cap = bool(low_cap_eval.get("gate_low_capital_viability", True))

        gross_edge = (
            after_cost_bps + base_eff_cost if np.isfinite(after_cost_bps) else float(np.nan)
        )
        gate_has_trades = validation_trades >= min_validation_trades
        gate_positive = bool(np.isfinite(after_cost_bps) and after_cost_bps > 0.0)
        gate_stressed = bool(np.isfinite(stressed_after_cost_bps) and stressed_after_cost_bps > 0.0)
        gate_edge_ratio = bool(
            np.isfinite(gross_edge) and gross_edge >= (edge_cost_k * max(base_eff_cost, 1e-9))
        )
        gate_tradable = (
            gate_has_trades
            and gate_positive
            and gate_stressed
            and gate_edge_ratio
            and (gate_retail if require_retail_viability else True)
            and (gate_low_cap if enforce_low_capital_viability else True)
        )

        results.append(
            {
                "candidate_id": str(row.get("candidate_id", "")).strip(),
                "symbol": str(row.get("symbol", "")).strip().upper(),
                "variant_id": str(variant.get("variant_id", "")).strip(),
                "variant_delay_bars": int(delay),
                "variant_one_trade_per_episode": bool(one_trade),
                "variant_cooldown_bars": int(cooldown),
                "is_baseline_variant": bool(
                    delay == baseline_delay and (not one_trade) and cooldown == 0
                ),
                "bridge_validation_trades": int(validation_trades),
                "bridge_validation_after_cost_bps": (
                    None if not np.isfinite(after_cost_bps) else float(after_cost_bps)
                ),
                "bridge_validation_stressed_after_cost_bps": (
                    None
                    if not np.isfinite(stressed_after_cost_bps)
                    else float(stressed_after_cost_bps)
                ),
                "bridge_effective_cost_bps_per_trade": float(base_eff_cost),
                "gate_bridge_tradable": bool(gate_tradable),
                "gate_bridge_has_trades_validation": bool(gate_has_trades),
                "gate_bridge_after_cost_positive_validation": bool(gate_positive),
                "gate_bridge_after_cost_stressed_positive_validation": bool(gate_stressed),
                "gate_bridge_edge_cost_ratio": bool(gate_edge_ratio),
            }
        )
    return results


def _evaluate_bridge_row(
    row: pd.Series,
    event_type: str,
    base_lookup: Mapping[str, Mapping[str, Any]],
    edge_cost_k: float,
    min_validation_trades: int,
    stressed_cost_multiplier: float,
    min_net_expectancy_bps: float,
    max_fee_plus_slippage_bps: float | None,
    max_daily_turnover_multiple: float | None,
    require_retail_viability: bool,
    micro_max_spread_stress: float,
    micro_max_depth_depletion: float,
    micro_max_sweep_pressure: float,
    micro_max_abs_imbalance: float,
    micro_min_feature_coverage: float,
    low_capital_contract: Mapping[str, Any] | None = None,
    enforce_low_capital_viability: bool = False,
) -> Tuple[Dict[str, Any], Dict[str, Any] | None]:
    metrics_rows, overlay_rows = evaluate_bridge_performance(
        pd.DataFrame([row]),
        event_type=event_type,
        base_lookup={k: dict(v) for k, v in base_lookup.items()},
        edge_cost_k=edge_cost_k,
        min_validation_trades=min_validation_trades,
        stressed_cost_multiplier=stressed_cost_multiplier,
        min_net_expectancy_bps=min_net_expectancy_bps,
        max_fee_plus_slippage_bps=max_fee_plus_slippage_bps,
        max_daily_turnover_multiple=max_daily_turnover_multiple,
        require_retail_viability=require_retail_viability,
        micro_thresholds={
            "max_spread_stress": micro_max_spread_stress,
            "max_depth_depletion": micro_max_depth_depletion,
            "max_sweep_pressure": micro_max_sweep_pressure,
            "max_abs_imbalance": micro_max_abs_imbalance,
            "min_feature_coverage": micro_min_feature_coverage,
        },
        low_capital_contract=dict(low_capital_contract or {}),
        enforce_low_capital_viability=enforce_low_capital_viability,
    )
    result = (
        metrics_rows[0]
        if metrics_rows
        else {
            "candidate_id": str(row.get("candidate_id", "")).strip(),
            "bridge_eval_status": "rejected:bridge_evaluation_failed",
            "gate_bridge_tradable": False,
        }
    )
    overlay = overlay_rows[0] if overlay_rows else None
    return result, overlay


def _policy_variant_flip_summary(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {
            "baseline_pass_policy_count": 0,
            "pass_to_fail_policy_count": 0,
            "fail_to_pass_policy_count": 0,
            "policy_variant_count": 0,
        }
    working = df.copy()
    baseline_mask = _bool_series(working, "is_baseline_variant", default=False)
    tradable_mask = _bool_series(working, "gate_bridge_tradable", default=False)
    baseline_by_candidate = (
        working.loc[baseline_mask, ["candidate_id"]]
        .assign(_baseline_pass=tradable_mask.loc[baseline_mask].astype(bool).values)
        .groupby("candidate_id")["_baseline_pass"]
        .max()
    )
    working["_baseline_pass"] = (
        working["candidate_id"].map(baseline_by_candidate).fillna(False).astype(bool)
    )
    pass_to_fail = int(
        working.loc[~baseline_mask & working["_baseline_pass"] & ~tradable_mask, "candidate_id"]
        .dropna()
        .astype(str)
        .nunique()
    )
    fail_to_pass = int(
        working.loc[~baseline_mask & ~working["_baseline_pass"] & tradable_mask, "candidate_id"]
        .dropna()
        .astype(str)
        .nunique()
    )
    return {
        "baseline_pass_policy_count": int(
            working.loc[baseline_mask & tradable_mask, "candidate_id"]
            .dropna()
            .astype(str)
            .nunique()
        ),
        "pass_to_fail_policy_count": pass_to_fail,
        "fail_to_pass_policy_count": fail_to_pass,
        "policy_variant_count": int(len(working)),
    }


def _load_symbol_calibrated_cost_bps(symbol: str, calibration_dir: Path) -> float | None:
    path = Path(calibration_dir) / f"{str(symbol).strip().upper()}.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    calibrated_cost = safe_float(payload.get("calibrated_cost_bps"), np.nan)
    if np.isfinite(calibrated_cost):
        return float(calibrated_cost)
    fee = safe_float(payload.get("base_fee_bps"), np.nan)
    slip = safe_float(payload.get("base_slippage_bps"), np.nan)
    if np.isfinite(fee) and np.isfinite(slip):
        return float(max(0.0, fee) + max(0.0, slip))
    return None


def _write_bridge_symbol_calibrations(
    *,
    calibrations: Dict[str, Any],
    calibration_dir: Path,
) -> List[Path]:
    ensure_dir(Path(calibration_dir))
    written: List[Path] = []
    for symbol, payload in sorted(calibrations.items()):
        symbol_name = re.sub(r"[^A-Za-z0-9_\\-]+", "", str(symbol).strip().upper())
        if not symbol_name:
            continue
        path = Path(calibration_dir) / f"{symbol_name}.json"
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        written.append(path)
    return written


def _make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate candidates on bridge/oos data.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--start", default="2024-01-01")
    parser.add_argument("--end", default="2024-01-02")
    parser.add_argument("--train_frac", type=float, default=0.5)
    parser.add_argument("--validation_frac", type=float, default=0.2)
    parser.add_argument("--embargo_days", type=int, default=0)
    parser.add_argument("--edge_cost_k", type=float, default=1.5)
    parser.add_argument("--stressed_cost_multiplier", type=float, default=2.0)
    parser.add_argument("--min_validation_trades", type=int, default=50)
    parser.add_argument("--mode", default="research")
    parser.add_argument("--candidate_mask", default="auto")
    parser.add_argument("--event_type", default="all")
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--micro_max_spread_stress", type=float, default=2.0)
    parser.add_argument("--micro_max_depth_depletion", type=float, default=0.70)
    parser.add_argument("--micro_max_sweep_pressure", type=float, default=2.5)
    parser.add_argument("--micro_max_abs_imbalance", type=float, default=0.90)
    parser.add_argument("--micro_min_feature_coverage", type=float, default=0.25)
    parser.add_argument("--objective_name", default="")
    parser.add_argument("--objective_spec", default=None)
    parser.add_argument("--retail_profile", default="")
    parser.add_argument("--retail_profiles_spec", default=None)
    return parser


def _resolve_bridge_policy(args: argparse.Namespace, data_root: Path) -> Dict[str, Any]:
    project_root = Path(__file__).resolve().parents[3]
    contract = resolve_objective_profile_contract(
        project_root=project_root,
        data_root=data_root,
        run_id=str(args.run_id),
        objective_name=(str(args.objective_name).strip() or None),
        objective_spec_path=(str(args.objective_spec).strip() or None),
        retail_profile_name=(str(args.retail_profile).strip() or None),
        retail_profiles_spec_path=(str(args.retail_profiles_spec).strip() or None),
    )
    return {
        "min_net_expectancy_bps": float(getattr(contract, "min_net_expectancy_bps", 0.0) or 0.0),
        "max_fee_plus_slippage_bps": getattr(contract, "max_fee_plus_slippage_bps", None),
        "max_daily_turnover_multiple": getattr(contract, "max_daily_turnover_multiple", None),
        "require_retail_viability": bool(getattr(contract, "require_retail_viability", False)),
        "low_capital_contract": dict(getattr(contract, "low_capital_contract", {}) or {}),
        "enforce_low_capital_viability": bool(
            getattr(contract, "require_low_capital_contract", False)
        ),
    }


def main() -> int:
    DATA_ROOT = get_data_root()
    parser = _make_parser()
    args = parser.parse_args()

    timeframe = normalize_timeframe(args.timeframe)
    out_dir = (
        Path(args.out_dir)
        if args.out_dir
        else bridge_event_out_dir(
            data_root=DATA_ROOT,
            run_id=args.run_id,
            event_type=args.event_type,
            timeframe=timeframe,
        )
    )
    ensure_dir(out_dir)

    stage_name = os.getenv(
        "BACKTEST_STAGE_INSTANCE_ID", f"bridge_evaluate_phase2__{args.event_type}_{timeframe}"
    )
    manifest = start_manifest(stage_name, args.run_id, vars(args), [], [])

    try:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        event_type = str(args.event_type).strip().upper()
        bridge_policy = _resolve_bridge_policy(args, DATA_ROOT)

        any_candidates = False
        for symbol in symbols:
            sym_out = out_dir / symbol
            ensure_dir(sym_out)
            cand_path = (
                phase2_event_out_dir(
                    data_root=DATA_ROOT,
                    run_id=args.run_id,
                    event_type=event_type,
                    timeframe=timeframe,
                )
                / "phase2_candidates.parquet"
            )

            if not cand_path.exists():
                logging.warning(f"No candidates found for {event_type} at {cand_path}")
                continue

            candidates = pd.read_parquet(cand_path)
            if candidates.empty:
                write_parquet(pd.DataFrame(), sym_out / "bridge_evaluation.parquet")
                (sym_out / "bridge_summary.json").write_text(
                    json.dumps(
                        _build_bridge_summary_payload(pd.DataFrame()), indent=2, sort_keys=True
                    ),
                    encoding="utf-8",
                )
                continue
            candidates = _filter_candidates_for_symbol(candidates, symbol)
            if candidates.empty:
                write_parquet(pd.DataFrame(), sym_out / "bridge_evaluation.parquet")
                (sym_out / "bridge_summary.json").write_text(
                    json.dumps(
                        _build_bridge_summary_payload(pd.DataFrame()), indent=2, sort_keys=True
                    ),
                    encoding="utf-8",
                )
                continue

            any_candidates = True

            # Load bridge results (dummy logic for this example - in reality, it would load from lake)
            # For now, we assume candidates contains bridge performance metrics
            # as it was produced by a previous pipeline step (e.g. phase2_cost_integration)

            # Delegate to service
            metrics_rows, overlay_rows = evaluate_bridge_performance(
                candidates,
                event_type=args.event_type,
                base_lookup={},
                edge_cost_k=args.edge_cost_k,
                min_validation_trades=args.min_validation_trades,
                stressed_cost_multiplier=args.stressed_cost_multiplier,
                min_net_expectancy_bps=bridge_policy["min_net_expectancy_bps"],
                max_fee_plus_slippage_bps=bridge_policy["max_fee_plus_slippage_bps"],
                max_daily_turnover_multiple=bridge_policy["max_daily_turnover_multiple"],
                require_retail_viability=bridge_policy["require_retail_viability"],
                micro_thresholds={
                    "max_spread_stress": args.micro_max_spread_stress,
                    "max_depth_depletion": args.micro_max_depth_depletion,
                    "max_sweep_pressure": args.micro_max_sweep_pressure,
                    "max_abs_imbalance": args.micro_max_abs_imbalance,
                    "min_feature_coverage": args.micro_min_feature_coverage,
                },
                low_capital_contract=bridge_policy["low_capital_contract"],
                enforce_low_capital_viability=bridge_policy["enforce_low_capital_viability"],
            )

            if metrics_rows:
                df_out = pd.DataFrame(metrics_rows)
                # B1: Ensure mandatory bridge metrics exist
                mandatory_metrics = [
                    "fill_feasibility",
                    "min_order_feasibility",
                    "tob_coverage",
                    "expected_spread_cost",
                    "realized_slippage_estimate",
                    "turnover",
                    "delay_sensitivity",
                    "one_trade_per_episode_sensitivity",
                    "cooldown_sensitivity",
                    "stress_cost_survival",
                ]
                for m in mandatory_metrics:
                    if m not in df_out.columns:
                        df_out[m] = np.nan

                # B2: Sign the artifacts
                df_out["bridge_run_id"] = args.run_id
                df_out["bridge_schema_version"] = "v2"
                if "bridge_certified" not in df_out.columns:
                    df_out["bridge_certified"] = True

                # Compute artifact hash
                payload = df_out.to_json(orient="records").encode("utf-8")
                import hashlib

                df_out["bridge_artifact_hash"] = hashlib.sha256(payload).hexdigest()

                write_parquet(df_out, sym_out / "bridge_evaluation.parquet")
                (sym_out / "bridge_summary.json").write_text(
                    json.dumps(_build_bridge_summary_payload(df_out), indent=2, sort_keys=True),
                    encoding="utf-8",
                )

        if not any_candidates:
            message = f"No candidates found for evaluation across all symbols for {event_type}. Skipping evaluation."
            logging.info(message)
            print(message, file=sys.stderr)
            finalize_manifest(
                manifest,
                "warning",
                stats={
                    "candidate_count": 0,
                    "evaluation_skipped": True,
                    "skip_reason": "no_candidates",
                },
            )
            return 1

        finalize_manifest(manifest, "success")
        return 0
    except Exception as e:
        logging.exception("Bridge evaluation failed")
        finalize_manifest(manifest, "failed", error=str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
