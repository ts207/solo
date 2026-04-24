from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from project import PROJECT_ROOT
from project.core.config import get_data_root
from project.io.utils import ensure_dir, write_parquet
from project.research.utils.decision_safety import fail_closed_bool, finite_ge
from project.specs.manifest import finalize_manifest, start_manifest
from project.specs.objective import resolve_objective_profile_contract


def _read_candidate_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        return pd.DataFrame(payload if isinstance(payload, list) else [])
    return pd.DataFrame()


def _load_candidates_table(run_id: str, candidates_path: str = "") -> tuple[pd.DataFrame, Path]:
    DATA_ROOT = get_data_root()
    candidates_root = DATA_ROOT / "reports" / "strategy_builder" / run_id
    promotions_root = DATA_ROOT / "reports" / "promotions" / run_id
    preferred_paths: List[Path] = []
    if str(candidates_path).strip():
        requested = Path(candidates_path)
        preferred_paths.extend(
            [requested, requested.with_suffix(".csv"), requested.with_suffix(".json")]
        )
    preferred_paths.extend(
        [
            candidates_root / "strategy_candidates.parquet",
            candidates_root / "strategy_candidates.csv",
            candidates_root / "strategy_candidates.json",
            promotions_root / "promoted_candidates.parquet",
            promotions_root / "promoted_candidates.csv",
        ]
    )
    seen: set[Path] = set()
    for path in preferred_paths:
        if path in seen:
            continue
        seen.add(path)
        if not path.exists():
            continue
        try:
            return _read_candidate_table(path), path
        except Exception as exc:
            logging.debug("Failed to read candidate table from %s: %s", path, exc)
    return pd.DataFrame(), preferred_paths[
        0
    ] if preferred_paths else candidates_root / "strategy_candidates.parquet"


def _bool_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(False, index=df.index, dtype=bool)
    return df[column].map(fail_closed_bool).astype(bool)


def _bool_series_default(df: pd.DataFrame, column: str, default: bool) -> pd.Series:
    if column not in df.columns:
        return pd.Series(default, index=df.index, dtype=bool)
    return df[column].map(fail_closed_bool).astype(bool)


def _first_numeric(
    df: pd.DataFrame, names: List[str], default: float = np.nan
) -> tuple[pd.Series, str]:
    for name in names:
        if name in df.columns:
            series = pd.to_numeric(df[name], errors="coerce")
            return series, name
    return pd.Series(default, index=df.index, dtype=float), ""


def _first_expectancy_bps(df: pd.DataFrame) -> tuple[pd.Series, str]:
    bps_names = [
        "bridge_validation_stressed_after_cost_bps",
        "bridge_validation_after_cost_bps",
        "net_expectancy_bps",
        "expectancy_bps",
        "expectancy_per_trade_after_cost_bps",
    ]
    for name in bps_names:
        if name in df.columns:
            return pd.to_numeric(df[name], errors="coerce"), name

    decimal_names = [
        "expectancy_after_multiplicity",
        "expectancy_per_trade",
    ]
    for name in decimal_names:
        if name in df.columns:
            return pd.to_numeric(df[name], errors="coerce") * 10_000.0, f"{name}*1e4"

    return pd.Series(np.nan, index=df.index, dtype=float), ""


def _allocation_viable_series(df: pd.DataFrame) -> pd.Series:
    if "allocation_policy" not in df.columns:
        return pd.Series(True, index=df.index, dtype=bool)
    out: List[bool] = []
    for value in df["allocation_policy"].tolist():
        if isinstance(value, dict):
            out.append(bool(value.get("allocation_viable", True)))
            continue
        text = str(value).strip()
        if not text:
            out.append(True)
            continue
        try:
            payload = json.loads(text)
        except Exception:
            out.append(True)
            continue
        out.append(
            bool(payload.get("allocation_viable", True)) if isinstance(payload, dict) else True
        )
    return pd.Series(out, index=df.index, dtype=bool)


def _normalize_candidate_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "event_type" not in out.columns and "event" in out.columns:
        out["event_type"] = out["event"]
    if "event_type" not in out.columns:
        out["event_type"] = "UNKNOWN_EVENT"
    out["event_type"] = out["event_type"].astype(str).str.strip()

    if "candidate_id" not in out.columns:
        out["candidate_id"] = [
            f"{event}_{idx}" for idx, event in enumerate(out["event_type"].astype(str).tolist())
        ]
    out["candidate_id"] = out["candidate_id"].astype(str).str.strip()
    if "status" not in out.columns:
        out["status"] = pd.NA
    return out


def _write_empty_selection_outputs(
    *,
    run_id: str,
    out_path: Path,
    out_csv_path: Path,
    summary_path: Path,
    outputs: List[Dict[str, object]],
    manifest: Dict[str, Any],
    min_expectancy_bps: float,
    min_events: int,
    min_oos_consistency: float,
    contract: Any,
    source_path: Path,
) -> int:
    empty = _normalize_candidate_frame(pd.DataFrame()).iloc[0:0].copy()
    empty["expectancy_bps"] = pd.Series(dtype=float)
    empty["sample_size"] = pd.Series(dtype=float)
    empty["selection_score"] = pd.Series(dtype=float)
    empty["profitability_score"] = pd.Series(dtype=float)
    empty["selection_reason"] = pd.Series(dtype="object")

    ensure_dir(out_path.parent)
    write_parquet(empty, out_path)
    empty.to_csv(out_csv_path, index=False)

    summary = {
        "run_id": run_id,
        "input_count": 0,
        "selected_count": 0,
        "selection_rate": 0.0,
        "min_expectancy_bps": float(min_expectancy_bps),
        "min_events": int(min_events),
        "min_oos_sign_consistency": float(min_oos_consistency),
        "expectancy_column_used": "",
        "sample_column_used": "",
        "selection_score_column_used": "",
        "oos_consistency_column_used": "",
        "require_retail_viability": bool(contract.require_retail_viability),
        "require_low_capital_contract": bool(contract.require_low_capital_contract),
        "objective_name": contract.objective_name,
        "retail_profile_name": contract.retail_profile_name,
        "no_candidates_found": True,
        "source_path": str(source_path),
    }
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    outputs.extend(
        [
            {"path": str(out_path), "rows": 0},
            {"path": str(out_csv_path), "rows": 0},
            {"path": str(summary_path), "rows": 1},
        ]
    )
    finalize_manifest(manifest, "success", stats=summary)
    logging.info("No strategy candidates available for %s; wrote empty selection outputs.", run_id)
    return 0


def main() -> int:
    DATA_ROOT = get_data_root()
    parser = argparse.ArgumentParser(
        description="Select profitable candidates for further analysis."
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--objective_name", default="")
    parser.add_argument("--objective_spec", default=None)
    parser.add_argument("--retail_profile", default="")
    parser.add_argument("--retail_profiles_spec", default=None)
    parser.add_argument("--min_expectancy_bps", type=float, default=None)
    parser.add_argument("--min_events", type=int, default=None)
    parser.add_argument("--max_candidates_per_event", type=int, default=0)
    parser.add_argument("--candidates_path", default="")
    parser.add_argument("--out_path", default="")
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(
        level=logging.INFO, handlers=handlers, format="%(asctime)s %(levelname)s %(message)s"
    )

    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    run_id = str(args.run_id).strip()
    if not run_id:
        raise ValueError("--run_id must be non-empty")

    out_root = DATA_ROOT / "reports" / "strategy_selection" / run_id
    out_path = (
        Path(args.out_path).resolve()
        if str(args.out_path).strip()
        else out_root / "profitable_strategies.parquet"
    )
    out_csv_path = out_path.with_suffix(".csv")
    summary_path = out_root / "profitability_summary.json"

    params = {
        "run_id": run_id,
        "symbols": symbols,
        "objective_name": str(args.objective_name),
        "retail_profile": str(args.retail_profile),
        "min_expectancy_bps_override": args.min_expectancy_bps,
        "min_events_override": args.min_events,
        "max_candidates_per_event": int(args.max_candidates_per_event),
        "candidates_path": str(args.candidates_path),
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("select_profitable_strategies", run_id, params, inputs, outputs)

    try:
        contract = resolve_objective_profile_contract(
            project_root=PROJECT_ROOT,
            data_root=DATA_ROOT,
            run_id=run_id,
            objective_name=str(args.objective_name).strip() or None,
            objective_spec_path=args.objective_spec,
            retail_profile_name=str(args.retail_profile).strip() or None,
            retail_profiles_spec_path=args.retail_profiles_spec,
            required=True,
        )

        min_expectancy_bps = (
            float(args.min_expectancy_bps)
            if args.min_expectancy_bps is not None
            else float(contract.min_net_expectancy_bps)
        )
        min_events = (
            int(args.min_events) if args.min_events is not None else int(contract.min_trade_count)
        )
        min_oos_consistency = float(contract.min_oos_sign_consistency)

        raw_df, source_path = _load_candidates_table(
            run_id, candidates_path=str(args.candidates_path)
        )
        inputs.append({"path": str(source_path), "rows": int(len(raw_df))})

        if raw_df.empty:
            return _write_empty_selection_outputs(
                run_id=run_id,
                out_path=out_path,
                out_csv_path=out_csv_path,
                summary_path=summary_path,
                outputs=outputs,
                manifest=manifest,
                min_expectancy_bps=min_expectancy_bps,
                min_events=min_events,
                min_oos_consistency=min_oos_consistency,
                contract=contract,
                source_path=source_path,
            )

        df = _normalize_candidate_frame(raw_df)

        expectancy, expectancy_col = _first_expectancy_bps(df)
        sample_size, sample_col = _first_numeric(
            df,
            ["n_events", "sample_size", "bridge_validation_trades", "naive_total_trades", "trades"],
            default=0.0,
        )
        selection_score, selection_col = _first_numeric(df, ["selection_score"], default=0.0)
        oos_consistency, oos_col = _first_numeric(
            df, ["oos_sign_consistency", "sign_consistency"], default=np.nan
        )
        expectancy_ok = expectancy.fillna(-np.inf) >= float(min_expectancy_bps)
        sample_ok = sample_size.fillna(0.0) >= float(min_events)
        strategy_candidate_mode = "strategy_candidate_id" in df.columns

        if strategy_candidate_mode:
            status_ok = ~df["status"].astype(str).str.upper().eq("REJECTED")
            executable_condition_ok = _bool_series_default(df, "executable_condition", True)
            executable_action_ok = _bool_series_default(df, "executable_action", True)
            allocation_ok = _allocation_viable_series(df)
            oos_metric_ok = (
                oos_consistency.map(lambda x: finite_ge(x, min_oos_consistency))
                if (oos_col or min_oos_consistency <= 0)
                else pd.Series(False, index=df.index, dtype=bool)
            )
            final_mask = (
                status_ok
                & executable_condition_ok
                & executable_action_ok
                & allocation_ok
                & oos_metric_ok
                & expectancy_ok
                & sample_ok
            )
        else:
            status_ok = df["status"].astype(str).str.upper().eq("PROMOTED")
            statistical_ok = _bool_series(df, "gate_promo_statistical")
            stability_ok = _bool_series(df, "gate_promo_stability")
            cost_ok = _bool_series(df, "gate_promo_cost_survival")
            control_ok = _bool_series(df, "gate_promo_negative_control")
            oos_gate_ok = _bool_series(df, "gate_promo_oos_validation")
            micro_ok = _bool_series(df, "gate_promo_microstructure")
            retail_ok = _bool_series(df, "gate_promo_retail_viability")
            low_cap_ok = _bool_series(df, "gate_promo_low_capital_viability")
            oos_metric_ok = (
                oos_consistency.map(lambda x: finite_ge(x, min_oos_consistency))
                if (oos_col or min_oos_consistency <= 0)
                else pd.Series(False, index=df.index, dtype=bool)
            )
            final_mask = (
                status_ok
                & statistical_ok
                & stability_ok
                & cost_ok
                & control_ok
                & oos_gate_ok
                & oos_metric_ok
                & micro_ok
                & (retail_ok if contract.require_retail_viability else True)
                & (low_cap_ok if contract.require_low_capital_contract else True)
                & expectancy_ok
                & sample_ok
            )

        selected = df.loc[final_mask].copy()
        selected["expectancy_bps"] = expectancy.loc[selected.index].astype(float)
        selected["sample_size"] = sample_size.loc[selected.index].astype(float)
        selected["selection_score"] = selection_score.loc[selected.index].astype(float)

        # Profitability score preserves NaN if either component is NaN to avoid false optimism
        selected["profitability_score"] = (
            selected["expectancy_bps"] + selected["selection_score"]
        ).astype(float)
        selected["selection_reason"] = "passed_profitability_contract"

        if int(args.max_candidates_per_event) > 0 and not selected.empty:
            selected = selected.sort_values(
                ["event_type", "profitability_score", "expectancy_bps"],
                ascending=[True, False, False],
            )
            selected = (
                selected.groupby("event_type", sort=False)
                .head(int(args.max_candidates_per_event))
                .copy()
            )

        selected = selected.sort_values(
            ["profitability_score", "expectancy_bps", "sample_size"],
            ascending=[False, False, False],
        ).reset_index(drop=True)

        ensure_dir(out_path.parent)
        write_parquet(selected, out_path)
        selected.to_csv(out_csv_path, index=False)

        summary = {
            "run_id": run_id,
            "input_count": int(len(df)),
            "selected_count": int(len(selected)),
            "selection_rate": float(len(selected) / len(df)) if len(df) else 0.0,
            "min_expectancy_bps": float(min_expectancy_bps),
            "min_events": int(min_events),
            "min_oos_sign_consistency": float(min_oos_consistency),
            "expectancy_column_used": expectancy_col,
            "sample_column_used": sample_col,
            "selection_score_column_used": selection_col,
            "oos_consistency_column_used": oos_col,
            "require_retail_viability": bool(contract.require_retail_viability),
            "require_low_capital_contract": bool(contract.require_low_capital_contract),
            "objective_name": contract.objective_name,
            "retail_profile_name": contract.retail_profile_name,
        }
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

        outputs.extend(
            [
                {"path": str(out_path), "rows": int(len(selected))},
                {"path": str(out_csv_path), "rows": int(len(selected))},
                {"path": str(summary_path), "rows": 1},
            ]
        )
        finalize_manifest(manifest, "success", stats=summary)
        return 0
    except Exception as exc:
        logging.exception("Profitability selection failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
