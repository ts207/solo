from __future__ import annotations
from project.core.config import get_data_root

from project.core.coercion import safe_float, safe_int, as_bool
from project.research.utils.decision_safety import (
    finite_ge,
    bool_gate,
    coerce_numeric_nan,
    nanmedian_or_nan,
    nanmax_or_nan,
)


import argparse
import json
import logging
import sys

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd
from project import PROJECT_ROOT

from project.io.utils import ensure_dir, write_parquet
from project.specs.ontology import load_run_manifest_hashes
from project.specs.manifest import finalize_manifest, start_manifest
from project.research.edge_identity import edge_id_from_row, structural_edge_components


def _normalize_event_type(row: Dict[str, Any]) -> str:
    token = str(
        row.get("canonical_event_type", row.get("event_type", row.get("event", "")))
    ).strip()
    return token or "UNKNOWN_EVENT"


def _load_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        if path.suffix == ".parquet":
            return pd.read_parquet(path)
        return pd.read_csv(path)
    except Exception as exc:
        logging.debug(f"Failed to load table from {path}: {exc}")
        return pd.DataFrame()


def _load_history(path: Path) -> pd.DataFrame:
    df = _load_table(path)
    if df.empty:
        return pd.DataFrame()
    return df


def _load_capital_footprint(path: Path) -> pd.DataFrame:
    df = _load_table(path)
    if df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "candidate_id" not in out.columns:
        return pd.DataFrame()
    out["candidate_id"] = out["candidate_id"].astype(str).str.strip()
    if "event_type" in out.columns:
        out["event_type"] = out["event_type"].astype(str).str.strip()
    else:
        out["event_type"] = ""
    keep = [
        "candidate_id",
        "event_type",
        "estimated_position_notional_usd",
        "slot_pressure_fraction",
        "leverage_usage_fraction",
        "gate_capital_slot_within_limit",
        "gate_capital_leverage_within_budget",
    ]
    for col in keep:
        if col not in out.columns:
            out[col] = np.nan if col.endswith("_usd") or col.endswith("_fraction") else None
    out = out[keep].drop_duplicates(subset=["candidate_id", "event_type"], keep="last")
    return out


def _effect_value(row: Dict[str, Any]) -> float:
    for key in (
        "effect_shrunk_state",
        "effect_shrunk_event",
        "effect_raw",
        "expectancy",
        "after_cost_expectancy_per_trade",
    ):
        val = coerce_numeric_nan(row.get(key))
        if np.isfinite(val):
            return val
    return np.nan


def _template_id(row: Dict[str, Any]) -> str:
    token = str(
        row.get(
            "template_id",
            row.get("template_verb", row.get("rule_template", row.get("template_family", ""))),
        )
    ).strip()
    return token or "UNKNOWN_TEMPLATE"


def _run_sort_value(value: str) -> Tuple[int, str]:
    token = str(value or "").strip()
    digits = "".join(ch for ch in token if ch.isdigit())
    if digits:
        try:
            return (int(digits), token)
        except ValueError:
            pass
    return (0, token)


def _effect_decay(values: Iterable[float]) -> float:
    y = np.asarray([safe_float(v, np.nan) for v in values], dtype=float)
    y = y[np.isfinite(y)]
    if y.size < 2:
        return 0.0
    x = np.arange(float(y.size), dtype=float)
    try:
        slope = np.polyfit(x, y, 1)[0]
    except Exception as exc:
        logging.debug(f"Failed to compute effect decay: {exc}")
        return 0.0

    if not np.isfinite(slope):
        return 0.0
    return float(slope)


def _build_observations(
    *,
    run_id: str,
    promoted_df: pd.DataFrame,
    audit_df: pd.DataFrame,
    capital_footprint_df: pd.DataFrame,
    ontology_spec_hash: str,
) -> pd.DataFrame:
    source_df = audit_df.copy() if not audit_df.empty else promoted_df.copy()
    if source_df.empty:
        return pd.DataFrame()
    if not capital_footprint_df.empty:
        merge_keys = ["candidate_id", "event_type"]
        source_df = source_df.copy()
        if "candidate_id" not in source_df.columns:
            source_df["candidate_id"] = ""
        source_df["candidate_id"] = source_df["candidate_id"].astype(str).str.strip()
        if "event_type" not in source_df.columns:
            source_df["event_type"] = source_df.get("event", "")
        source_df["event_type"] = source_df["event_type"].astype(str).str.strip()
        source_df = source_df.merge(
            capital_footprint_df,
            on=merge_keys,
            how="left",
            suffixes=("", "_capital"),
        )

    rows: List[Dict[str, Any]] = []
    observed_at = datetime.now(timezone.utc).isoformat()
    for row in source_df.to_dict(orient="records"):
        record = dict(row)
        record["run_id"] = run_id
        record["candidate_id"] = str(record.get("candidate_id", "")).strip()
        if not record["candidate_id"]:
            continue
        record["event_type"] = _normalize_event_type(record)
        comps = structural_edge_components(record)
        record["edge_id"] = edge_id_from_row(record)
        record["template_id"] = _template_id(record)
        record["template_family"] = comps.template_family
        record["direction_rule"] = comps.direction_rule
        record["signal_polarity_logic"] = comps.signal_polarity_logic
        record["promotion_decision"] = (
            str(record.get("promotion_decision", record.get("status", "rejected"))).strip().lower()
        )
        if record["promotion_decision"] not in {"promoted", "rejected"}:
            record["promotion_decision"] = (
                "promoted"
                if str(record.get("status", "")).strip().upper() == "PROMOTED"
                else "rejected"
            )
        record["promotion_score"] = coerce_numeric_nan(record.get("promotion_score"))
        record["effect_value"] = _effect_value(record)
        record["stability_score"] = coerce_numeric_nan(record.get("stability_score"))

        record["estimated_position_notional_usd"] = safe_float(
            record.get("estimated_position_notional_usd"), np.nan
        )
        record["slot_pressure_fraction"] = safe_float(record.get("slot_pressure_fraction"), np.nan)
        record["leverage_usage_fraction"] = safe_float(
            record.get("leverage_usage_fraction"), np.nan
        )
        record["gate_capital_slot_within_limit"] = bool_gate(
            record.get("gate_capital_slot_within_limit")
        )
        record["gate_capital_leverage_within_budget"] = bool_gate(
            record.get("gate_capital_leverage_within_budget")
        )

        record["observed_at_utc"] = observed_at
        record["ontology_spec_hash"] = str(
            record.get("ontology_spec_hash", ontology_spec_hash)
        ).strip()
        rows.append(record)
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    # Drop complex object columns that cause Parquet serialization errors
    cols_to_drop = [
        c
        for c in out.columns
        if "schema" in c
        or "trace" in c
        or c == "audit_statuses"
        or out[c].dtype == object
        and isinstance(
            out[c].dropna().iloc[0] if not out[c].dropna().empty else None, (list, dict, np.ndarray)
        )
    ]
    out = out.drop(columns=cols_to_drop, errors="ignore")
    out = out.drop_duplicates(subset=["run_id", "candidate_id", "event_type"], keep="last")
    return out


def _aggregate_registry(observations: pd.DataFrame) -> pd.DataFrame:
    if observations.empty:
        return pd.DataFrame()

    work = observations.copy()
    work["is_promoted"] = work["promotion_decision"].astype(str).str.lower() == "promoted"
    work["run_sort"] = work["run_id"].map(_run_sort_value)

    rows: List[Dict[str, Any]] = []
    for edge_id, sub in work.groupby("edge_id", sort=False):
        ordered = sub.sort_values(by=["run_sort", "observed_at_utc", "candidate_id"], kind="stable")
        first = ordered.iloc[0]
        last = ordered.iloc[-1]
        effect_series = (
            pd.to_numeric(ordered["effect_value"], errors="coerce")
            .replace([np.inf, -np.inf], np.nan)
            .dropna()
        )
        stability_series = (
            pd.to_numeric(ordered["stability_score"], errors="coerce")
            .replace([np.inf, -np.inf], np.nan)
            .dropna()
        )
        slot_pressure_series = (
            pd.to_numeric(
                ordered.get("slot_pressure_fraction", pd.Series(dtype=float)),
                errors="coerce",
            )
            .replace([np.inf, -np.inf], np.nan)
            .dropna()
        )
        leverage_usage_series = (
            pd.to_numeric(
                ordered.get("leverage_usage_fraction", pd.Series(dtype=float)),
                errors="coerce",
            )
            .replace([np.inf, -np.inf], np.nan)
            .dropna()
        )
        rows.append(
            {
                "edge_id": str(edge_id),
                "candidate_id": str(last.get("candidate_id", "")).strip(),
                "promotion_score": coerce_numeric_nan(last.get("promotion_score")),
                "promotion_decision": str(last.get("promotion_decision", "rejected"))
                .strip()
                .lower(),
                "event_type": str(last.get("event_type", "UNKNOWN_EVENT")).strip(),
                "template_id": str(last.get("template_id", "UNKNOWN_TEMPLATE")).strip(),
                "template_family": str(last.get("template_family", "UNKNOWN_TEMPLATE")).strip(),
                "direction_rule": str(last.get("direction_rule", "UNKNOWN_DIRECTION")).strip(),
                "signal_polarity_logic": str(
                    last.get("signal_polarity_logic", "UNKNOWN_POLARITY")
                ).strip(),
                "first_seen_run": str(first.get("run_id", "")).strip(),
                "last_seen_run": str(last.get("run_id", "")).strip(),
                "times_promoted": int(ordered["is_promoted"].sum()),
                "times_tested": int(len(ordered)),
                "median_effect": nanmedian_or_nan(effect_series),
                "effect_decay_rate": _effect_decay(effect_series.tolist()),
                "stability_median": nanmedian_or_nan(stability_series),
                "capital_slot_pressure_median": nanmedian_or_nan(slot_pressure_series),
                "capital_slot_pressure_max": nanmax_or_nan(slot_pressure_series),
                "capital_leverage_usage_median": nanmedian_or_nan(leverage_usage_series),
                "capital_leverage_usage_max": nanmax_or_nan(leverage_usage_series),
                "capital_slot_limit_breaches": int((slot_pressure_series > 1.0).sum()),
                "capital_leverage_budget_breaches": int((leverage_usage_series > 1.0).sum()),
            }
        )
    out = pd.DataFrame(rows)
    out = out.sort_values(by=["event_type", "template_id", "edge_id"], kind="stable").reset_index(
        drop=True
    )
    return out


def _empty_observations_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "run_id",
            "candidate_id",
            "event_type",
            "edge_id",
            "template_id",
            "template_family",
            "direction_rule",
            "signal_polarity_logic",
            "promotion_decision",
            "promotion_score",
            "effect_value",
            "stability_score",
            "estimated_position_notional_usd",
            "slot_pressure_fraction",
            "leverage_usage_fraction",
            "gate_capital_slot_within_limit",
            "gate_capital_leverage_within_budget",
            "observed_at_utc",
            "ontology_spec_hash",
        ]
    )


def _empty_registry_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "edge_id",
            "candidate_id",
            "promotion_score",
            "promotion_decision",
            "event_type",
            "template_id",
            "template_family",
            "direction_rule",
            "signal_polarity_logic",
            "first_seen_run",
            "last_seen_run",
            "times_promoted",
            "times_tested",
            "median_effect",
            "effect_decay_rate",
            "stability_median",
            "capital_slot_pressure_median",
            "capital_slot_pressure_max",
            "capital_leverage_usage_median",
            "capital_leverage_usage_max",
            "capital_slot_limit_breaches",
            "capital_leverage_budget_breaches",
        ]
    )


def main(argv: List[str] | None = None) -> int:
    DATA_ROOT = get_data_root()
    parser = argparse.ArgumentParser(
        description="Append promoted-candidate lineage and aggregate edge registry."
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--promoted_candidates_path", default=None)
    parser.add_argument("--promotion_audit_path", default=None)
    parser.add_argument("--promotion_capital_footprint_path", default=None)
    parser.add_argument(
        "--baseline_id",
        default="default",
        help="Immutable baseline ID to consume for cross-run edge history.",
    )
    parser.add_argument("--history_baselines_root", default=None)
    parser.add_argument("--history_candidates_root", default=None)
    parser.add_argument("--baseline_observations_path", default=None)
    parser.add_argument("--baseline_registry_path", default=None)
    parser.add_argument(
        "--promote_baseline",
        type=int,
        default=0,
        help="If 1, emit a new immutable baseline snapshot from baseline+current run.",
    )
    parser.add_argument(
        "--promote_baseline_id",
        default=None,
        help="Override new baseline id when --promote_baseline=1.",
    )
    parser.add_argument("--out_path", default=None)
    args = parser.parse_args(argv)

    promoted_path = (
        Path(args.promoted_candidates_path)
        if args.promoted_candidates_path
        else DATA_ROOT / "reports" / "promotions" / args.run_id / "promoted_candidates.parquet"
    )
    audit_path = (
        Path(args.promotion_audit_path)
        if args.promotion_audit_path
        else DATA_ROOT
        / "reports"
        / "promotions"
        / args.run_id
        / "promotion_statistical_audit.parquet"
    )
    if not audit_path.exists() and not args.promotion_audit_path:
        legacy_audit_path = (
            DATA_ROOT / "reports" / "promotions" / args.run_id / "promotion_audit.parquet"
        )
        if legacy_audit_path.exists():
            audit_path = legacy_audit_path
    capital_footprint_path = (
        Path(args.promotion_capital_footprint_path)
        if args.promotion_capital_footprint_path
        else DATA_ROOT
        / "reports"
        / "promotions"
        / args.run_id
        / "promotion_capital_footprint.parquet"
    )
    baseline_id = str(args.baseline_id or "default").strip() or "default"
    history_baselines_root = (
        Path(args.history_baselines_root)
        if args.history_baselines_root
        else DATA_ROOT / "runs" / "history_baselines"
    )
    history_candidates_root = (
        Path(args.history_candidates_root)
        if args.history_candidates_root
        else DATA_ROOT / "runs" / "history_candidates"
    )
    baseline_dir = history_baselines_root / baseline_id
    baseline_observations_path = (
        Path(args.baseline_observations_path)
        if args.baseline_observations_path
        else baseline_dir / "edge_observations.parquet"
    )
    baseline_registry_path = (
        Path(args.baseline_registry_path)
        if args.baseline_registry_path
        else baseline_dir / "edge_registry.parquet"
    )
    candidate_dir = history_candidates_root / args.run_id
    candidate_observations_path = candidate_dir / "edge_observations.parquet"
    candidate_registry_path = candidate_dir / "edge_registry.parquet"
    out_path = (
        Path(args.out_path)
        if args.out_path
        else DATA_ROOT / "runs" / args.run_id / "research" / "edge_registry.parquet"
    )
    ensure_dir(history_baselines_root)
    ensure_dir(history_candidates_root)
    ensure_dir(candidate_dir)
    ensure_dir(out_path.parent)

    manifest = start_manifest("update_edge_registry", args.run_id, vars(args), [], [])
    try:
        run_manifest_hashes = load_run_manifest_hashes(DATA_ROOT, args.run_id)
        ontology_spec_hash = str(run_manifest_hashes.get("ontology_spec_hash", "") or "").strip()

        promoted_df = _load_table(promoted_path)
        audit_df = _load_table(audit_path)
        capital_footprint_df = _load_capital_footprint(capital_footprint_path)
        observations_new = _build_observations(
            run_id=args.run_id,
            promoted_df=promoted_df,
            audit_df=audit_df,
            capital_footprint_df=capital_footprint_df,
            ontology_spec_hash=ontology_spec_hash,
        )

        history_existing = _load_history(baseline_observations_path)
        no_promotion_observations = False
        if observations_new.empty:
            no_promotion_observations = True
            history_all = (
                history_existing.copy()
                if not history_existing.empty
                else _empty_observations_frame()
            )
            registry_df = (
                _aggregate_registry(history_all)
                if not history_all.empty
                else _empty_registry_frame()
            )
            if registry_df.empty:
                registry_df = _empty_registry_frame()
        else:
            if ontology_spec_hash and "ontology_spec_hash" in observations_new.columns:
                mismatch = observations_new[
                    observations_new["ontology_spec_hash"].astype(str).str.strip()
                    != ontology_spec_hash
                ]
                if not mismatch.empty:
                    raise ValueError(
                        "Ontology hash mismatch inside promotion artifacts for edge registry update."
                    )

            if history_existing.empty:
                history_all = observations_new
            else:
                history_all = pd.concat([history_existing, observations_new], ignore_index=True)
                history_all = history_all.drop_duplicates(
                    subset=["run_id", "candidate_id", "event_type"],
                    keep="last",
                )

            registry_df = _aggregate_registry(history_all)
            if registry_df.empty:
                raise ValueError("Edge registry aggregation produced no rows.")

        write_parquet(history_all, candidate_observations_path)
        write_parquet(registry_df, candidate_registry_path)
        write_parquet(registry_df, out_path)

        promoted_baseline_id = ""
        promoted_baseline_observations_path = ""
        promoted_baseline_registry_path = ""
        if bool(int(args.promote_baseline)):
            promoted_baseline_id = (
                str(args.promote_baseline_id).strip()
                if str(args.promote_baseline_id or "").strip()
                else f"{baseline_id}__{args.run_id}"
            )
            promoted_baseline_dir = history_baselines_root / promoted_baseline_id
            ensure_dir(promoted_baseline_dir)
            promoted_obs_path = promoted_baseline_dir / "edge_observations.parquet"
            promoted_reg_path = promoted_baseline_dir / "edge_registry.parquet"
            write_parquet(history_all, promoted_obs_path)
            write_parquet(registry_df, promoted_reg_path)
            promoted_baseline_observations_path = str(promoted_obs_path)
            promoted_baseline_registry_path = str(promoted_reg_path)

        summary = {
            "run_id": args.run_id,
            "baseline_id": baseline_id,
            "new_observations": int(len(observations_new)),
            "no_promotion_observations": bool(no_promotion_observations),
            "history_observations_total": int(len(history_all)),
            "edge_count_total": int(len(registry_df)),
            "paths": {
                "promoted_candidates_path": str(promoted_path),
                "promotion_audit_path": str(audit_path),
                "promotion_capital_footprint_path": str(capital_footprint_path),
                "baseline_observations_path": str(baseline_observations_path),
                "baseline_registry_path": str(baseline_registry_path),
                "candidate_observations_path": str(candidate_observations_path),
                "candidate_registry_path": str(candidate_registry_path),
                "run_snapshot_path": str(out_path),
                "promoted_baseline_observations_path": promoted_baseline_observations_path,
                "promoted_baseline_registry_path": promoted_baseline_registry_path,
            },
            "promoted_baseline_id": promoted_baseline_id,
        }
        (out_path.parent / "edge_registry_summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True),
            encoding="utf-8",
        )

        finalize_manifest(
            manifest,
            "success",
            stats={
                "new_observations": int(len(observations_new)),
                "no_promotion_observations": bool(no_promotion_observations),
                "history_observations_total": int(len(history_all)),
                "edge_count_total": int(len(registry_df)),
                "capital_footprint_rows": int(len(capital_footprint_df)),
                "baseline_id": baseline_id,
                "baseline_observations_path": str(baseline_observations_path),
                "baseline_registry_path": str(baseline_registry_path),
                "candidate_observations_path": str(candidate_observations_path),
                "candidate_registry_path": str(candidate_registry_path),
                "promoted_baseline_id": promoted_baseline_id,
                "run_snapshot_path": (
                    str(out_path.relative_to(PROJECT_ROOT.parent))
                    if str(out_path).startswith(str(PROJECT_ROOT.parent))
                    else str(out_path)
                ),
            },
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
