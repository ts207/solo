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
from project.io.utils import ensure_dir, read_parquet, read_table_auto, write_parquet
from project.research.candidates.builder import (
    build_compiled_blueprint_strategy_candidate,
    build_edge_strategy_candidate,
    build_promoted_strategy_candidate,
)
from project.research.candidates.filtering import (
    checklist_decision,
    load_candidate_detail,
    load_promoted_blueprints,
)
from project.research.candidates.ranking import (
    behavior_equivalence_key,
    candidate_rank_key,
)
from project.research.candidates.shaping import sanitize_id
from project.specs.loader import load_retail_profile
from project.specs.manifest import finalize_manifest, start_manifest

_build_edge_strategy_candidate = build_edge_strategy_candidate


def _synthesize_fractional_allocation_policy(
    profile: Dict[str, Any], retail_profile_cfg: Dict[str, Any]
) -> Dict[str, Any]:
    turnover = float(profile.get("turnover_proxy_mean", 0.0))
    cost = float(profile.get("effective_cost_bps", 0.0))
    net_exp = float(profile.get("net_expectancy_bps", 0.0))

    max_turnover = float(retail_profile_cfg.get("max_daily_turnover_multiple", 1.0))
    max_cost = float(retail_profile_cfg.get("max_fee_plus_slippage_bps", 10.0))
    min_exp = float(retail_profile_cfg.get("min_net_expectancy_bps", 2.0))

    scale = 1.0
    if turnover > max_turnover and max_turnover > 0:
        scale = min(scale, max_turnover / turnover)
    if cost > max_cost and max_cost > 0:
        scale = min(scale, max_cost / cost)
    if net_exp < min_exp and net_exp > 0:
        scale = min(scale, max(0.05, net_exp / min_exp))

    return {
        "mode": "fractional_top_quantile",
        "signal_take_rate": float(np.clip(scale, 0.05, 1.0)),
        "max_participation_rate": 0.25,
        "allocation_viable": bool(scale >= 0.05),
        "projected_turnover_multiple": float(turnover * scale),
    }


def _read_optional_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    frame = read_table_auto(path)
    return frame if isinstance(frame, pd.DataFrame) else pd.DataFrame()


def _load_edge_candidates_df(*, data_root: Path, run_id: str) -> pd.DataFrame:
    base = data_root / "reports" / "edge_candidates" / run_id
    for path in (
        base / "edge_candidates_normalized.parquet",
        base / "edge_candidates_normalized.csv",
    ):
        df = _read_optional_table(path)
        if not df.empty:
            return df
    return pd.DataFrame()


def _load_compiled_blueprints(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _load_promoted_candidate_metrics(
    *, data_root: Path, run_id: str
) -> Dict[tuple[str, str], Dict[str, Any]]:
    base = data_root / "reports" / "promotions" / run_id
    df = pd.DataFrame()
    for path in (
        base / "promoted_candidates.parquet",
        base / "promoted_candidates.csv",
        base / "promotion_audit.parquet",
    ):
        df = _read_optional_table(path)
        if not df.empty:
            break
    if df.empty:
        return {}
    out: Dict[tuple[str, str], Dict[str, Any]] = {}
    for row in df.to_dict("records"):
        candidate_id = str(row.get("candidate_id", "")).strip()
        event_type = str(row.get("event_type", row.get("event", ""))).strip()
        if candidate_id and event_type:
            out[(candidate_id, event_type)] = row
    return out


def _fallback_edge_detail(row: Dict[str, Any]) -> Dict[str, Any]:
    action = str(row.get("action", "")).strip()
    if not action:
        direction = pd.to_numeric(row.get("direction", row.get("direction_sign")), errors="coerce")
        if pd.notna(direction) and float(direction) > 0.0:
            action = "enter_long_market"
        elif pd.notna(direction) and float(direction) < 0.0:
            action = "enter_short_market"
        else:
            action = "no_action"
    condition = str(row.get("condition", "")).strip() or "all"
    return {"condition": condition, "action": action}


def _net_expectancy_bps_from_row(row: Dict[str, Any]) -> float:
    for key in (
        "net_expectancy_bps",
        "bridge_validation_after_cost_bps",
        "after_cost_expectancy_per_trade",
        "expectancy_after_multiplicity",
        "expectancy_per_trade",
    ):
        value = pd.to_numeric(row.get(key), errors="coerce")
        if pd.notna(value):
            result = float(value)
            if key.endswith("_per_trade") or key == "expectancy_after_multiplicity":
                result *= 1e4
            return result
    return 0.0


def _apply_fractional_allocation(
    *,
    row: Dict[str, Any],
    enabled: bool,
    retail_profile_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    normalized = dict(row)
    if not enabled:
        normalized["allocation_policy_json"] = json.dumps(
            {
                "mode": "full",
                "signal_take_rate": 1.0,
                "max_participation_rate": 1.0,
                "allocation_viable": True,
            },
            sort_keys=True,
        )
        normalized["fractional_allocation_applied"] = False
        return normalized

    raw_policy = str(normalized.get("allocation_policy_json", "")).strip()
    if raw_policy:
        existing_flag = normalized.get("fractional_allocation_applied")
        if (
            existing_flag is None
            or (isinstance(existing_flag, float) and not np.isfinite(existing_flag))
            or str(existing_flag).strip() == ""
        ):
            normalized["fractional_allocation_applied"] = (
                "fractional_top_quantile" in raw_policy.lower()
            )
        return normalized

    effective_cost_bps = 0.0
    for key in (
        "effective_cost_bps",
        "avg_dynamic_cost_bps",
        "bridge_effective_cost_bps_per_trade",
    ):
        value = pd.to_numeric(normalized.get(key), errors="coerce")
        if pd.notna(value):
            effective_cost_bps = float(value)
            break

    turnover_value = pd.to_numeric(normalized.get("turnover_proxy_mean"), errors="coerce")
    policy = _synthesize_fractional_allocation_policy(
        {
            "turnover_proxy_mean": float(turnover_value) if pd.notna(turnover_value) else 0.0,
            "effective_cost_bps": effective_cost_bps,
            "net_expectancy_bps": _net_expectancy_bps_from_row(normalized),
        },
        retail_profile_cfg=retail_profile_cfg,
    )
    normalized["allocation_policy_json"] = json.dumps(policy, sort_keys=True)
    normalized["fractional_allocation_applied"] = True
    return normalized


def _build_alpha_bundle_candidates(
    *, run_id: str, data_root: Path, symbols: List[str]
) -> List[Dict[str, Any]]:
    scores_path = (
        data_root / "feature_store" / "alpha_bundle" / run_id / "alpha_bundle_scores.parquet"
    )
    if not scores_path.exists():
        return []
    scores_df = read_parquet(scores_path)
    if scores_df.empty or "symbol" not in scores_df.columns or "score" not in scores_df.columns:
        return []

    filtered = scores_df[scores_df["symbol"].astype(str).str.upper().isin(symbols)].copy()
    if filtered.empty:
        return []

    rows: List[Dict[str, Any]] = []
    grouped = filtered.groupby(filtered["symbol"].astype(str).str.upper(), sort=True)
    for symbol, frame in grouped:
        scores = pd.to_numeric(frame["score"], errors="coerce").fillna(0.0)
        mean_score = float(scores.mean())
        mean_abs_score = float(scores.abs().mean())
        condition = f"symbol_{symbol}" if len(symbols) > 1 else "all"
        rows.append(
            {
                "strategy_candidate_id": sanitize_id(f"alpha_bundle_{run_id}_{symbol}"),
                "candidate_id": f"alpha_bundle_{symbol}",
                "source_type": "alpha_bundle",
                "execution_family": "cross_sectional_alpha",
                "base_strategy": "dsl_interpreter_v1",
                "event": "alpha_bundle",
                "condition": condition,
                "action": "no_action",
                "executable_condition": True,
                "executable_action": True,
                "status": "CANDIDATE",
                "n_events": int(len(frame)),
                "edge_score": mean_abs_score,
                "expectancy_per_trade": mean_score,
                "expectancy_after_multiplicity": mean_score,
                "stability_proxy": mean_abs_score,
                "robustness_score": mean_abs_score,
                "quality_score": mean_abs_score,
                "selection_score": mean_abs_score,
                "symbols": symbols,
                "candidate_symbol": symbol,
                "run_symbols": symbols,
                "rollout_eligible": False,
                "deployment_type": "single_symbol",
                "deployment_symbols": [symbol],
                "allocation_policy": {
                    "mode": "full",
                    "signal_take_rate": 1.0,
                    "max_participation_rate": 1.0,
                    "allocation_viable": True,
                },
                "fractional_allocation_applied": False,
                "strategy_instances": [
                    {
                        "strategy_id": f"dsl_interpreter_v1_{symbol}",
                        "base_strategy": "dsl_interpreter_v1",
                        "symbol": symbol,
                        "strategy_params": {
                            "alpha_bundle": {
                                "score_mean": mean_score,
                                "score_abs_mean": mean_abs_score,
                            },
                            "condition": condition,
                            "action": "no_action",
                        },
                    }
                ],
                "risk_controls": {
                    "entry_delay_bars": 0,
                    "size_scale": 1.0,
                    "block_entries": False,
                    "reentry_mode": "immediate",
                },
                "notes": [f"Derived from alpha bundle scores at {scores_path}."],
            }
        )
    return rows


def _limit_rows_per_event(rows: List[Dict[str, Any]], *, limit: int) -> List[Dict[str, Any]]:
    if limit <= 0:
        return list(rows)
    out: List[Dict[str, Any]] = []
    event_counts: Dict[str, int] = {}
    for row in rows:
        event = str(row.get("event", "unknown")).lower()
        if event != "alpha_bundle" and event_counts.get(event, 0) >= limit:
            continue
        out.append(row)
        event_counts[event] = event_counts.get(event, 0) + 1
    return out


def _render_summary_md(run_id: str, candidates: List[Dict[str, Any]]) -> str:
    lines = [
        "# Strategy Candidate Selection",
        "",
        f"- Run ID: `{run_id}`",
        f"- Candidate count: `{len(candidates)}`",
        "",
        "## Ranked candidates",
        "",
        "| rank | strategy_candidate_id | source_type | event | action | selection_score | n_events |",
        "|---:|---|---|---|---|---:|---:|",
    ]
    for idx, item in enumerate(candidates, start=1):
        lines.append(
            f"| {idx} | `{item['strategy_candidate_id']}` | `{item['source_type']}` | "
            f"`{item['event']}` | `{item['action']}` | {item['selection_score']:.6f} | {int(item['n_events'])} |"
        )
    return "\n".join(lines) + "\n"


def _tabularize_rows(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    flattened: List[Dict[str, Any]] = []
    for row in rows:
        flat: Dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, (dict, list)):
                flat[key] = json.dumps(value, sort_keys=True)
            else:
                flat[key] = value
        flattened.append(flat)
    return pd.DataFrame(flattened)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build strategy candidates.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--top_k_per_event", type=int, default=2)
    parser.add_argument("--max_candidates_per_event", type=int, default=2)
    parser.add_argument("--max_candidates", type=int, default=20)
    parser.add_argument("--min_edge_score", type=float, default=0.0)
    parser.add_argument("--include_alpha_bundle", type=int, default=1)
    parser.add_argument("--ignore_checklist", type=int, default=0)
    parser.add_argument("--allow_non_promoted", type=int, default=0)
    parser.add_argument("--allow_missing_candidate_detail", type=int, default=0)
    parser.add_argument("--enable_fractional_allocation", type=int, default=1)
    parser.add_argument("--retail_profile", default="capital_constrained")
    parser.add_argument("--blueprints_file", default=None)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    data_root = get_data_root()
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        print("--symbols must include at least one symbol", file=sys.stderr)
        return 1

    out_dir = (
        Path(args.out_dir)
        if args.out_dir
        else data_root / "reports" / "strategy_builder" / args.run_id
    )
    ensure_dir(out_dir)

    manifest = start_manifest("build_strategy_candidates", args.run_id, vars(args), [], [])

    try:
        blueprints_file = (
            Path(args.blueprints_file)
            if args.blueprints_file
            else data_root / "reports" / "strategy_blueprints" / args.run_id / "blueprints.jsonl"
        )
        if not int(args.ignore_checklist):
            decision = checklist_decision(run_id=args.run_id, data_root=data_root)
            if decision != "PROMOTE":
                empty_rows: List[Dict[str, Any]] = []
                (out_dir / "strategy_candidates.json").write_text("[]", encoding="utf-8")
                _tabularize_rows(empty_rows).to_csv(
                    out_dir / "strategy_candidates.csv", index=False
                )
                write_parquet(_tabularize_rows(empty_rows), out_dir / "strategy_candidates.parquet")
                finalize_manifest(
                    manifest, "success", stats={"strategy_count": 0, "checklist_decision": decision}
                )
                return 0

        compiled_blueprints = _load_compiled_blueprints(blueprints_file)
        promoted_payloads, _ = load_promoted_blueprints(run_id=args.run_id, data_root=data_root)
        promoted_metrics = _load_promoted_candidate_metrics(data_root=data_root, run_id=args.run_id)

        retail_profile_cfg = load_retail_profile(
            profile_name=str(args.retail_profile).strip() or "capital_constrained",
            explicit_path=None,
            required=False,
            project_root=PROJECT_ROOT,
        )
        if not isinstance(retail_profile_cfg, dict):
            retail_profile_cfg = {}

        edge_df = _load_edge_candidates_df(data_root=data_root, run_id=args.run_id)
        if not edge_df.empty:
            edge_df["edge_score"] = pd.to_numeric(edge_df["edge_score"], errors="coerce").fillna(
                0.0
            )
            edge_df = edge_df[edge_df["edge_score"] >= float(args.min_edge_score)].copy()
            if not int(args.allow_non_promoted):
                edge_df = edge_df[
                    edge_df["status"]
                    .astype(str)
                    .str.upper()
                    .isin(["PROMOTED", "PROMOTED_RESEARCH"])
                ].copy()

        strategy_rows: List[Dict[str, Any]] = []
        if compiled_blueprints:
            for blueprint in compiled_blueprints:
                event_type = str(blueprint.get("event_type", blueprint.get("event", ""))).strip()
                candidate_id = str(blueprint.get("candidate_id", "")).strip()
                metrics = promoted_metrics.get((candidate_id, event_type), {})
                cand = build_compiled_blueprint_strategy_candidate(
                    blueprint=blueprint,
                    metrics=metrics,
                    symbols=symbols,
                )
                if cand:
                    strategy_rows.append(cand)
        else:
            for payload in promoted_payloads:
                cand = build_promoted_strategy_candidate(
                    blueprint=payload.get("blueprint", {}),
                    promotion=payload.get("promotion", {}),
                    symbols=symbols,
                )
                if cand:
                    strategy_rows.append(cand)

        if not edge_df.empty:
            for _, row in edge_df.iterrows():
                row_dict = row.to_dict()
                source_path = Path(str(row_dict.get("source_path", "")))
                detail = load_candidate_detail(
                    source_path=source_path, candidate_id=str(row_dict.get("candidate_id", ""))
                )
                if not detail and int(args.allow_missing_candidate_detail):
                    detail = _fallback_edge_detail(row_dict)
                if not detail:
                    continue
                row_dict = _apply_fractional_allocation(
                    row=row_dict,
                    enabled=bool(int(args.enable_fractional_allocation)),
                    retail_profile_cfg=retail_profile_cfg,
                )
                strategy_rows.append(
                    build_edge_strategy_candidate(row=row_dict, detail=detail, symbols=symbols)
                )

        if int(args.include_alpha_bundle):
            strategy_rows.extend(
                _build_alpha_bundle_candidates(
                    run_id=args.run_id,
                    data_root=data_root,
                    symbols=symbols,
                )
            )

        strategy_rows = sorted(strategy_rows, key=candidate_rank_key)

        deduped_rows: List[Dict[str, Any]] = []
        seen_keys = set()
        for row in strategy_rows:
            key = behavior_equivalence_key(row)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped_rows.append(row)

        deduped_rows = _limit_rows_per_event(deduped_rows, limit=int(args.top_k_per_event))
        final_rows = _limit_rows_per_event(deduped_rows, limit=int(args.max_candidates_per_event))[
            : int(args.max_candidates)
        ]

        for rank, row in enumerate(final_rows, start=1):
            row["rank"] = rank

        out_table = _tabularize_rows(final_rows)
        out_parquet = out_dir / "strategy_candidates.parquet"
        out_csv = out_dir / "strategy_candidates.csv"
        write_parquet(out_table, out_parquet)
        out_table.to_csv(out_csv, index=False)
        out_json = out_dir / "strategy_candidates.json"
        out_json.write_text(json.dumps(final_rows, indent=2), encoding="utf-8")
        out_md = out_dir / "selection_summary.md"
        out_md.write_text(_render_summary_md(args.run_id, final_rows), encoding="utf-8")

        finalize_manifest(manifest, "success", stats={"strategy_candidate_count": len(final_rows)})
        return 0
    except Exception as exc:
        logging.exception("Strategy candidate build failed")
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
