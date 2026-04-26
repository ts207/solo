from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.artifacts import (
    checklist_path,
    kpi_scorecard_path,
    promotion_summary_path,
    release_signoff_path,
    run_manifest_path,
)
from project.core.coercion import as_bool, safe_float
from project.core.config import get_data_root
from project.io.utils import read_table_auto
from project.research.recommendations.checklist import build_checklist_payload
from project.specs.manifest import finalize_manifest, start_manifest

CHECKLIST_GATE_PROFILES: dict[str, dict[str, int]] = {
    "discovery": {
        "min_edge_candidates": 1,
        "min_promoted_candidates": 1,
        "min_bridge_tradable_candidates": 1,
        "require_expectancy_exists": 1,
        "min_robust_survivors": 1,
    },
    "promotion": {
        "min_edge_candidates": 1,
        "min_promoted_candidates": 1,
        "min_bridge_tradable_candidates": 1,
        "require_expectancy_exists": 1,
        "min_robust_survivors": 1,
    },
}


def _parse_args() -> argparse.Namespace:
    DATA_ROOT = get_data_root()
    parser = argparse.ArgumentParser(description="Generate checklist.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument(
        "--gate_profile",
        choices=["discovery", "promotion", "custom", "synthetic"],
        default="discovery",
    )
    parser.add_argument("--reports_root", default=str(DATA_ROOT / "reports"))
    parser.add_argument("--runs_root", default=str(DATA_ROOT / "runs"))
    parser.add_argument("--out_dir", default="")
    parser.add_argument("--retail_profile", default="capital_constrained")
    parser.add_argument("--min_edge_candidates", type=int, default=1)
    parser.add_argument("--min_promoted_candidates", type=int, default=1)
    parser.add_argument("--min_bridge_tradable_candidates", type=int, default=1)
    parser.add_argument("--min_bridge_tradable_promoted_candidates", type=int, default=1)
    parser.add_argument("--min_expectancy_evidence", type=int, default=1)
    parser.add_argument("--min_robust_survivors", type=int, default=1)
    parser.add_argument("--max_capital_slot_pressure_over_limit_count", type=int, default=0)
    parser.add_argument("--max_capital_leverage_over_budget_count", type=int, default=0)
    parser.add_argument("--require_expectancy_exists", type=int, default=1)
    parser.add_argument("--require_stability_pass", type=int, default=1)
    parser.add_argument("--require_capacity_pass", type=int, default=1)
    return parser.parse_args()


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _metric_value(payload: dict[str, Any], name: str, default: float = 0.0) -> float:
    value = payload.get("metrics", {}).get(name, {}).get("value")
    if value is None:
        return float(default)
    return safe_float(value, default, context=f"kpi_metric:{name}")


def _read_table(parquet_path: Path, csv_path: Path) -> pd.DataFrame:
    frame = read_table_auto(parquet_path)
    if isinstance(frame, pd.DataFrame) and (not frame.empty or parquet_path.exists()):
        return frame
    frame = read_table_auto(csv_path)
    if isinstance(frame, pd.DataFrame):
        return frame
    return pd.DataFrame()


def _existing_manifest_inputs(paths: list[Path]) -> list[dict[str, str]]:
    return [{"path": str(path)} for path in paths if path.exists()]


def _apply_checklist_gate_profile(args: argparse.Namespace) -> argparse.Namespace:
    profile = str(getattr(args, "gate_profile", "custom")).strip().lower()
    if profile in {"discovery", "synthetic"}:
        args.require_stability_pass = 0
        args.require_capacity_pass = 0
    return args


def _edge_candidate_metrics(
    *,
    edge_parquet_path: Path,
    edge_csv_path: Path,
    edge_json_path: Path,
    promoted_candidates_parquet_path: Path,
    promoted_candidates_csv_path: Path,
    promotion_audit_parquet_path: Path,
    promotion_audit_csv_path: Path,
    promotion_summary_path: Path,
) -> dict[str, Any]:
    edge_df = _read_table(edge_parquet_path, edge_csv_path)
    promoted_df = _read_table(promoted_candidates_parquet_path, promoted_candidates_csv_path)
    promo_df = _read_table(promotion_audit_parquet_path, promotion_audit_csv_path)
    promoted_count = 0
    bridge_tradable_promoted = 0
    if not promoted_df.empty:
        promoted_count = len(promoted_df)
        if "gate_bridge_tradable" in promoted_df.columns:
            bridge_tradable_promoted = int(
                promoted_df.get("gate_bridge_tradable", pd.Series(False, index=promoted_df.index)).map(
                    as_bool
                ).sum()
            )
        else:
            bridge_tradable_promoted = promoted_count
    elif not promo_df.empty:
        decision = (
            promo_df.get("promotion_decision", pd.Series("", index=promo_df.index))
            .astype(str)
            .str.lower()
        )
        promoted = decision.eq("promoted")
        bridge = promo_df.get("gate_bridge_tradable", pd.Series(False, index=promo_df.index)).map(
            as_bool
        )
        promoted_count = int(promoted.sum())
        bridge_tradable_promoted = int((bridge & promoted).sum())
    if not edge_df.empty:
        status = edge_df.get("status", pd.Series("", index=edge_df.index)).astype(str).str.upper()
        promoted = status.eq("PROMOTED")
        bridge = edge_df.get("gate_bridge_tradable", pd.Series(False, index=edge_df.index)).map(
            as_bool
        )
        return {
            "source": "edge_candidates_parquet"
            if edge_parquet_path.exists()
            else "edge_candidates_csv",
            "rows": len(edge_df),
            "promoted": int(promoted_count or promoted.sum()),
            "bridge_tradable": int(bridge.sum()),
            "bridge_tradable_promoted": int(
                bridge_tradable_promoted or int((bridge & promoted).sum())
            ),
        }

    if not promo_df.empty:
        bridge = promo_df.get("gate_bridge_tradable", pd.Series(False, index=promo_df.index)).map(
            as_bool
        )
        return {
            "source": "promotion_audit_parquet"
            if promotion_audit_parquet_path.exists()
            else "promotion_audit_csv",
            "rows": len(promo_df),
            "promoted": int(promoted_count),
            "bridge_tradable": int(bridge.sum()),
            "bridge_tradable_promoted": int(bridge_tradable_promoted),
        }

    return {
        "source": "missing",
        "rows": 0,
        "promoted": 0,
        "bridge_tradable": 0,
        "bridge_tradable_promoted": 0,
    }


def _hydrate_kpi_payload_with_promotion_fallback(
    *,
    kpi_payload: dict[str, Any],
    promotion_audit_parquet_path: Path,
    promotion_audit_csv_path: Path,
) -> dict[str, Any]:
    if kpi_payload.get("metrics"):
        return dict(kpi_payload)
    promo_df = _read_table(promotion_audit_parquet_path, promotion_audit_csv_path)
    if promo_df.empty:
        return dict(kpi_payload)
    out = dict(kpi_payload)
    out["hydrated_with_promotion_fallback"] = True
    out["metrics"] = {
        "trade_count": {
            "value": float(
                pd.to_numeric(promo_df.get("n_events"), errors="coerce").fillna(0.0).sum()
            )
        },
        "net_expectancy_bps": {
            "value": float(
                pd.to_numeric(
                    promo_df.get("bridge_validation_stressed_after_cost_bps"), errors="coerce"
                )
                .dropna()
                .mean()
            )
        },
        "oos_sign_consistency": {
            "value": float(
                pd.to_numeric(promo_df.get("sign_consistency"), errors="coerce").dropna().mean()
            )
        },
        "turnover_proxy_mean": {
            "value": float(
                pd.to_numeric(promo_df.get("turnover_proxy_mean"), errors="coerce").dropna().mean()
            )
        },
        "max_drawdown_pct": {
            "value": float(
                pd.to_numeric(promo_df.get("naive_max_drawdown"), errors="coerce").dropna().min()
            )
        },
    }
    return out


def _build_payload(
    *,
    run_id: str,
    args,
    edge_metrics: dict[str, Any],
    expectancy_payload: dict[str, Any],
    robustness_payload: dict[str, Any],
    paths: dict[str, str],
    capital_footprint_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config = {
        "min_edge_candidates": int(getattr(args, "min_edge_candidates", 1)),
        "min_promoted_candidates": int(getattr(args, "min_promoted_candidates", 1)),
        "min_bridge_tradable_candidates": int(getattr(args, "min_bridge_tradable_candidates", 1)),
        "require_expectancy_exists": bool(getattr(args, "require_expectancy_exists", 1)),
        "min_robust_survivors": int(getattr(args, "min_robust_survivors", 1)),
    }
    payload = build_checklist_payload(
        run_id=run_id,
        edge_metrics=edge_metrics,
        expectancy_payload=expectancy_payload,
        robustness_payload=robustness_payload,
        capital_footprint_payload=capital_footprint_payload,
        config=config,
        paths=paths,
    )

    survivor_definition = str(robustness_payload.get("survivor_definition", "unknown")).strip()
    survivors = robustness_payload.get("survivors", [])
    survivor_count = len(survivors) if isinstance(survivors, list) else 0
    for gate in payload["gates"]:
        if gate["name"] == "robust_survivor_count":
            gate["note"] = f"definition={survivor_definition}"
    payload["metrics"]["robust_survivor_count"] = survivor_count
    payload["metrics"]["bridge_tradable_promoted"] = int(
        edge_metrics.get("bridge_tradable_promoted", 0)
    )

    capital_payload = dict(capital_footprint_payload or {})
    slot_pressure = int(capital_payload.get("slot_pressure_over_limit_count", 0))
    leverage_over = int(capital_payload.get("leverage_over_budget_count", 0))
    max_slot = int(getattr(args, "max_capital_slot_pressure_over_limit_count", 0))
    max_lev = int(getattr(args, "max_capital_leverage_over_budget_count", 0))
    payload["gates"].append(
        {
            "name": "capital_slot_pressure_over_limit_count",
            "passed": bool(slot_pressure <= max_slot),
            "observed": slot_pressure,
            "threshold": max_slot,
            "note": "",
        }
    )
    payload["gates"].append(
        {
            "name": "capital_leverage_over_budget_count",
            "passed": bool(leverage_over <= max_lev),
            "observed": leverage_over,
            "threshold": max_lev,
            "note": "",
        }
    )
    payload["decision"] = (
        "PROMOTE" if all(g["passed"] for g in payload["gates"]) else "KEEP_RESEARCH"
    )
    return payload


def _build_release_signoff(
    *,
    run_id: str,
    checklist_payload: dict[str, Any],
    run_manifest_payload: dict[str, Any],
    kpi_payload: dict[str, Any],
) -> dict[str, Any]:
    hard_gates = dict(run_manifest_payload.get("objective_hard_gates", {}))
    retail_cfg = dict(run_manifest_payload.get("retail_profile_config", {}))
    overrides = list(run_manifest_payload.get("non_production_overrides", []) or [])

    gates = [
        {
            "name": "checklist_promote_decision",
            "passed": str(checklist_payload.get("decision", "")).strip().upper() == "PROMOTE",
            "observed": checklist_payload.get("decision", ""),
            "threshold": "PROMOTE",
            "note": "",
        },
        {
            "name": "kpi_trade_count",
            "passed": _metric_value(kpi_payload, "trade_count")
            >= safe_float(hard_gates.get("min_trade_count"), 0.0),
            "observed": _metric_value(kpi_payload, "trade_count"),
            "threshold": safe_float(hard_gates.get("min_trade_count"), 0.0),
            "note": "",
        },
        {
            "name": "kpi_oos_sign_consistency",
            "passed": _metric_value(kpi_payload, "oos_sign_consistency")
            >= safe_float(hard_gates.get("min_oos_sign_consistency"), 0.0),
            "observed": _metric_value(kpi_payload, "oos_sign_consistency"),
            "threshold": safe_float(hard_gates.get("min_oos_sign_consistency"), 0.0),
            "note": "",
        },
        {
            "name": "kpi_max_drawdown_pct",
            "passed": abs(_metric_value(kpi_payload, "max_drawdown_pct"))
            <= safe_float(hard_gates.get("max_drawdown_pct"), 1.0),
            "observed": _metric_value(kpi_payload, "max_drawdown_pct"),
            "threshold": safe_float(hard_gates.get("max_drawdown_pct"), 1.0),
            "note": "",
        },
        {
            "name": "retail_net_expectancy_bps",
            "passed": _metric_value(kpi_payload, "net_expectancy_bps")
            >= safe_float(retail_cfg.get("min_net_expectancy_bps"), 0.0),
            "observed": _metric_value(kpi_payload, "net_expectancy_bps"),
            "threshold": safe_float(retail_cfg.get("min_net_expectancy_bps"), 0.0),
            "note": "",
        },
        {
            "name": "retail_turnover_proxy_mean",
            "passed": _metric_value(kpi_payload, "turnover_proxy_mean")
            <= safe_float(retail_cfg.get("max_daily_turnover_multiple"), np.inf),
            "observed": _metric_value(kpi_payload, "turnover_proxy_mean"),
            "threshold": safe_float(retail_cfg.get("max_daily_turnover_multiple"), np.inf),
            "note": "",
        },
        {
            "name": "override_audit_clean",
            "passed": len(overrides) == 0,
            "observed": len(overrides),
            "threshold": 0,
            "note": "",
        },
    ]
    failure_reasons = [f"{g['name']} failed" for g in gates if not g["passed"]]
    return {
        "run_id": run_id,
        "decision": "APPROVE_RELEASE" if not failure_reasons else "BLOCK_RELEASE",
        "failure_reasons": failure_reasons,
        "gates": gates,
        "override_audit": {
            "non_production_override_count": len(overrides),
            "non_production_overrides": overrides,
        },
    }


def main() -> int:
    args = _apply_checklist_gate_profile(_parse_args())
    reports_root = Path(args.reports_root)
    runs_root = Path(args.runs_root)

    expectancy_path = reports_root / "expectancy" / args.run_id / "conditional_expectancy.json"
    robustness_path = (
        reports_root / "expectancy" / args.run_id / "conditional_expectancy_robustness.json"
    )
    manifest_path = run_manifest_path(args.run_id, runs_root.parent)
    kpi_path = kpi_scorecard_path(args.run_id, runs_root.parent)

    edge_dir = reports_root / "edge_candidates" / args.run_id
    promo_dir = reports_root / "promotions" / args.run_id
    out_dir = Path(args.out_dir) if args.out_dir else runs_root / args.run_id / "research_checklist"
    checklist_out_path = out_dir / "checklist.json"
    release_signoff_out_path = out_dir / "release_signoff.json"
    inputs = _existing_manifest_inputs(
        [
            expectancy_path,
            robustness_path,
            manifest_path,
            kpi_path,
            edge_dir / "edge_candidates_normalized.parquet",
            promo_dir / "promoted_candidates.parquet",
            promo_dir / "promotion_statistical_audit.parquet",
        ]
    )
    outputs = [
        {"path": str(checklist_out_path if args.out_dir else checklist_path(args.run_id, runs_root.parent))},
        {
            "path": str(
                release_signoff_out_path
                if args.out_dir
                else release_signoff_path(args.run_id, runs_root.parent)
            )
        },
    ]
    manifest = start_manifest(
        "generate_recommendations_checklist", args.run_id, vars(args), inputs, outputs
    )
    try:
        edge_metrics = _edge_candidate_metrics(
            edge_parquet_path=edge_dir / "edge_candidates_normalized.parquet",
            edge_csv_path=edge_dir / "edge_candidates_normalized.csv",
            edge_json_path=edge_dir / "edge_candidates_normalized.json",
            promoted_candidates_parquet_path=promo_dir / "promoted_candidates.parquet",
            promoted_candidates_csv_path=promo_dir / "promoted_candidates.csv",
            promotion_audit_parquet_path=promo_dir / "promotion_statistical_audit.parquet",
            promotion_audit_csv_path=promo_dir / "promotion_statistical_audit.csv",
            promotion_summary_path=promotion_summary_path(args.run_id, reports_root.parent),
        )
        payload = _build_payload(
            run_id=args.run_id,
            args=args,
            edge_metrics=edge_metrics,
            expectancy_payload=_load_json(expectancy_path),
            robustness_payload=_load_json(robustness_path),
            capital_footprint_payload={},
            paths={"expectancy": str(expectancy_path), "robustness": str(robustness_path)},
        )

        kpi_payload = _hydrate_kpi_payload_with_promotion_fallback(
            kpi_payload=_load_json(kpi_path),
            promotion_audit_parquet_path=promo_dir / "promotion_statistical_audit.parquet",
            promotion_audit_csv_path=promo_dir / "promotion_statistical_audit.csv",
        )
        payload["release_signoff"] = _build_release_signoff(
            run_id=args.run_id,
            checklist_payload=payload,
            run_manifest_payload=_load_json(manifest_path),
            kpi_payload=kpi_payload,
        )

        out_dir.mkdir(parents=True, exist_ok=True)
        checklist_out_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
        )
        release_signoff_out_path.write_text(
            json.dumps(payload["release_signoff"], indent=2, sort_keys=True), encoding="utf-8"
        )
        print(json.dumps({"decision": payload["decision"], "out_dir": str(out_dir)}, indent=2))
        finalize_manifest(
            manifest,
            "success" if payload["decision"] == "PROMOTE" else "warning",
            stats={
                "decision": payload["decision"],
                "gate_count": len(payload.get("gates", [])),
                "failed_gate_count": int(sum(not bool(g.get("passed")) for g in payload["gates"])),
                "promoted_edge_candidates": int(
                    payload.get("metrics", {}).get("edge_candidate_promoted", 0)
                ),
                "bridge_tradable_candidates": int(
                    payload.get("metrics", {}).get("bridge_tradable_candidates", 0)
                ),
                "robust_survivor_count": int(
                    payload.get("metrics", {}).get("robust_survivor_count", 0)
                ),
            },
        )
        return 0
    except Exception as exc:
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
