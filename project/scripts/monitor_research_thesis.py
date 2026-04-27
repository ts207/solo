"""
Monitor report for a research thesis.

Reads stable per-experiment artifacts and writes a dated JSON snapshot to:
  data/reports/monitor/<thesis_slug>/<timestamp>.json

Usage:
  PYTHONPATH=. python3 project/scripts/monitor_research_thesis.py
  PYTHONPATH=. python3 project/scripts/monitor_research_thesis.py --run_id stat_stretch_04 --data_root data
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Thesis identity
# ---------------------------------------------------------------------------
THESIS_SLUG = "oasrep_chop_long_48b"
THESIS_PROGRAM_ID = (
    "edge_cell_stat_stretch_04_overshoot_after_shock_repair_chop_ms_trend_state"
    "_chop_long_48b_mean_reversion_btcusdt_v1"
)
THESIS_CANDIDATE_ID = "BTCUSDT::cand_39664efa62c478d3"
THESIS_HYPOTHESIS_ID = "hyp_76f36aab418eceb80f5a"

THESIS_META = {
    "event": "OVERSHOOT_AFTER_SHOCK",
    "context": "chop",
    "direction": "long",
    "horizon_bars": 48,
    "template": "mean_reversion",
    "symbol_canonical": "BTCUSDT",
    "status": "research_promoted_monitor_only",
}

DEPLOYMENT_GATE = {
    "robustness": 0.70,
    "t_net": 2.0,
    "net_bps_positive": True,
}

ETH_PROGRAM_ID = "oasrep_chop_long_48b_eth_v1"


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def _load_eval_results(data_root: Path, run_id: str, program_id: str) -> pd.Series | None:
    path = data_root / "artifacts" / "experiments" / program_id / run_id / "evaluation_results.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    if df.empty:
        return None
    return df.iloc[0]


def _load_validation_bundle(data_root: Path, run_id: str) -> dict:
    path = data_root / "reports" / "validation" / run_id / "validation_bundle.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _load_promotion_trace(data_root: Path, run_id: str) -> pd.Series | None:
    path = data_root / "reports" / "promotions" / run_id / "research_decision_trace.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    if df.empty:
        return None
    match = df[df.index == THESIS_CANDIDATE_ID] if THESIS_CANDIDATE_ID in df.index else df
    return match.iloc[0] if not match.empty else df.iloc[0]


def _safe_float(v, default: float | None = None) -> float | None:
    try:
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return default


def _robustness_gate_progress(robustness: float | None) -> float | None:
    if robustness is None:
        return None
    return round(robustness / DEPLOYMENT_GATE["robustness"], 4)


# ---------------------------------------------------------------------------
# Core report builder
# ---------------------------------------------------------------------------

def build_report(*, run_id: str, data_root: Path) -> dict:
    now = datetime.now(timezone.utc)

    # 1. Canonical BTC evaluation
    eval_row = _load_eval_results(data_root, run_id, THESIS_PROGRAM_ID)

    n = int(eval_row["n"]) if eval_row is not None else None
    mean_bps = _safe_float(eval_row["mean_return_bps"]) if eval_row is not None else None
    net_bps = _safe_float(eval_row["mean_return_net_bps"]) if eval_row is not None else None
    hit_rate = _safe_float(eval_row["hit_rate"]) if eval_row is not None else None
    mae_bps = _safe_float(eval_row["mae_mean_bps"]) if eval_row is not None else None
    mfe_bps = _safe_float(eval_row["mfe_mean_bps"]) if eval_row is not None else None
    t_net = _safe_float(eval_row["t_stat_net"]) if eval_row is not None else None
    robustness = _safe_float(eval_row["robustness_score"]) if eval_row is not None else None
    cost_bps = _safe_float(eval_row["expected_cost_bps_per_trade"]) if eval_row is not None else None
    sharpe = _safe_float(eval_row["sharpe"]) if eval_row is not None else None
    stress_score = _safe_float(eval_row["stress_score"]) if eval_row is not None else None
    placebo_shift = _safe_float(eval_row["placebo_shift_effect"]) if eval_row is not None else None
    placebo_random = _safe_float(eval_row["placebo_random_entry_effect"]) if eval_row is not None else None

    # 2. Validation status
    vbundle = _load_validation_bundle(data_root, run_id)
    validated_candidates = vbundle.get("validated_candidates", [])
    validation_status = "not_found"
    if validated_candidates:
        rec = validated_candidates[0]
        validation_status = rec.get("decision", {}).get("status", "unknown")

    # 3. Promotion status
    promo_row = _load_promotion_trace(data_root, run_id)
    promotion_decision = None
    promotion_track = None
    if promo_row is not None:
        promotion_decision = str(promo_row.get("promotion_decision", "")) or None
        promotion_track = str(promo_row.get("promotion_track", "")) or None

    # 4. ETH cross-symbol
    eth_row = _load_eval_results(data_root, run_id, ETH_PROGRAM_ID)
    eth_summary = None
    if eth_row is not None:
        eth_summary = {
            "symbol": "ETHUSDT",
            "n": int(eth_row["n"]) if bool(pd.notna(eth_row["n"])) else None,
            "t_net": _safe_float(eth_row["t_stat_net"]),
            "robustness": _safe_float(eth_row["robustness_score"]),
            "net_bps": _safe_float(eth_row["mean_return_net_bps"]),
            "sign_consistent_with_btc": (
                (v := _safe_float(eth_row["mean_return_net_bps"])) is not None and v > 0
            ),
        }

    # 5. Gate assessment
    gate_progress = _robustness_gate_progress(robustness)
    gates_passed = {
        "t_net_ge_2": t_net is not None and t_net >= 2.0,
        "robustness_ge_0_70": robustness is not None and robustness >= 0.70,
        "net_bps_positive": net_bps is not None and net_bps > 0,
    }
    deployment_ready = all(gates_passed.values())

    return {
        "schema_version": "monitor_report_v1",
        "generated_at_utc": now.isoformat(),
        "thesis_slug": THESIS_SLUG,
        "thesis": THESIS_META,
        "source_run_id": run_id,
        "source_candidate_id": THESIS_CANDIDATE_ID,

        # Core forward metrics
        "events_seen": n,
        "paper_trades_seen": n,
        "mean_forward_bps": mean_bps,
        "net_forward_bps": net_bps,
        "hit_rate": hit_rate,
        "max_adverse_excursion_bps": mae_bps,
        "max_favorable_excursion_bps": mfe_bps,

        # Statistical strength (proxy for rolling metrics until true forward data exists)
        "current_forward_n": n,
        "rolling_t_net": t_net,
        "rolling_robustness_proxy": robustness,

        # Gate tracking
        "current_robustness": robustness,
        "gate_target_robustness": DEPLOYMENT_GATE["robustness"],
        "gate_progress_to_0_70": gate_progress,
        "gates_passed": gates_passed,
        "deployment_ready": deployment_ready,
        "deployment_blocker": (
            None if deployment_ready else
            "robustness < 0.70 (fold sparsity — ~78 events/year in chop/BTC)"
        ),

        # Supporting diagnostics
        "cost_bps_per_trade": cost_bps,
        "sharpe": sharpe,
        "stress_score": stress_score,
        "placebo_shift_effect": placebo_shift,
        "placebo_random_entry_effect": placebo_random,

        # Validation and promotion status
        "validation_status": validation_status,
        "promotion_decision": promotion_decision,
        "promotion_track": promotion_track,

        # Cross-symbol
        "cross_symbol": eth_summary,

        # Human-readable summary
        "summary": _build_summary(
            n=n, t_net=t_net, robustness=robustness, net_bps=net_bps,
            gate_progress=gate_progress, deployment_ready=deployment_ready,
        ),
    }


def _fmt_float(value: float | None, digits: int = 3) -> str:
    return "NA" if value is None else f"{value:.{digits}f}"


def _fmt_bps(value: float | None) -> str:
    return "NA" if value is None else f"{value:.1f}"


def _fmt_pct(value: float | None) -> str:
    return "NA" if value is None else f"{value:.1%}"


def _build_summary(
    *, n: int | None, t_net: float | None, robustness: float | None,
    net_bps: float | None, gate_progress: float | None, deployment_ready: bool,
) -> str:
    lines = [
        f"n={n if n is not None else 'NA'}  "
        f"t_net={_fmt_float(t_net)}  "
        f"robustness={_fmt_float(robustness)}  "
        f"net_bps={_fmt_bps(net_bps)}",
        f"gate_progress={_fmt_pct(gate_progress)}  deployment_ready={deployment_ready}",
    ]
    if not deployment_ready:
        if robustness is not None:
            lines.append(
                f"Gap to gate: robustness needs {0.70 - robustness:.3f} more "
                f"(current {robustness:.3f} → target 0.70)"
            )
        else:
            lines.append("Gap to gate: robustness data missing (target 0.70)")
    return "  |  ".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Monitor a research thesis and write a dated report.")
    parser.add_argument("--run_id", default="stat_stretch_04")
    parser.add_argument("--data_root", default="data")
    parser.add_argument("--out_dir", default=None, help="Override output directory")
    parser.add_argument("--dry_run", action="store_true", help="Print report without writing")
    args = parser.parse_args(argv)

    data_root = Path(args.data_root)
    report = build_report(run_id=args.run_id, data_root=data_root)

    out = json.dumps(report, indent=2)

    if args.dry_run:
        print(out)
        return 0

    date_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.out_dir) if args.out_dir else data_root / "reports" / "monitor" / THESIS_SLUG
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{date_str}.json"
    out_path.write_text(out)
    print(out)
    print(f"\nWrote: {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
