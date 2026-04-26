from __future__ import annotations

from typing import Any


def gate_result(
    name: str, passed: bool, observed: Any, threshold: Any, note: str = ""
) -> dict[str, Any]:
    return {
        "name": name,
        "passed": bool(passed),
        "observed": observed,
        "threshold": threshold,
        "note": note,
    }


def build_checklist_payload(
    run_id: str,
    edge_metrics: dict[str, Any],
    expectancy_payload: dict[str, Any],
    robustness_payload: dict[str, Any],
    capital_footprint_payload: dict[str, Any] | None,
    config: dict[str, Any],
    paths: dict[str, str],
) -> dict[str, Any]:
    reasons: list[str] = []
    gates: list[dict[str, Any]] = []

    # 1. Edge Candidates Gate
    rows = int(edge_metrics.get("rows", 0))
    min_rows = int(config.get("min_edge_candidates", 1))
    ok = rows >= min_rows
    gates.append(gate_result("edge_candidates_generated", ok, rows, min_rows))
    if not ok:
        reasons.append(f"edge candidates below threshold ({rows} < {min_rows})")

    # 2. Promoted Candidates Gate
    promoted = int(edge_metrics.get("promoted", 0))
    min_promoted = int(config.get("min_promoted_candidates", 1))
    ok = promoted >= min_promoted
    gates.append(gate_result("promoted_edge_candidates", ok, promoted, min_promoted))
    if not ok:
        reasons.append(f"promoted edge candidates below threshold ({promoted} < {min_promoted})")

    # 3. Bridge Tradable Gate
    bridge = int(edge_metrics.get("bridge_tradable", 0))
    min_bridge = int(config.get("min_bridge_tradable_candidates", 1))
    ok = bridge >= min_bridge
    gates.append(gate_result("bridge_tradable_candidates", ok, bridge, min_bridge))
    if not ok:
        reasons.append(f"bridge-tradable candidates below threshold ({bridge} < {min_bridge})")

    # 4. Expectancy Existence
    exp_exists = bool(expectancy_payload.get("expectancy_exists", False))
    req_exp = bool(config.get("require_expectancy_exists", True))
    ok = exp_exists if req_exp else True
    gates.append(gate_result("expectancy_exists", ok, exp_exists, req_exp))
    if not ok:
        reasons.append("expectancy_exists is false")

    # 5. Robust Survivors
    survivors = robustness_payload.get("survivors", [])
    if not isinstance(survivors, list):
        survivors = robustness_payload.get("robust_survivors", [])
    count = len(survivors)
    min_surv = int(config.get("min_robust_survivors", 1))
    ok = count >= min_surv
    gates.append(gate_result("robust_survivor_count", ok, count, min_surv))
    if not ok:
        reasons.append(f"robust survivors below threshold ({count} < {min_surv})")

    decision = "PROMOTE" if all(g["passed"] for g in gates) else "KEEP_RESEARCH"

    return {
        "run_id": run_id,
        "decision": decision,
        "gates": gates,
        "failure_reasons": reasons,
        "config": config,
        "metrics": {
            "edge_candidate_rows": rows,
            "edge_candidate_promoted": promoted,
            "bridge_tradable_candidates": bridge,
            "expectancy_exists": exp_exists,
            "robust_survivor_count": count,
        },
        "inputs": paths,
    }
