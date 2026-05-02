#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from project.discover.reporting import build_discover_summary, explain_empty_discovery

BLOCKED_CLASSIFICATIONS = {
    "missing_diagnostics",
    "no_qualifying_events",
    "search_space_too_narrow",
    "hypotheses_rejected_pre_metrics",
    "zero_feasible_hypotheses",
    "zero_generated_hypotheses",
    "zero_valid_metrics_rows",
    "invalid_or_insufficient_metrics",
}

LOW_VALUE_CLASSIFICATIONS = {
    "too_few_events",
    "oos_validation_failed",
    "expectancy_died_after_costs",
    "stressed_cost_expectancy_failed",
    "multiplicity_gate_failed",
    "regime_stability_failed",
    "final_gate_failed",
}

REQUIRED_CONFIRMATION_STEPS = [
    "governed_reproduction",
    "candidate_autopsy",
    "year_split",
    "forward_confirmation",
]

FORBIDDEN_RESCUE_ACTIONS = [
    "change_horizon",
    "drop_bad_years",
    "loosen_gates",
    "switch_context_without_mechanism",
    "add_symbols_without_mechanism",
]


def _make_target(target: str, run_id: str, data_root: Path | None) -> str:
    command = f"make {target} RUN_ID={run_id}"
    if data_root is not None:
        command += f" DATA_ROOT={data_root}"
    return command


def _evidence_class_for_status(status: str) -> str:
    if status == "review_candidate":
        return "candidate_signal"
    if status in {"blocked", "rejected", "validate_ready"}:
        return status
    return "blocked"


def _next_safe_command_for_status(status: str, run_id: str, data_root: Path | None) -> str:
    if status == "validate_ready":
        return _make_target("validate", run_id, data_root)
    if status == "review_candidate":
        return "Review top_candidates and write a candidate autopsy before any validation decision."
    if status == "rejected":
        return "Record the rejection reason and move to the next bounded cell."
    return _make_target("explain-empty", run_id, data_root)


def _status_for_summary(summary: dict[str, Any]) -> tuple[str, str, list[str], list[str]]:
    counts = summary.get("counts", {}) if isinstance(summary.get("counts"), dict) else {}
    diagnostics = (
        summary.get("diagnostics", {}) if isinstance(summary.get("diagnostics"), dict) else {}
    )
    top = (
        summary.get("top_candidates", {}) if isinstance(summary.get("top_candidates"), dict) else {}
    )
    rows = top.get("rows", []) if isinstance(top.get("rows"), list) else []

    candidates_total = int(counts.get("candidates_total", 0) or 0)
    valid_metrics_rows = int(diagnostics.get("valid_metrics_rows", 0) or 0)
    feasible = int(diagnostics.get("feasible_hypotheses", 0) or 0)
    generated = int(diagnostics.get("hypotheses_generated", 0) or 0)
    bridge_candidates = int(diagnostics.get("bridge_candidates_rows", 0) or 0)

    if candidates_total <= 0:
        empty = explain_empty_discovery(
            run_id=str(summary.get("run_id", "")),
            data_root=Path(str(summary.get("data_root", ""))) if summary.get("data_root") else None,
        )
        classification = str(empty.get("classification", "unknown") or "unknown")

        if classification in BLOCKED_CLASSIFICATIONS:
            return (
                "blocked",
                classification,
                [
                    "Do not validate or promote this run.",
                    "Inspect validated_plan.json and phase2_diagnostics.json.",
                    "Fix data coverage, event/template compatibility, or search-space feasibility first.",
                ],
                ["edge validate run", "edge promote run", "edge deploy bind-config"],
            )

        if classification in LOW_VALUE_CLASSIFICATIONS:
            return (
                "rejected",
                classification,
                [
                    "Treat this as a real negative/low-value result, not a mechanical failure.",
                    "Record the failure reason and move to the next bounded cell.",
                ],
                ["edge promote run", "edge deploy bind-config"],
            )

        return (
            "blocked",
            classification,
            ["Discovery emitted no candidates; inspect empty-run diagnostics before continuing."],
            ["edge validate run", "edge promote run", "edge deploy bind-config"],
        )

    if generated <= 0 or feasible <= 0 or valid_metrics_rows <= 0:
        if feasible <= 0:
            classification = "zero_feasible_hypotheses"
        elif valid_metrics_rows <= 0:
            classification = "zero_valid_metrics_rows"
        else:
            classification = "zero_generated_hypotheses"
        return (
            "blocked",
            classification,
            [
                "Candidates exist but diagnostics indicate generated/feasible/valid metric counts are zero.",
                "Inspect phase2_diagnostics.json and validated_plan.json before trusting rankings.",
            ],
            ["edge validate run", "edge promote run", "edge deploy bind-config"],
        )

    if bridge_candidates > 0:
        return (
            "validate_ready",
            "bridge_candidates_present",
            [
                "Run edge validate run on this run_id.",
                "After validation, require promotion_ready_candidates.parquet before promotion.",
                "Use forward-confirm before deploy-profile promotion when an unseen window exists.",
            ],
            ["edge deploy bind-config", "edge deploy live-run"],
        )

    if rows:
        return (
            "review_candidate",
            "candidates_present_but_no_final_bridge_survivors",
            [
                "Review top candidates manually before validation.",
                "Do not widen the search automatically as a rescue tactic.",
                "Prefer moving to the next bounded cell unless the top row has a clear mechanical issue.",
            ],
            ["edge promote run", "edge deploy bind-config", "edge deploy live-run"],
        )

    return (
        "blocked",
        "unranked_candidates",
        [
            "Candidates exist but no top-candidate rows were rankable; inspect candidate schema and diagnostics."
        ],
        ["edge validate run", "edge promote run", "edge deploy bind-config"],
    )


def build_discover_doctor_report(
    *,
    run_id: str,
    data_root: Path | None = None,
    top_k: int = 10,
) -> dict[str, Any]:
    summary = build_discover_summary(run_id=run_id, data_root=data_root, top_k=top_k)
    status, classification, next_actions, forbidden_actions = _status_for_summary(summary)
    evidence_class = _evidence_class_for_status(status)

    counts = summary.get("counts", {}) if isinstance(summary.get("counts"), dict) else {}
    empty_diagnostics = None
    if int(counts.get("candidates_total", 0) or 0) <= 0:
        empty_diagnostics = explain_empty_discovery(run_id=run_id, data_root=data_root)

    return {
        "kind": "discover_doctor",
        "run_id": str(run_id),
        "status": status,
        "evidence_class": evidence_class,
        "classification": classification,
        "requires": REQUIRED_CONFIRMATION_STEPS,
        "next_actions": next_actions,
        "next_safe_command": _next_safe_command_for_status(status, str(run_id), data_root),
        "forbidden_actions": forbidden_actions,
        "forbidden_rescue_actions": FORBIDDEN_RESCUE_ACTIONS,
        "summary": summary,
        "empty_diagnostics": empty_diagnostics,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Inspect discovery artifacts and return an agent-safe edge-discovery decision."
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--data_root")
    parser.add_argument("--top_k", type=int, default=10)
    args = parser.parse_args(argv)

    report = build_discover_doctor_report(
        run_id=args.run_id,
        data_root=Path(args.data_root) if args.data_root else None,
        top_k=int(args.top_k),
    )
    print(json.dumps(report, indent=2, sort_keys=True, default=str))

    return 0 if report.get("status") in {"validate_ready", "review_candidate"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
