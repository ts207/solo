from __future__ import annotations

import sys
from typing import Any


def argv_flag_value(argv: list[str], flag: str) -> str | None:
    for idx, token in enumerate(argv):
        if token == flag:
            if idx + 1 < len(argv):
                return str(argv[idx + 1]).strip()
            return None
        if token.startswith(f"{flag}="):
            return str(token.split("=", 1)[1]).strip()
    return None


def argv_flag_present(argv: list[str], flag: str) -> bool:
    return argv_flag_value(argv, flag) is not None


def argv_flag_truthy(argv: list[str], flag: str) -> bool:
    raw = argv_flag_value(argv, flag)
    if raw is None:
        return False
    try:
        return bool(int(raw))
    except (TypeError, ValueError):
        return str(raw).strip().lower() not in {"", "0", "false", "no"}


def collect_forbidden_production_overrides(args: Any) -> list[str]:
    forbidden = {
        "strategy_blueprint_allow_fallback": "strategy_blueprint_allow_fallback",
        "strategy_blueprint_allow_non_executable_conditions": "strategy_blueprint_allow_non_executable_conditions",
        "strategy_blueprint_allow_naive_entry_fail": "strategy_blueprint_allow_naive_entry_fail",
        "strategy_builder_allow_non_promoted": "strategy_builder_allow_non_promoted",
        "strategy_builder_allow_missing_candidate_detail": "strategy_builder_allow_missing_candidate_detail",
        "promotion_allow_fallback_evidence": "promotion_allow_fallback_evidence",
    }
    enabled: list[str] = []
    for attr, label in forbidden.items():
        try:
            active = bool(int(getattr(args, attr, 0) or 0))
        except (TypeError, ValueError):
            active = bool(getattr(args, attr, 0))
        if active:
            enabled.append(label)
    return enabled


def evaluate_startup_guards(*, args: Any, non_production_overrides: list[str]) -> str | None:
    if bool(int(getattr(args, "strategy_blueprint_allow_fallback", 0) or 0)):
        return (
            "INV_NO_FALLBACK_IN_MEASUREMENT: "
            "strategy blueprint fallback cannot be enabled for measured runs"
        )

    forbidden_production_overrides = collect_forbidden_production_overrides(args)
    if str(args.mode).strip().lower() == "production" and forbidden_production_overrides:
        return (
            "Non-production override flags are strictly forbidden in production mode: "
            + ", ".join(forbidden_production_overrides)
        )

    if (
        bool(int(getattr(args, "ci_fail_on_non_production_overrides", 0) or 0))
        and non_production_overrides
    ):
        return "CI override guard blocked run"

    return None


def fail_run(
    *,
    run_manifest: dict[str, Any],
    run_id: str,
    stage_timings: list[tuple[str, float]],
    stage_instance_timings: list[tuple[str, float]],
    write_run_manifest: Any,
    finalize_run_manifest: Any,
    failed_stage: str,
    message: str,
    stderr: bool = True,
) -> int:
    print(message, file=sys.stderr if stderr else sys.stdout)
    finalize_run_manifest(
        run_manifest=run_manifest,
        status="failed",
        stage_timings=stage_timings,
        stage_instance_timings=stage_instance_timings,
        failed_stage=failed_stage,
        failed_stage_instance=failed_stage,
    )
    write_run_manifest(run_id, run_manifest)
    return 1
