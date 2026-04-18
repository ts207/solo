from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Tuple


StageReasonCode = Literal["selected", "skipped", "failed", "artifact_mismatch", "dependency_missing"]


@dataclass(frozen=True)
class PlannedStage:
    """A single stage in an execution plan with its reason for inclusion/exclusion."""
    stage_name: str
    script_path: str
    reason_code: StageReasonCode
    base_args: Tuple[str, ...] = ()
    stage_instance_id: str = ""
    notes: str = ""

    @property
    def is_active(self) -> bool:
        return self.reason_code == "selected"


@dataclass(frozen=True)
class ExecutionPlan:
    """Typed, inspectable plan produced by the pipeline planner before any stage runs.

    Carries the full intent of a run — which stages are selected, which are skipped,
    and why — so the runner can execute and the verifier can compare against actuals.
    """
    run_id: str
    planned_at: str
    stages: Tuple[PlannedStage, ...]
    run_mode: str = "research"
    symbols: Tuple[str, ...] = ()
    timeframe: str = "5m"
    experiment_config: str = ""
    registry_root: str = ""
    raw_args: Dict[str, Any] = field(default_factory=dict)

    @property
    def active_stages(self) -> Tuple[PlannedStage, ...]:
        return tuple(s for s in self.stages if s.is_active)

    @property
    def skipped_stages(self) -> Tuple[PlannedStage, ...]:
        return tuple(s for s in self.stages if s.reason_code == "skipped")

    def explain(self) -> str:
        """Return a human-readable plan summary."""
        lines = [
            f"ExecutionPlan run_id={self.run_id} mode={self.run_mode}",
            f"  symbols: {', '.join(self.symbols) or '(none)'}",
            f"  timeframe: {self.timeframe}",
            f"  stages ({len(self.active_stages)} active, {len(self.skipped_stages)} skipped):",
        ]
        for stage in self.stages:
            marker = "✓" if stage.is_active else "·"
            tag = f"  [{marker}] {stage.stage_name}"
            if stage.reason_code != "selected":
                tag += f"  ({stage.reason_code})"
            if stage.notes:
                tag += f"  — {stage.notes}"
            lines.append(tag)
        return "\n".join(lines)


@dataclass(frozen=True)
class StageVerificationResult:
    """Actual execution outcome for a single stage."""
    stage_name: str
    stage_instance_id: str
    planned_reason_code: StageReasonCode
    actual_outcome: Literal["success", "failure", "skipped", "not_reached"]
    duration_sec: float = 0.0
    notes: str = ""

    @property
    def matches_plan(self) -> bool:
        if self.planned_reason_code == "selected":
            return self.actual_outcome == "success"
        if self.planned_reason_code == "skipped":
            return self.actual_outcome == "skipped"
        return True


@dataclass(frozen=True)
class ExecutionVerificationReport:
    """Post-run report comparing planned execution against actuals.

    Produced after a run completes (success or failure) by comparing the
    ExecutionPlan intent against the recorded stage outcomes in the run manifest.
    """
    run_id: str
    verified_at: str
    plan_stage_count: int
    actual_stage_count: int
    results: Tuple[StageVerificationResult, ...]
    final_status: str = "unknown"

    @property
    def mismatches(self) -> Tuple[StageVerificationResult, ...]:
        return tuple(r for r in self.results if not r.matches_plan)

    @property
    def passed(self) -> bool:
        return not self.mismatches and self.final_status == "success"

    def summary(self) -> str:
        lines = [
            f"ExecutionVerificationReport run_id={self.run_id}",
            f"  final_status: {self.final_status}",
            f"  planned: {self.plan_stage_count} stages, actual: {self.actual_stage_count}",
            f"  mismatches: {len(self.mismatches)}",
        ]
        for r in self.mismatches:
            lines.append(
                f"  ! {r.stage_name}: planned={r.planned_reason_code}, actual={r.actual_outcome}"
            )
        return "\n".join(lines)


def build_execution_plan(
    run_id: str,
    planned_at: str,
    stage_specs: List[Tuple[str, str, List[str], StageReasonCode]],
    *,
    run_mode: str = "research",
    symbols: List[str] | None = None,
    timeframe: str = "5m",
    experiment_config: str = "",
    registry_root: str = "",
    raw_args: Dict[str, Any] | None = None,
) -> ExecutionPlan:
    """Construct an ExecutionPlan from the planner's stage list.

    stage_specs: list of (stage_name, script_path, base_args, reason_code)
    """
    stages = tuple(
        PlannedStage(
            stage_name=name,
            script_path=script,
            reason_code=reason,
            base_args=tuple(str(a) for a in args),
        )
        for name, script, args, reason in stage_specs
    )
    return ExecutionPlan(
        run_id=run_id,
        planned_at=planned_at,
        stages=stages,
        run_mode=run_mode,
        symbols=tuple(symbols or []),
        timeframe=timeframe,
        experiment_config=experiment_config,
        registry_root=registry_root,
        raw_args=dict(raw_args or {}),
    )


def verify_execution(
    plan: ExecutionPlan,
    run_manifest: Dict[str, Any],
    *,
    verified_at: str = "",
) -> ExecutionVerificationReport:
    """Build a verification report from a completed run manifest vs the plan."""
    from datetime import datetime, timezone

    ts = verified_at or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    stage_timings: Dict[str, float] = dict(run_manifest.get("stage_timings_sec", {}))
    failed_stage: str = str(run_manifest.get("failed_stage", "") or "")
    final_status: str = str(run_manifest.get("status", "unknown"))

    results: List[StageVerificationResult] = []
    for planned in plan.stages:
        name = planned.stage_name
        if name in stage_timings:
            if name == failed_stage:
                actual: Literal["success", "failure", "skipped", "not_reached"] = "failure"
            else:
                actual = "success"
        elif planned.reason_code == "skipped":
            actual = "skipped"
        else:
            actual = "not_reached"
        results.append(
            StageVerificationResult(
                stage_name=name,
                stage_instance_id=planned.stage_instance_id,
                planned_reason_code=planned.reason_code,
                actual_outcome=actual,
                duration_sec=stage_timings.get(name, 0.0),
            )
        )

    return ExecutionVerificationReport(
        run_id=plan.run_id,
        verified_at=ts,
        plan_stage_count=len(plan.stages),
        actual_stage_count=len(stage_timings),
        results=tuple(results),
        final_status=final_status,
    )
