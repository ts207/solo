from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from project.pipelines.execution_engine import (
    run_dag,
)
from project.pipelines.execution_engine import (
    run_stage as _engine_run_stage,
)
from project.pipelines.execution_plan import (
    ExecutionPlan,
    ExecutionVerificationReport,
    verify_execution,
)
from project.pipelines.pipeline_defaults import (
    DATA_ROOT,
    build_timing_map,
    utc_now_iso,
)


def run_stage(stage: str, script: Path, base_args: List[str], run_id: str, **kwargs) -> bool:
    """Executes a single pipeline stage using the execution engine."""
    # Map kwargs to expected execution_engine.run_stage names
    engine_kwargs = {
        "data_root": DATA_ROOT,
        "strict_recommendations_checklist": bool(
            kwargs.get("strict_recommendations_checklist", False)
        ),
        "feature_schema_version": str(kwargs.get("feature_schema_version", "")),
        "current_pipeline_session_id": str(kwargs.get("current_pipeline_session_id", "")),
        "current_stage_instance_id": kwargs.get("stage_instance_id", stage),
        "stage_cache_meta": kwargs.get("stage_cache_meta", {}),
    }

    return _engine_run_stage(
        stage=stage, script_path=script, base_args=base_args, run_id=run_id, **engine_kwargs
    )


def seed_run_manifest(
    run_manifest: Dict[str, Any],
    run_id: str,
    existing_manifest: Dict[str, Any],
    resume_from_failed_stage: bool,
    write_run_manifest: Callable[[str, Dict[str, Any]], None],
) -> None:
    """Seeds the run manifest, potentially merging state from an existing one."""
    if resume_from_failed_stage and existing_manifest:
        # Carry over some state from existing manifest
        run_manifest["stage_cache_meta"] = existing_manifest.get("stage_cache_meta", {})
        run_manifest["stage_timings_sec"] = existing_manifest.get("stage_timings_sec", {})
        run_manifest["stage_instance_timings_sec"] = existing_manifest.get(
            "stage_instance_timings_sec", {}
        )

    write_run_manifest(run_id, run_manifest)


def finalize_run_manifest(
    run_manifest: Dict[str, Any],
    status: str,
    stage_timings: List[Tuple[str, float]],
    stage_instance_timings: List[Tuple[str, float]],
    **kwargs,
) -> None:
    """Finalizes the run manifest with terminal status and timings."""
    run_manifest["status"] = status
    run_manifest["finished_at"] = utc_now_iso()
    stage_timings_map = dict(run_manifest.get("stage_timings_sec", {}))
    stage_timings_map.update(build_timing_map(stage_timings))
    run_manifest["stage_timings_sec"] = stage_timings_map

    stage_instance_timings_map = dict(run_manifest.get("stage_instance_timings_sec", {}))
    stage_instance_timings_map.update(build_timing_map(stage_instance_timings))
    run_manifest["stage_instance_timings_sec"] = stage_instance_timings_map
    if status == "success":
        run_manifest["failed_stage"] = None
        run_manifest["failed_stage_instance"] = None
        run_manifest.setdefault("terminal_status", "completed")
    elif status == "failed":
        run_manifest.setdefault("terminal_status", "failed_mechanical")

    # Capture additional metadata if provided
    if "failed_stage" in kwargs:
        run_manifest["failed_stage"] = kwargs["failed_stage"]
    if "failed_stage_instance" in kwargs:
        run_manifest["failed_stage_instance"] = kwargs["failed_stage_instance"]
    if "checklist_decision" in kwargs:
        run_manifest["checklist_decision"] = kwargs["checklist_decision"]


class ExecutionRunner:
    """Thin contract-backed wrapper over the DAG execution engine."""

    def __init__(
        self,
        *,
        feature_schema_version: str,
        current_pipeline_session_id: str,
        run_stage_fn: Optional[Callable] = None,
    ) -> None:
        self.feature_schema_version = str(feature_schema_version)
        self.current_pipeline_session_id = str(current_pipeline_session_id)
        self._run_stage_fn = run_stage_fn or run_stage

    def execute(
        self,
        *,
        plan: ExecutionPlan,
        args: Any,
        run_id: str,
        stages: Mapping[str, Any],
        planned_stage_instances: List[str],
        resume_from_index: int,
        execution_requested: bool,
        run_manifest: Dict[str, Any],
        stage_timings: List[Tuple[str, float]],
        stage_instance_timings: List[Tuple[str, float]],
        write_run_manifest: Callable[[str, dict], None],
        write_run_kpi_scorecard: Callable[[str, dict], None],
        apply_run_terminal_audit: Callable[[str, dict], None],
        load_checklist_decision: Callable[[str], Optional[str]],
        last_stage_cache_meta: Dict[str, Dict[str, object]],
    ) -> Dict[str, Any]:
        if not execution_requested:
            print("Execution not requested. Planning complete.")
            return {"status": "ok"}

        if not isinstance(stages, Mapping):
            raise TypeError(
                "Execution engine requires a DAG-based Mapping[str, StageDefinition], "
                f"got {type(stages)}"
            )

        os.environ["BACKTEST_PIPELINE_SESSION_ID"] = self.current_pipeline_session_id
        os.environ["BACKTEST_FEATURE_SCHEMA_VERSION"] = self.feature_schema_version

        import threading

        failed_info = {"instance": None, "name": None}
        failed_lock = threading.Lock()

        def worker_wrapper(args_tuple):
            s_inst, s_name, script, base_args, rid = args_tuple
            t0 = time.perf_counter()
            ok = self._run_stage_fn(
                stage=s_name,
                script=script,
                base_args=base_args,
                run_id=rid,
                stage_instance_id=s_inst,
                feature_schema_version=self.feature_schema_version,
                current_pipeline_session_id=self.current_pipeline_session_id,
                stage_cache_meta=last_stage_cache_meta,
                strict_recommendations_checklist=getattr(
                    args, "strict_recommendations_checklist", False
                ),
            )
            elapsed = time.perf_counter() - t0

            if not ok:
                with failed_lock:
                    if failed_info["instance"] is None:
                        failed_info["instance"] = s_inst
                        failed_info["name"] = s_name

            return s_inst, s_name, ok, elapsed, {}

        print(
            "\n>>> Executing DAG Pipeline "
            f"(run_id={run_id}, planned_families={sorted({stage.stage_family for stage in plan.active_stages if stage.stage_family})})"
        )

        completed_already = set(run_manifest.get("stage_instance_timings_sec", {}).keys())
        failed_inst = run_manifest.get("failed_stage_instance")
        if failed_inst and failed_inst in completed_already:
            completed_already.remove(failed_inst)

        all_ok, dag_timings = run_dag(
            plan=stages,
            run_id=run_id,
            max_workers=max(1, int(getattr(args, "max_analyzer_workers", 1))),
            worker_fn=worker_wrapper,
            completed_already=completed_already,
            continue_on_failure=False,
        )

        for s_inst, s_name, elapsed, _ in dag_timings:
            stage_timings.append((s_name, elapsed))
            stage_instance_timings.append((s_inst, elapsed))

        if not all_ok:
            return {
                "status": "failed",
                "failed_stage": failed_info["name"],
                "failed_stage_instance": failed_info["instance"],
                "checklist_decision": load_checklist_decision(run_id),
            }

        return {
            "status": "ok",
            "checklist_decision": load_checklist_decision(run_id),
        }

    def build_verification_report(
        self,
        *,
        plan: ExecutionPlan,
        run_manifest: Dict[str, Any],
        data_root: Path | None = None,
        verified_at: str = "",
    ) -> ExecutionVerificationReport:
        return verify_execution(
            plan,
            run_manifest,
            verified_at=verified_at,
            data_root=data_root or DATA_ROOT,
        )


def execute_pipeline_stages(
    *,
    args: Any,
    run_id: str,
    stages: Mapping[str, Any],
    planned_stage_instances: List[str],
    resume_from_index: int,
    execution_requested: bool,
    run_manifest: Dict[str, Any],
    stage_timings: List[Tuple[str, float]],
    stage_instance_timings: List[Tuple[str, float]],
    write_run_manifest: Callable[[str, dict], None],
    write_run_kpi_scorecard: Callable[[str, dict], None],
    apply_run_terminal_audit: Callable[[str, dict], None],
    load_checklist_decision: Callable[[str], Optional[str]],
    last_stage_cache_meta: Dict[str, Dict[str, object]],
    feature_schema_version: str,
    current_pipeline_session_id: str,
    run_stage_fn: Optional[Callable] = None,
) -> Dict[str, Any]:
    """Backward-compatible function wrapper around ExecutionRunner."""
    runner = ExecutionRunner(
        feature_schema_version=feature_schema_version,
        current_pipeline_session_id=current_pipeline_session_id,
        run_stage_fn=run_stage_fn,
    )
    plan = ExecutionPlan(
        run_id=run_id,
        planned_at=utc_now_iso(),
        stages=tuple(),
        run_mode=str(getattr(args, "mode", "research") or "research"),
    )
    return runner.execute(
        plan=plan,
        args=args,
        run_id=run_id,
        stages=stages,
        planned_stage_instances=planned_stage_instances,
        resume_from_index=resume_from_index,
        execution_requested=execution_requested,
        run_manifest=run_manifest,
        stage_timings=stage_timings,
        stage_instance_timings=stage_instance_timings,
        write_run_manifest=write_run_manifest,
        write_run_kpi_scorecard=write_run_kpi_scorecard,
        apply_run_terminal_audit=apply_run_terminal_audit,
        load_checklist_decision=load_checklist_decision,
        last_stage_cache_meta=last_stage_cache_meta,
    )
