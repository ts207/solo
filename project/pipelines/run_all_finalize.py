from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from project.operator.run_semantics import classify_terminal_status
from project.research.reports import write_operator_outputs_for_run


def handle_runtime_postflight(
    *,
    run_manifest: dict[str, Any],
    run_id: str,
    runtime_invariants_mode: str,
    preflight: dict[str, Any],
    stage_execution: dict[str, Any],
    stage_timings: list[tuple[str, float]],
    stage_instance_timings: list[tuple[str, float]],
    refresh_runtime_lineage_fields: Any,
    run_runtime_postflight_audit: Any,
    apply_runtime_postflight_to_manifest: Any,
    enforce_runtime_postflight: Any,
    emit_failure_messages: Any,
    finalize_run_manifest: Any,
    write_run_manifest: Any,
) -> int | None:
    if runtime_invariants_mode == "off":
        return None

    refresh_runtime_lineage_fields(
        run_manifest,
        run_id=run_id,
        determinism_replay_checks_requested=preflight["determinism_replay_checks_requested"],
        oms_replay_checks_requested=preflight["oms_replay_checks_requested"],
    )

    runtime_postflight = run_runtime_postflight_audit(
        run_id,
        determinism_replay_checks=preflight["determinism_replay_checks_requested"],
    )
    run_manifest["runtime_watermark_violation_count"] = int(
        runtime_postflight.get("watermark_violation_count", 0) or 0
    )
    run_manifest["runtime_postflight_event_source_path"] = str(
        runtime_postflight.get("event_source_path", "") or ""
    )
    runtime_postflight_status = apply_runtime_postflight_to_manifest(
        run_manifest=run_manifest,
        runtime_postflight=runtime_postflight,
    )
    if (
        runtime_postflight_status != "pass"
        or int(run_manifest.get("runtime_watermark_violation_count", 0) or 0) > 0
    ):
        run_manifest["runtime_invariants_status"] = "violations"
    if (
        int(run_manifest.get("runtime_firewall_violation_count", 0) or 0) > 0
        or str(run_manifest.get("determinism_status", "")).strip().lower() == "failed"
        or str(run_manifest.get("oms_replay_status", "")).strip().lower() == "failed"
    ):
        run_manifest["runtime_invariants_status"] = "violations"

    should_fail, postflight_messages = enforce_runtime_postflight(
        run_manifest=run_manifest,
        runtime_postflight_status=runtime_postflight_status,
        runtime_invariants_mode=runtime_invariants_mode,
        determinism_replay_checks_requested=preflight["determinism_replay_checks_requested"],
        oms_replay_checks_requested=preflight["oms_replay_checks_requested"],
    )
    if runtime_invariants_mode == "enforce" and (
        int(run_manifest.get("runtime_firewall_violation_count", 0) or 0) > 0
        or str(run_manifest.get("determinism_status", "")).strip().lower() == "failed"
        or str(run_manifest.get("oms_replay_status", "")).strip().lower() == "failed"
    ):
        should_fail = True
        if "Runtime lineage validation failed" not in postflight_messages:
            postflight_messages.append("Runtime lineage validation failed")
    if should_fail:
        emit_failure_messages(postflight_messages)
        run_manifest["runtime_invariants_status"] = "violations"
        finalize_run_manifest(
            run_manifest=run_manifest,
            status="failed",
            stage_timings=stage_timings,
            stage_instance_timings=stage_instance_timings,
            checklist_decision=stage_execution.get("checklist_decision"),
            auto_continue_applied=bool(stage_execution.get("auto_continue_applied")),
            auto_continue_reason=str(stage_execution.get("auto_continue_reason")),
            non_production_overrides=list(stage_execution.get("non_production_overrides", [])),
            failed_stage="runtime_invariants_postflight",
            failed_stage_instance="runtime_invariants_postflight",
        )
        write_run_manifest(run_id, run_manifest)
        return 1

    return None


def finalize_successful_run(
    *,
    run_manifest: dict[str, Any],
    run_id: str,
    preflight: dict[str, Any],
    stage_execution: dict[str, Any],
    stage_timings: list[tuple[str, float]],
    stage_instance_timings: list[tuple[str, float]],
    finalize_run_manifest: Any,
    apply_run_terminal_audit: Any,
    maybe_emit_run_hash: Any,
    write_run_manifest: Any,
    write_run_kpi_scorecard: Any,
    print_artifact_summary: Any,
    write_run_comparison_report: Any | None = None,
    data_root: Path | None = None,
) -> int:
    if data_root is not None:
        try:
            from project.pipelines.pipeline_provenance import (
                reconcile_run_manifest_from_stage_manifests,
            )

            reconciled = reconcile_run_manifest_from_stage_manifests(
                run_id,
                data_root=data_root,
            )
        except Exception as exc:
            run_manifest["reconciliation_status"] = "failed"
            run_manifest["reconciliation_error"] = str(exc)
            finalize_run_manifest(
                run_manifest=run_manifest,
                status="failed",
                terminal_status="failed_mechanical",
                stage_timings=stage_timings,
                stage_instance_timings=stage_instance_timings,
                checklist_decision=stage_execution.get("checklist_decision"),
                auto_continue_applied=bool(stage_execution.get("auto_continue_applied")),
                auto_continue_reason=str(stage_execution.get("auto_continue_reason")),
                non_production_overrides=list(stage_execution.get("non_production_overrides", [])),
                failed_stage="run_reconciliation",
                failed_stage_instance="run_reconciliation",
            )
            semantics = classify_terminal_status(run_id=run_id, manifest=run_manifest, data_root=data_root)
            run_manifest.update({k: v for k, v in semantics.items() if k != "reflection"})
            semantics = classify_terminal_status(run_id=run_id, manifest=run_manifest, data_root=data_root)
            run_manifest.update({k: v for k, v in semantics.items() if k != "reflection"})
            write_run_manifest(run_id, run_manifest)
            write_run_kpi_scorecard(run_id, run_manifest)
            print(
                f"Run reconciliation failed before success finalization: {exc}",
                file=sys.stderr,
            )
            return 1
        if str(reconciled.get("status", "")).strip().lower() != "success":
            run_manifest["reconciliation_status"] = str(reconciled.get("status", ""))
            run_manifest["reconciliation_error"] = (
                "required stage outputs missing or incomplete"
            )
            finalize_run_manifest(
                run_manifest=run_manifest,
                status="failed",
                terminal_status="failed_mechanical",
                stage_timings=stage_timings,
                stage_instance_timings=stage_instance_timings,
                checklist_decision=stage_execution.get("checklist_decision"),
                auto_continue_applied=bool(stage_execution.get("auto_continue_applied")),
                auto_continue_reason=str(stage_execution.get("auto_continue_reason")),
                non_production_overrides=list(stage_execution.get("non_production_overrides", [])),
                failed_stage="run_reconciliation",
                failed_stage_instance="run_reconciliation",
            )
            write_run_manifest(run_id, run_manifest)
            write_run_kpi_scorecard(run_id, run_manifest)
            print(
                "Run reconciliation did not confirm required outputs; refusing success finalization",
                file=sys.stderr,
            )
            return 1

    finalize_run_manifest(
        run_manifest=run_manifest,
        status="success",
        stage_timings=stage_timings,
        stage_instance_timings=stage_instance_timings,
        checklist_decision=stage_execution.get("checklist_decision"),
        auto_continue_applied=bool(stage_execution.get("auto_continue_applied")),
        auto_continue_reason=str(stage_execution.get("auto_continue_reason")),
        non_production_overrides=list(stage_execution.get("non_production_overrides", [])),
    )
    apply_run_terminal_audit(run_id, run_manifest)
    if preflight["emit_run_hash_requested"]:
        hash_payload = dict(run_manifest)
        hash_payload.pop("run_hash", None)
        hash_payload.pop("run_hash_status", None)
        digest = hashlib.blake2b(
            json.dumps(hash_payload, sort_keys=True, default=str).encode(),
            digest_size=32,
        ).hexdigest()
        run_manifest["run_hash"] = f"blake2b_256:{digest}"
        run_manifest["run_hash_status"] = "computed"
    else:
        run_manifest["run_hash_status"] = "disabled"
    baseline_run_id = str(preflight.get("research_compare_baseline_run_id", "") or "").strip()
    if baseline_run_id and write_run_comparison_report is not None and data_root is not None:
        try:
            comparison_path = write_run_comparison_report(
                data_root=data_root,
                baseline_run_id=baseline_run_id,
                candidate_run_id=run_id,
                thresholds=preflight.get("research_compare_thresholds", {}),
                drift_mode=str(preflight.get("research_compare_drift_mode", "warn") or "warn"),
            )
            comparison_payload = {}
            try:
                comparison_payload = json.loads(comparison_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                comparison_payload = {}
            assessment = dict(comparison_payload.get("assessment", {}))
            run_manifest["research_comparison_status"] = "written"
            run_manifest["research_comparison_baseline_run_id"] = baseline_run_id
            run_manifest["research_comparison_report_path"] = str(comparison_path)
            run_manifest["research_comparison_summary_path"] = str(
                comparison_path.with_name("research_run_comparison_summary.md")
            )
            run_manifest["research_comparison_assessment_status"] = str(
                assessment.get("status", "unknown")
            )
            run_manifest["research_comparison_violation_count"] = int(
                assessment.get("violation_count", 0) or 0
            )
            run_manifest["research_comparison_violations"] = list(assessment.get("violations", []))
            if str(assessment.get("status", "")).strip().lower() == "fail":
                finalize_run_manifest(
                    run_manifest=run_manifest,
                    status="failed",
                    stage_timings=stage_timings,
                    stage_instance_timings=stage_instance_timings,
                    checklist_decision=stage_execution.get("checklist_decision"),
                    auto_continue_applied=bool(stage_execution.get("auto_continue_applied")),
                    auto_continue_reason=str(stage_execution.get("auto_continue_reason")),
                    non_production_overrides=list(
                        stage_execution.get("non_production_overrides", [])
                    ),
                    failed_stage="research_comparison",
                    failed_stage_instance="research_comparison",
                )
                semantics = classify_terminal_status(run_id=run_id, manifest=run_manifest, data_root=data_root)
                run_manifest.update({k: v for k, v in semantics.items() if k != "reflection"})
                maybe_emit_run_hash(run_manifest)
                write_run_manifest(run_id, run_manifest)
                write_run_kpi_scorecard(run_id, run_manifest)
                emit_message = (
                    f"Research comparison drift exceeded thresholds for baseline {baseline_run_id}"
                )
                print(emit_message, file=sys.stderr)
                return 1
        except Exception as exc:
            run_manifest["research_comparison_status"] = "failed"
            run_manifest["research_comparison_baseline_run_id"] = baseline_run_id
            run_manifest["research_comparison_error"] = str(exc)
    else:
        run_manifest["research_comparison_status"] = "skipped"
    semantics = classify_terminal_status(run_id=run_id, manifest=run_manifest, data_root=data_root)
    run_manifest.update({k: v for k, v in semantics.items() if k != "reflection"})
    maybe_emit_run_hash(run_manifest)
    
    # Run Validation Stage
    if data_root is not None:
        try:
            from project.research.services.evaluation_service import (
                ValidationService,
                select_stage_candidate_table,
            )
            
            val_svc = ValidationService(data_root=data_root)
            tables = val_svc.load_candidate_tables(run_id)
            
            # Validation should consume upstream stage candidates, not promotion artifacts.
            candidates_df = select_stage_candidate_table(tables)
            
            if not candidates_df.empty:
                bundle = val_svc.run_validation_stage(
                    run_id=run_id, 
                    candidates_df=candidates_df, 
                    program_id=str(run_manifest.get("program_id", "")) or None
                )
                run_manifest["validation_status"] = "completed"
                run_manifest["validation_validated_count"] = len(bundle.validated_candidates)
                run_manifest["validation_rejected_count"] = len(bundle.rejected_candidates)
            else:
                run_manifest["validation_status"] = "no_candidates"
        except Exception as exc:
            run_manifest["validation_status"] = "failed"
            run_manifest["validation_error"] = str(exc)

    write_run_manifest(run_id, run_manifest)
    write_run_kpi_scorecard(run_id, run_manifest)
    if data_root is not None:
        try:
            write_operator_outputs_for_run(
                run_id=run_id,
                program_id=str(run_manifest.get("program_id", "") or "") or None,
                data_root=data_root,
            )
        except Exception as exc:
            run_manifest["operator_summary_status"] = "failed"
            run_manifest["operator_summary_error"] = str(exc)
            write_run_manifest(run_id, run_manifest)

    print(f"Pipeline run completed: {run_id}")
    print_artifact_summary(run_id)
    return 0
