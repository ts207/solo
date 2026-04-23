
from __future__ import annotations

from pathlib import Path
from typing import Any

from project.research.knowledge.reflection import build_run_reflection


def classify_terminal_status(*, run_id: str, manifest: dict[str, Any], data_root: Path | None = None) -> dict[str, Any]:
    reflection = build_run_reflection(run_id=run_id, data_root=data_root)
    mechanical = str(reflection.get('mechanical_outcome', '') or '').strip().lower()
    statistical = str(reflection.get('statistical_outcome', '') or '').strip().lower()
    manifest_status = str(manifest.get('status', '') or '').strip().lower()

    if manifest_status == 'success':
        terminal_status = 'completed_with_contract_warnings' if mechanical in {'artifact_contract_failure', 'warning_only', 'partial_success'} else 'completed'
    elif mechanical in {'mechanical_failure', 'artifact_contract_failure'}:
        terminal_status = 'failed_mechanical'
    elif mechanical == 'data_quality_failure':
        terminal_status = 'failed_data_quality'
    elif str(manifest.get('failed_stage', '') or '').strip() == 'runtime_invariants_postflight':
        terminal_status = 'failed_runtime_invariants'
    elif statistical in {'no_signal', 'weak_signal', 'inconclusive_due_to_sample'}:
        terminal_status = 'failed_statistical'
    else:
        terminal_status = 'failed_mechanical'

    completed = int(reflection.get('completed_stage_count', 0) or 0)
    planned = int(reflection.get('planned_stage_count', 0) or 0)
    return {
        'terminal_status': terminal_status,
        'mechanical_outcome': mechanical,
        'statistical_outcome': statistical,
        'resume_recommended': terminal_status in {'failed_mechanical', 'failed_data_quality', 'failed_runtime_invariants'} and completed < planned,
        'completed_stage_count': completed,
        'planned_stage_count': planned,
        'warning_stage_count': int(reflection.get('warning_stage_count', 0) or 0),
        'reflection': reflection,
    }
