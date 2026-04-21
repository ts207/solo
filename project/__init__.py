from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent


def report(*args, **kwargs):
    from project.operator.stability import write_regime_split_report

    return write_regime_split_report(*args, **kwargs)


def diagnose(*args, **kwargs):
    from project.operator.stability import write_negative_result_diagnostics

    return write_negative_result_diagnostics(*args, **kwargs)


def run(run_id: str, data_root: Path | None = None):
    from project.research.services.evaluation_service import (
        ValidationService,
        select_stage_candidate_table,
    )

    val_svc = ValidationService(data_root=data_root)
    tables = val_svc.load_candidate_tables(run_id)
    candidates_df = select_stage_candidate_table(tables)
    return val_svc.run_validation_stage(run_id, candidates_df)
