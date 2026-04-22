from __future__ import annotations
from pathlib import Path
from project.research.services.evaluation_service import (
    ValidationService,
    select_stage_candidate_table,
)
from project.operator.stability import (
    write_regime_split_report as report,
    write_negative_result_diagnostics as diagnose
)

def run(run_id: str, data_root: Path | None = None):
    val_svc = ValidationService(data_root=data_root)
    tables = val_svc.load_candidate_tables(run_id)
    candidates_df = select_stage_candidate_table(tables)
    return val_svc.run_validation_stage(run_id, candidates_df)
