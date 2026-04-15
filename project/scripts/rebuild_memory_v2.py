import pandas as pd
from pathlib import Path
from project.core.config import get_data_root
from project.research.knowledge.memory import (
    build_tested_regions_snapshot,
    write_memory_table,
    compute_region_statistics,
    compute_event_statistics,
    compute_template_statistics,
    compute_context_statistics,
)

run_id = "synthetic_2025_full_year"
program_id = "synthetic_2025_full_year"
data_root = get_data_root()

print(f"Rebuilding memory for {program_id} using run {run_id}...")

incoming_tested = build_tested_regions_snapshot(
    run_id=run_id, program_id=program_id, data_root=data_root
)
print(f"Built snapshot with {len(incoming_tested)} rows.")
print("Snapshot head:")
print(incoming_tested[["candidate_id", "mean_return_bps"]].head())

if not incoming_tested.empty:
    write_memory_table(program_id, "tested_regions", incoming_tested, data_root=data_root)
    # Verify immediately after writing
    df_verify = pd.read_parquet(
        data_root / "artifacts" / "experiments" / program_id / "memory" / "tested_regions.parquet"
    )
    print("Verified from file immediately after write:")
    print(df_verify[["candidate_id", "mean_return_bps"]].head())

    write_memory_table(
        program_id,
        "region_statistics",
        compute_region_statistics(incoming_tested),
        data_root=data_root,
    )
    write_memory_table(
        program_id,
        "event_statistics",
        compute_event_statistics(incoming_tested),
        data_root=data_root,
    )
    write_memory_table(
        program_id,
        "template_statistics",
        compute_template_statistics(incoming_tested),
        data_root=data_root,
    )
    write_memory_table(
        program_id,
        "context_statistics",
        compute_context_statistics(incoming_tested),
        data_root=data_root,
    )
    print("Memory tables written successfully.")
else:
    print("Snapshot empty, nothing to write.")
