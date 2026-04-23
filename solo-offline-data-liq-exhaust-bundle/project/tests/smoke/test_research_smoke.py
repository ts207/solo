from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.reliability.cli_smoke import run_smoke_cli


def test_research_smoke(tmp_path: Path):
    summary = run_smoke_cli("research", root=tmp_path, storage_mode="auto")
    assert summary["research"]["candidate_rows"] >= 2

    output_dir = Path(summary["research"]["output_dir"])
    candidate_files = sorted(output_dir.glob("phase2_candidates*"))
    assert candidate_files, f"No phase2_candidates artifact under {output_dir}"

    df = (
        pd.read_parquet(candidate_files[0])
        if candidate_files[0].suffix == ".parquet"
        else pd.read_csv(candidate_files[0])
    )

    if "gate_phase2_final" in df.columns:
        # Full gate column present: assert gate was applied and some candidates rejected
        assert not df["gate_phase2_final"].all(), (
            "All candidates passed gate_phase2_final — gate bypass suspected"
        )
        has_fail_reason = df["fail_reasons"].fillna("").str.len() > 0
        assert has_fail_reason.any(), (
            "No candidates have fail_reasons — rejection path not exercised"
        )
    else:
        # gate_phase2_final not present at this pipeline stage; assert multiplicity gate
        assert "gate_multiplicity" in df.columns, (
            "Neither gate_phase2_final nor gate_multiplicity found — no behavioral gate column present"
        )
        # Gate should reject at least some candidates (not all pass)
        assert not df["gate_multiplicity"].all(), (
            "All candidates passed gate_multiplicity — gate bypass suspected"
        )
        # is_discovery=False records are the rejection path
        assert "is_discovery" in df.columns, (
            "is_discovery column missing — rejection path not observable"
        )
        assert (~df["is_discovery"]).any(), (
            "All candidates marked is_discovery=True — rejection path not exercised"
        )
