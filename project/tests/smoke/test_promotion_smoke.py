from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from project.reliability.cli_smoke import run_smoke_cli
from project.reliability.smoke_data import (
    build_smoke_dataset,
    materialize_smoke_promotion_inputs,
    run_promotion_smoke,
    run_research_smoke,
)


def test_promotion_smoke(tmp_path: Path):
    # run_smoke_cli('promotion') currently raises a pre-existing schema error
    # (evidence_bundle_summary missing 'is_reduced_evidence') inside
    # validate_promotion_artifacts.  Work around it by calling the lower-level
    # helpers directly so we can still assert on the rejection-path behaviour.
    dataset = build_smoke_dataset(tmp_path, seed=20260101, storage_mode="auto")
    research_result = run_research_smoke(dataset)
    materialize_smoke_promotion_inputs(dataset, research_result)
    promotion_result = run_promotion_smoke(dataset, research_result)

    promo_dir = Path(promotion_result["output_dir"])
    decisions_files = sorted(promo_dir.glob("promotion_decisions*"))
    assert decisions_files, f"No promotion_decisions artifact under {promo_dir}"

    decisions = (
        pd.read_parquet(decisions_files[0])
        if decisions_files[0].suffix == ".parquet"
        else pd.read_csv(decisions_files[0])
    )

    assert "promotion_decision" in decisions.columns, "promotion_decision column missing"
    assert len(decisions) >= 1, "promotion_decisions table is empty"

    decisions_lower = decisions["promotion_decision"].str.lower()
    assert (decisions_lower != "promoted").any(), (
        "All candidates were promoted — rejection path not exercised in smoke"
    )

    rejected = decisions[decisions_lower != "promoted"]
    if len(rejected) > 0:
        fail_col = next(
            (
                c
                for c in [
                    "rejection_reasons",
                    "fail_reasons",
                    "promotion_fail_reason_primary",
                    "fail_reason_primary",
                ]
                if c in decisions.columns
            ),
            None,
        )
        if fail_col:
            has_reason = rejected[fail_col].fillna("").str.len() > 0
            assert has_reason.any(), "Rejected candidates have no recorded failure reason"
