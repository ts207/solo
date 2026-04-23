"""
Tests for audit_historical_stat_integrity script.

This is a P2 item: the script is a first-pass audit tool, not a full verifier.
Tests cover file selection logic and report generation.
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from project.scripts.audit_historical_stat_integrity import (
    REQUIRED_MULTIPLICITY_COLS,
    audit_parquet_artifact,
)


def test_audit_detects_missing_multiplicity_fields():
    df = pd.DataFrame({
        "candidate_id": ["a", "b"],
        "p_value": [0.01, 0.02],
        "estimate_bps": [5.0, 3.0],
    })
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "candidates.parquet"
        df.to_parquet(path)
        
        findings = audit_parquet_artifact(path)
        
        assert len(findings) >= 1
        missing = [f for f in findings if "missing_multiplicity_fields" in f.get("reason", "")]
        assert len(missing) >= 1
        assert all(c in missing[0]["reason"] for c in ["num_tests_family", "num_tests_campaign", "num_tests_effective"])


def test_audit_passes_artifact_with_all_fields():
    df = pd.DataFrame({
        "candidate_id": ["a", "b"],
        "p_value_for_fdr": [0.01, 0.02],
        "estimate_bps": [5.0, 3.0],
        "num_tests_family": [10, 10],
        "num_tests_campaign": [50, 50],
        "num_tests_effective": [25, 25],
        "t_stat": [2.5, 2.0],
        "train_n_obs": [100, 100],
        "validation_n_obs": [50, 50],
    })
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "candidates.parquet"
        df.to_parquet(path)
        
        findings = audit_parquet_artifact(path)
        
        assert len(findings) == 0


def test_audit_flags_legacy_p_value_without_fdr_column():
    df = pd.DataFrame({
        "candidate_id": ["a", "b"],
        "p_value": [0.01, 0.02],
        "num_tests_family": [10, 10],
        "num_tests_campaign": [50, 50],
        "num_tests_effective": [25, 25],
    })
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "candidates.parquet"
        df.to_parquet(path)
        
        findings = audit_parquet_artifact(path)
        
        legacy = [f for f in findings if "legacy_p_value_columns" in f.get("reason", "")]
        assert len(legacy) >= 1


def test_audit_flags_missing_split_sample_counts():
    df = pd.DataFrame({
        "candidate_id": ["a", "b"],
        "p_value_for_fdr": [0.01, 0.02],
        "t_stat": [2.5, 2.0],
        "num_tests_family": [10, 10],
        "num_tests_campaign": [50, 50],
        "num_tests_effective": [25, 25],
    })
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "evaluated_candidates.parquet"
        df.to_parquet(path)
        
        findings = audit_parquet_artifact(path)
        
        missing_split = [f for f in findings if "missing_split_sample_counts" in f.get("reason", "")]
        assert len(missing_split) >= 1


def test_audit_handles_read_error_gracefully():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "corrupted.parquet"
        path.write_text("not a parquet file")
        
        findings = audit_parquet_artifact(path)
        
        assert len(findings) == 1
        assert "read_error" in findings[0]["reason"]


def test_required_multiplicity_columns_constant():
    assert REQUIRED_MULTIPLICITY_COLS == {
        "num_tests_family",
        "num_tests_campaign",
        "num_tests_effective",
    }