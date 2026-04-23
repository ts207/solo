"""
Tests for search-burden accounting (Workstream B).

Tests cover:
- run summary generation
- propagation into promotion outputs
- evidence bundle serialization
- legacy artifact compatibility with default-filled burden fields
- multi-lineage / multi-family counts
- reconstructed-late burden sets search_burden_estimated=True
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from project.research.contracts.search_burden import (
    SEARCH_BURDEN_FIELDS,
    SEARCH_BURDEN_NUMERIC_FIELDS,
    DEFAULT_SEARCH_BURDEN_VERSION,
    default_search_burden_dict,
    normalize_search_burden_frame,
    merge_search_burden_columns,
    build_search_burden_summary,
    write_search_burden_summary,
    load_search_burden_summary,
)


class TestSearchBurdenContract:
    """Test the canonical search-burden contract."""

    def test_search_burden_fields_list_complete(self):
        expected_fields = {
            "search_proposals_attempted",
            "search_candidates_generated",
            "search_candidates_scored",
            "search_candidates_eligible",
            "search_parameterizations_attempted",
            "search_mutations_attempted",
            "search_directions_tested",
            "search_confirmations_attempted",
            "search_trigger_variants_attempted",
            "search_family_count",
            "search_lineage_count",
            "search_scope_version",
            "search_burden_estimated",
        }
        assert set(SEARCH_BURDEN_FIELDS) == expected_fields

    def test_default_search_burden_dict(self):
        burden = default_search_burden_dict()
        for field in SEARCH_BURDEN_NUMERIC_FIELDS:
            assert burden[field] == 0
        assert burden["search_scope_version"] == DEFAULT_SEARCH_BURDEN_VERSION
        assert burden["search_burden_estimated"] is False

    def test_default_search_burden_dict_estimated(self):
        burden = default_search_burden_dict(estimated=True)
        assert burden["search_burden_estimated"] is True

    def test_default_search_burden_dict_custom_version(self):
        burden = default_search_burden_dict(scope_version="custom_v1")
        assert burden["search_scope_version"] == "custom_v1"


class TestNormalizeSearchBurdenFrame:
    """Test DataFrame normalization."""

    def test_empty_frame_gets_defaults(self):
        df = pd.DataFrame()
        result = normalize_search_burden_frame(df)
        for field in SEARCH_BURDEN_NUMERIC_FIELDS:
            assert field in result.columns
        assert "search_scope_version" in result.columns
        assert "search_burden_estimated" in result.columns

    def test_existing_values_preserved(self):
        df = pd.DataFrame({
            "candidate_id": ["a", "b"],
            "search_candidates_generated": [100, 200],
            "search_family_count": [5, 10],
        })
        result = normalize_search_burden_frame(df)
        assert result.loc[0, "search_candidates_generated"] == 100
        assert result.loc[1, "search_candidates_generated"] == 200
        assert result.loc[0, "search_family_count"] == 5
        # Missing fields should be defaulted
        assert result.loc[0, "search_lineage_count"] == 0

    def test_missing_numeric_fields_filled_with_zero(self):
        df = pd.DataFrame({
            "candidate_id": ["a"],
        })
        result = normalize_search_burden_frame(df)
        for field in SEARCH_BURDEN_NUMERIC_FIELDS:
            assert result.loc[0, field] == 0

    def test_null_numeric_values_filled_with_zero(self):
        df = pd.DataFrame({
            "candidate_id": ["a"],
            "search_candidates_generated": [None],
        })
        result = normalize_search_burden_frame(df)
        assert result.loc[0, "search_candidates_generated"] == 0


class TestMergeSearchBurdenColumns:
    """Test merging search-burden columns into DataFrames."""

    def test_missing_columns_filled_from_defaults(self):
        df = pd.DataFrame({"candidate_id": ["a"]})
        defaults = default_search_burden_dict()
        result = merge_search_burden_columns(df, defaults=defaults)
        for field in SEARCH_BURDEN_FIELDS:
            assert field in result.columns

    def test_existing_values_not_overwritten(self):
        df = pd.DataFrame({
            "candidate_id": ["a"],
            "search_candidates_generated": [42],
        })
        result = merge_search_burden_columns(df)
        assert result.loc[0, "search_candidates_generated"] == 42

    def test_idempotent(self):
        df = pd.DataFrame({"candidate_id": ["a"]})
        result1 = merge_search_burden_columns(df)
        result2 = merge_search_burden_columns(result1)
        for field in SEARCH_BURDEN_FIELDS:
            assert result1.loc[0, field] == result2.loc[0, field]


class TestBuildSearchBurdenSummary:
    """Test search-burden summary construction."""

    def test_build_with_all_fields(self):
        summary = build_search_burden_summary(
            proposals_attempted=100,
            candidates_generated=50,
            candidates_scored=40,
            candidates_eligible=20,
            parameterizations_attempted=30,
            mutations_attempted=10,
            directions_tested=5,
            confirmations_attempted=8,
            trigger_variants_attempted=3,
            family_count=10,
            lineage_count=15,
        )
        assert summary["search_proposals_attempted"] == 100
        assert summary["search_candidates_generated"] == 50
        assert summary["search_candidates_eligible"] == 20
        assert summary["search_family_count"] == 10
        assert summary["search_lineage_count"] == 15
        assert summary["search_burden_estimated"] is False

    def test_estimated_mode(self):
        summary = build_search_burden_summary(estimated=True)
        assert summary["search_burden_estimated"] is True

    def test_crowded_families_included(self):
        summary = build_search_burden_summary(
            crowded_families=["f1", "f2"],
            crowded_lineages=["L1"],
        )
        assert summary["crowded_families"] == ["f1", "f2"]
        assert summary["crowded_lineages"] == ["L1"]


class TestWriteAndLoadSearchBurdenSummary:
    """Test artifact emission and loading."""

    def test_write_and_load_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            summary = build_search_burden_summary(
                proposals_attempted=100,
                candidates_generated=50,
                family_count=10,
                lineage_count=15,
            )
            paths = write_search_burden_summary(summary, tmpdir)
            
            assert Path(paths["json_path"]).exists()
            assert Path(paths["md_path"]).exists()
            
            loaded = load_search_burden_summary(tmpdir)
            assert loaded is not None
            assert loaded["search_proposals_attempted"] == 100
            assert loaded["search_candidates_generated"] == 50

    def test_load_returns_none_when_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = load_search_burden_summary(tmpdir)
            assert result is None

    def test_md_contains_summary_sections(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            summary = build_search_burden_summary(
                proposals_attempted=100,
                candidates_generated=50,
                family_count=10,
                lineage_count=15,
                crowded_families=["f1"],
                crowded_lineages=["L1"],
            )
            paths = write_search_burden_summary(summary, tmpdir)
            md_content = Path(paths["md_path"]).read_text(encoding="utf-8")
            
            assert "## Totals" in md_content
            assert "## Scope" in md_content
            assert "## Crowded Families" in md_content
            assert "## Crowded Lineages" in md_content
            assert "`f1`" in md_content
            assert "`L1`" in md_content


class TestEvidenceBundleSearchBurden:
    """Test evidence bundle search-burden propagation."""

    def test_bundle_to_flat_record_includes_search_burden(self):
        from project.research.validation.evidence_bundle import bundle_to_flat_record
        
        bundle = {
            "candidate_id": "test_1",
            "event_type": "EVENT_1",
            "run_id": "run_001",
            "sample_definition": {"n_events": 100, "symbol": "BTCUSDT"},
            "effect_estimates": {"estimate_bps": 5.0},
            "uncertainty_estimates": {"q_value": 0.03},
            "stability_tests": {"stability_score": 0.8, "sign_consistency": 0.9},
            "falsification_results": {"passes_control": True},
            "cost_robustness": {"cost_survival_ratio": 0.7},
            "multiplicity_adjustment": {"q_value_program": 0.04},
            "metadata": {"hypothesis_id": "h1"},
            "promotion_decision": {"promotion_status": "promoted", "promotion_track": "standard"},
            "policy_version": "test_v1",
            "bundle_version": "test_v1",
            "search_burden": {
                "search_proposals_attempted": 100,
                "search_candidates_generated": 50,
                "search_candidates_eligible": 20,
                "search_mutations_attempted": 10,
                "search_family_count": 5,
                "search_lineage_count": 8,
                "search_burden_estimated": False,
                "search_scope_version": "phase1_v1",
            },
        }
        
        record = bundle_to_flat_record(bundle)
        
        assert record["search_proposals_attempted"] == 100
        assert record["search_candidates_generated"] == 50
        assert record["search_candidates_eligible"] == 20
        assert record["search_mutations_attempted"] == 10
        assert record["search_family_count"] == 5
        assert record["search_lineage_count"] == 8
        assert record["search_burden_estimated"] is False
        assert record["search_scope_version"] == "phase1_v1"

    def test_bundle_to_flat_record_defaults_missing_fields(self):
        from project.research.validation.evidence_bundle import bundle_to_flat_record
        
        bundle = {
            "candidate_id": "test_1",
            "event_type": "EVENT_1",
            "run_id": "run_001",
            "sample_definition": {"n_events": 100},
            "effect_estimates": {},
            "uncertainty_estimates": {},
            "stability_tests": {},
            "falsification_results": {},
            "cost_robustness": {},
            "multiplicity_adjustment": {},
            "metadata": {},
            "promotion_decision": {"promotion_status": "rejected"},
            "policy_version": "test_v1",
            "bundle_version": "test_v1",
        }
        
        record = bundle_to_flat_record(bundle)
        
        assert record["search_proposals_attempted"] == 0
        assert record["search_candidates_generated"] == 0
        assert record["search_burden_estimated"] is False
        assert record["search_scope_version"] == "phase1_v1"


class TestSchemaPropagation:
    """Test schema normalization includes search-burden fields."""

    def test_promotion_audit_schema_has_search_burden(self):
        from project.contracts.schemas import get_schema_contract
        
        schema = get_schema_contract("promotion_audit")
        for field in SEARCH_BURDEN_FIELDS:
            assert field in schema.optional_columns, f"Missing {field} in promotion_audit schema"

    def test_promoted_candidates_schema_has_search_burden(self):
        from project.contracts.schemas import get_schema_contract
        
        schema = get_schema_contract("promoted_candidates")
        for field in SEARCH_BURDEN_FIELDS:
            assert field in schema.optional_columns, f"Missing {field} in promoted_candidates schema"

    def test_evidence_bundle_summary_schema_has_search_burden(self):
        from project.contracts.schemas import get_schema_contract
        
        schema = get_schema_contract("evidence_bundle_summary")
        for field in SEARCH_BURDEN_FIELDS:
            assert field in schema.optional_columns, f"Missing {field} in evidence_bundle_summary schema"


class TestLegacyArtifactCompatibility:
    """Test backward compatibility with legacy artifacts."""

    def test_normalize_dataframe_adds_missing_search_burden(self):
        from project.contracts.schemas import normalize_dataframe_for_schema
        
        df = pd.DataFrame({
            "candidate_id": ["a"],
            "event_type": ["EVENT_1"],
            "promotion_decision": ["promoted"],
            "promotion_track": ["standard"],
        })
        result = normalize_dataframe_for_schema(df, "promotion_audit")
        
        for field in SEARCH_BURDEN_FIELDS:
            assert field in result.columns

    def test_estimated_mode_for_reconstructed_late_burden(self):
        burden = default_search_burden_dict(estimated=True)
        assert burden["search_burden_estimated"] is True
        for field in SEARCH_BURDEN_NUMERIC_FIELDS:
            assert burden[field] == 0


class TestMultiLineageMultiFamily:
    """Test multi-lineage and multi-family counts."""

    def test_build_summary_with_multi_lineage(self):
        summary = build_search_burden_summary(
            family_count=15,
            lineage_count=42,
        )
        assert summary["search_family_count"] == 15
        assert summary["search_lineage_count"] == 42

    def test_crowded_families_and_lineages_tracked(self):
        summary = build_search_burden_summary(
            crowded_families=["f1", "f2", "f3"],
            crowded_lineages=["L1", "L2"],
            repeated_failure_lineages=["L3"],
        )
        assert len(summary["crowded_families"]) == 3
        assert len(summary["crowded_lineages"]) == 2
        assert len(summary["repeated_failure_lineages"]) == 1
