"""
Tests for Phase 6 — Advanced Trigger Discovery Lane.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from project.domain.hypotheses import HypothesisSpec, TriggerType
from project.research.trigger_discovery.candidate_generation import (
    generate_parameter_sweep,
    generate_feature_clusters,
    TriggerProposal,
)
from project.research.trigger_discovery.candidate_clustering import (
    extract_excursions,
    cluster_excursions,
)
from project.research.trigger_discovery.registry_comparison import compute_registry_overlaps
from project.research.trigger_discovery.candidate_scoring import score_trigger_candidates
from project.research.trigger_discovery.proposal_emission import emit_proposals, generate_suggested_registry_payload


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def dummy_features() -> pd.DataFrame:
    timestamps = pd.date_range("2024-01-01", periods=100, freq="5min", tz="UTC")
    df = pd.DataFrame({
        "timestamp": timestamps,
        "symbol": ["BTCUSDT"] * 100,
        "close": 100.0 + np.sin(np.linspace(0, 10, 100)) * 10,  # some variance
        "rv_pct": np.random.normal(0.01, 0.002, 100),
        "spread_bps": np.random.normal(2.0, 0.5, 100),
        "split_label": ["train"] * 60 + ["test"] * 40,
    })
    
    # Inject excursions after min_periods (20)
    df.loc[50:55, "rv_pct"] = 0.5   # Massive vol shock
    df.loc[50:55, "spread_bps"] = 25.0 # Contiguous spread shock
    
    df.loc[70:72, "rv_pct"] = 0.4 
    
    return df


@pytest.fixture
def dummy_proposal() -> TriggerProposal:
    from project.domain.hypotheses import TriggerSpec
    spec = HypothesisSpec(
        trigger=TriggerSpec.feature_predicate("rv_pct", "==", 1.0),
        direction="long",
        horizon="12b",
        template_id="continuation"
    )
    return TriggerProposal(
        candidate_trigger_id="cand_1",
        source_lane="test_lane",
        detector_family="vol_shock",
        parameterization={"z_threshold": 3.0},
        dominant_features=["rv_pct"],
        suggested_trigger_name="TEST_SHOCK",
        spec=spec
    )


# ---------------------------------------------------------------------------
# Lane A: Parameter Sweep Tests
# ---------------------------------------------------------------------------

class TestParameterSweepGeneration:
    def test_parameter_sweep_generates_correct_proposals(self, dummy_features):
        grid = {"vol_shock": {"z_threshold": [1.0, 2.0]}}
        proposals, feat_out = generate_parameter_sweep(dummy_features, grid)
        
        # We asked for 2 thresholds
        assert len(proposals) == 2
        
        p1: TriggerProposal = proposals[0]
        assert p1.detector_family == "vol_shock"
        assert "z_threshold" in p1.parameterization
        
        # Ensure they target mask columns generated in feat_out
        mask_col = p1.spec.trigger.feature
        assert mask_col in feat_out.columns
        assert feat_out[mask_col].dtype == bool

    def test_parameter_sweep_graceful_on_missing_underlying(self, dummy_features):
        # Rename rv_pct to break proxy lookup
        df = dummy_features.rename(columns={"rv_pct": "unrelated"})
        grid = {"vol_shock": {"z_threshold": [2.0]}}
        proposals, feat_out = generate_parameter_sweep(df, grid)
        
        assert len(proposals) == 0


# ---------------------------------------------------------------------------
# Lane B: Feature Extraction & Clustering
# ---------------------------------------------------------------------------

class TestFeatureClustering:
    def test_extract_excursions_detects_spikes(self, dummy_features):
        out = extract_excursions(dummy_features, ["rv_pct", "spread_bps"], threshold_z=2.0)
        assert not out.empty
        assert "any_excursion" in out.columns
        
        # Index 50-55 should be captured (past min_periods=20)
        assert out.loc[52, "any_excursion"] == True
        assert out.loc[52, "rv_pct"] == True

    def test_cluster_excursions_groups_signatures(self, dummy_features):
        excursions = pd.DataFrame({
            "rv_pct": [True, True, True, False, False, False, True, True, True],
            "spread_bps": [True, True, True, False, False, False, False, False, False],
            "any_excursion": [True] * 3 + [False] * 3 + [True] * 3
        })
        
        # 3 hits of (rv_pct, spread_bps), 3 hits of (rv_pct)
        clusters = cluster_excursions(excursions, ["rv_pct", "spread_bps"], min_support=2)
        
        assert len(clusters) == 2
        counts = {c["suggested_trigger_family"]: c["support_count"] for c in clusters}
        assert 3 in counts.values()
        
    def test_generate_feature_clusters_e2e(self, dummy_features):
        proposals, feat_out = generate_feature_clusters(
            dummy_features, ["rv_pct", "spread_bps"], min_support=2
        )
        assert len(proposals) > 0
        
        p = proposals[0]
        assert p.source_lane == "feature_cluster"
        assert len(p.dominant_features) > 0
        
        mask_col = p.spec.trigger.feature
        assert mask_col in feat_out.columns


# ---------------------------------------------------------------------------
# Registry Comparison Tests
# ---------------------------------------------------------------------------

class TestRegistryComparison:
    def test_compute_registry_overlaps_returns_similarity(self, dummy_features):
        # We manually inject a canonical event footprint to test overlap
        from project.events.event_specs import EVENT_REGISTRY_SPECS
        from unittest.mock import Mock
        
        mock_spec = Mock()
        mock_spec.signal_column = "test_canonical_event"
        
        # Create dummy footprint
        dummy_features["test_canonical_event"] = False
        dummy_features.loc[10:15, "test_canonical_event"] = True
        
        # Mock the registry temporarily mapping an event to this column
        with patch.dict(EVENT_REGISTRY_SPECS, {"TEST_CANONICAL": mock_spec}):
            
            # Perfect overlap proposal
            proposal_mask = dummy_features["test_canonical_event"].copy()
            res = compute_registry_overlaps(proposal_mask, dummy_features)
            
            assert res["registry_similarity_score"] == 1.0
            assert res["registry_redundancy_flag"] is True
            assert res["nearest_existing_trigger_id"] == "TEST_CANONICAL"
            
            # Partial overlap proposal
            partial_mask = pd.Series(False, index=dummy_features.index)
            partial_mask.loc[10:12] = True
            partial_mask.loc[20:25] = True
            res_partial = compute_registry_overlaps(partial_mask, dummy_features)
            
            assert 0.0 < res_partial["registry_similarity_score"] < 1.0
            
            # Distinct proposal
            distant_mask = pd.Series(False, index=dummy_features.index)
            distant_mask.loc[50:60] = True
            res_dist = compute_registry_overlaps(distant_mask, dummy_features)
            
            assert res_dist["registry_similarity_score"] == 0.0
            assert res_dist["registry_redundancy_flag"] is False


# ---------------------------------------------------------------------------
# Trigger Scoring Tests
# ---------------------------------------------------------------------------

class TestTriggerScoring:
    def test_score_trigger_candidates_populates_quality_metrics(self, dummy_features, dummy_proposal):
        # Needs at least 20 hits to pass default min_sample_size
        indices = list(range(1, 40))
        dummy_features.loc[indices, "rv_pct"] = 1.0
        
        # We expect CandidateScoring to dispatch to normal evaluator
        scored_df = score_trigger_candidates([dummy_proposal], dummy_features)
        
        if scored_df.empty:
            from project.research.search.evaluator import evaluate_hypothesis_batch
            m = evaluate_hypothesis_batch([dummy_proposal.spec], dummy_features)
            print(m.iloc[0].to_dict())
            
        assert not scored_df.empty
        assert "trigger_candidate_quality_score" in scored_df.columns
        assert "fold_stability_score" in scored_df.columns
        assert "lineage_burden_penalty" in scored_df.columns
        
        score = scored_df.loc[0, "trigger_candidate_quality_score"]
        # As long as score is formulated
        assert isinstance(score, float)
        assert scored_df.loc[0, "suggested_trigger_name"] == dummy_proposal.suggested_trigger_name

    def test_empty_input_returns_empty_df(self):
        scored_df = score_trigger_candidates([], pd.DataFrame())
        assert scored_df.empty


# ---------------------------------------------------------------------------
# Emission Tests
# ---------------------------------------------------------------------------

class TestProposalEmission:
    def test_generate_suggested_registry_payload(self):
        row = pd.Series({
            "suggested_trigger_name": "TEST_CLUSTER",
            "detector_family": "excursion_cluster",
            "parameterization": {"threshold": 2.5},
            "source_lane": "feature_mining"
        })
        payload = generate_suggested_registry_payload(row)
        
        assert payload["event_type"] == "TEST_CLUSTER"
        assert payload["governance"]["operational_role"] == "candidate_trigger"
        assert payload["governance"]["deployment_disposition"] == "pending_manual_review"

    def test_emit_proposals_creates_files(self, tmp_path):
        import json
        scored = pd.DataFrame([{
            "candidate_trigger_id": "c1",
            "suggested_trigger_name": "MINED_A",
            "detector_family": "sweep",
            "trigger_candidate_quality_score": 1.5,
            "support_count": 50,
            "parameterization": {"z": 2},
            "warnings": ""
        }])
        
        emit_proposals(scored, tmp_path)
        
        assert (tmp_path / "candidate_trigger_proposals.jsonl").exists()
        assert (tmp_path / "candidate_trigger_scored.parquet").exists()
        assert (tmp_path / "candidate_trigger_report.md").exists()
        
        with open(tmp_path / "candidate_trigger_proposals.jsonl") as f:
            lines = f.readlines()
            assert len(lines) == 1
            data = json.loads(lines[0])
            assert "suggested_registry_payload" in data
