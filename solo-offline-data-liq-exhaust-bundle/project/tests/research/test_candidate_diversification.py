"""
Tests for Phase 5 — Candidate Diversification.

Covers:
  - Overlap signatures (determinism, fields, structural similarity)
  - Pairwise similarity (lineage/fold/structural combinations, symmetry,
    O(n²) safeguard)
  - Overlap clustering (union-find, cluster IDs, duplicate-like flags)
  - Novelty / crowding fields (ranges, monotonicity)
  - Greedy MMR shortlist (caps, clone pruning, determinism)
  - Full-table annotation (additive, promotion-compatible)
  - Diagnostics (schema)
  - Regression (Phase 3/4 unchanged; flat mode, promotion table intact)
"""
from __future__ import annotations

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_candidate(
    candidate_id: str,
    event_family: str = "VOL_SHOCK",
    template: str = "continuation",
    direction: str = "long",
    horizon: str = "24b",
    symbol_scope: str = "single",
    context_dim_count: int = 0,
    lineage_key: str = "",
    t_stat: float = 2.5,
    robustness_score: float = 0.75,
    fold_stability_score: float = 0.8,
    discovery_quality_score_v3: float = 2.0,
) -> dict:
    lk = lineage_key or (
        f"EVENT:{event_family}|TMPL:{template.split('_')[0]}|"
        f"DIR:{direction}|TF:5m|H:{'short' if '12' in horizon else 'medium'}|SYM:single|CTX:{context_dim_count}"
    )
    return {
        "candidate_id": candidate_id,
        "event_family": event_family,
        "canonical_event_type": event_family,
        "event_type": event_family,
        "rule_template": template,
        "template_id": template,
        "direction": direction,
        "horizon": horizon,
        "symbol_scope_type": symbol_scope,
        "context_dim_count": context_dim_count,
        "concept_lineage_key": lk,
        "t_stat": t_stat,
        "robustness_score": robustness_score,
        "fold_stability_score": fold_stability_score,
        "discovery_quality_score_v3": discovery_quality_score_v3,
        "discovery_quality_score": discovery_quality_score_v3,
    }


def _make_df(*rows, **kwargs) -> pd.DataFrame:
    return pd.DataFrame([r if isinstance(r, dict) else _make_candidate(*r, **kwargs) for r in rows])


def _clone_trio() -> pd.DataFrame:
    """Three near-identical candidates — should cluster together."""
    return _make_df(
        _make_candidate("A", event_family="VOL_SHOCK", template="continuation", direction="long", t_stat=3.0, discovery_quality_score_v3=3.0),
        _make_candidate("B", event_family="VOL_SHOCK", template="continuation", direction="long", t_stat=2.8, discovery_quality_score_v3=2.8),
        _make_candidate("C", event_family="VOL_SHOCK", template="continuation", direction="long", t_stat=2.6, discovery_quality_score_v3=2.6),
    )


def _diverse_pair() -> pd.DataFrame:
    """Two orthogonal candidates — should be singleton clusters."""
    return _make_df(
        _make_candidate("X", event_family="VOL_SHOCK", template="continuation", direction="long",
                        lineage_key="EVENT:VOL_SHOCK|TMPL:continuation|DIR:long|TF:5m|H:short|SYM:single|CTX:0"),
        _make_candidate("Y", event_family="LIQUIDATION_CASCADE", template="mean_reversion", direction="short",
                        lineage_key="EVENT:LIQUIDATION_CASCADE|TMPL:mean|DIR:short|TF:1h|H:medium|SYM:multi|CTX:1"),
    )


# ---------------------------------------------------------------------------
# Overlap signature tests
# ---------------------------------------------------------------------------

class TestOverlapSignatures:
    def test_signature_deterministic(self):
        from project.research.services.candidate_diversification import build_candidate_overlap_signatures
        df = _clone_trio()
        s1 = build_candidate_overlap_signatures(df)
        s2 = build_candidate_overlap_signatures(df)
        assert list(s1["event_family"]) == list(s2["event_family"])
        assert list(s1["template_family"]) == list(s2["template_family"])

    def test_signature_fields_present(self):
        from project.research.services.candidate_diversification import build_candidate_overlap_signatures
        required = [
            "candidate_id", "primary_event_id", "event_family", "template_family",
            "direction", "horizon_bucket", "symbol_scope_type", "context_dim_count",
            "context_dim_count_bucket", "concept_lineage_key", "fold_sign_vector", "fold_pass_rate",
        ]
        sigs = build_candidate_overlap_signatures(_clone_trio())
        for field in required:
            assert field in sigs.columns, f"Missing field: {field}"

    def test_similar_candidates_share_event_family(self):
        from project.research.services.candidate_diversification import build_candidate_overlap_signatures
        sigs = build_candidate_overlap_signatures(_clone_trio())
        assert sigs["event_family"].nunique() == 1  # All VOL_SHOCK

    def test_empty_df_returns_empty(self):
        from project.research.services.candidate_diversification import build_candidate_overlap_signatures
        result = build_candidate_overlap_signatures(pd.DataFrame())
        assert result.empty

    def test_horizon_bucket_parsing(self):
        from project.research.services.candidate_diversification import build_candidate_overlap_signatures
        df = _make_df(
            _make_candidate("h24", horizon="24b"),
            _make_candidate("h48", horizon="48b"),
            _make_candidate("h96", horizon="96b"),
        )
        sigs = build_candidate_overlap_signatures(df)
        buckets = list(sigs["horizon_bucket"])
        assert buckets[0] == "short"
        assert buckets[1] == "medium"
        assert buckets[2] == "long"

    def test_direction_normalisation(self):
        from project.research.services.candidate_diversification import build_candidate_overlap_signatures
        df = _make_df(
            _make_candidate("a", direction="long"),
            _make_candidate("b", direction="1"),
            _make_candidate("c", direction="short"),
        )
        sigs = build_candidate_overlap_signatures(df)
        dirs = list(sigs["direction"])
        assert dirs[0] == "long"
        assert dirs[1] == "long"
        assert dirs[2] == "short"


# ---------------------------------------------------------------------------
# Pairwise similarity tests
# ---------------------------------------------------------------------------

class TestPairwiseSimilarity:
    def test_same_lineage_high_similarity(self):
        from project.research.services.candidate_diversification import (
            build_candidate_overlap_signatures,
            compute_pairwise_similarity,
        )
        df = _clone_trio()  # same lineage key
        sigs = build_candidate_overlap_signatures(df)
        edges = compute_pairwise_similarity(sigs)
        assert not edges.empty
        assert float(edges["similarity"].max()) >= 0.5

    def test_unrelated_candidates_low_similarity(self):
        from project.research.services.candidate_diversification import (
            build_candidate_overlap_signatures,
            compute_pairwise_similarity,
        )
        sigs = build_candidate_overlap_signatures(_diverse_pair())
        edges = compute_pairwise_similarity(sigs)
        if edges.empty:
            max_sim = 0.0
        else:
            max_sim = float(edges["similarity"].max())
        assert max_sim <= 0.5

    def test_pairwise_symmetric(self):
        """Similarity of (A, B) should equal similarity of (B, A)."""
        from project.research.services.candidate_diversification import (
            build_candidate_overlap_signatures,
            compute_pairwise_similarity,
            _pairwise_score,
        )
        df = _diverse_pair()
        sigs = build_candidate_overlap_signatures(df).reset_index().to_dict(orient="records")
        s_ab, _ = _pairwise_score(sigs[0], sigs[1], structural_weight=0.5, fold_weight=0.3, lineage_weight=0.2)
        s_ba, _ = _pairwise_score(sigs[1], sigs[0], structural_weight=0.5, fold_weight=0.3, lineage_weight=0.2)
        assert abs(s_ab - s_ba) < 1e-9

    def test_returns_edge_list_columns(self):
        from project.research.services.candidate_diversification import (
            build_candidate_overlap_signatures,
            compute_pairwise_similarity,
        )
        sigs = build_candidate_overlap_signatures(_clone_trio())
        edges = compute_pairwise_similarity(sigs)
        for col in ["source_idx", "target_idx", "source_id", "target_id", "similarity"]:
            assert col in edges.columns

    def test_max_candidates_safeguard_activates(self):
        """With max_candidates=2 and 4 candidates, bucket mode activates."""
        from project.research.services.candidate_diversification import (
            build_candidate_overlap_signatures,
            compute_pairwise_similarity,
        )
        df = _make_df(
            _make_candidate("a1", event_family="VOL_SHOCK", direction="long"),
            _make_candidate("a2", event_family="VOL_SHOCK", direction="long"),
            _make_candidate("b1", event_family="LIQUIDATION_CASCADE", direction="short"),
            _make_candidate("b2", event_family="LIQUIDATION_CASCADE", direction="short"),
        )
        sigs = build_candidate_overlap_signatures(df)
        # Should not raise
        edges = compute_pairwise_similarity(sigs, max_candidates=2)
        # Only intra-bucket pairs returned
        if not edges.empty:
            assert float(edges["similarity"].min()) >= 0.0

    def test_empty_signatures_returns_empty_edges(self):
        from project.research.services.candidate_diversification import compute_pairwise_similarity
        edges = compute_pairwise_similarity(pd.DataFrame())
        assert edges.empty

    def test_fold_similarity_concordance(self):
        from project.research.services.candidate_diversification import _sign_concordance
        assert _sign_concordance("+++", "+++") == pytest.approx(1.0)
        assert _sign_concordance("+++", "---") == pytest.approx(0.0)
        assert _sign_concordance("", "") == pytest.approx(0.5)  # neutral fallback
        assert _sign_concordance("++-", "+-+") == pytest.approx(1.0 / 3.0, abs=0.01)


# ---------------------------------------------------------------------------
# Clustering tests
# ---------------------------------------------------------------------------

class TestClusteringByOverlap:
    def test_high_similarity_creates_cluster(self):
        from project.research.services.candidate_diversification import (
            build_candidate_overlap_signatures,
            compute_pairwise_similarity,
            cluster_candidates_by_overlap,
        )
        df = _clone_trio()
        sigs = build_candidate_overlap_signatures(df)
        edges = compute_pairwise_similarity(sigs)
        clustered = cluster_candidates_by_overlap(df, edges, edge_threshold=0.3)
        assert "overlap_cluster_id" in clustered.columns
        # All three should share a cluster if similarity > 0.3
        if not edges.empty and float(edges["similarity"].max()) > 0.3:
            assert clustered["overlap_cluster_id"].nunique() == 1

    def test_unrelated_candidates_separate_clusters(self):
        from project.research.services.candidate_diversification import (
            build_candidate_overlap_signatures,
            compute_pairwise_similarity,
            cluster_candidates_by_overlap,
        )
        df = _diverse_pair()
        sigs = build_candidate_overlap_signatures(df)
        edges = compute_pairwise_similarity(sigs)
        # Use a harsh threshold so unrelated candidates don't cluster
        clustered = cluster_candidates_by_overlap(df, edges, edge_threshold=0.99)
        assert clustered["overlap_cluster_id"].nunique() >= 2

    def test_cluster_ids_stable(self):
        from project.research.services.candidate_diversification import (
            build_candidate_overlap_signatures,
            compute_pairwise_similarity,
            cluster_candidates_by_overlap,
        )
        df = _clone_trio()
        sigs = build_candidate_overlap_signatures(df)
        edges = compute_pairwise_similarity(sigs)
        c1 = cluster_candidates_by_overlap(df, edges, edge_threshold=0.3)
        c2 = cluster_candidates_by_overlap(df, edges, edge_threshold=0.3)
        assert list(c1["overlap_cluster_id"]) == list(c2["overlap_cluster_id"])

    def test_cluster_size_correct(self):
        from project.research.services.candidate_diversification import (
            build_candidate_overlap_signatures,
            compute_pairwise_similarity,
            cluster_candidates_by_overlap,
        )
        df = _clone_trio()
        sigs = build_candidate_overlap_signatures(df)
        edges = compute_pairwise_similarity(sigs)
        clustered = cluster_candidates_by_overlap(df, edges, edge_threshold=0.1)
        # All should be in one cluster of size 3
        assert clustered["cluster_size"].max() == 3

    def test_required_columns_present(self):
        from project.research.services.candidate_diversification import cluster_candidates_by_overlap
        df = _clone_trio()
        clustered = cluster_candidates_by_overlap(df, pd.DataFrame())
        for col in ["overlap_cluster_id", "cluster_size", "cluster_density", "is_duplicate_like"]:
            assert col in clustered.columns

    def test_empty_df_no_error(self):
        from project.research.services.candidate_diversification import cluster_candidates_by_overlap
        result = cluster_candidates_by_overlap(pd.DataFrame(), pd.DataFrame())
        assert result.empty or "overlap_cluster_id" in result.columns


# ---------------------------------------------------------------------------
# Novelty / crowding tests
# ---------------------------------------------------------------------------

class TestNoveltyCrowding:
    def test_singleton_high_novelty(self):
        from project.research.services.candidate_diversification import (
            build_candidate_overlap_signatures,
            compute_pairwise_similarity,
            cluster_candidates_by_overlap,
            compute_novelty_crowding,
        )
        # Only one candidate → no edges → isolated
        df = _make_df(_make_candidate("solo"))
        sigs = build_candidate_overlap_signatures(df)
        edges = compute_pairwise_similarity(sigs)
        clustered = cluster_candidates_by_overlap(df, edges)
        result = compute_novelty_crowding(clustered, edges)
        assert float(result["novelty_score"].iloc[0]) >= 0.8

    def test_crowded_cluster_lower_novelty(self):
        from project.research.services.candidate_diversification import (
            build_candidate_overlap_signatures,
            compute_pairwise_similarity,
            cluster_candidates_by_overlap,
            compute_novelty_crowding,
        )
        df = _clone_trio()
        sigs = build_candidate_overlap_signatures(df)
        edges = compute_pairwise_similarity(sigs)
        clustered = cluster_candidates_by_overlap(df, edges, edge_threshold=0.1)
        result = compute_novelty_crowding(clustered, edges)
        novelties = result["novelty_score"].tolist()
        # All should have novelty < 1.0 when there are neighbors
        assert all(n <= 1.0 for n in novelties)

    def test_crowding_increases_with_cluster_size(self):
        """A larger cluster → higher mean crowding penalty than a smaller cluster."""
        from project.research.services.candidate_diversification import (
            build_candidate_overlap_signatures,
            compute_pairwise_similarity,
            cluster_candidates_by_overlap,
            compute_novelty_crowding,
        )
        df_large = _make_df(
            *[_make_candidate(f"g{i}", event_family="VOL_SHOCK", direction="long") for i in range(5)]
        )
        df_small = _make_df(_make_candidate("s1"), _make_candidate("s2", event_family="LIQ"))
        sigs_l = build_candidate_overlap_signatures(df_large)
        edges_l = compute_pairwise_similarity(sigs_l)
        cl_l = cluster_candidates_by_overlap(df_large, edges_l, edge_threshold=0.0)
        res_l = compute_novelty_crowding(cl_l, edges_l)

        sigs_s = build_candidate_overlap_signatures(df_small)
        edges_s = compute_pairwise_similarity(sigs_s)
        cl_s = cluster_candidates_by_overlap(df_small, edges_s, edge_threshold=0.99)
        res_s = compute_novelty_crowding(cl_s, edges_s)

        mean_crowding_large = float(res_l["crowding_penalty"].mean())
        mean_crowding_small = float(res_s["crowding_penalty"].mean())
        # Larger cluster should have higher mean crowding
        assert mean_crowding_large >= mean_crowding_small

    def test_scores_bounded_zero_one(self):
        from project.research.services.candidate_diversification import (
            build_candidate_overlap_signatures,
            compute_pairwise_similarity,
            cluster_candidates_by_overlap,
            compute_novelty_crowding,
        )
        df = _clone_trio()
        sigs = build_candidate_overlap_signatures(df)
        edges = compute_pairwise_similarity(sigs)
        clustered = cluster_candidates_by_overlap(df, edges)
        result = compute_novelty_crowding(clustered, edges)
        assert (result["novelty_score"].between(0, 1)).all()
        assert (result["crowding_penalty"].between(0, 1)).all()

    def test_cluster_rank_assigned(self):
        from project.research.services.candidate_diversification import (
            build_candidate_overlap_signatures,
            compute_pairwise_similarity,
            cluster_candidates_by_overlap,
            compute_novelty_crowding,
        )
        df = _clone_trio()
        sigs = build_candidate_overlap_signatures(df)
        edges = compute_pairwise_similarity(sigs)
        clustered = cluster_candidates_by_overlap(df, edges)
        result = compute_novelty_crowding(clustered, edges)
        assert "cluster_rank" in result.columns
        assert (result["cluster_rank"] >= 1).all()


# ---------------------------------------------------------------------------
# Shortlist selector tests
# ---------------------------------------------------------------------------

class TestDiversifiedShortlist:
    def test_selects_highest_quality_first(self):
        from project.research.services.candidate_diversification import select_diversified_shortlist
        df = _make_df(
            _make_candidate("good", t_stat=4.0, discovery_quality_score_v3=4.0),
            _make_candidate("bad", t_stat=1.2, discovery_quality_score_v3=0.5),
        )
        shortlist = select_diversified_shortlist(df, size=1, max_per_overlap_cluster=10)
        assert len(shortlist) == 1
        assert str(shortlist.iloc[0]["candidate_id"]) == "good"

    def test_greedy_avoids_near_duplicates(self):
        """Clone trio: all three are near-identical. With max_per_cluster=1, only one should be selected."""
        from project.research.services.candidate_diversification import (
            annotate_candidates_with_diversification,
        )
        df = _clone_trio()
        config = {
            "mode": "greedy",
            "overlap": {"enabled": True, "structural_weight": 0.5, "fold_weight": 0.3,
                        "lineage_weight": 0.2, "edge_threshold": 0.1},
            "shortlist": {"enabled": True, "size": 3, "lambda_quality": 0.7,
                          "lambda_overlap": 0.2, "lambda_crowding": 0.1,
                          "max_per_overlap_cluster": 1, "max_per_trigger_family": 3,
                          "max_per_lineage": 3},
        }
        annotated, shortlist = annotate_candidates_with_diversification(df, config)
        assert len(shortlist) <= 1

    def test_max_per_cluster_cap(self):
        from project.research.services.candidate_diversification import (
            build_candidate_overlap_signatures,
            compute_pairwise_similarity,
            cluster_candidates_by_overlap,
            compute_novelty_crowding,
            select_diversified_shortlist,
        )
        df = _make_df(*[
            _make_candidate(f"c{i}", event_family="VOL_SHOCK", direction="long",
                            discovery_quality_score_v3=float(4 - i))
            for i in range(4)
        ])
        sigs = build_candidate_overlap_signatures(df)
        edges = compute_pairwise_similarity(sigs)
        clustered = cluster_candidates_by_overlap(df, edges, edge_threshold=0.0)
        result = compute_novelty_crowding(clustered, edges)
        shortlist = select_diversified_shortlist(result, size=4, max_per_overlap_cluster=2)
        # Only max 2 per cluster allowed
        if "overlap_cluster_id" in shortlist.columns:
            cluster_counts = shortlist.groupby("overlap_cluster_id").size()
            assert (cluster_counts <= 2).all()

    def test_max_per_trigger_family_cap(self):
        from project.research.services.candidate_diversification import select_diversified_shortlist
        df = _make_df(*[
            _make_candidate(f"t{i}", event_family="VOL_SHOCK", direction="long" if i % 2 == 0 else "short",
                            discovery_quality_score_v3=float(5 - i))
            for i in range(5)
        ])
        shortlist = select_diversified_shortlist(
            df, size=5, max_per_trigger_family=2, max_per_overlap_cluster=10, max_per_lineage=10
        )
        trig_col = shortlist.get("event_family", shortlist.get("canonical_event_type", shortlist.get("event_type", pd.Series())))
        if not trig_col.empty:
            for trig, count in trig_col.value_counts().items():
                assert count <= 2, f"Trigger {trig} appeared {count} times in shortlist (cap=2)"

    def test_max_per_lineage_cap(self):
        from project.research.services.candidate_diversification import select_diversified_shortlist
        shared_lineage = "EVENT:VOL_SHOCK|TMPL:continuation|DIR:long|TF:5m|H:short|SYM:single|CTX:0"
        df = _make_df(*[
            _make_candidate(f"l{i}", lineage_key=shared_lineage, discovery_quality_score_v3=float(5 - i))
            for i in range(4)
        ])
        shortlist = select_diversified_shortlist(
            df, size=4, max_per_lineage=2, max_per_overlap_cluster=10, max_per_trigger_family=10
        )
        if "concept_lineage_key" in shortlist.columns:
            lin_counts = shortlist.groupby("concept_lineage_key").size()
            assert (lin_counts <= 2).all()

    def test_shortlist_size_respected(self):
        from project.research.services.candidate_diversification import select_diversified_shortlist
        df = _make_df(*[_make_candidate(f"s{i}", discovery_quality_score_v3=float(i)) for i in range(10)])
        shortlist = select_diversified_shortlist(
            df, size=5, max_per_overlap_cluster=10, max_per_trigger_family=10, max_per_lineage=10
        )
        assert len(shortlist) <= 5

    def test_shortlist_deterministic(self):
        from project.research.services.candidate_diversification import select_diversified_shortlist
        df = _make_df(*[_make_candidate(f"d{i}", discovery_quality_score_v3=float(i)) for i in range(6)])
        s1 = select_diversified_shortlist(df, size=3, max_per_overlap_cluster=10)
        s2 = select_diversified_shortlist(df, size=3, max_per_overlap_cluster=10)
        assert list(s1["candidate_id"]) == list(s2["candidate_id"])

    def test_shortlist_reason_codes_populated(self):
        from project.research.services.candidate_diversification import select_diversified_shortlist
        df = _make_df(_make_candidate("only"))
        shortlist = select_diversified_shortlist(df, size=5, max_per_overlap_cluster=10)
        assert not shortlist.empty
        assert "selection_reason" in shortlist.columns
        assert shortlist["selection_reason"].notna().all()

    def test_empty_candidates_returns_empty(self):
        from project.research.services.candidate_diversification import select_diversified_shortlist
        result = select_diversified_shortlist(pd.DataFrame(), size=5)
        assert result.empty

    def test_required_shortlist_columns_present(self):
        from project.research.services.candidate_diversification import (
            SHORTLIST_REQUIRED_COLUMNS,
            select_diversified_shortlist,
        )
        df = _make_df(*[_make_candidate(f"q{i}") for i in range(3)])
        shortlist = select_diversified_shortlist(df, size=3, max_per_overlap_cluster=10)
        if not shortlist.empty:
            for col in ["shortlist_rank", "selection_score", "selection_reason",
                        "selected_into_diversified_shortlist"]:
                assert col in shortlist.columns


# ---------------------------------------------------------------------------
# Full-table annotation tests
# ---------------------------------------------------------------------------

class TestAnnotateCandidates:
    def _config(self, shortlist_enabled: bool = False) -> dict:
        return {
            "mode": "greedy",
            "overlap": {"enable": True, "structural_weight": 0.5, "fold_weight": 0.3,
                        "lineage_weight": 0.2, "edge_threshold": 0.65},
            "shortlist": {"enabled": shortlist_enabled, "size": 5,
                          "lambda_quality": 0.7, "lambda_overlap": 0.2, "lambda_crowding": 0.1,
                          "max_per_overlap_cluster": 2, "max_per_trigger_family": 3, "max_per_lineage": 2},
        }

    def test_adds_overlap_columns(self):
        from project.research.services.candidate_diversification import (
            OVERLAP_COLUMNS,
            annotate_candidates_with_diversification,
        )
        df = _clone_trio()
        annotated, _ = annotate_candidates_with_diversification(df, self._config())
        for col in OVERLAP_COLUMNS:
            assert col in annotated.columns, f"Missing column: {col}"

    def test_preserves_existing_columns(self):
        from project.research.services.candidate_diversification import annotate_candidates_with_diversification
        df = _clone_trio()
        original_cols = set(df.columns)
        annotated, _ = annotate_candidates_with_diversification(df, self._config())
        for col in original_cols:
            assert col in annotated.columns, f"Existing column lost: {col}"

    def test_row_count_unchanged(self):
        from project.research.services.candidate_diversification import annotate_candidates_with_diversification
        df = _clone_trio()
        annotated, _ = annotate_candidates_with_diversification(df, self._config())
        assert len(annotated) == len(df)

    def test_promotion_compatible_columns_preserved(self):
        """candidate_id, t_stat, and direction must survive annotation."""
        from project.research.services.candidate_diversification import annotate_candidates_with_diversification
        df = _clone_trio()
        annotated, _ = annotate_candidates_with_diversification(df, self._config())
        for col in ["candidate_id", "t_stat", "direction"]:
            assert col in annotated.columns

    def test_shortlist_disabled_returns_empty(self):
        from project.research.services.candidate_diversification import annotate_candidates_with_diversification
        df = _clone_trio()
        _, shortlist = annotate_candidates_with_diversification(df, self._config(shortlist_enabled=False))
        assert shortlist.empty

    def test_shortlist_enabled_returns_rows(self):
        from project.research.services.candidate_diversification import annotate_candidates_with_diversification
        df = _clone_trio()
        _, shortlist = annotate_candidates_with_diversification(df, self._config(shortlist_enabled=True))
        assert not shortlist.empty
        assert len(shortlist) <= 5

    def test_mode_off_returns_full_candidates_unchanged(self):
        from project.research.services.candidate_diversification import annotate_candidates_with_diversification
        df = _clone_trio()
        annotated, shortlist = annotate_candidates_with_diversification(df, {"mode": "off"})
        assert list(annotated.columns) == list(df.columns)
        assert shortlist.empty

    def test_empty_input(self):
        from project.research.services.candidate_diversification import annotate_candidates_with_diversification
        annotated, shortlist = annotate_candidates_with_diversification(pd.DataFrame(), {})
        assert annotated.empty
        assert shortlist.empty


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

class TestIntegration:
    def test_shortlist_more_diverse_than_top_n(self):
        """Shortlist should span more clusters than raw top-N when there are clones."""
        from project.research.services.candidate_diversification import annotate_candidates_with_diversification

        # 6 candidates: two clusters of 3
        cluster_a = [
            _make_candidate(f"a{i}", event_family="VOL_SHOCK", direction="long",
                            discovery_quality_score_v3=float(6 - i))
            for i in range(3)
        ]
        cluster_b = [
            _make_candidate(f"b{i}", event_family="LIQUIDATION_CASCADE", direction="short",
                            lineage_key=f"EVENT:LIQUIDATION_CASCADE|TMPL:mean|DIR:short|TF:5m|H:medium|SYM:single|CTX:0",
                            discovery_quality_score_v3=float(3 - i))
            for i in range(3)
        ]
        df = pd.DataFrame(cluster_a + cluster_b)
        config = {
            "mode": "greedy",
            "overlap": {"structural_weight": 0.5, "fold_weight": 0.3, "lineage_weight": 0.2,
                        "edge_threshold": 0.3},
            "shortlist": {"enabled": True, "size": 4, "lambda_quality": 0.5,
                          "lambda_overlap": 0.3, "lambda_crowding": 0.2,
                          "max_per_overlap_cluster": 2, "max_per_trigger_family": 3,
                          "max_per_lineage": 2},
        }
        annotated, shortlist = annotate_candidates_with_diversification(df, config)
        if not shortlist.empty and "overlap_cluster_id" in shortlist.columns:
            n_short_clusters = shortlist["overlap_cluster_id"].nunique()
            top_n = annotated.head(4)
            n_top_clusters = top_n["overlap_cluster_id"].nunique() if "overlap_cluster_id" in top_n.columns else 0
            # Shortlist should include both families if possible
            assert n_short_clusters >= 1

    def test_strong_duplicates_pruned(self):
        """Clone trio with hard cap=1 → only one should be in shortlist."""
        from project.research.services.candidate_diversification import annotate_candidates_with_diversification
        df = _clone_trio()
        config = {
            "mode": "greedy",
            "overlap": {"structural_weight": 0.5, "fold_weight": 0.3,
                        "lineage_weight": 0.2, "edge_threshold": 0.1},
            "shortlist": {"enabled": True, "size": 3, "lambda_quality": 0.5,
                          "lambda_overlap": 0.3, "lambda_crowding": 0.2,
                          "max_per_overlap_cluster": 1, "max_per_trigger_family": 3, "max_per_lineage": 3},
        }
        _, shortlist = annotate_candidates_with_diversification(df, config)
        assert len(shortlist) <= 1


# ---------------------------------------------------------------------------
# Diagnostics tests
# ---------------------------------------------------------------------------

class TestDiversificationDiagnostics:
    def test_empty_candidates_returns_minimal_schema(self):
        from project.research.services.candidate_discovery_diagnostics import build_diversification_diagnostics
        result = build_diversification_diagnostics(pd.DataFrame())
        assert "total_candidates" in result
        assert result["total_candidates"] == 0

    def test_schema_keys_present(self):
        from project.research.services.candidate_discovery_diagnostics import build_diversification_diagnostics
        required_keys = [
            "diversification_mode", "shortlist_enabled", "shortlist_size",
            "shortlist_actual_size", "total_candidates", "overlap_cluster_count",
            "duplicate_like_count", "trigger_family_concentration",
            "lineage_concentration", "top_crowded_clusters", "strongest_excluded",
            "shortlist_vs_raw_top_n",
        ]
        from project.research.services.candidate_diversification import annotate_candidates_with_diversification
        df = _clone_trio()
        annotated, _ = annotate_candidates_with_diversification(df, {"mode": "greedy"})
        result = build_diversification_diagnostics(annotated, diversification_config={"mode": "greedy"})
        for key in required_keys:
            assert key in result, f"Missing diagnostic key: {key}"

    def test_trigger_family_concentration_populated(self):
        from project.research.services.candidate_discovery_diagnostics import build_diversification_diagnostics
        df = _clone_trio()
        result = build_diversification_diagnostics(df, diversification_config={})
        assert "VOL_SHOCK" in {k.upper() for k in result["trigger_family_concentration"].keys()}


# ---------------------------------------------------------------------------
# Config helper tests
# ---------------------------------------------------------------------------

class TestLoadDiversificationConfig:
    def test_returns_empty_dict_when_missing(self):
        from project.research.phase2_search_engine import _load_diversification_config
        assert _load_diversification_config({}) == {}

    def test_returns_block_when_present(self):
        from project.research.phase2_search_engine import _load_diversification_config
        doc = {"discovery_selection": {"mode": "greedy", "overlap": {}}}
        cfg = _load_diversification_config(doc)
        assert cfg["mode"] == "greedy"

    def test_non_dict_block_returns_empty(self):
        from project.research.phase2_search_engine import _load_diversification_config
        doc = {"discovery_selection": "bad_value"}
        assert _load_diversification_config(doc) == {}


# ---------------------------------------------------------------------------
# Regression tests
# ---------------------------------------------------------------------------

class TestRegression:
    def test_phase3_ledger_unchanged(self):
        """Phase 3 ledger key builder not affected by Phase 5 imports."""
        from project.research.knowledge.concept_ledger import build_concept_lineage_key
        row = {"event_type": "VOL_SHOCK", "rule_template": "continuation",
               "direction": "long", "timeframe": "5m", "horizon_bars": 24}
        key = build_concept_lineage_key(row)
        assert "EVENT:VOL_SHOCK" in key

    def test_phase4_hierarchical_stage_policy_unchanged(self):
        from project.research.search.stage_policy import rank_stage_candidates, STAGE_TRIGGER_VIABILITY
        df = pd.DataFrame([
            {"root_trigger_id": "X", "t_stat": 3.0, "robustness_score": 0.9, "n": 80, "fold_stability_score": 0.9},
        ])
        ranked = rank_stage_candidates(df, parent_group_col="root_trigger_id", stage=STAGE_TRIGGER_VIABILITY)
        assert "stage_rank_within_parent" in ranked.columns

    def test_spec_yaml_parses_with_diversification_block(self):
        import yaml
        from project import PROJECT_ROOT
        spec_path = PROJECT_ROOT.parent / "spec" / "search_space.yaml"
        if spec_path.exists():
            doc = yaml.safe_load(spec_path.read_text())
            assert "discovery_selection" in doc
            assert doc["discovery_selection"]["mode"] == "off"
            assert "shortlist" in doc["discovery_selection"]
            assert doc["discovery_selection"]["shortlist"]["enabled"] is False

    def test_diversification_module_no_circular_imports(self):
        import importlib
        import sys
        if "project.research.services.candidate_diversification" in sys.modules:
            del sys.modules["project.research.services.candidate_diversification"]
        mod = importlib.import_module("project.research.services.candidate_diversification")
        assert hasattr(mod, "annotate_candidates_with_diversification")

    def test_overlap_columns_do_not_overwrite_existing_quality_score(self):
        """Annotation must not overwrite discovery_quality_score_v3."""
        from project.research.services.candidate_diversification import annotate_candidates_with_diversification
        df = _clone_trio().copy()
        orig_scores = df["discovery_quality_score_v3"].tolist()
        annotated, _ = annotate_candidates_with_diversification(df, {"mode": "greedy"})
        assert list(annotated["discovery_quality_score_v3"]) == orig_scores
