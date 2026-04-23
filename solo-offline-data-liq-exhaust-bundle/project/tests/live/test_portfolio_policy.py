from project.portfolio.admission_policy import PortfolioAdmissionPolicy


def test_resolve_overlap_winners_picks_highest_rank():
    policy = PortfolioAdmissionPolicy()
    candidates = [
        {
            "thesis_id": "T1",
            "overlap_group_id": "G1",
            "support_score": 0.5,
            "contradiction_penalty": 0.1,
            "sample_size": 100,
        },
        {
            "thesis_id": "T2",
            "overlap_group_id": "G1",
            "support_score": 0.6,
            "contradiction_penalty": 0.1,
            "sample_size": 50,
        },
    ]
    winners = policy.resolve_overlap_winners(candidates, active_groups=set())
    assert len(winners) == 1
    assert winners[0]["thesis_id"] == "T2"


def test_is_thesis_admissible_blocks_if_group_occupied():
    policy = PortfolioAdmissionPolicy()
    result = policy.is_thesis_admissible("T1", "G1", active_groups={"G1"})
    assert result.admissible is False
    assert result.reason == "blocked_by_active_group_member"
