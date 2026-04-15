from __future__ import annotations

import pandas as pd

from project.research.multiplicity import apply_multiplicity_controls


def test_hierarchical_fdr_simes_and_within_family_bh():
    rows = [
        {"candidate_id": "strong_signal", "family_id": "fam_a", "p_value": 0.001},
        {"candidate_id": "weak_signal", "family_id": "fam_a", "p_value": 0.20},
    ]
    for idx in range(1, 10):
        rows.append(
            {"candidate_id": f"noise_{idx}", "family_id": f"fam_{idx + 1}", "p_value": 0.90}
        )
    raw_df = pd.DataFrame(rows)

    out = apply_multiplicity_controls(raw_df=raw_df, max_q=0.05)
    strong = out[out["candidate_id"] == "strong_signal"].iloc[0]
    weak = out[out["candidate_id"] == "weak_signal"].iloc[0]
    noise = out[out["candidate_id"] == "noise_1"].iloc[0]

    # fam_a has Simes p-val = min(0.001*2/1, 0.20*2/2) = 0.002.
    # Global BH across 10 families => q_family = 0.002 * 10 / 1 = 0.02 <= 0.05.
    assert bool(strong["is_discovery_family"]) is True
    assert bool(weak["is_discovery_family"]) is True

    # Within fam_a: strong q = 0.001 * 2 / 1 = 0.002 <= 0.05
    assert bool(strong["is_discovery"]) is True
    # Within fam_a: weak q = 0.20 * 2 / 2 = 0.20 > 0.05
    assert bool(weak["is_discovery"]) is False

    # Noise families are completely discarded
    assert bool(noise["is_discovery_family"]) is False
    assert bool(noise["is_discovery"]) is False
    assert bool(strong["gate_multiplicity"]) is True
    assert bool(strong["gate_multiplicity_strict"]) is True
    assert int(strong["num_tests_family"]) == 2
    assert int(strong["num_tests_effective"]) == 2
    assert int(strong["num_tests_campaign"]) == 10


def test_apply_multiplicity_controls_research_excludes_low_sample_rows():
    raw_df = pd.DataFrame(
        [
            {
                "candidate_id": "low_sample",
                "family_id": "fam_a",
                "p_value": 1e-8,
                "sample_size": 10,
            },
            {"candidate_id": "eligible", "family_id": "fam_b", "p_value": 1e-3, "sample_size": 120},
            {"candidate_id": "noise", "family_id": "fam_c", "p_value": 0.9, "sample_size": 150},
        ]
    )

    out = apply_multiplicity_controls(
        raw_df=raw_df,
        max_q=0.05,
        mode="research",
        min_sample_size=50,
    )

    low = out[out["candidate_id"] == "low_sample"].iloc[0]
    eligible = out[out["candidate_id"] == "eligible"].iloc[0]

    assert bool(low["is_discovery"]) is False
    assert float(low["q_value"]) == 1.0
    assert bool(eligible["is_discovery"]) is True
    assert bool(low["multiplicity_pool_eligible"]) is False
    assert bool(eligible["multiplicity_pool_eligible"]) is True
    assert int(eligible["num_tests_family"]) == 1
    assert int(eligible["num_tests_effective"]) == 1


def test_apply_multiplicity_controls_falls_back_to_raw_p_value_column() -> None:
    raw_df = pd.DataFrame(
        [
            {"candidate_id": "cand_1", "family_id": "fam_a", "p_value_raw": 0.01},
            {"candidate_id": "cand_2", "family_id": "fam_b", "p_value_raw": 0.90},
        ]
    )

    out = apply_multiplicity_controls(raw_df=raw_df, max_q=0.05)

    assert "q_value" in out.columns
    assert float(out.loc[out["candidate_id"] == "cand_1", "q_value"].iloc[0]) <= 0.05


def test_side_policy_both_counts_as_two_effective_tests():
    rows = [
        {"candidate_id": "long_a", "family_id": "fam_a", "p_value": 0.01, "side_policy": "long"},
        {"candidate_id": "short_a", "family_id": "fam_a", "p_value": 0.02, "side_policy": "short"},
        {"candidate_id": "both_a", "family_id": "fam_a", "p_value": 0.03, "side_policy": "both"},
    ]
    raw_df = pd.DataFrame(rows)
    out = apply_multiplicity_controls(raw_df=raw_df, max_q=0.05)

    fam_a_rows = out[out["family_id"] == "fam_a"]
    assert int(fam_a_rows.iloc[0]["num_tests_family"]) == 3
    assert int(fam_a_rows.iloc[0]["num_tests_effective"]) == 4
    assert int(fam_a_rows.iloc[0]["num_tests_campaign"]) == 4


def test_backward_compatibility_aliases():
    rows = [{"candidate_id": "test_1", "family_id": "fam_x", "p_value": 0.01}]
    raw_df = pd.DataFrame(rows)
    out = apply_multiplicity_controls(raw_df=raw_df, max_q=0.05)

    row = out.iloc[0]
    assert "num_tests_family" in out.columns
    assert "num_tests_effective" in out.columns
    assert "num_tests_campaign" in out.columns
    assert "num_tests_primary_event_id" in out.columns
    assert "num_tests_event_family" in out.columns
    assert int(row["num_tests_primary_event_id"]) == int(row["num_tests_family"])
    assert int(row["num_tests_event_family"]) == int(row["num_tests_family"])
