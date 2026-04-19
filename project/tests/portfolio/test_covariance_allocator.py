from __future__ import annotations

import pandas as pd
import pytest

from project.portfolio.covariance import covariance_exposure_multiplier, estimate_return_covariance
from project.portfolio.engine import PortfolioDecisionEngine, ThesisIntent


def _intent(thesis_id: str, overlap_group_id: str) -> ThesisIntent:
    return ThesisIntent(
        thesis_id=thesis_id,
        symbol="BTCUSDT",
        family="vol",
        overlap_group_id=overlap_group_id,
        requested_notional=10_000.0,
        support_score=1.0,
    )


def test_covariance_multiplier_reduces_highly_correlated_incremental_exposure() -> None:
    covariance = pd.DataFrame(
        [[1.0, 0.9], [0.9, 1.0]],
        index=["T1", "T2"],
        columns=["T1", "T2"],
    )

    multiplier = covariance_exposure_multiplier(
        "T2",
        covariance,
        {"T1": 10_000.0},
        correlation_limit=0.5,
    )

    assert multiplier == pytest.approx(0.5 / 0.9)


def test_correlated_theses_receive_less_aggregate_size_inside_caps() -> None:
    correlated = pd.DataFrame(
        [[1.0, 0.9], [0.9, 1.0]],
        index=["T1", "T2"],
        columns=["T1", "T2"],
    )
    uncorrelated = pd.DataFrame(
        [[1.0, 0.0], [0.0, 1.0]],
        index=["T1", "T2"],
        columns=["T1", "T2"],
    )
    intents = [_intent("T1", "OG1"), _intent("T2", "OG2")]

    correlated_decisions = PortfolioDecisionEngine(
        family_budgets={"vol": 100_000.0},
        symbol_caps={"BTCUSDT": 100_000.0},
        correlation_limit=0.5,
        thesis_covariance=correlated,
    ).decide(intents)
    uncorrelated_decisions = PortfolioDecisionEngine(
        family_budgets={"vol": 100_000.0},
        symbol_caps={"BTCUSDT": 100_000.0},
        correlation_limit=0.5,
        thesis_covariance=uncorrelated,
    ).decide(intents)

    correlated_total = sum(decision.allocated_notional for decision in correlated_decisions)
    uncorrelated_total = sum(decision.allocated_notional for decision in uncorrelated_decisions)

    assert correlated_total < uncorrelated_total
    assert correlated_decisions[1].covariance_multiplier < 1.0


def test_estimate_return_covariance_aligns_thesis_samples() -> None:
    covariance = estimate_return_covariance(
        {
            "T1": [0.01, -0.02, 0.03],
            "T2": [0.02, -0.01, 0.04],
        }
    )

    assert list(covariance.index) == ["T1", "T2"]
    assert covariance.loc["T1", "T1"] > 0.0
