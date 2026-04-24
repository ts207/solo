from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from project.domain.hypotheses import HypothesisSpec, TriggerSpec, TriggerType
from project.research.search.evaluator_utils import trigger_mask


def test_transition_strict_semantics() -> None:
    # A -> B -> B -> A -> B
    # ColumnRegistry.state_cols("A") includes "state_a", "ms_a", "a"
    features = pd.DataFrame(
        {
            "state_a": [1, 0, 0, 1, 0],
            "state_b": [0, 1, 1, 0, 1],
        }
    )

    # Mock registry to bypass validation
    with patch("project.domain.hypotheses.get_domain_registry") as mock_registry:
        mock_reg = MagicMock()
        mock_reg.valid_state_ids = ["A", "B"]
        mock_registry.return_value = mock_reg

        trigger = TriggerSpec(
            trigger_type=TriggerType.TRANSITION,
            from_state="A",
            to_state="B",
        )

        spec = HypothesisSpec(
            template_id="continuation",
            trigger=trigger,
            direction="long",
            horizon="12b",
        )

    mask = trigger_mask(spec, features)

    # Expected fires at index 1 (A->B) and index 4 (A->B)
    # Index 0: No history, cannot fire.
    # Index 1: prev A=1, curr B=1. Fire.
    # Index 2: prev B=1, curr B=1. No fire.
    # Index 3: prev B=1, curr A=1. No fire.
    # Index 4: prev A=1, curr B=1. Fire.

    expected = [False, True, False, False, True]
    assert list(mask) == expected


def test_transition_no_history_at_start() -> None:
    # First bar is B, but we don't know if it was A before. Should NOT fire.
    features = pd.DataFrame(
        {
            "state_a": [0, 1],
            "state_b": [1, 0],
        }
    )

    with patch("project.domain.hypotheses.get_domain_registry") as mock_registry:
        mock_reg = MagicMock()
        mock_reg.valid_state_ids = ["A", "B"]
        mock_registry.return_value = mock_reg

        trigger = TriggerSpec(
            trigger_type=TriggerType.TRANSITION,
            from_state="A",
            to_state="B",
        )

        spec = HypothesisSpec(
            template_id="continuation",
            trigger=trigger,
            direction="long",
            horizon="12b",
        )

    mask = trigger_mask(spec, features)
    assert not mask.iloc[0]
