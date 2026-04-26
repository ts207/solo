from __future__ import annotations

import pytest

from project.research.direction_semantics import (
    normalize_side_policy,
    resolve_candidate_action,
    resolve_effect_sign,
)


def test_normalize_side_policy_rejects_unknown_values():
    with pytest.raises(ValueError):
        normalize_side_policy("long_only")


def test_resolve_effect_sign_directional_tracks_event_direction():
    assert (
        resolve_effect_sign(
            template_verb="continuation",
            side_policy="directional",
            event_direction=1,
            label_target="fwd_return_h",
        )
        == 1
    )
    assert (
        resolve_effect_sign(
            template_verb="continuation",
            side_policy="directional",
            event_direction=-1,
            label_target="fwd_return_h",
        )
        == -1
    )


def test_resolve_effect_sign_contrarian_flips_event_direction():
    assert (
        resolve_effect_sign(
            template_verb="mean_reversion",
            side_policy="contrarian",
            event_direction=1,
            label_target="fwd_return_h",
        )
        == -1
    )
    assert (
        resolve_effect_sign(
            template_verb="mean_reversion",
            side_policy="contrarian",
            event_direction=-1,
            label_target="fwd_return_h",
        )
        == 1
    )


def test_resolve_effect_sign_gate_templates_are_nondirectional():
    assert (
        resolve_effect_sign(
            template_verb="only_if_regime",
            side_policy="both",
            event_direction=1,
            label_target="gate",
        )
        == 0
    )


def test_resolve_candidate_action_uses_entry_gate_skip_for_gate_templates():
    assert (
        resolve_candidate_action(
            template_verb="only_if_liquidity",
            side_policy="both",
            label_target="gate",
        )
        == "entry_gate_skip"
    )
