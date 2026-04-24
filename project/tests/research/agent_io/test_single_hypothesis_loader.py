from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from project.research.agent_io.proposal_schema import (
    _load_single_hypothesis_proposal,
    compile_single_hypothesis_to_agent_proposal,
    load_operator_proposal,
)
from project.research.agent_io.proposal_to_experiment import translate_and_validate_proposal
from project.tests.research.agent_io.test_issue_proposal import _write_registry


def _single_hypothesis_payload() -> dict:
    return {
        "program_id": "btc_volshock_single",
        "description": "Bounded VOL_SHOCK continuation slice",
        "run_mode": "research",
        "objective_name": "retail_profitability",
        "promotion_profile": "research",
        "symbols": ["BTCUSDT"],
        "timeframe": "5m",
        "start": "2026-01-01",
        "end": "2026-01-31",
        "instrument_classes": ["crypto"],
        "hypothesis": {
            "trigger": {
                "type": "event",
                "event_id": "VOL_SHOCK",
            },
            "template": "continuation",
            "direction": "long",
            "horizon_bars": 12,
            "entry_lag_bars": 1,
        },
    }


def _canonical_event_h24_payload() -> dict:
    return {
        "program_id": "volshock_btc_long_12b",
        "description": "Canonical VOL_SHOCK continuation slice",
        "run_mode": "research",
        "objective_name": "retail_profitability",
        "promotion_profile": "research",
        "symbols": ["BTCUSDT"],
        "timeframe": "5m",
        "start": "2026-01-01",
        "end": "2026-01-31",
        "instrument_classes": ["crypto"],
        "hypothesis": {
            "anchor": {
                "type": "event",
                "event_id": "VOL_SHOCK",
            },
            "filters": {},
            "sampling_policy": {"entry_lag_bars": 1},
            "template": {"id": "continuation"},
            "direction": "long",
            "horizon_bars": 24,
        },
    }


def test_load_operator_proposal_rejects_legacy_and_single_hypothesis_by_default() -> None:
    legacy_payload = {
        "program_id": "legacy_campaign",
        "start": "2026-01-01",
        "end": "2026-01-31",
        "symbols": ["BTCUSDT"],
        "trigger_space": {"allowed_trigger_types": ["EVENT"], "events": {"include": ["VOL_SHOCK"]}},
        "templates": ["continuation"],
        "horizons_bars": [12],
        "directions": ["long"],
        "entry_lags": [1],
    }

    with pytest.raises(ValueError, match="no longer supported"):
        load_operator_proposal(legacy_payload)
    with pytest.raises(ValueError, match="no longer supported"):
        load_operator_proposal(_single_hypothesis_payload())


def test_compile_single_hypothesis_to_agent_proposal_preserves_atomic_shape() -> None:
    compiled = compile_single_hypothesis_to_agent_proposal(
        _load_single_hypothesis_proposal(_single_hypothesis_payload())
    )

    assert compiled.symbols == ["BTCUSDT"]
    assert compiled.templates == ["continuation"]
    assert compiled.directions == ["long"]
    assert compiled.horizons_bars == [12]
    assert compiled.entry_lags == [1]


@pytest.mark.parametrize(
    ("field_path", "value", "match"),
    [
        (("symbols",), ["BTCUSDT", "ETHUSDT"], "exactly 1 symbol"),
        (("hypothesis", "template"), ["continuation", "mean_reversion"], "single string"),
        (("hypothesis", "direction"), ["long", "short"], "single string"),
        (("hypothesis", "horizon_bars"), [12, 24], "single integer"),
        (("hypothesis", "entry_lag_bars"), [1, 2], "single integer"),
    ],
)
def test_single_hypothesis_loader_rejects_multi_value_fields(
    field_path: tuple[str, ...],
    value: object,
    match: str,
) -> None:
    payload = _single_hypothesis_payload()
    cursor = payload
    for key in field_path[:-1]:
        cursor = cursor[key]
    cursor[field_path[-1]] = value

    with pytest.raises(ValueError, match=match):
        _load_single_hypothesis_proposal(payload)


def test_single_hypothesis_loader_requires_event_id_for_event_trigger() -> None:
    payload = _single_hypothesis_payload()
    payload["hypothesis"]["trigger"] = {"type": "event"}

    with pytest.raises(ValueError, match="event triggers require"):
        _load_single_hypothesis_proposal(payload)


def test_single_hypothesis_loader_rejects_mixed_legacy_fields() -> None:
    payload = _single_hypothesis_payload()
    payload["trigger_space"] = {
        "allowed_trigger_types": ["EVENT"],
        "events": {"include": ["VOL_SHOCK"]},
    }

    with pytest.raises(ValueError, match="must not include legacy AgentProposal fields"):
        _load_single_hypothesis_proposal(payload)


def test_single_hypothesis_trigger_compilation_supports_state_and_feature_predicate() -> None:
    state_payload = _single_hypothesis_payload()
    state_payload["hypothesis"]["trigger"] = {
        "type": "state",
        "state_id": "HIGH_VOL_REGIME",
    }
    feature_payload = _single_hypothesis_payload()
    feature_payload["hypothesis"]["trigger"] = {
        "type": "feature_predicate",
        "feature": "funding_rate_scaled",
        "operator": ">=",
        "threshold": 2.0,
    }

    state = compile_single_hypothesis_to_agent_proposal(_load_single_hypothesis_proposal(state_payload))
    feature = compile_single_hypothesis_to_agent_proposal(_load_single_hypothesis_proposal(feature_payload))

    assert state.trigger_space["allowed_trigger_types"] == ["STATE"]
    assert state.trigger_space["states"]["include"] == ["HIGH_VOL_REGIME"]
    assert feature.trigger_space["allowed_trigger_types"] == ["FEATURE_PREDICATE"]
    assert feature.trigger_space["feature_predicates"]["include"] == [
        {"feature": "funding_rate_scaled", "operator": ">=", "threshold": 2.0}
    ]


def test_translate_and_validate_proposal_rejects_single_hypothesis_payload(tmp_path: Path) -> None:
    registry_root = tmp_path / "registries"
    proposal_path = tmp_path / "single_hypothesis.yaml"
    _write_registry(registry_root)
    proposal_path.write_text(
        yaml.safe_dump(_single_hypothesis_payload(), sort_keys=False),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="no longer supported"):
        translate_and_validate_proposal(
            proposal_path,
            registry_root=registry_root,
            out_dir=tmp_path / "bundle",
        )


def test_translate_and_validate_proposal_rejects_legacy_payload(tmp_path: Path) -> None:
    registry_root = tmp_path / "registries"
    proposal_path = tmp_path / "legacy.yaml"
    _write_registry(registry_root)
    proposal_path.write_text(
        yaml.safe_dump(
            {
                "program_id": "legacy_campaign",
                "start": "2026-01-01",
                "end": "2026-01-31",
                "symbols": ["BTCUSDT"],
                "trigger_space": {"allowed_trigger_types": ["EVENT"], "events": {"include": ["VOL_SHOCK"]}},
                "templates": ["continuation"],
                "horizons_bars": [12],
                "directions": ["long"],
                "entry_lags": [1],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="no longer supported"):
        translate_and_validate_proposal(
            proposal_path,
            registry_root=registry_root,
            out_dir=tmp_path / "bundle",
        )


def test_load_operator_proposal_accepts_structured_hypothesis_format() -> None:
    payload = {
        "program_id": "structured_campaign",
        "start": "2026-01-01",
        "end": "2026-01-31",
        "symbols": ["BTCUSDT"],
        "timeframe": "1h",
        "hypothesis": {
            "anchor": {"type": "event", "event_id": "VOL_SHOCK"},
            "template": {"id": "continuation"},
            "direction": "long",
            "horizon_bars": 12,
            "sampling_policy": {"entry_lag_bars": 1},
        },
    }

    proposal = load_operator_proposal(payload)

    assert proposal.templates == ["continuation"]
    assert proposal.trigger_space["allowed_trigger_types"] == ["EVENT"]
    assert proposal.trigger_space["events"]["include"] == ["VOL_SHOCK"]
    assert proposal.horizons_bars == [12]
    assert proposal.directions == ["long"]
    assert proposal.entry_lags == [1]


def test_canonical_event_h24_example_loads_as_structured_front_door() -> None:
    proposal = load_operator_proposal(_canonical_event_h24_payload())

    assert proposal.program_id == "volshock_btc_long_12b"
    assert proposal.symbols == ["BTCUSDT"]
    assert proposal.templates == ["continuation"]
    assert proposal.directions == ["long"]
    assert proposal.horizons_bars == [24]
    assert proposal.entry_lags == [1]
    assert proposal.trigger_space["allowed_trigger_types"] == ["EVENT"]
    assert proposal.trigger_space["events"]["include"] == ["VOL_SHOCK"]


def test_canonical_event_h24_example_translates_through_existing_experiment_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "project.research.agent_io.proposal_to_experiment._build_experiment_plan",
        lambda *args, **kwargs: type(
            "Plan",
            (),
            {
                "program_id": "volshock_btc_long_12b",
                "estimated_hypothesis_count": 1,
                "required_detectors": ["vol_shock"],
                "required_features": ["ret_1"],
                "required_states": [],
            },
        )(),
    )

    result = translate_and_validate_proposal(
        _canonical_event_h24_payload(),
        registry_root=Path("project/configs/registries"),
        out_dir=tmp_path / "bundle",
    )

    assert result["proposal"]["templates"] == ["continuation"]
    assert result["proposal"]["horizons_bars"] == [24]
    assert result["proposal"]["trigger_space"]["events"]["include"] == ["VOL_SHOCK"]
    assert int(result["validated_plan"]["estimated_hypothesis_count"]) == 1
