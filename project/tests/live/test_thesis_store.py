from __future__ import annotations

import json
from pathlib import Path

import pytest

from project.artifacts import live_thesis_index_path, promoted_theses_path
from project.core.exceptions import CompatibilityRequiredError, DataIntegrityError
from project.live.thesis_store import ThesisStore


def _write_store_fixture(root: Path, run_id: str) -> None:
    thesis_path = promoted_theses_path(run_id, root)
    thesis_path.parent.mkdir(parents=True, exist_ok=True)
    thesis_path.write_text(
        json.dumps(
            {
                "schema_version": "promoted_theses_v1",
                "run_id": run_id,
                "generated_at_utc": "2026-03-30T00:00:00Z",
                "thesis_count": 2,
                "active_thesis_count": 1,
                "pending_thesis_count": 1,
                "theses": [
                    {
                        "thesis_id": "thesis::run_1::cand_1",
                        "status": "active",
                        "symbol_scope": {
                            "mode": "single_symbol",
                            "symbols": ["BTCUSDT"],
                            "candidate_symbol": "BTCUSDT",
                        },
                        "timeframe": "5m",
                        "primary_event_id": "VOL_SHOCK",
                        "event_family": "VOL_SHOCK",
                        "canonical_regime": "VOLATILITY_TRANSITION",
                        "event_side": "long",
                        "required_context": {"symbol": "BTCUSDT"},
                        "supportive_context": {},
                        "expected_response": {"direction": "long"},
                        "invalidation": {"metric": "adverse_proxy", "operator": ">", "value": 0.02},
                        "risk_notes": [],
                        "evidence": {
                            "sample_size": 120,
                            "validation_samples": 60,
                            "test_samples": 60,
                            "estimate_bps": 12.0,
                            "net_expectancy_bps": 9.0,
                            "q_value": 0.01,
                            "stability_score": 0.9,
                            "cost_survival_ratio": 1.0,
                            "tob_coverage": 0.95,
                            "rank_score": 1.0,
                            "promotion_track": "deploy",
                            "policy_version": "v1",
                            "bundle_version": "b1",
                        },
                        "lineage": {
                            "run_id": run_id,
                            "candidate_id": "cand_1",
                            "hypothesis_id": "hyp_1",
                            "plan_row_id": "plan_1",
                            "blueprint_id": "bp_1",
                            "proposal_id": "proposal_1",
                        },
                        "requirements": {
                            "trigger_events": ["VOL_SHOCK"],
                            "confirmation_events": ["LIQUIDITY_VACUUM"],
                            "required_episodes": [],
                            "disallowed_regimes": [],
                        },
                        "source": {
                            "event_contract_ids": ["VOL_SHOCK", "LIQUIDITY_VACUUM"],
                            "episode_contract_ids": [],
                        },
                    },
                    {
                        "thesis_id": "thesis::run_1::cand_2",
                        "status": "pending_blueprint",
                        "symbol_scope": {
                            "mode": "single_symbol",
                            "symbols": ["ETHUSDT"],
                            "candidate_symbol": "ETHUSDT",
                        },
                        "timeframe": "15m",
                        "primary_event_id": "OI_FLUSH",
                        "event_family": "OI_FLUSH",
                        "canonical_regime": "POSITIONING_EXPANSION",
                        "event_side": "short",
                        "required_context": {"symbol": "ETHUSDT"},
                        "supportive_context": {},
                        "expected_response": {"direction": "short"},
                        "invalidation": {},
                        "risk_notes": ["missing_blueprint_invalidation"],
                        "evidence": {
                            "sample_size": 80,
                            "validation_samples": 40,
                            "test_samples": 40,
                            "estimate_bps": -8.0,
                            "net_expectancy_bps": -3.0,
                            "q_value": 0.02,
                            "stability_score": 0.8,
                            "cost_survival_ratio": 0.9,
                            "tob_coverage": 0.9,
                            "rank_score": 0.7,
                            "promotion_track": "research",
                            "policy_version": "v1",
                            "bundle_version": "b1",
                        },
                        "lineage": {
                            "run_id": run_id,
                            "candidate_id": "cand_2",
                            "hypothesis_id": "hyp_2",
                            "plan_row_id": "plan_2",
                            "blueprint_id": "",
                            "proposal_id": "",
                        },
                    },
                ],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    live_thesis_index_path(root).write_text(
        json.dumps(
            {
                "schema_version": "promoted_thesis_index_v1",
                "latest_run_id": run_id,
                "default_resolution_disabled": True,
                "runs": {
                    run_id: {
                        "output_path": str(thesis_path),
                        "thesis_count": 2,
                        "active_thesis_count": 1,
                        "pending_thesis_count": 1,
                    }
                },
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_thesis_store_filters_active_by_symbol_and_event_id(tmp_path: Path) -> None:
    _write_store_fixture(tmp_path, "run_1")

    store = ThesisStore.from_run_id("run_1", data_root=tmp_path)
    active = store.active_theses(symbol="BTCUSDT", timeframe="5m", event_id="VOL_SHOCK")

    assert len(active) == 1
    assert active[0].lineage.blueprint_id == "bp_1"


def test_thesis_store_filters_active_by_canonical_regime(tmp_path: Path) -> None:
    _write_store_fixture(tmp_path, "run_1")

    store = ThesisStore.from_run_id("run_1", data_root=tmp_path)
    active = store.active_theses(symbol="BTCUSDT", timeframe="5m", canonical_regime="VOLATILITY_TRANSITION")

    assert len(active) == 1
    assert active[0].canonical_regime == "VOLATILITY_TRANSITION"


def test_thesis_store_event_id_filter_matches_clause_event_ids(tmp_path: Path) -> None:
    _write_store_fixture(tmp_path, "run_1")

    store = ThesisStore.from_run_id("run_1", data_root=tmp_path)
    active = store.active_theses(symbol="BTCUSDT", timeframe="5m", event_id="LIQUIDITY_VACUUM")

    assert len(active) == 1
    assert active[0].primary_event_id == "VOL_SHOCK"


def test_thesis_store_event_family_filter_matches_event_family_only(tmp_path: Path) -> None:
    _write_store_fixture(tmp_path, "run_1")

    store = ThesisStore.from_run_id("run_1", data_root=tmp_path)
    active = store.active_theses(symbol="BTCUSDT", timeframe="5m", event_family="VOL_SHOCK")

    assert len(active) == 1
    assert active[0].event_family == "VOL_SHOCK"


def test_thesis_store_event_id_and_family_use_distinct_filter_paths(tmp_path: Path) -> None:
    _write_store_fixture(tmp_path, "run_1")

    store = ThesisStore.from_run_id("run_1", data_root=tmp_path)

    by_event_id = store.active_theses(symbol="BTCUSDT", timeframe="5m", event_id="LIQUIDITY_VACUUM")
    by_event_family = store.active_theses(symbol="BTCUSDT", timeframe="5m", event_family="LIQUIDITY_VACUUM")

    assert len(by_event_id) == 1
    assert by_event_id[0].thesis_id == "thesis::run_1::cand_1"
    assert by_event_family == []


def test_thesis_store_latest_requires_explicit_compatibility_opt_in(tmp_path: Path) -> None:
    _write_store_fixture(tmp_path, "run_1")

    with pytest.raises(RuntimeError, match="Implicit latest thesis resolution is disabled"):
        ThesisStore.latest(data_root=tmp_path)


def test_thesis_store_loads_latest_index_in_compatibility_mode(tmp_path: Path) -> None:
    _write_store_fixture(tmp_path, "run_1")

    store = ThesisStore.latest(data_root=tmp_path, allow_implicit_latest=True)

    assert store.run_id == "run_1"
    assert len(store.filter(status="pending_blueprint")) == 1


def test_thesis_store_raises_on_corrupted_payload(tmp_path: Path) -> None:
    thesis_path = promoted_theses_path("run_bad", tmp_path)
    thesis_path.parent.mkdir(parents=True, exist_ok=True)
    thesis_path.write_text("{not-json", encoding="utf-8")

    with pytest.raises(DataIntegrityError):
        ThesisStore.from_run_id("run_bad", data_root=tmp_path)


def test_thesis_store_rejects_legacy_but_interpretable_payload(tmp_path: Path) -> None:
    thesis_path = promoted_theses_path("run_legacy", tmp_path)
    thesis_path.parent.mkdir(parents=True, exist_ok=True)
    thesis_path.write_text(
        json.dumps(
            {
                "run_id": "run_legacy",
                "theses": [],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(CompatibilityRequiredError, match="legacy_but_interpretable"):
        ThesisStore.from_run_id("run_legacy", data_root=tmp_path)


def test_thesis_store_raises_on_corrupted_latest_index(tmp_path: Path) -> None:
    live_thesis_index_path(tmp_path).parent.mkdir(parents=True, exist_ok=True)
    live_thesis_index_path(tmp_path).write_text("{not-json", encoding="utf-8")

    with pytest.raises(DataIntegrityError):
        ThesisStore.latest(data_root=tmp_path, allow_implicit_latest=True)


def test_thesis_store_rejects_legacy_latest_index(tmp_path: Path) -> None:
    live_thesis_index_path(tmp_path).parent.mkdir(parents=True, exist_ok=True)
    live_thesis_index_path(tmp_path).write_text(
        json.dumps(
            {
                "latest_run_id": "run_1",
                "runs": {"run_1": {"output_path": "missing.json"}},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(CompatibilityRequiredError, match="legacy_but_interpretable"):
        ThesisStore.latest(data_root=tmp_path, allow_implicit_latest=True)
