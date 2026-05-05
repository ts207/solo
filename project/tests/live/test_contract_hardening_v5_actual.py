from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from project.artifacts import promoted_theses_path
from project.live.contracts.promoted_thesis import (
    LiveApproval,
    PromotedThesis,
    RuntimeThesisManifest,
    ThesisCapProfile,
    ThesisEvidence,
    ThesisLineage,
)
from project.live.thesis_store import ThesisStore
from project.research.promotion.promotion_decisions import (
    _compatibility_promotion_block,
    _side_policy_resolution_block,
)


def _base_thesis(**kwargs):
    payload = dict(
        thesis_id="t1",
        status="active",
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        evidence=ThesisEvidence(sample_size=10),
        lineage=ThesisLineage(run_id="r1", candidate_id="c1"),
    )
    payload.update(kwargs)
    return PromotedThesis(**payload)


def test_promotion_compatibility_block_is_authoritative() -> None:
    blocked, reason = _compatibility_promotion_block(
        {
            "compatibility_status": "research_only",
            "compatibility_promotion_allowed": False,
            "compatibility_reason_codes": "missing_event_template_rule",
        }
    )
    assert blocked is True
    assert reason == "missing_event_template_rule"


def test_promotion_side_policy_requires_polarity_when_direction_is_auto() -> None:
    blocked, reason = _side_policy_resolution_block(
        {"template_id": "breakout_followthrough", "side_policy": "directional", "direction": "auto"}
    )
    assert blocked is True
    assert reason == "side_policy_resolution_missing_event_polarity"


def test_promotion_side_policy_accepts_explicit_direction() -> None:
    blocked, _ = _side_policy_resolution_block(
        {"template_id": "breakout_followthrough", "side_policy": "directional", "direction": "long"}
    )
    assert blocked is False


def test_thesis_store_runtime_manifest_required_rejects_incomplete_live_manifest(tmp_path: Path) -> None:
    thesis = _base_thesis(
        deployment_state="live_enabled",
        runtime_manifest=RuntimeThesisManifest(
            thesis_id="t1",
            promotion_state="live_enabled",
            allowed_runtime_modes=["trading"],
            expires_at_utc=(datetime.now(UTC) + timedelta(days=1)).isoformat(),
        ),
        live_approval=LiveApproval(live_approval_status="approved"),
        cap_profile=ThesisCapProfile(max_notional=1.0),
    )
    path = promoted_theses_path("r1", tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": "promoted_theses_v1",
                "run_id": "r1",
                "generated_at_utc": "2026-01-01T00:00:00Z",
                "thesis_count": 1,
                "active_thesis_count": 1,
                "pending_thesis_count": 0,
                "theses": [thesis.model_dump()],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="runtime_manifest missing required hashes"):
        ThesisStore.from_run_id(
            "r1",
            data_root=tmp_path,
            strict_live_gate=False,
            require_runtime_manifest=True,
            runtime_mode="trading",
        )
