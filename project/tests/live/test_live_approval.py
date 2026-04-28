import json
import pytest
from pathlib import Path
from project.live.live_approval import LiveApprovalArtifact, load_live_approval
from project.live.cap_profiles import TINY_LIVE_V1

def test_live_approval_validation_pass():
    artifact = LiveApprovalArtifact(
        thesis_id="test_thesis",
        approved_by="operator",
        approved_at_utc="2026-04-27T00:00:00Z",
        cap_profile_id="tiny_live_v1",
        risk_acknowledgement=True
    )
    artifact.validate_for_live()

def test_live_approval_validation_fail_no_risk_ack():
    artifact = LiveApprovalArtifact(
        thesis_id="test_thesis",
        approved_by="operator",
        approved_at_utc="2026-04-27T00:00:00Z",
        cap_profile_id="tiny_live_v1",
        risk_acknowledgement=False
    )
    with pytest.raises(ValueError, match="missing risk_acknowledgement"):
        artifact.validate_for_live()

def test_live_approval_validation_fail_invalid_cap():
    artifact = LiveApprovalArtifact(
        thesis_id="test_thesis",
        approved_by="operator",
        approved_at_utc="2026-04-27T00:00:00Z",
        cap_profile_id="invalid_cap",
        risk_acknowledgement=True
    )
    with pytest.raises(ValueError, match="Invalid cap_profile_id"):
        artifact.validate_for_live()

def test_load_live_approval(tmp_path):
    path = tmp_path / "live_approval.json"
    data = {
        "thesis_id": "test_thesis",
        "approved_by": "operator",
        "approved_at_utc": "2026-04-27T00:00:00Z",
        "cap_profile_id": "tiny_live_v1",
        "risk_acknowledgement": True
    }
    path.write_text(json.dumps(data))
    
    artifact = load_live_approval(path)
    assert artifact.thesis_id == "test_thesis"
    assert artifact.cap_profile_id == "tiny_live_v1"
    assert artifact.risk_acknowledgement is True

def test_load_live_approval_missing_file():
    with pytest.raises(FileNotFoundError):
        load_live_approval(Path("non_existent.json"))
