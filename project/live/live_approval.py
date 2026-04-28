from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from project.live.cap_profiles import validate_cap_profile_id


class LiveApprovalArtifact(BaseModel):
    model_config = ConfigDict(frozen=True)

    schema_version: Literal["live_approval_v1"] = "live_approval_v1"
    thesis_id: str = Field(min_length=1)
    approved_state: Literal["live_enabled"] = "live_enabled"
    approved_by: str = Field(min_length=1)
    approved_at_utc: str = Field(min_length=1)
    cap_profile_id: str = Field(min_length=1)
    risk_acknowledgement: bool = False

    def validate_for_live(self) -> None:
        if not self.risk_acknowledgement:
            raise ValueError(f"Approval for thesis {self.thesis_id} missing risk_acknowledgement")

        if not validate_cap_profile_id(self.cap_profile_id):
            raise ValueError(f"Invalid cap_profile_id: {self.cap_profile_id}")


def load_live_approval(path: Path) -> LiveApprovalArtifact:
    if not path.exists():
        raise FileNotFoundError(f"Live approval artifact missing: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))
    return LiveApprovalArtifact.model_validate(data)
