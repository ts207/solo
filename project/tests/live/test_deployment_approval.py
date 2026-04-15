from __future__ import annotations

import pytest

from project.core.exceptions import DataIntegrityError
from project.live.contracts.deployment_approval import DeploymentApprovalRecord


def test_deployment_approval_from_file_raises_on_malformed_json(tmp_path):
    path = tmp_path / "approval_record.json"
    path.write_text("{not valid json", encoding="utf-8")

    with pytest.raises(DataIntegrityError, match="Failed to read deployment approval record"):
        DeploymentApprovalRecord.from_file(path)
