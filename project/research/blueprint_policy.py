from __future__ import annotations

from typing import Any

from project.spec_registry import load_blueprint_policy_spec


def load_blueprint_policy(policy_path: str | None = None) -> dict[str, Any]:
    return dict(load_blueprint_policy_spec(policy_path))
