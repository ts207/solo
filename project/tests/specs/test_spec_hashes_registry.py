from __future__ import annotations

from project.specs.utils import get_spec_hashes
from project.tests.conftest import REPO_ROOT


def test_get_spec_hashes_uses_canonical_registry_paths():
    hashes = get_spec_hashes(REPO_ROOT)
    assert isinstance(hashes, dict)
    assert "gates.yaml" in hashes
    assert all(".yaml" in k for k in hashes)
