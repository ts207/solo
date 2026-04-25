from __future__ import annotations

from project.spec_validation.directionality import validate_event_directionality_contracts


def test_authored_event_directionality_contracts_are_complete() -> None:
    assert validate_event_directionality_contracts() == []
