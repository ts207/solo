from __future__ import annotations

from typing import Iterable

from project.domain.compiled_registry import get_domain_registry
from project.domain.models import ThesisDefinition
from project.live.contracts import PromotedThesis


def resolve_thesis_definition_ids(*candidate_ids: str) -> ThesisDefinition | None:
    registry = get_domain_registry()
    for candidate_id in candidate_ids:
        token = str(candidate_id or "").strip().upper()
        if not token:
            continue
        definition = registry.get_thesis(token)
        if definition is not None:
            return definition
    return None


def get_thesis_definition(thesis_id: str) -> ThesisDefinition | None:
    return resolve_thesis_definition_ids(thesis_id)


def resolve_promoted_thesis_definition(thesis: PromotedThesis) -> ThesisDefinition | None:
    return resolve_thesis_definition_ids(*_candidate_ids_for_promoted_thesis(thesis))


def _candidate_ids_for_promoted_thesis(thesis: PromotedThesis) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in _candidate_id_values(thesis):
        token = str(raw or "").strip().upper()
        if token and token not in seen:
            ordered.append(token)
            seen.add(token)
    return tuple(ordered)


def _candidate_id_values(thesis: PromotedThesis) -> Iterable[str]:
    yield thesis.thesis_id
    yield thesis.lineage.candidate_id
    yield thesis.lineage.hypothesis_id
