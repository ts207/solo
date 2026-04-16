from __future__ import annotations

import functools

from project.domain.models import DomainRegistry
from project.domain.registry_loader import (
    compile_domain_registry_from_sources,
    load_domain_registry_from_graph,
)
from project.spec_registry import clear_caches


@functools.lru_cache(maxsize=1)
def get_domain_registry() -> DomainRegistry:
    registry = load_domain_registry_from_graph()
    assert registry is not None
    return registry


def refresh_domain_registry(*, rebuild_from_sources: bool = False) -> DomainRegistry:
    clear_caches()
    get_domain_registry.cache_clear()
    if rebuild_from_sources:
        return compile_domain_registry_from_sources()
    return get_domain_registry()
