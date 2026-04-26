from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from project.research.knowledge.build_static_knowledge import build_static_knowledge
    from project.research.knowledge.memory import (
        MemoryPaths,
        build_failures_snapshot,
        build_tested_regions_snapshot,
        compute_context_statistics,
        compute_event_statistics,
        compute_region_statistics,
        compute_template_statistics,
        ensure_memory_store,
        memory_paths,
        read_memory_table,
        read_reflections,
        write_memory_table,
        write_reflection,
    )
    from project.research.knowledge.query import (
        query_adjacent_regions,
        query_agent_knobs,
        query_memory_rows,
        query_static_rows,
    )
    from project.research.knowledge.reflection import build_run_reflection

_EXPORTS = {
    "MemoryPaths": ("project.research.knowledge.memory", "MemoryPaths"),
    "build_failures_snapshot": ("project.research.knowledge.memory", "build_failures_snapshot"),
    "build_run_reflection": ("project.research.knowledge.reflection", "build_run_reflection"),
    "build_static_knowledge": (
        "project.research.knowledge.build_static_knowledge",
        "build_static_knowledge",
    ),
    "build_tested_regions_snapshot": (
        "project.research.knowledge.memory",
        "build_tested_regions_snapshot",
    ),
    "compute_context_statistics": (
        "project.research.knowledge.memory",
        "compute_context_statistics",
    ),
    "compute_event_statistics": ("project.research.knowledge.memory", "compute_event_statistics"),
    "compute_region_statistics": ("project.research.knowledge.memory", "compute_region_statistics"),
    "compute_template_statistics": (
        "project.research.knowledge.memory",
        "compute_template_statistics",
    ),
    "ensure_memory_store": ("project.research.knowledge.memory", "ensure_memory_store"),
    "memory_paths": ("project.research.knowledge.memory", "memory_paths"),
    "query_agent_knobs": ("project.research.knowledge.query", "query_agent_knobs"),
    "query_adjacent_regions": ("project.research.knowledge.query", "query_adjacent_regions"),
    "query_memory_rows": ("project.research.knowledge.query", "query_memory_rows"),
    "query_static_rows": ("project.research.knowledge.query", "query_static_rows"),
    "read_reflections": ("project.research.knowledge.memory", "read_reflections"),
    "read_memory_table": ("project.research.knowledge.memory", "read_memory_table"),
    "write_reflection": ("project.research.knowledge.memory", "write_reflection"),
    "write_memory_table": ("project.research.knowledge.memory", "write_memory_table"),
}

__all__ = [
    "MemoryPaths",
    "build_failures_snapshot",
    "build_run_reflection",
    "build_static_knowledge",
    "build_tested_regions_snapshot",
    "compute_context_statistics",
    "compute_event_statistics",
    "compute_region_statistics",
    "compute_template_statistics",
    "ensure_memory_store",
    "memory_paths",
    "query_adjacent_regions",
    "query_agent_knobs",
    "query_memory_rows",
    "query_static_rows",
    "read_memory_table",
    "read_reflections",
    "write_memory_table",
    "write_reflection",
]


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = _EXPORTS[name]
    return getattr(import_module(module_name), attr_name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
