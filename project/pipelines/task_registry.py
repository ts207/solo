from __future__ import annotations

from collections.abc import Callable

# Registry of importable Tasks.
# A Task is a function with signature: def run_task(run_id: str, args: List[str]) -> int
_TASKS: dict[str, Callable[[str, list[str]], int]] = {}


def register_task(name: str, func: Callable[[str, list[str]], int]) -> None:
    """Register a function as an in-process Task."""
    _TASKS[name] = func


def get_task(name: str) -> Callable[[str, list[str]], int] | None:
    """Retrieve a registered Task."""
    return _TASKS.get(name)


def list_tasks() -> list[str]:
    return sorted(_TASKS.keys())
