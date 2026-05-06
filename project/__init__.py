from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent


def __getattr__(name: str):
    if name in {"discover", "promote", "validate"}:
        import importlib

        return importlib.import_module(f"{__name__}.{name}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
