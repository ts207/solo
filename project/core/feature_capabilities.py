from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Any, Callable

LOGGER = logging.getLogger(__name__)

# Registry of feature loaders. This avoids direct module imports in cross-domain code.
# The registry lives in core, but features modules register themselves into it.
_FEATURE_LOADERS: Dict[str, Callable[[Path, str, str], Any]] = {}


def register_feature_loader(name: str, loader: Callable[[Path, str, str], Any]) -> None:
    _FEATURE_LOADERS[name] = loader


def has_feature_family(name: str) -> bool:
    return name in _FEATURE_LOADERS


def resolve_feature_loader(name: str) -> Callable[[Path, str, str], Any] | None:
    return _FEATURE_LOADERS.get(name)
