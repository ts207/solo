from __future__ import annotations

import importlib
from typing import Iterable, Tuple

DETECTOR_FAMILY_MODULES: Tuple[str, ...] = (
    "project.events.families.basis",
    "project.events.families.funding",
    "project.events.families.liquidation",
    "project.events.families.liquidity",
    "project.events.families.oi",
    "project.events.families.canonical_proxy",
    "project.events.families.volatility",
    "project.events.families.regime",
    "project.events.families.temporal",
    "project.events.families.desync",
    "project.events.families.trend",
    "project.events.families.statistical",
    "project.events.families.exhaustion",
    "project.events.families.sequence",
    "project.events.families.interaction",
    "project.events.detectors.extended_detectors",
)


def load_detector_family_modules(modules: Iterable[str] = DETECTOR_FAMILY_MODULES) -> None:
    for module_name in modules:
        importlib.import_module(module_name)
