from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional


class ModeId(str, Enum):
    A = "A"
    B = "B"
    C = "C"
    D = "D"
    E = "E"
    F = "F"


@dataclass(frozen=True)
class DiscoveryBenchmarkMode:
    mode_id: str
    label: str
    search_topology: str
    scoring_version: str
    fold_validation: str
    ledger_adjustment: str
    shortlist_selection: str

    @property
    def is_runnable(self) -> bool:
        return True


_MODES: Dict[str, DiscoveryBenchmarkMode] = {
    "A": DiscoveryBenchmarkMode(
        mode_id="A",
        label="baseline_flat",
        search_topology="flat",
        scoring_version="v1",
        fold_validation="disabled",
        ledger_adjustment="disabled",
        shortlist_selection="disabled",
    ),
    "B": DiscoveryBenchmarkMode(
        mode_id="B",
        label="flat_v2_scoring",
        search_topology="flat",
        scoring_version="v2",
        fold_validation="disabled",
        ledger_adjustment="disabled",
        shortlist_selection="disabled",
    ),
    "C": DiscoveryBenchmarkMode(
        mode_id="C",
        label="flat_v2_with_folds",
        search_topology="flat",
        scoring_version="v2",
        fold_validation="enabled",
        ledger_adjustment="disabled",
        shortlist_selection="disabled",
    ),
    "D": DiscoveryBenchmarkMode(
        mode_id="D",
        label="hierarchical_v2_with_folds",
        search_topology="hierarchical",
        scoring_version="v2",
        fold_validation="enabled",
        ledger_adjustment="disabled",
        shortlist_selection="disabled",
    ),
    "E": DiscoveryBenchmarkMode(
        mode_id="E",
        label="hierarchical_v2_folds_ledger",
        search_topology="hierarchical",
        scoring_version="v2",
        fold_validation="enabled",
        ledger_adjustment="enabled",
        shortlist_selection="disabled",
    ),
    "F": DiscoveryBenchmarkMode(
        mode_id="F",
        label="hierarchical_v2_folds_ledger_diversified",
        search_topology="hierarchical",
        scoring_version="v2",
        fold_validation="enabled",
        ledger_adjustment="enabled",
        shortlist_selection="enabled",
    ),
}

_LABEL_TO_MODE: Dict[str, DiscoveryBenchmarkMode] = {
    m.label: m for m in _MODES.values()
}


def get_mode(mode_id_or_label: str) -> Optional[DiscoveryBenchmarkMode]:
    if mode_id_or_label in _MODES:
        return _MODES[mode_id_or_label]
    return _LABEL_TO_MODE.get(mode_id_or_label)


def all_modes() -> List[DiscoveryBenchmarkMode]:
    return list(_MODES.values())


def runnable_modes() -> List[DiscoveryBenchmarkMode]:
    return [m for m in all_modes() if m.is_runnable]


def validate_mode_progression() -> bool:
    modes = all_modes()
    if len(modes) != 6:
        return False
    expected_order = ["A", "B", "C", "D", "E", "F"]
    return [m.mode_id for m in modes] == expected_order
