from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from project.specs.gates import (
    load_all_families_spec as _shared_load_all_families_spec,
    load_family_spec as _shared_load_family_spec,
    load_gates_spec as _shared_load_gates_spec,
    select_phase2_gate_spec as _shared_select_phase2_gate_spec,
)


def _load_gates_spec(repo_root: Path) -> Dict[str, Any]:
    return _shared_load_gates_spec(repo_root)


def _select_phase2_gate_spec(
    gates_spec: Dict[str, Any],
    *,
    mode: str,
    gate_profile: str,
) -> Dict[str, Any]:
    return _shared_select_phase2_gate_spec(
        gates_spec,
        mode=mode,
        gate_profile=gate_profile,
    )


def _load_family_spec(repo_root: Path) -> Dict[str, Any]:
    return _shared_load_all_families_spec(repo_root)
