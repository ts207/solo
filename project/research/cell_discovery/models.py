from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

ExecutabilityClass = Literal["runtime", "research_only", "supportive_only"]


@dataclass(frozen=True)
class EventAtom:
    atom_id: str
    event_family: str
    event_type: str
    directions: tuple[str, ...]
    templates: tuple[str, ...]
    horizons: tuple[str, ...]
    required_feature_keys: tuple[str, ...] = ()
    thesis_eligible: bool = True
    research_only: bool = False


@dataclass(frozen=True)
class ContextCell:
    cell_id: str
    dimension: str
    values: tuple[str, ...]
    required_feature_key: str = ""
    executability_class: ExecutabilityClass = "research_only"
    max_conjunction_depth: int = 1
    supportive_context: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class HorizonSet:
    horizons: tuple[str, ...]


@dataclass(frozen=True)
class ContrastRule:
    rule_id: str
    rule_type: str
    required: bool = True
    min_lift_bps: float | None = None


@dataclass(frozen=True)
class RankingPolicy:
    min_support: int = 30
    min_forward_net_mean_bps: float = 0.0
    min_contrast_lift_bps: float = 0.0
    max_search_hypotheses: int = 1000
    forward_weight: float = 0.40
    expectancy_weight: float = 0.25
    stability_weight: float = 0.15
    contrast_weight: float = 0.15
    simplicity_weight: float = 0.05


@dataclass(frozen=True)
class DiscoveryRegistry:
    event_atoms: tuple[EventAtom, ...]
    context_cells: tuple[ContextCell, ...]
    horizons: HorizonSet
    ranking_policy: RankingPolicy
    contrast_rules: tuple[ContrastRule, ...] = ()
    spec_version: str = "edge_cells_v1"


@dataclass(frozen=True)
class CompileResult:
    run_id: str
    search_spec_path: Path
    experiment_path: Path
    lineage_path: Path
    skipped_cells_path: Path
    estimated_hypothesis_count: int
    cell_count: int
    family_counts: dict[str, int] = field(default_factory=dict)
    skipped_cell_count: int = 0


@dataclass(frozen=True)
class DataFeasibilityResult:
    status: str
    report_path: Path
    payload: dict[str, Any]
