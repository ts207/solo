from __future__ import annotations

from typing import Dict, List, Tuple
from project import PROJECT_ROOT
from project.pipelines.pipeline_defaults import DATA_ROOT
from project.pipelines.pipeline_provenance import data_fingerprint


def validate_phase2_event_chain(phase2_event_chain, event_registry_specs) -> List[str]:
    import project.events.detectors.registry as _det_reg

    _det_reg.load_all_detectors()
    issues = []
    for etype, script, _ in phase2_event_chain:
        if etype not in event_registry_specs:
            issues.append(f"Missing event spec/registry entry for {etype}")
        script_path = PROJECT_ROOT / "research" / str(script)
        if not script_path.exists():
            issues.append(f"Missing phase2 analyzer script for {etype}: {script}")
        if _det_reg.get_detector(etype) is None:
            issues.append(f"No registered detector for {etype}")
    return issues


def compute_data_fingerprint(
    symbols: List[str],
    run_id: str,
    *,
    runtime_invariants: Dict[str, object] | None = None,
    objective_profile: Dict[str, object] | None = None,
    effective_config_hash: str | None = None,
) -> Tuple[str, Dict[str, object]]:
    digest, lineage = data_fingerprint(
        symbols,
        run_id,
        project_root=PROJECT_ROOT,
        data_root=DATA_ROOT,
        runtime_invariants=runtime_invariants,
        objective_profile=objective_profile,
        effective_config_hash=effective_config_hash,
    )
    lake = lineage.get("lake", {}) if isinstance(lineage, dict) else {}
    if isinstance(lake, dict):
        lineage.setdefault("file_count", int(lake.get("file_count", 0) or 0))
        lineage.setdefault("lake_digest", str(lake.get("digest", "")))
    return digest, lineage
