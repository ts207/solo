from __future__ import annotations

from pathlib import Path
from typing import Any

from project.core.config import get_data_root
from project.domain.hypotheses import HypothesisSpec
from project.io.utils import atomic_write_json, ensure_dir, read_json, read_parquet
from project.research.agent_io.proposal_schema import load_operator_proposal


def _phase2_candidate_path(data_root: Path, run_id: str) -> Path:
    return data_root / "reports" / "phase2" / run_id / "phase2_candidates.parquet"


def _load_frozen_thesis(
    run_id: str,
    proposal_path: Path | None = None,
    candidate_id: str | None = None,
    data_root: Path | None = None,
) -> HypothesisSpec:
    root = Path(data_root) if data_root is not None else get_data_root()

    # Priority 1: Explicit --proposal path
    if proposal_path and proposal_path.exists():
        proposal = load_operator_proposal(proposal_path)
        # Note: Depending on proposal structure, might need specific mapping
        # Assume proposal.hypothesis is convertible to HypothesisSpec
        return HypothesisSpec.from_dict(proposal.hypothesis.to_dict())

    # Priority 2: promoted_theses.json
    thesis_json_path = root / "live" / "theses" / run_id / "promoted_theses.json"
    if thesis_json_path.exists():
        payload = read_json(thesis_json_path)
        theses = payload.get("theses", [])
        if candidate_id:
            for t in theses:
                if t.get("candidate_id") == candidate_id:
                    return HypothesisSpec.from_dict(t)
        elif theses:
            return HypothesisSpec.from_dict(theses[0])

    # Priority 3: run_manifest.json
    manifest_path = root / "runs" / run_id / "run_manifest.json"
    if manifest_path.exists():
        manifest = read_json(manifest_path)
        frozen_proposal = manifest.get("proposal_path")
        if frozen_proposal:
            proposal = load_operator_proposal(Path(frozen_proposal))
            return HypothesisSpec.from_dict(proposal.hypothesis.to_dict())

    # Priority 4: candidate_id lookup in phase2_candidates.parquet (NO SORTING)
    if candidate_id:
        p2_path = _phase2_candidate_path(root, run_id)
        if p2_path.exists():
            df = read_parquet(p2_path)
            match = df[df["candidate_id"] == candidate_id]
            if not match.empty:
                return HypothesisSpec.from_dict(match.iloc[0].to_dict())

    raise ValueError(f"No frozen thesis identity found for run {run_id}")


def build_forward_confirmation_payload(
    *,
    run_id: str,
    window: str,
    data_root: Path | None = None,
) -> dict[str, Any]:
    raise RuntimeError(
        "forward-confirm currently cannot use phase2 candidate snapshots; "
        "implement oos_frozen_thesis_replay_v1"
    )


def forward_confirm(
    *,
    run_id: str,
    window: str,
    data_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(data_root) if data_root is not None else get_data_root()
    payload = build_forward_confirmation_payload(run_id=run_id, window=window, data_root=root)
    out_dir = root / "reports" / "validation" / str(run_id)
    ensure_dir(out_dir)
    out_path = out_dir / "forward_confirmation.json"
    atomic_write_json(out_path, payload)
    payload["path"] = str(out_path)
    return payload


__all__ = ["build_forward_confirmation_payload", "forward_confirm"]
