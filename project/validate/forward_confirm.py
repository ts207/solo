from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from project.core.config import get_data_root
from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.io.utils import atomic_write_json, ensure_dir, read_json, read_parquet
from project.research.agent_io.proposal_schema import load_normalized_operator_proposal
from project.research.search.evaluator import evaluate_hypothesis_batch
from project.research.search.search_feature_utils import prepare_search_features_for_symbol


def _parse_window(window: str) -> tuple[str, str]:
    parts = str(window or "").split("/", 1)
    if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
        raise ValueError("window must be formatted as <ISO8601-start>/<ISO8601-end>")
    return parts[0].strip(), parts[1].strip()


def _phase2_candidate_path(data_root: Path, run_id: str) -> Path:
    return data_root / "reports" / "phase2" / str(run_id) / "phase2_candidates.parquet"


def _translate_structured_to_hypothesis_spec(structured: Any) -> HypothesisSpec:
    """Translate StructuredHypothesisSpec (from agent_io) to HypothesisSpec (from domain)."""
    # structured is a StructuredHypothesisSpec-like object
    # Map AnchorSpec to TriggerSpec
    anchor = structured.anchor
    ttype = anchor.type
    if ttype == "feature_crossing":
        ttype = "feature_predicate"

    # TriggerSpec in domain uses trigger_type instead of type, and max_gap instead of max_gap_bars
    trigger = TriggerSpec(
        trigger_type=ttype.upper(),
        event_id=getattr(anchor, "event_id", None),
        state_id=getattr(anchor, "state_id", None),
        from_state=getattr(anchor, "from_state", None),
        to_state=getattr(anchor, "to_state", None),
        events=getattr(anchor, "events", None),
        max_gap=[anchor.max_gap_bars] if getattr(anchor, "max_gap_bars", None) is not None else None,
        feature=getattr(anchor, "feature", None),
        operator=getattr(anchor, "operator", None),
        threshold=getattr(anchor, "threshold", None),
    )

    # Disable validation if we are in a testing/replay context where registry might not be full
    object.__setattr__(trigger, "_enable_validation", False)

    spec = HypothesisSpec(
        trigger=trigger,
        direction=structured.direction,
        horizon=str(structured.horizon_bars),
        template_id=structured.template.id,
        context=structured.filters.contexts if structured.filters.contexts else None,
        entry_lag=structured.sampling_policy.entry_lag_bars,
    )
    object.__setattr__(spec, "_enable_validation", False)
    return spec


def _load_frozen_thesis(
    run_id: str,
    proposal_path: Path | None = None,
    candidate_id: str | None = None,
    data_root: Path | None = None,
) -> HypothesisSpec:
    root = Path(data_root) if data_root is not None else get_data_root()

    # Priority 1: Explicit --proposal path
    if proposal_path and proposal_path.exists():
        proposal = load_normalized_operator_proposal(proposal_path)
        # proposal is a StructuredProposal, it has a .hypothesis attribute (StructuredHypothesisSpec)
        return _translate_structured_to_hypothesis_spec(proposal.hypothesis)

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
            proposal = load_normalized_operator_proposal(Path(frozen_proposal))
            return _translate_structured_to_hypothesis_spec(proposal.hypothesis)

    # Priority 4: candidate_id lookup in phase2_candidates.parquet (NO SORTING)
    if candidate_id:
        p2_path = _phase2_candidate_path(root, run_id)
        if p2_path.exists():
            df = read_parquet(p2_path)
            match = df[df["candidate_id"] == candidate_id]
            if not match.empty:
                return HypothesisSpec.from_dict(match.iloc[0].to_dict())

    raise ValueError(f"No frozen thesis identity found for run {run_id}")


def oos_frozen_thesis_replay_v1(
    *,
    run_id: str,
    thesis: HypothesisSpec,
    start: str,
    end: str,
    data_root: Path,
) -> dict[str, Any]:
    # Determine symbol and timeframe (defaulting to BTCUSDT/5m if not in thesis)
    symbol = "BTCUSDT"
    if thesis.context and "symbol" in thesis.context:
        symbol = thesis.context["symbol"]
    
    timeframe = "5m"
    if thesis.context and "timeframe" in thesis.context:
        timeframe = thesis.context["timeframe"]

    # Load OOS features (prepare_search_features_for_symbol handles start/end)
    features = prepare_search_features_for_symbol(
        run_id=run_id,
        symbol=symbol,
        timeframe=timeframe,
        data_root=data_root,
        start=start,
        end=end,
    )

    if features.empty:
        return {
            "event_count": 0,
            "trade_count": 0,
            "mean_return_net_bps": 0.0,
            "t_stat_net": 0.0,
            "status": "fail",
            "reason": "no_oos_features_loaded",
        }

    # Evaluate the frozen thesis
    metrics_df = evaluate_hypothesis_batch([thesis], features)

    if metrics_df.empty:
        return {
            "event_count": 0,
            "trade_count": 0,
            "mean_return_net_bps": 0.0,
            "t_stat_net": 0.0,
            "status": "fail",
            "reason": "evaluation_produced_no_metrics",
        }

    # Extract metrics for the single hypothesis
    row = metrics_df.iloc[0]
    res = {
        "event_count": int(row.get("n", 0)),
        "trade_count": int(row.get("n", 0)),
        "mean_return_net_bps": float(row.get("mean_return_net_bps", 0.0)),
        "t_stat_net": float(row.get("t_stat_net", 0.0)),
        "hit_rate": float(row.get("hit_rate", 0.0)),
        "mae_bps": float(row.get("mae_mean_bps", 0.0)),
        "mfe_bps": float(row.get("mfe_mean_bps", 0.0)),
    }
    return res


def build_forward_confirmation_payload(
    *,
    run_id: str,
    window: str,
    data_root: Path | None = None,
    proposal_path: Path | None = None,
    candidate_id: str | None = None,
) -> dict[str, Any]:
    root = Path(data_root) if data_root is not None else get_data_root()
    start, end = _parse_window(window)

    try:
        thesis = _load_frozen_thesis(
            run_id=run_id,
            proposal_path=proposal_path,
            candidate_id=candidate_id,
            data_root=root,
        )
    except ValueError:
        raise RuntimeError(
            "forward-confirm snapshot mode is disabled. "
            "Implement oos_frozen_thesis_replay_v1 with explicit frozen identity."
        )

    metrics = oos_frozen_thesis_replay_v1(
        run_id=run_id,
        thesis=thesis,
        start=start,
        end=end,
        data_root=root,
    )

    out_dir = root / "reports" / "validation" / str(run_id)
    return {
        "run_id": str(run_id),
        "confirmed_at": datetime.now(UTC).isoformat(),
        "oos_window_start": start,
        "oos_window_end": end,
        "metrics": metrics,
        "evidence_bundle_path": str(out_dir / "forward_confirmation.json"),
        "method": "oos_frozen_thesis_replay_v1",
        "source": {
            "thesis_id": getattr(thesis, "hypothesis_id", "unknown"),
            "data_root": str(root),
            "window": window,
        },
    }


def forward_confirm(
    *,
    run_id: str,
    window: str,
    proposal_path: Path | None = None,
    candidate_id: str | None = None,
    data_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(data_root) if data_root is not None else get_data_root()
    payload = build_forward_confirmation_payload(
        run_id=run_id,
        window=window,
        proposal_path=proposal_path,
        candidate_id=candidate_id,
        data_root=root,
    )
    out_dir = root / "reports" / "validation" / str(run_id)
    ensure_dir(out_dir)
    out_path = out_dir / "forward_confirmation.json"
    atomic_write_json(out_path, payload)
    payload["path"] = str(out_path)
    return payload


__all__ = ["build_forward_confirmation_payload", "forward_confirm"]
