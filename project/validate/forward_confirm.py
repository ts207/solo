from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.core.config import get_data_root
from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.io.utils import atomic_write_json, ensure_dir, read_json, read_parquet
from project.research.agent_io.proposal_schema import load_normalized_operator_proposal
from project.research.search.evaluator import EvaluationContext, _weighted_newey_west_mean_std, _excursion_stats
from project.research.search.search_feature_utils import prepare_search_features_for_symbol
from project.research.phase2_cost_model import expected_cost_per_trade_bps

log = logging.getLogger(__name__)


def _to_utc_ts(value: str | pd.Timestamp) -> pd.Timestamp:
    """Normalize input to UTC-aware pandas Timestamp."""
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _parse_window(window: str) -> tuple[str, str]:
    parts = str(window or "").split("/", 1)
    if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
        raise ValueError("window must be formatted as <ISO8601-start>/<ISO8601-end>")
    return parts[0].strip(), parts[1].strip()


def _phase2_candidate_path(data_root: Path, run_id: str) -> Path:
    return data_root / "reports" / "phase2" / str(run_id) / "phase2_candidates.parquet"


def _assert_oos_window_non_overlapping(
    *,
    oos_start: str,
    oos_end: str,
    research_start: str | None,
    research_end: str | None,
) -> None:
    if not research_start or not research_end:
        raise ValueError(
            "forward-confirm requires research_start and research_end to verify OOS isolation"
        )

    o_start = _to_utc_ts(oos_start)
    o_end = _to_utc_ts(oos_end)
    r_start = _to_utc_ts(research_start)
    r_end = _to_utc_ts(research_end)

    if not (o_start > r_end or o_end < r_start):
        raise ValueError(
            f"forward-confirm OOS window [{oos_start}..{oos_end}] overlaps "
            f"research window [{research_start}..{research_end}]"
        )


def _translate_structured_to_hypothesis_spec(structured: Any) -> HypothesisSpec:
    """Translate StructuredHypothesisSpec (from agent_io) to HypothesisSpec (from domain)."""
    anchor = structured.anchor
    ttype = anchor.type
    if ttype == "feature_crossing":
        ttype = "feature_predicate"

    trigger = TriggerSpec(
        trigger_type=ttype.upper(),
        event_id=str(getattr(anchor, "event_id", "")).strip().upper() if getattr(anchor, "event_id", None) else None,
        state_id=getattr(anchor, "state_id", None),
        from_state=getattr(anchor, "from_state", None),
        to_state=getattr(anchor, "to_state", None),
        events=getattr(anchor, "events", None),
        max_gap=[anchor.max_gap_bars] if getattr(anchor, "max_gap_bars", None) is not None else None,
        feature=getattr(anchor, "feature", None),
        operator=getattr(anchor, "operator", None),
        threshold=getattr(anchor, "threshold", None),
    )

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


def _hypothesis_spec_from_promoted_dict(d: dict[str, Any]) -> HypothesisSpec:
    """Extract HypothesisSpec fields from a promoted thesis dictionary."""
    from project.live.contracts import PromotedThesis
    try:
        thesis = PromotedThesis.model_validate(d)
    except Exception as exc:
        raise ValueError(f"invalid promoted thesis schema: {exc}") from exc

    trigger_events = thesis.requirements.trigger_events
    if not trigger_events:
        raise ValueError("Promoted thesis missing trigger_events")

    trigger = TriggerSpec(
        trigger_type="EVENT",
        event_id=trigger_events[0],
    )
    object.__setattr__(trigger, "_enable_validation", False)

    spec = HypothesisSpec(
        trigger=trigger,
        direction=thesis.event_side if thesis.event_side in ("long", "short") else "long",
        horizon=str(thesis.expected_response.get("time_stop_bars", "24")),
        template_id="promoted_identity",
        context=thesis.required_context,
    )
    object.__setattr__(spec, "_enable_validation", False)
    return spec


def _load_frozen_thesis(
    run_id: str,
    proposal_path: Path | None = None,
    candidate_id: str | None = None,
    data_root: Path | None = None,
) -> tuple[HypothesisSpec, str | None, str | None]:
    root = Path(data_root) if data_root is not None else get_data_root()
    research_start = None
    research_end = None

    # Priority 1: Explicit --proposal path
    if proposal_path is not None:
        if not proposal_path.exists():
             raise FileNotFoundError(f"proposal not found: {proposal_path}")
        proposal = load_normalized_operator_proposal(proposal_path)
        research_start = getattr(proposal, "start", None)
        research_end = getattr(proposal, "end", None)
        return _translate_structured_to_hypothesis_spec(proposal.hypothesis), research_start, research_end

    # Priority 2: promoted_theses.json
    thesis_json_path = root / "live" / "theses" / run_id / "promoted_theses.json"
    if thesis_json_path.exists():
        payload = read_json(thesis_json_path)
        theses = payload.get("theses", [])
        if not theses:
             raise ValueError(f"No theses found in {thesis_json_path}")

        target_thesis = None
        if candidate_id:
            for t in theses:
                if t.get("lineage", {}).get("candidate_id") == candidate_id:
                    target_thesis = t
                    break
            if not target_thesis:
                 raise ValueError(f"Candidate {candidate_id} not found in promoted theses")
        else:
            if len(theses) > 1:
                raise ValueError(
                    f"Ambiguous promoted run {run_id} has {len(theses)} theses. "
                    "Please provide --candidate_id."
                )
            target_thesis = theses[0]

        research_start = target_thesis.get("lineage", {}).get("research_start")
        research_end = target_thesis.get("lineage", {}).get("research_end")
        return _hypothesis_spec_from_promoted_dict(target_thesis), research_start, research_end

    # Priority 3: run_manifest.json
    manifest_path = root / "runs" / run_id / "run_manifest.json"
    if manifest_path.exists():
        manifest = read_json(manifest_path)
        research_start = manifest.get("start")
        research_end = manifest.get("end")
        frozen_proposal = manifest.get("proposal_path")
        if frozen_proposal:
            # Try absolute then relative to root
            p_path = Path(frozen_proposal)
            if not p_path.exists():
                p_path = root / frozen_proposal
            if p_path.exists():
                proposal = load_normalized_operator_proposal(p_path)
                return _translate_structured_to_hypothesis_spec(proposal.hypothesis), research_start, research_end

    # Priority 4: candidate_id lookup in phase2_candidates.parquet (NO SORTING)
    if candidate_id:
        p2_path = _phase2_candidate_path(root, run_id)
        if p2_path.exists():
            df = read_parquet(p2_path)
            match = df[df["candidate_id"] == candidate_id]
            if not match.empty:
                row_dict = match.iloc[0].to_dict()
                # If we load from phase2_candidates, we might still lack research window info
                # unless it's in the row.
                return HypothesisSpec.from_dict(row_dict), research_start, research_end

    raise ValueError(f"No frozen thesis identity found for run {run_id}")


def oos_frozen_thesis_replay_v1(
    *,
    run_id: str,
    thesis: HypothesisSpec,
    start: str,
    end: str,
    data_root: Path,
) -> dict[str, Any]:
    # Determine symbol and timeframe (FAILS if not in thesis and not in context)
    symbol = None
    if thesis.context and "symbol" in thesis.context:
        symbol = str(thesis.context["symbol"]).upper()

    if not symbol:
        raise ValueError("Cannot resolve symbol from frozen thesis identity. Ensure it is in context.")

    timeframe = None
    if thesis.context and "timeframe" in thesis.context:
        timeframe = thesis.context["timeframe"]

    if not timeframe:
        log.warning("Timeframe not found in thesis context, defaulting to 5m for replay.")
        timeframe = "5m"

    # Load OOS features - NO FUTURE DATA (end=end)
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
            "status": "fail",
            "reason": "no_oos_features_loaded",
        }

    eval_context = EvaluationContext(features)
    mask, mask_reason = eval_context.event_mask(thesis, use_context_quality=True)
    if mask is None or not mask.any():
        return {
            "event_count": 0,
            "trade_count": 0,
            "status": "fail",
            "reason": mask_reason or "no_trigger_hits",
        }

    hbars = int(thesis.horizon)
    fwd = eval_context.forward_returns(hbars)
    event_returns = fwd[mask].dropna()

    if event_returns.empty:
         return {
            "event_count": 0,
            "trade_count": 0,
            "status": "fail",
            "reason": "no_valid_forward_returns",
        }

    # Strict OOS filtering: signal_ts >= start AND exit_ts <= end
    ts = pd.to_datetime(features["timestamp"], utc=True)
    oos_start_ts = _to_utc_ts(start)
    oos_end_ts = _to_utc_ts(end)

    # signal_tss corresponds to the trigger events
    signal_tss = ts.iloc[event_returns.index]
    signal_tss.index = event_returns.index

    # exit_ts = signal_ts + horizon
    exit_indices = event_returns.index + hbars
    valid_exit_mask = exit_indices < len(ts)

    exit_tss = pd.Series(index=event_returns.index, dtype="object")
    if valid_exit_mask.any():
        exit_tss.loc[valid_exit_mask] = ts.iloc[exit_indices[valid_exit_mask]].values

    exit_tss = pd.to_datetime(exit_tss, utc=True)

    # Horizon rule: drop signals if exit_ts > oos_end
    oos_mask = (signal_tss >= oos_start_ts) & (exit_tss <= oos_end_ts)

    filtered_returns = event_returns[oos_mask]
    if filtered_returns.empty:
        return {
            "event_count": 0,
            "trade_count": 0,
            "status": "fail",
            "reason": "all_events_filtered_by_oos_boundary",
        }

    # Signed returns
    direction_sign = 1.0 if thesis.direction == "long" else -1.0 if thesis.direction == "short" else 1.0
    signed = filtered_returns * direction_sign

    # Costs
    per_trade_cost_bps = expected_cost_per_trade_bps(
        features.loc[filtered_returns.index],
        thesis,
        cost_spec={"cost_bps": 2.0},
    ).reindex(filtered_returns.index).fillna(2.0).astype(float)
    signed_net = signed - per_trade_cost_bps

    # Aggregate metrics (Newey-West)
    weights = eval_context.weights[mask].loc[filtered_returns.index]
    gross_mean, gross_std, t_stat_gross = _weighted_newey_west_mean_std(signed, weights, horizon_bars=hbars)
    net_mean, net_std, t_stat_net = _weighted_newey_west_mean_std(signed_net, weights, horizon_bars=hbars)

    # Excursions
    exc_mask = pd.Series(False, index=features.index)
    exc_mask.loc[filtered_returns.index] = True
    maes, mfes = _excursion_stats(features["close"], exc_mask, hbars, direction_sign)

    n = len(filtered_returns)
    res = {
        "event_count": n,
        "trade_count": n,
        "mean_return_net_bps": round(float(net_mean), 4),
        "t_stat_net": round(float(t_stat_net), 4),
        "hit_rate": round(float((signed_net > 0).mean()), 4),
        "mae_bps": round(float(maes.mean() * 10_000.0), 4),
        "mfe_bps": round(float(mfes.mean() * 10_000.0), 4),
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
    oos_start, oos_end = _parse_window(window)

    thesis, research_start, research_end = _load_frozen_thesis(
        run_id=run_id,
        proposal_path=proposal_path,
        candidate_id=candidate_id,
        data_root=root,
    )

    _assert_oos_window_non_overlapping(
        oos_start=oos_start,
        oos_end=oos_end,
        research_start=research_start,
        research_end=research_end,
    )

    metrics = oos_frozen_thesis_replay_v1(
        run_id=run_id,
        thesis=thesis,
        start=oos_start,
        end=oos_end,
        data_root=root,
    )

    out_dir = root / "reports" / "validation" / str(run_id)
    return {
        "run_id": str(run_id),
        "confirmed_at": datetime.now(UTC).isoformat(),
        "oos_window_start": oos_start,
        "oos_window_end": oos_end,
        "metrics": metrics,
        "evidence_bundle_path": str(out_dir / "forward_confirmation.json"),
        "method": "oos_frozen_thesis_replay_v1",
        "source": {
            "thesis_id": getattr(thesis, "hypothesis_id", "unknown") if hasattr(thesis, "hypothesis_id") else "unknown",
            "data_root": str(root),
            "window": window,
            "research_start": research_start,
            "research_end": research_end,
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
