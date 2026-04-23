from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import yaml
from project.core.exceptions import DataIntegrityError
from project.domain.compiled_registry import get_domain_registry
from project.research.search.bridge_adapter import canonical_bridge_event_type
from project.research.context_labels import canonicalize_context_label
from project.spec_registry.search_space import DEFAULT_EVENT_PRIORITY_WEIGHT as _DEFAULT_QUALITY

_LOG = logging.getLogger(__name__)


def _as_str_list(values: Any) -> List[str]:
    if values is None:
        return []
    if isinstance(values, str):
        token = values.strip()
        return [token] if token else []
    if not isinstance(values, (list, tuple, set)):
        return []
    return [str(value).strip() for value in values if str(value).strip()]


def _read_memory_table(*args: Any, **kwargs: Any) -> pd.DataFrame:
    from project.research import campaign_controller as _controller

    return _controller.read_memory_table(*args, **kwargs)


def _memory_state_key(state_id: str) -> str:
    return canonical_bridge_event_type("state", f"state:{state_id}")


def _memory_transition_key(from_state: str, to_state: str) -> str:
    return canonical_bridge_event_type("transition", f"transition:{from_state}→{to_state}")


def _memory_feature_predicate_key(feature: str, operator: str, threshold: Any) -> str:
    return canonical_bridge_event_type("feature_predicate", f"pred:{feature}{operator}{threshold}")


def _memory_sequence_key(events: List[str], gap: int) -> str:
    payload = "|".join(events) + f"|gap={gap}"
    seq_id = "SEQ_" + hashlib.sha256(payload.encode()).hexdigest()[:12].upper()
    return canonical_bridge_event_type("sequence", f"seq:{seq_id}")


def _memory_interaction_key(left: str, right: str, op: str, lag: int) -> str:
    payload = f"{left}|{op}|{right}|lag={lag}"
    int_id = "INT_" + hashlib.sha256(payload.encode()).hexdigest()[:12].upper()
    return canonical_bridge_event_type("interaction", f"int:{int_id}({op})")


def _parse_transition_event_type(event_type: str) -> tuple[str, str] | None:
    text = str(event_type or "").strip()
    prefix = "TRANSITION_"
    if not text.startswith(prefix):
        return None
    rest = text[len(prefix) :]
    parts = rest.split("_STATE_", 1)
    if len(parts) != 2:
        return None
    from_state = f"{parts[0]}_STATE"
    to_state = parts[1] if parts[1].endswith("_STATE") else f"{parts[1]}_STATE"
    if not from_state or not to_state:
        return None
    return from_state, to_state


def build_proposal_from_memory_scope(
    ctrl: Any,
    scope: Dict[str, Any],
    *,
    description: str,
    promotion_enabled: bool,
    date_scope: tuple[str, str],
    default_horizons: List[int],
) -> Optional[Dict[str, Any]]:
    trigger_payload = {}
    raw_trigger_payload = scope.get("trigger_payload_json", scope.get("trigger_payload", "{}"))
    if isinstance(raw_trigger_payload, dict):
        trigger_payload = raw_trigger_payload
    else:
        try:
            trigger_payload = json.loads(str(raw_trigger_payload or "{}"))
        except Exception:
            trigger_payload = {}
    if not isinstance(trigger_payload, dict):
        trigger_payload = {}

    trigger_type = str(scope.get("trigger_type", "EVENT")).strip().upper() or "EVENT"
    event_type = str(scope.get("event_type", "")).strip()
    template_id = str(scope.get("template_id", "")).strip()
    raw_contexts = scope.get("contexts", {})
    contexts = raw_contexts if isinstance(raw_contexts, dict) else {}
    templates = [template_id] if template_id else (
        ctrl._templates_for_event(event_type)
        if trigger_type == "EVENT" and event_type
        else ["mean_reversion", "continuation"]
    )

    horizon_token = str(scope.get("horizon", "")).strip().lower()
    parsed_horizon = (
        int(horizon_token[:-1])
        if horizon_token.endswith("b") and horizon_token[:-1].isdigit()
        else int(horizon_token)
        if horizon_token.isdigit()
        else None
    )
    horizons = [parsed_horizon] if parsed_horizon is not None else list(default_horizons)
    direction = str(scope.get("direction", "")).strip().lower()
    directions = [direction] if direction else None
    raw_entry_lag = scope.get("entry_lag", scope.get("entry_lag_bars"))
    try:
        entry_lags = [int(raw_entry_lag)] if raw_entry_lag not in (None, "") else None
    except (TypeError, ValueError):
        entry_lags = None

    kwargs: Dict[str, Any] = {
        "events": [],
        "templates": templates,
        "horizons": horizons,
        "directions": directions,
        "entry_lags": entry_lags,
        "description": description,
        "promotion_enabled": promotion_enabled,
        "date_scope": date_scope,
        "trigger_type": trigger_type,
        "contexts": contexts,
        "canonical_regimes": _as_str_list(scope.get("canonical_regimes", []))
        if "canonical_regimes" in scope
        else [],
        "subtypes": _as_str_list(scope.get("subtypes", [])) if "subtypes" in scope else [],
        "phases": _as_str_list(scope.get("phases", [])) if "phases" in scope else [],
        "evidence_modes": _as_str_list(scope.get("evidence_modes", []))
        if "evidence_modes" in scope
        else [],
    }
    if trigger_type == "EVENT":
        if not event_type:
            return None
        kwargs["events"] = [event_type]
    elif trigger_type == "STATE":
        state_id = str(scope.get("state_id", trigger_payload.get("state_id", ""))).strip()
        if not state_id and event_type.startswith("STATE_"):
            state_id = event_type[len("STATE_") :]
        if not state_id:
            return None
        kwargs["states"] = [state_id]
    elif trigger_type == "TRANSITION":
        from_state = str(scope.get("from_state", trigger_payload.get("from_state", ""))).strip()
        to_state = str(scope.get("to_state", trigger_payload.get("to_state", ""))).strip()
        if (not from_state or not to_state) and event_type:
            parsed = _parse_transition_event_type(event_type)
            if parsed is not None:
                from_state, to_state = parsed
        if not from_state or not to_state:
            return None
        kwargs["transitions"] = [{"from_state": from_state, "to_state": to_state}]
    elif trigger_type == "FEATURE_PREDICATE":
        feature = str(scope.get("feature", trigger_payload.get("feature", ""))).strip()
        operator = str(scope.get("operator", trigger_payload.get("operator", ""))).strip()
        threshold = scope.get("threshold", trigger_payload.get("threshold"))
        if not feature or not operator or threshold in (None, ""):
            return None
        kwargs["feature_predicates"] = [
            {"feature": feature, "operator": operator, "threshold": threshold}
        ]
    elif trigger_type == "SEQUENCE":
        sequences = scope.get("sequences")
        if not isinstance(sequences, dict):
            events = trigger_payload.get("events")
            max_gap = trigger_payload.get("max_gap")
            if not isinstance(events, list) or not events:
                return None
            sequences = {
                "include": [events],
                "max_gaps_bars": max_gap if isinstance(max_gap, list) and max_gap else [6, 12],
            }
        kwargs["sequences"] = sequences
    elif trigger_type == "INTERACTION":
        interactions = scope.get("interactions")
        if not isinstance(interactions, list) or not interactions:
            left = str(trigger_payload.get("left", "")).strip()
            right = str(trigger_payload.get("right", "")).strip()
            op = str(trigger_payload.get("op", "")).strip().upper()
            lag = int(trigger_payload.get("lag", 6) or 6)
            if not left or not right or not op:
                return None
            interactions = [{"left": left, "right": right, "op": op, "lag": lag}]
        kwargs["interactions"] = interactions
    else:
        return None

    return ctrl._build_proposal(**kwargs)


def step_scan_frontier(ctrl: Any, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for trigger_type in ctrl.config.scan_trigger_types:
        result = ctrl._step_scan_for_type(trigger_type, mem)
        if result is not None:
            return result
    _LOG.info("STEP 4 SCAN: all trigger-type tiers exhausted.")
    return None


def step_scan_for_type(
    ctrl: Any, trigger_type: str, mem: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    t = trigger_type.upper()
    if t == "EVENT":
        return ctrl._step_scan_events(mem)
    if t == "STATE":
        return ctrl._step_scan_states(mem)
    if t == "TRANSITION":
        return ctrl._step_scan_transitions(mem)
    if t == "FEATURE_PREDICATE":
        return ctrl._step_scan_feature_predicates(mem)
    if t == "SEQUENCE":
        return ctrl._step_scan_sequences(mem)
    if t == "INTERACTION":
        return ctrl._step_scan_interactions(mem)
    _LOG.warning("STEP 4 SCAN: unknown trigger_type=%s — skipping.", trigger_type)
    return None


def step_scan_events(ctrl: Any, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    avoid_events: Set[str] = set(mem.get("avoid_event_types", set()))
    event_to_regime = ctrl._event_to_regime_map()
    regime_to_events = ctrl._executable_regime_event_fanout()

    tested_events: Set[str] = set()
    tested_regimes: Set[str] = set()
    try:
        tested_df = _read_memory_table(ctrl.config.program_id, "tested_regions", data_root=ctrl.data_root)
        if not tested_df.empty and "event_type" in tested_df.columns:
            tested_events = set(tested_df["event_type"].astype(str).unique())
            if "canonical_regime" in tested_df.columns:
                tested_regimes = set(
                    tested_df["canonical_regime"].astype(str).str.strip().str.upper()
                ) - {""}
    except Exception:
        _LOG.warning("Failed to read superseded stages from memory", exc_info=True)

    if ctrl.ledger_path.exists():
        try:
            ledger = pd.read_parquet(ctrl.ledger_path)
            if "trigger_payload" in ledger.columns:
                def _eid(payload: object) -> Optional[str]:
                    try:
                        parsed = json.loads(str(payload))
                        value = str(parsed.get("event_id", "")).strip()
                        return value or None
                    except Exception:
                        return None

                tested_events |= set(ledger["trigger_payload"].apply(_eid).dropna().astype(str))
        except Exception:
            _LOG.warning("Failed to extract tested events from campaign ledger; skipping.", exc_info=True)

    for event_id in tested_events:
        regime = event_to_regime.get(event_id, "")
        if regime:
            tested_regimes.add(regime)

    regime_candidates: Dict[str, List[str]] = {}
    for regime, event_ids in regime_to_events.items():
        if regime in tested_regimes:
            continue
        candidates = [
            event_id
            for event_id in event_ids
            if event_id not in tested_events and event_id not in avoid_events
        ]
        if candidates:
            regime_candidates[regime] = candidates

    if not regime_candidates:
        _LOG.info("STEP 4 SCAN [EVENT]: frontier exhausted.")
        return None

    best_regime = max(
        regime_candidates,
        key=lambda regime: max(
            ctrl._quality_weights.get(event_id, _DEFAULT_QUALITY)
            for event_id in regime_candidates[regime]
        ),
    )
    candidates = sorted(
        regime_candidates[best_regime],
        key=lambda event_id: ctrl._quality_weights.get(event_id, _DEFAULT_QUALITY),
        reverse=True,
    )
    best_family = best_regime
    to_test = candidates[:3]
    _LOG.info(
        "STEP 4 SCAN [EVENT regime=%s]: events=%s quality=%s",
        best_regime,
        to_test,
        [ctrl._quality_weights.get(event_id, _DEFAULT_QUALITY) for event_id in to_test],
    )
    return ctrl._build_proposal(
        events=to_test,
        canonical_regimes=[best_regime],
        templates=["mean_reversion", "continuation"],
        horizons=[12, 24],
        description=f"EVENT scan [{best_family}] — {', '.join(to_test)}",
        promotion_enabled=False,
        date_scope=ctrl.config.scan_event_date_scope,
        trigger_type="EVENT",
        contexts=ctrl._context_for_proposal(),
    )


def step_scan_states(ctrl: Any, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    del mem
    ss_states = ctrl._load_search_space_states()
    if not ss_states:
        return None

    tested_states: Set[str] = set()
    try:
        tested_df = _read_memory_table(ctrl.config.program_id, "tested_regions", data_root=ctrl.data_root)
        if not tested_df.empty and "event_type" in tested_df.columns and "trigger_type" in tested_df.columns:
            state_rows = tested_df[tested_df["trigger_type"].astype(str) == "STATE"]
            if not state_rows.empty:
                tested_states = {
                    str(value)[len("STATE_") :] if str(value).startswith("STATE_") else str(value)
                    for value in state_rows["event_type"].astype(str).unique()
                }
    except Exception:
        _LOG.warning("Failed to read superseded stages from memory", exc_info=True)

    candidates = [state_id for state_id in ss_states if state_id not in tested_states]
    if not candidates:
        _LOG.info("STEP 4 SCAN [STATE]: frontier exhausted.")
        return None

    to_test = candidates[:4]
    _LOG.info("STEP 4 SCAN [STATE]: states=%s", to_test)
    return ctrl._build_proposal(
        events=[],
        templates=["mean_reversion", "continuation"],
        horizons=[12, 24],
        description=f"STATE scan — {', '.join(to_test)}",
        promotion_enabled=False,
        date_scope=ctrl.config.scan_general_date_scope,
        trigger_type="STATE",
        states=to_test,
        contexts=ctrl._context_for_proposal(),
    )


def step_scan_transitions(ctrl: Any, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    del mem
    ss_transitions = ctrl._load_search_space_transitions()
    if not ss_transitions:
        return None

    tested_keys: Set[str] = set()
    try:
        tested_df = _read_memory_table(ctrl.config.program_id, "tested_regions", data_root=ctrl.data_root)
        if not tested_df.empty and "trigger_type" in tested_df.columns:
            tr_rows = tested_df[tested_df["trigger_type"].astype(str) == "TRANSITION"]
            if not tr_rows.empty and "event_type" in tr_rows.columns:
                tested_keys = set(tr_rows["event_type"].astype(str).unique())
    except Exception:
        _LOG.warning("Failed to read superseded stages from memory", exc_info=True)

    candidates = [
        transition
        for transition in ss_transitions
        if _memory_transition_key(transition["from_state"], transition["to_state"])
        not in tested_keys
    ]
    if not candidates:
        _LOG.info("STEP 4 SCAN [TRANSITION]: frontier exhausted.")
        return None

    to_test = candidates[:3]
    labels = [f"{transition['from_state']}→{transition['to_state']}" for transition in to_test]
    _LOG.info("STEP 4 SCAN [TRANSITION]: %s", labels)
    return ctrl._build_proposal(
        events=[],
        templates=["mean_reversion", "continuation"],
        horizons=[12, 24],
        description=f"TRANSITION scan — {', '.join(labels)}",
        promotion_enabled=False,
        date_scope=ctrl.config.scan_general_date_scope,
        trigger_type="TRANSITION",
        transitions=to_test,
        contexts=ctrl._context_for_proposal(),
    )


def step_scan_feature_predicates(ctrl: Any, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    del mem
    static_preds = ctrl._load_search_space_predicates()
    mi_preds = ctrl._load_mi_candidate_predicates()

    def _pred_key(pred: Dict[str, Any]) -> str:
        return f"{pred['feature']}|{pred['operator']}|{pred['threshold']}"

    seen_keys: set[str] = set()
    merged: List[Dict[str, Any]] = []
    for pred in static_preds:
        key = _pred_key(pred)
        if key not in seen_keys:
            seen_keys.add(key)
            merged.append(pred)

    for pred in sorted(mi_preds, key=lambda item: float(item.get("mi_score", 0.0)), reverse=True):
        key = _pred_key(pred)
        if key not in seen_keys:
            seen_keys.add(key)
            merged.append(pred)

    if not merged:
        _LOG.info("STEP 4 SCAN [FEATURE_PREDICATE]: no predicates available.")
        return None

    tested_keys: set[str] = set()
    try:
        tested_df = _read_memory_table(ctrl.config.program_id, "tested_regions", data_root=ctrl.data_root)
        if not tested_df.empty and "trigger_type" in tested_df.columns:
            fp_rows = tested_df[tested_df["trigger_type"].astype(str) == "FEATURE_PREDICATE"]
            if not fp_rows.empty and "event_type" in fp_rows.columns:
                tested_keys = set(fp_rows["event_type"].astype(str).unique())
    except Exception:
        _LOG.warning("Failed to read superseded stages from memory", exc_info=True)

    candidates = [
        pred
        for pred in merged
        if _memory_feature_predicate_key(
            pred["feature"], pred["operator"], pred["threshold"]
        )
        not in tested_keys
    ]
    if not candidates:
        _LOG.info("STEP 4 SCAN [FEATURE_PREDICATE]: frontier exhausted.")
        return None

    to_test = candidates[:8]
    mi_count = sum(1 for pred in to_test if pred.get("source") == "mi_scan")
    _LOG.info(
        "STEP 4 SCAN [FEATURE_PREDICATE]: %d predicates (%d static, %d MI-generated)",
        len(to_test),
        len(to_test) - mi_count,
        mi_count,
    )
    return ctrl._build_proposal(
        events=[],
        templates=["mean_reversion", "continuation"],
        horizons=[12, 24],
        description=f"FEATURE_PREDICATE scan — {len(to_test)} predicates ({mi_count} MI)",
        promotion_enabled=False,
        date_scope=ctrl.config.scan_general_date_scope,
        trigger_type="FEATURE_PREDICATE",
        feature_predicates=to_test,
        contexts=ctrl._context_for_proposal(),
    )


def step_scan_sequences(ctrl: Any, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    del mem
    pairs = ctrl._find_weak_signal_event_pairs()
    if not pairs:
        _LOG.info("STEP 4 SCAN [SEQUENCE]: no weak-signal pairs found.")
        return None

    tested_keys: set[str] = set()
    try:
        tested_df = _read_memory_table(ctrl.config.program_id, "tested_regions", data_root=ctrl.data_root)
        if not tested_df.empty and "trigger_type" in tested_df.columns:
            seq_rows = tested_df[tested_df["trigger_type"].astype(str) == "SEQUENCE"]
            if not seq_rows.empty and "event_type" in seq_rows.columns:
                tested_keys = set(seq_rows["event_type"].astype(str).unique())
    except Exception:
        _LOG.warning("Failed to read superseded stages from memory", exc_info=True)

    sequences: List[List[str]] = []
    for pair in pairs:
        sequence = list(pair)
        candidate_keys = {_memory_sequence_key(sequence, gap) for gap in [6, 12]}
        if candidate_keys.intersection(tested_keys):
            continue
        sequences.append(sequence)
        if len(sequences) >= 5:
            break
    if not sequences:
        _LOG.info("STEP 4 SCAN [SEQUENCE]: frontier exhausted.")
        return None

    labels = [f"{left}→{right}" for left, right in sequences]
    _LOG.info("STEP 4 SCAN [SEQUENCE]: pairs=%s", labels)
    return ctrl._build_proposal(
        events=[],
        templates=["mean_reversion", "continuation"],
        horizons=[12, 24],
        description=f"SEQUENCE scan — {', '.join(labels)}",
        promotion_enabled=False,
        date_scope=ctrl.config.scan_general_date_scope,
        trigger_type="SEQUENCE",
        sequences={"include": sequences, "max_gaps_bars": [6, 12]},
        contexts=ctrl._context_for_proposal(),
    )


def step_scan_interactions(ctrl: Any, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    del mem
    motifs = ctrl._load_interaction_motifs()
    if not motifs:
        _LOG.info("STEP 4 SCAN [INTERACTION]: no motifs in interaction_registry.yaml.")
        return None

    tested_keys: Set[str] = set()
    try:
        tested_df = _read_memory_table(ctrl.config.program_id, "tested_regions", data_root=ctrl.data_root)
        if not tested_df.empty and "trigger_type" in tested_df.columns:
            int_rows = tested_df[tested_df["trigger_type"].astype(str) == "INTERACTION"]
            if not int_rows.empty and "event_type" in int_rows.columns:
                tested_keys = set(int_rows["event_type"].astype(str).unique())
    except Exception:
        _LOG.warning("Failed to read superseded stages from memory", exc_info=True)

    def _motif_key(motif: Dict[str, Any]) -> str:
        return f"{motif['left']}|{motif['op']}|{motif['right']}"

    candidates = [
        motif
        for motif in motifs
        if _memory_interaction_key(
            motif["left"], motif["right"], motif["op"].upper(), int(motif.get("lag", 6))
        )
        not in tested_keys
    ]
    if not candidates:
        _LOG.info("STEP 4 SCAN [INTERACTION]: frontier exhausted.")
        return None

    to_test = candidates[:3]
    labels = [f"{motif['left']} {motif['op']} {motif['right']}" for motif in to_test]
    _LOG.info("STEP 4 SCAN [INTERACTION]: %s", labels)
    interactions = [
        {
            "left": motif["left"],
            "right": motif["right"],
            "op": motif["op"].upper(),
            "lag": int(motif.get("lag", 6)),
        }
        for motif in to_test
    ]
    return ctrl._build_proposal(
        events=[],
        templates=["mean_reversion", "continuation"],
        horizons=[12, 24],
        description=f"INTERACTION scan — {', '.join(labels)}",
        promotion_enabled=False,
        date_scope=ctrl.config.scan_general_date_scope,
        trigger_type="INTERACTION",
        interactions=interactions,
        contexts=ctrl._context_for_proposal(),
    )


def load_interaction_motifs(ctrl: Any) -> List[Dict[str, Any]]:
    try:
        candidates = [
            ctrl._search_space_path.parent / "grammar" / "interaction_registry.yaml",
            Path(__file__).parent.parent.parent.parent / "spec" / "grammar" / "interaction_registry.yaml",
        ]
        path = next((candidate for candidate in candidates if candidate.exists()), None)
        if path is None:
            return []
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        motifs = raw.get("motifs", [])
        return [
            motif
            for motif in motifs
            if isinstance(motif, dict) and "left" in motif and "right" in motif and "op" in motif
        ]
    except Exception:
        _LOG.warning("Failed to load search space component from %s", ctrl._search_space_path, exc_info=True)
        return []


def step_scan_frontier_cross_family(ctrl: Any, mem: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    enabled_events = [
        event_id
        for event_ids in ctrl._executable_regime_event_fanout().values()
        for event_id in event_ids
    ]
    event_to_regime = ctrl._event_to_regime_map()

    tested_events: Set[str] = set()
    try:
        tested_df = _read_memory_table(ctrl.config.program_id, "tested_regions", data_root=ctrl.data_root)
        if not tested_df.empty and "event_type" in tested_df.columns:
            tested_events = set(tested_df["event_type"].astype(str).unique())
    except Exception:
        _LOG.warning("Failed to read superseded stages from memory", exc_info=True)

    if ctrl.ledger_path.exists():
        try:
            ledger = pd.read_parquet(ctrl.ledger_path)
            if "trigger_payload" in ledger.columns:
                def _eid(payload: object) -> Optional[str]:
                    try:
                        parsed = json.loads(str(payload))
                        value = str(parsed.get("event_id", "")).strip()
                        return value or None
                    except Exception:
                        return None

                tested_events |= set(ledger["trigger_payload"].apply(_eid).dropna().astype(str))
        except Exception:
            _LOG.warning(
                "Failed to extract tested events from campaign ledger (step 4); skipping.",
                exc_info=True,
            )

    avoid_events: Set[str] = mem["avoid_event_types"]
    candidates = [
        event_id for event_id in enabled_events if event_id not in tested_events and event_id not in avoid_events
    ]
    if not candidates:
        _LOG.info("STEP 4 EXPLORE (cross-family): frontier exhausted.")
        return None

    candidates.sort(key=lambda event_id: ctrl._quality_weights.get(event_id, _DEFAULT_QUALITY), reverse=True)
    to_test = candidates[:5]
    regimes = {event_to_regime.get(event_id, "?") for event_id in to_test}
    _LOG.info("STEP 4 EXPLORE (cross-regime=%s): events=%s", sorted(regimes), to_test)
    return ctrl._build_proposal(
        events=to_test,
        canonical_regimes=sorted(regime for regime in regimes if regime and regime != "?"),
        templates=["mean_reversion", "continuation"],
        horizons=[12, 24],
        description=f"Cross-family explore — {', '.join(to_test)}",
        promotion_enabled=False,
        date_scope=ctrl.config.scan_general_date_scope,
        trigger_type="EVENT",
        contexts=ctrl._context_for_proposal(),
    )


def load_search_space_states(ctrl: Any) -> List[str]:
    try:
        if not ctrl._search_space_path.exists():
            return []
        raw = yaml.safe_load(ctrl._search_space_path.read_text(encoding="utf-8"))
        return [str(state_id) for state_id in (raw or {}).get("triggers", {}).get("states", [])]
    except Exception:
        _LOG.warning("Failed to load search space component from %s", ctrl._search_space_path, exc_info=True)
        return []


def load_search_space_transitions(ctrl: Any) -> List[Dict[str, str]]:
    try:
        if not ctrl._search_space_path.exists():
            return []
        raw = yaml.safe_load(ctrl._search_space_path.read_text(encoding="utf-8"))
        out = []
        for transition in (raw or {}).get("triggers", {}).get("transitions", []):
            if isinstance(transition, dict) and "from" in transition and "to" in transition:
                out.append({"from_state": str(transition["from"]), "to_state": str(transition["to"])})
        return out
    except Exception:
        _LOG.warning("Failed to load search space component from %s", ctrl._search_space_path, exc_info=True)
        return []


def load_search_space_predicates(ctrl: Any) -> List[Dict[str, Any]]:
    try:
        if not ctrl._search_space_path.exists():
            return []
        raw = yaml.safe_load(ctrl._search_space_path.read_text(encoding="utf-8"))
        preds = (raw or {}).get("triggers", {}).get("feature_predicates", [])
        return [pred for pred in preds if isinstance(pred, dict) and "feature" in pred]
    except Exception:
        _LOG.warning("Failed to load search space component from %s", ctrl._search_space_path, exc_info=True)
        return []


def load_mi_candidate_predicates(ctrl: Any) -> List[Dict[str, Any]]:
    try:
        feature_mi_root = ctrl.data_root / "reports" / "feature_mi"
        if not feature_mi_root.exists():
            return []
        candidates = sorted(
            feature_mi_root.rglob("candidate_predicates.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if not candidates:
            return []
        source_path = candidates[0]
        raw = json.loads(source_path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise DataIntegrityError(f"MI predicate artifact {source_path} did not contain a JSON list")
        valid = [
            pred
            for pred in raw
            if isinstance(pred, dict) and all(key in pred for key in ("feature", "operator", "threshold"))
        ]
        valid.sort(key=lambda pred: float(pred.get("mi_score", 0.0)), reverse=True)
        return valid
    except Exception as exc:
        _LOG.warning("Failed to load MI candidate predicates", exc_info=True)
        raise DataIntegrityError(f"Failed to load MI candidate predicates: {exc}") from exc


def find_weak_signal_event_pairs(ctrl: Any) -> List[tuple]:
    try:
        tested_df = _read_memory_table(ctrl.config.program_id, "tested_regions", data_root=ctrl.data_root)
    except Exception:
        _LOG.warning("Failed to load search space component from %s", ctrl._search_space_path, exc_info=True)
        return []

    if tested_df.empty:
        return []

    required = {"event_type", "mean_return_bps", "gate_promo_statistical"}
    if not required.issubset(tested_df.columns):
        return []

    candidates = tested_df[
        (pd.to_numeric(tested_df["mean_return_bps"], errors="coerce").fillna(0) > 0)
        & (tested_df["gate_promo_statistical"].astype(str).str.lower().isin(["false", "0", "fail"]))
        & (
            tested_df["trigger_type"].astype(str) == "EVENT"
            if "trigger_type" in tested_df.columns
            else True
        )
    ].copy()
    if candidates.empty or "event_type" not in candidates.columns:
        return []

    agg = (
        candidates.groupby("event_type")["mean_return_bps"]
        .apply(lambda series: pd.to_numeric(series, errors="coerce").mean())
        .sort_values(ascending=False)
    )
    top_events = list(agg.head(6).index)
    pairs = []
    for idx, left in enumerate(top_events):
        for right in top_events[idx + 1:]:
            pairs.append((left, right))
    return pairs[:5]


def templates_for_event(ctrl: Any, event_id: str) -> List[str]:
    events_registry = ctrl.registries.events.get("events", {})
    family = str(events_registry.get(event_id, {}).get("family", "")).strip()
    template_reg = ctrl.registries.templates.get("families", {})
    templates: List[str] = template_reg.get(family, {}).get("allowed_templates", [])
    return templates or ["mean_reversion", "continuation"]


def build_proposal(
    ctrl: Any,
    *,
    events: List[str],
    templates: List[str],
    horizons: List[int],
    directions: Optional[List[str]] = None,
    entry_lags: Optional[List[int]] = None,
    description: str,
    promotion_enabled: bool,
    date_scope: tuple[str, str],
    trigger_type: str = "EVENT",
    states: Optional[List[str]] = None,
    transitions: Optional[List[Dict[str, str]]] = None,
    feature_predicates: Optional[List[Dict[str, Any]]] = None,
    sequences: Optional[Dict[str, Any]] = None,
    interactions: Optional[List[Dict[str, Any]]] = None,
    contexts: Optional[Dict[str, List[str]]] = None,
    canonical_regimes: Optional[List[str]] = None,
    subtypes: Optional[List[str]] = None,
    phases: Optional[List[str]] = None,
    evidence_modes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    start, end = date_scope
    trigger_space: Dict[str, Any] = {"allowed_trigger_types": [trigger_type]}
    if trigger_type == "EVENT":
        trigger_space["events"] = {"include": events}
        trigger_space["canonical_regimes"] = list(canonical_regimes or [])
        trigger_space["subtypes"] = list(subtypes or [])
        trigger_space["phases"] = list(phases or [])
        trigger_space["evidence_modes"] = list(evidence_modes or [])
    elif trigger_type == "STATE":
        trigger_space["states"] = {"include": states or []}
    elif trigger_type == "TRANSITION":
        trigger_space["transitions"] = {"include": transitions or []}
    elif trigger_type == "FEATURE_PREDICATE":
        trigger_space["feature_predicates"] = {"include": feature_predicates or []}
    elif trigger_type == "SEQUENCE":
        trigger_space["sequences"] = sequences or {"include": [], "max_gaps_bars": [6, 12]}
    elif trigger_type == "INTERACTION":
        trigger_space["interactions"] = {"include": interactions or []}

    return {
        "program_id": ctrl.config.program_id,
        "run_mode": "research",
        "description": description,
        "instrument_scope": {
            "instrument_classes": ["crypto"],
            "symbols": ["BTCUSDT"],
            "timeframe": "5m",
            "start": start,
            "end": end,
        },
        "trigger_space": trigger_space,
        "templates": {"include": templates},
        "evaluation": {
            "horizons_bars": horizons,
            "directions": directions or ["long", "short"],
            "entry_lags": entry_lags or [1, 2],
        },
        "contexts": {"include": contexts or {}},
        "search_control": {
            "max_hypotheses_total": 1000,
            "max_hypotheses_per_template": 500,
            "max_hypotheses_per_event_family": 500,
        },
        "promotion": {"enabled": promotion_enabled},
    }


def context_for_proposal(ctrl: Any) -> Dict[str, List[str]]:
    if not ctrl.config.enable_context_conditioning:
        return {}
    allowed_contexts = ctrl.registries.contexts.get("context_dimensions", {})
    registry = get_domain_registry()
    selected_dimensions = list(getattr(ctrl.config, "proposal_context_dimensions", []) or [])
    out: Dict[str, List[str]] = {}
    for dimension in selected_dimensions:
        meta = allowed_contexts.get(str(dimension), {})
        values: list[str] = []
        seen: set[str] = set()
        compiled_labels = set(registry.context_labels_for_family(str(dimension)))
        for raw_value in list(meta.get("allowed_values", [])):
            token = canonicalize_context_label(str(dimension), raw_value)
            if not token or token in seen:
                continue
            if compiled_labels and token not in compiled_labels:
                continue
            values.append(token)
            seen.add(token)
        if values:
            out[str(dimension)] = values
    return out
