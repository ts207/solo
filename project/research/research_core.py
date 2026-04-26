from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.core.config import get_data_root


def get_research_data_root() -> Path:
    return get_data_root()

# --- Candidate Schema ---

CANONICAL_CANDIDATE_COLUMNS = [
    "candidate_id",
    "family_id",
    "run_id",
    "symbol",
    "event_type",
    "template_verb",
    "horizon",
    "state_id",
    "condition_label",
    "effect_raw",
    "effect_shrunk_state",
    "p_value",
    "q_value",
    "is_discovery",
    "n_events",
    "selection_score",
    "robustness_score",
    "effective_sample_size",
    "gate_phase2_final",
    "fail_reasons",
    "fail_gate_primary",
    "fail_reason_primary",
    "effective_lag_bars",
    "gate_bridge_tradable",
    "bridge_fail_gate_primary",
    "bridge_fail_reason_primary",
    "promotion_fail_gate_primary",
    "promotion_fail_reason_primary",
    "gate_promo_statistical",
    "gate_promo_stability",
    "gate_promo_cost_survival",
    "gate_promo_negative_control",
    "gate_promo_hypothesis_audit",
]


def ensure_candidate_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure DataFrame matches the canonical candidate schema.
    Missing columns are filled with fail-closed, type-stable defaults.
    Unknown numeric metrics stay NaN rather than fabricated constants.
    """
    out = df.copy()
    bool_cols = {c for c in CANONICAL_CANDIDATE_COLUMNS if c.startswith("gate_")} | {"is_discovery"}
    numeric_cols = {
        "effect_raw",
        "effect_shrunk_state",
        "p_value",
        "q_value",
        "n_events",
        "selection_score",
        "robustness_score",
        "effective_sample_size",
        "effective_lag_bars",
    }
    for col in CANONICAL_CANDIDATE_COLUMNS:
        if col in out.columns:
            continue
        if col in bool_cols:
            out[col] = "missing_evidence"
        elif col in numeric_cols:
            out[col] = np.nan
        else:
            out[col] = ""

    # Do not coerce 3-state gates to bools. Just handle NaNs.
    for col in bool_cols & set(out.columns):
        if out[col].dtype == bool:
            # Map legacy booleans to the new string contract
            out[col] = (
                out[col]
                .map({True: "pass", False: "fail", pd.NA: "missing_evidence"})
                .fillna("missing_evidence")
            )
        else:
            out[col] = out[col].fillna("missing_evidence").astype(str)

    for col in numeric_cols & set(out.columns):
        out[col] = pd.to_numeric(out[col], errors="coerce")

    return out[
        CANONICAL_CANDIDATE_COLUMNS
        + [c for c in out.columns if c not in CANONICAL_CANDIDATE_COLUMNS]
    ]


# --- Edge Identity ---


@dataclass(frozen=True)
class StructuralEdgeComponents:
    event_type: str
    template_family: str
    direction_rule: str
    signal_polarity_logic: str


def structural_edge_components(row: Mapping[str, Any]) -> StructuralEdgeComponents:
    def _norm_token(value: Any, *, default: str) -> str:
        token = str(value or "").strip().upper()
        return token if token else default

    def _first_present(row: Mapping[str, Any], *keys: str) -> str:
        for key in keys:
            value = str(row.get(key, "")).strip()
            if value:
                return value
        return ""

    event_type = _norm_token(
        _first_present(row, "canonical_event_type", "event_type", "event"),
        default="UNKNOWN_EVENT",
    )
    template_family = _norm_token(
        _first_present(row, "template_family", "template_id", "template_verb", "rule_template"),
        default="UNKNOWN_TEMPLATE",
    )
    direction_rule = _norm_token(
        _first_present(row, "direction_rule", "direction", "trade_direction", "action"),
        default="UNKNOWN_DIRECTION",
    )
    signal_polarity_logic = _norm_token(
        _first_present(
            row, "signal_polarity_logic", "side_policy", "polarity_logic", "signal_polarity"
        ),
        default="UNKNOWN_POLARITY",
    )
    return StructuralEdgeComponents(
        event_type=event_type,
        template_family=template_family,
        direction_rule=direction_rule,
        signal_polarity_logic=signal_polarity_logic,
    )


def edge_id_from_components(components: StructuralEdgeComponents) -> str:
    payload = {
        "event_type": components.event_type,
        "template_family": components.template_family,
        "direction_rule": components.direction_rule,
        "signal_polarity_logic": components.signal_polarity_logic,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "edge_" + hashlib.sha256(encoded).hexdigest()[:20]


def edge_id_from_row(row: Mapping[str, Any]) -> str:
    return edge_id_from_components(structural_edge_components(row))


def write_research_integrity_report(
    run_id: str,
    data_root: Path,
    stats: dict[str, Any],
) -> Path:
    """
    N2: Add selection-bias accounting artifact.
    Writes a run-level "research integrity report".
    """
    out_dir = data_root / "reports" / "integrity" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    report = {
        "run_id": run_id,
        "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
        "search_space": {
            "num_tested_hypotheses": int(stats.get("num_tested", 0)),
            "surviving_raw_p": int(stats.get("surviving_raw_p", 0)),
            "surviving_fdr": int(stats.get("surviving_fdr", 0)),
            "surviving_bridge": int(stats.get("surviving_bridge", 0)),
        },
        "null_control_diagnostics": stats.get("null_controls", {}),
        "bias_accounting": {
            "exploratory_vs_confirmatory": str(stats.get("run_mode", "unknown")),
            "selection_bias_est_bps": float(stats.get("est_selection_bias_bps", 0.0)),
        },
    }

    report_path = out_dir / "research_integrity_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    return report_path
