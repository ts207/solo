from __future__ import annotations

import json
import os
from collections.abc import Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from project.core.config import get_data_root
from project.core.exceptions import DataIntegrityError
from project.io.utils import ensure_dir, read_parquet, write_parquet
from project.research.knowledge.schemas import (
    EVIDENCE_LEDGER_COLUMNS,
    FAILURE_COLUMNS,
    PROPOSAL_AUDIT_COLUMNS,
    REFLECTION_COLUMNS,
    TESTED_REGION_COLUMNS,
    canonical_json,
    region_key,
    stable_hash,
)
from project.research.search.bridge_adapter import canonical_bridge_event_type
from project.research.services.pathing import resolve_phase2_candidates_path
from project.specs.manifest import load_run_manifest

# Phase 1.3 — gate names that indicate each failure cause class.
# Used by classify_failure_cause() to populate failure_cause_class on tested regions.
_MECHANICAL_GATE_PREFIXES = (
    "stage_failed",
    "run_failed_stage",
    "artifact_contract",
    "missing_event_column",
    "missing_state_column",
    "missing_feature_column",
    "missing_context_state_column",
    "missing_sequence_column",
    "unsupported_trigger",
)
_COST_GATES = frozenset(
    [
        "gate_promo_cost_survival",
        "gate_promo_retail_cost_budget",
        "gate_promo_retail_net_expectancy",
        "gate_promo_retail_turnover",
        "gate_promo_stressed_cost_survival",
        "gate_promo_delayed_entry_stress",
    ]
)
_MARKET_GATES = frozenset(
    [
        "gate_promo_stability_gate",
        "gate_promo_stability_score",
        "gate_promo_stability_sign_consistency",
        "gate_promo_microstructure",
        "gate_promo_oos_validation",
        "gate_promo_dsr",
        "gate_promo_placebo_controls",
        "gate_promo_negative_control_fail",
        "gate_promo_baseline_beats_complexity",
        "gate_promo_timeframe_consensus",
    ]
)
_OVERFIT_GATES = frozenset(
    [
        "gate_promo_multiplicity_diagnostics",
        "gate_promo_negative_control_missing",
        "gate_promo_hypothesis_not_executed",
        "gate_promo_hypothesis_missing_audit",
    ]
)


def classify_failure_cause(primary_fail_gate: str, train_n_obs: int) -> str:
    """Return a failure cause class string for a candidate row.

    Classes:
        mechanical        — pipeline or artifact failure; evidence is unreliable
        insufficient_sample — gate fired but sample was too small to trust conclusion
        cost              — strategy unprofitable net of execution costs
        market            — statistically clean rejection; no edge in this region
        overfitting       — multiplicity / audit / control failure
    """
    gate = str(primary_fail_gate or "").strip().lower()
    if not gate:
        return "mechanical" if int(train_n_obs or 0) == 0 else "market"
    for prefix in _MECHANICAL_GATE_PREFIXES:
        if gate.startswith(prefix):
            return "mechanical"
    # Insufficient sample: any gate fired on very small sample
    if int(train_n_obs or 0) < 30:
        return "insufficient_sample"
    if gate in _COST_GATES:
        return "cost"
    if gate in _MARKET_GATES:
        return "market"
    if gate in _OVERFIT_GATES:
        return "overfitting"
    # Default: treat unknown gates with adequate sample as market
    return "market"

_TABLES = {
    "tested_regions": TESTED_REGION_COLUMNS,
    "region_statistics": [
        "region_key",
        "runs_tested",
        "times_evaluated",
        "times_promoted",
        "eval_rate",
        "promotion_rate",
        "avg_q_value",
        "avg_after_cost_expectancy",
        "median_after_cost_expectancy",
        "avg_stressed_after_cost_expectancy",
        "median_stressed_after_cost_expectancy",
        "recent_after_cost_expectancy",
        "recent_stressed_after_cost_expectancy",
        "positive_after_cost_rate",
        "positive_stressed_after_cost_rate",
        "tradable_rate",
        "statistical_pass_rate",
        "avg_robustness_score",
        "dominant_fail_gate",
        "last_tested_at",
    ],
    "event_statistics": [
        "event_type",
        "runs_tested",
        "times_evaluated",
        "times_promoted",
        "promotion_rate",
        "avg_q_value",
        "avg_after_cost_expectancy",
        "median_after_cost_expectancy",
        "avg_stressed_after_cost_expectancy",
        "median_stressed_after_cost_expectancy",
        "recent_after_cost_expectancy",
        "recent_stressed_after_cost_expectancy",
        "positive_after_cost_rate",
        "positive_stressed_after_cost_rate",
        "tradable_rate",
        "statistical_pass_rate",
        "dominant_fail_gate",
        "last_tested_at",
    ],
    "template_statistics": [
        "template_id",
        "runs_tested",
        "times_evaluated",
        "times_promoted",
        "promotion_rate",
        "avg_q_value",
        "avg_after_cost_expectancy",
        "median_after_cost_expectancy",
        "avg_stressed_after_cost_expectancy",
        "median_stressed_after_cost_expectancy",
        "recent_after_cost_expectancy",
        "recent_stressed_after_cost_expectancy",
        "positive_after_cost_rate",
        "positive_stressed_after_cost_rate",
        "tradable_rate",
        "statistical_pass_rate",
        "dominant_fail_gate",
        "last_tested_at",
    ],
    "context_statistics": [
        "context_hash",
        "context_json",
        "runs_tested",
        "times_evaluated",
        "times_promoted",
        "promotion_rate",
        "avg_q_value",
        "avg_after_cost_expectancy",
        "median_after_cost_expectancy",
        "avg_stressed_after_cost_expectancy",
        "median_stressed_after_cost_expectancy",
        "recent_after_cost_expectancy",
        "recent_stressed_after_cost_expectancy",
        "positive_after_cost_rate",
        "positive_stressed_after_cost_rate",
        "tradable_rate",
        "statistical_pass_rate",
        "dominant_fail_gate",
        "last_tested_at",
    ],
    "failures": FAILURE_COLUMNS,
    "proposals": PROPOSAL_AUDIT_COLUMNS,
    "reflections": REFLECTION_COLUMNS,
    "evidence_ledger": EVIDENCE_LEDGER_COLUMNS,
}


@dataclass(frozen=True)
class MemoryPaths:
    root: Path
    tested_regions: Path
    region_statistics: Path
    event_statistics: Path
    template_statistics: Path
    context_statistics: Path
    failures: Path
    proposals: Path
    reflections: Path
    belief_state: Path
    next_actions: Path
    proposals_dir: Path
    evidence_ledger: Path | None = None

    def __post_init__(self) -> None:
        if self.evidence_ledger is None:
            object.__setattr__(self, "evidence_ledger", self.root / "evidence_ledger.parquet")


def memory_paths(program_id: str, *, data_root: Path | None = None) -> MemoryPaths:
    resolved_data_root = Path(data_root) if data_root is not None else get_data_root()
    root = resolved_data_root / "artifacts" / "experiments" / str(program_id) / "memory"
    return MemoryPaths(
        root=root,
        tested_regions=root / "tested_regions.parquet",
        region_statistics=root / "region_statistics.parquet",
        event_statistics=root / "event_statistics.parquet",
        template_statistics=root / "template_statistics.parquet",
        context_statistics=root / "context_statistics.parquet",
        failures=root / "failures.parquet",
        proposals=root / "proposals.parquet",
        reflections=root / "reflections.parquet",
        evidence_ledger=root / "evidence_ledger.parquet",
        belief_state=root / "belief_state.json",
        next_actions=root / "next_actions.json",
        proposals_dir=root / "proposals",
    )


def ensure_memory_store(program_id: str, *, data_root: Path | None = None) -> MemoryPaths:
    paths = memory_paths(program_id, data_root=data_root)
    paths.root.mkdir(parents=True, exist_ok=True)
    paths.proposals_dir.mkdir(parents=True, exist_ok=True)
    for table_name, columns in _TABLES.items():
        path = getattr(paths, table_name)
        if not path.exists():
            _write_memory_frame(pd.DataFrame(columns=columns), path)
    if not paths.belief_state.exists():
        paths.belief_state.write_text(
            json.dumps(
                {
                    "current_focus": "",
                    "avoid_regions": [],
                    "promising_regions": [],
                    "open_repairs": [],
                    "last_reflection_run_id": "",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
    if not paths.next_actions.exists():
        paths.next_actions.write_text(
            json.dumps(
                {
                    "repair": [],
                    "exploit": [],
                    "retest": [],
                    "explore_adjacent": [],
                    "hold": [],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
    return paths


def read_memory_table(
    program_id: str, table_name: str, *, data_root: Path | None = None
) -> pd.DataFrame:
    paths = ensure_memory_store(program_id, data_root=data_root)
    path = getattr(paths, table_name)
    return _read_best_available(path)


def write_memory_table(
    program_id: str,
    table_name: str,
    df: pd.DataFrame,
    *,
    data_root: Path | None = None,
) -> Path:
    paths = ensure_memory_store(program_id, data_root=data_root)
    path = getattr(paths, table_name)
    columns = _TABLES.get(table_name)
    out_df = df.copy()
    if columns is not None:
        for column in columns:
            if column not in out_df.columns:
                out_df[column] = None
        out_df = out_df.reindex(columns=columns)
    _write_memory_frame(out_df, path)
    return path


def write_reflection(
    memory_paths_or_program_id: MemoryPaths | str,
    df: pd.DataFrame,
    *,
    data_root: Path | None = None,
) -> Path:
    normalized = _normalize_reflection_frame(df, program_id=_reflection_program_id(memory_paths_or_program_id))
    if isinstance(memory_paths_or_program_id, MemoryPaths):
        existing = _read_best_available(memory_paths_or_program_id.reflections)
        combined = pd.concat([existing, normalized], ignore_index=True) if not existing.empty else normalized
        _write_memory_frame(combined, memory_paths_or_program_id.reflections)
        return memory_paths_or_program_id.reflections

    program_id = str(memory_paths_or_program_id)
    existing = read_memory_table(program_id, "reflections", data_root=data_root)
    combined = pd.concat([existing, normalized], ignore_index=True) if not existing.empty else normalized
    return write_memory_table(program_id, "reflections", combined, data_root=data_root)


def read_reflections(
    memory_paths_or_program_id: MemoryPaths | str,
    program_id: str | None = None,
    *,
    data_root: Path | None = None,
) -> pd.DataFrame:
    if isinstance(memory_paths_or_program_id, MemoryPaths):
        reflections = _read_best_available(memory_paths_or_program_id.reflections)
        effective_program_id = program_id or _reflection_program_id(memory_paths_or_program_id)
    else:
        effective_program_id = str(memory_paths_or_program_id)
        reflections = read_memory_table(effective_program_id, "reflections", data_root=data_root)

    out = reflections.copy()
    if effective_program_id and "program_id" in out.columns:
        out = out[out["program_id"].astype(str) == str(effective_program_id)].reset_index(drop=True)
    return _reflection_compat_view(out)


@contextmanager
def _canonical_parquet_write_mode() -> Iterable[None]:
    original = os.environ.get("BACKTEST_FORCE_CSV_FALLBACK")
    os.environ["BACKTEST_FORCE_CSV_FALLBACK"] = "0"
    try:
        yield
    finally:
        if original is None:
            os.environ.pop("BACKTEST_FORCE_CSV_FALLBACK", None)
        else:
            os.environ["BACKTEST_FORCE_CSV_FALLBACK"] = original


def _write_memory_frame(df: pd.DataFrame, path: Path) -> Path:
    ensure_dir(path.parent)
    with _canonical_parquet_write_mode():
        written_path, _ = write_parquet(df, path)
    return written_path


def _read_best_available(path: Path) -> pd.DataFrame:
    if path.exists():
        return read_parquet(path)
    csv_path = path.with_suffix(".csv")
    if csv_path.exists():
        return pd.read_csv(csv_path)
    return pd.DataFrame()


def _reflection_program_id(memory_paths_or_program_id: MemoryPaths | str) -> str | None:
    if isinstance(memory_paths_or_program_id, MemoryPaths):
        return None
    return str(memory_paths_or_program_id)


def _normalize_reflection_frame(
    df: pd.DataFrame,
    *,
    program_id: str | None = None,
) -> pd.DataFrame:
    out = df.copy()
    if "program_id" not in out.columns and program_id:
        out["program_id"] = str(program_id)
    if "run_id" not in out.columns:
        out["run_id"] = out.get("reflection_id", "")
    if "created_at" not in out.columns:
        out["created_at"] = out.get("timestamp")
    if "market_findings" not in out.columns:
        out["market_findings"] = out.get("observation")
    if "statistical_outcome" not in out.columns:
        out["statistical_outcome"] = out.get("insight_type")
    if "recommended_next_action" not in out.columns:
        out["recommended_next_action"] = out.get("action")
    if "run_status" not in out.columns:
        out["run_status"] = out.get("status")
    for column in REFLECTION_COLUMNS:
        if column not in out.columns:
            out[column] = None
    return out.reindex(columns=REFLECTION_COLUMNS)


def _reflection_compat_view(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "reflection_id" not in out.columns:
        out["reflection_id"] = out.get("run_id")
    if "timestamp" not in out.columns:
        out["timestamp"] = out.get("created_at")
    if "observation" not in out.columns:
        out["observation"] = out.get("market_findings")
    if "insight_type" not in out.columns:
        out["insight_type"] = out.get("statistical_outcome")
    if "action" not in out.columns:
        out["action"] = out.get("recommended_next_action")
    if "status" not in out.columns:
        out["status"] = out.get("run_status")
    return out


def _parse_json_payload(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _label_from_trigger_payload(trigger_payload: dict[str, Any]) -> str:
    trigger_type = str(trigger_payload.get("trigger_type", "")).strip().lower()
    if trigger_type == "event":
        event_id = str(trigger_payload.get("event_id", "")).strip().upper()
        return f"event:{event_id}" if event_id else ""
    if trigger_type == "state":
        state_id = str(trigger_payload.get("state_id", "")).strip().upper()
        return f"state:{state_id}" if state_id else ""
    if trigger_type == "transition":
        from_state = str(trigger_payload.get("from_state", "")).strip().upper()
        to_state = str(trigger_payload.get("to_state", "")).strip().upper()
        if from_state and to_state:
            return f"transition:{from_state}→{to_state}"
    if trigger_type == "feature_predicate":
        feature = str(trigger_payload.get("feature", "")).strip()
        operator = str(trigger_payload.get("operator", "")).strip()
        threshold = trigger_payload.get("threshold")
        if feature and operator and threshold not in (None, ""):
            return f"pred:{feature}{operator}{threshold}"
    if trigger_type == "sequence":
        sequence_id = str(trigger_payload.get("sequence_id", "")).strip().upper()
        return f"seq:{sequence_id}" if sequence_id else ""
    if trigger_type == "interaction":
        interaction_id = str(trigger_payload.get("interaction_id", "")).strip().upper()
        op = str(trigger_payload.get("op", "")).strip().lower()
        if interaction_id:
            return f"int:{interaction_id}({op})" if op else f"int:{interaction_id}"
    return ""


def _canonical_event_type_from_trigger(
    *,
    trigger_type: str,
    trigger_key: str,
    trigger_payload: dict[str, Any],
    existing_event_type: str,
) -> str:
    if existing_event_type:
        return existing_event_type
    normalized_type = str(trigger_type or "").strip().lower()
    if normalized_type == "event":
        event_id = str(trigger_payload.get("event_id", "") or "").strip().upper()
        if event_id:
            return event_id
        trigger_text = str(trigger_key or "").strip()
        if trigger_text.lower().startswith("event:"):
            return trigger_text.split(":", 1)[1].strip().upper()
        return trigger_text.upper()
    if normalized_type == "state":
        state_id = str(trigger_payload.get("state_id", "") or "").strip().upper()
        if state_id:
            return canonical_bridge_event_type("state", f"state:{state_id}")
    if normalized_type == "transition":
        from_state = str(trigger_payload.get("from_state", "") or "").strip().upper()
        to_state = str(trigger_payload.get("to_state", "") or "").strip().upper()
        if from_state and to_state:
            return canonical_bridge_event_type(
                "transition",
                f"transition:{from_state}→{to_state}",
            )
    if normalized_type == "feature_predicate":
        feature = str(trigger_payload.get("feature", "") or "").strip()
        operator = str(trigger_payload.get("operator", "") or "").strip()
        threshold = trigger_payload.get("threshold")
        if feature and operator and threshold not in (None, ""):
            return canonical_bridge_event_type(
                "feature_predicate",
                f"pred:{feature}{operator}{threshold}",
            )
    if normalized_type in {"sequence", "interaction"} and trigger_key:
        return canonical_bridge_event_type(normalized_type, trigger_key)
    return str(trigger_key or "").strip()


def build_tested_regions_snapshot(
    *,
    run_id: str,
    program_id: str,
    data_root: Path | None = None,
) -> pd.DataFrame:
    resolved_data_root = Path(data_root) if data_root is not None else get_data_root()
    manifest_path = resolved_data_root / "runs" / run_id / "run_manifest.json"
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            manifest = {}
    else:
        manifest = load_run_manifest(run_id)
        if not isinstance(manifest, dict):
            manifest = {}
    # Prioritize high-fidelity discovery metrics for research runs
    phase2_path = resolve_phase2_candidates_path(
        data_root=resolved_data_root,
        run_id=run_id,
    )
    promotion_path = (
        resolved_data_root
        / "reports"
        / "promotions"
        / run_id
        / "promotion_statistical_audit.parquet"
    )
    edge_path = (
        resolved_data_root
        / "reports"
        / "edge_candidates"
        / run_id
        / "edge_candidates_normalized.parquet"
    )

    df = _read_best_available(phase2_path)
    if df.empty:
        df = _read_best_available(promotion_path)
    if df.empty:
        df = _read_best_available(edge_path)
    if df.empty:
        # Fallback for old directory structure or specific event discovery
        discovery_paths = list(
            (resolved_data_root / "reports" / "phase2" / run_id).glob(
                "**/phase2_candidates.parquet"
            )
        )
        for p in discovery_paths:
            df = _read_best_available(p)
            if not df.empty:
                break

    if df.empty:
        hypothesis_root = resolved_data_root / "reports" / "phase2" / run_id / "hypotheses"
        evaluated_frames: list[pd.DataFrame] = []
        if hypothesis_root.exists():
            for evaluated_path in sorted(hypothesis_root.glob("*/evaluated_hypotheses.parquet")):
                evaluated_df = _read_best_available(evaluated_path)
                if not evaluated_df.empty:
                    symbol_scope = evaluated_path.parent.name
                    evaluated_df = evaluated_df.copy()
                    if "symbol" not in evaluated_df.columns:
                        evaluated_df["symbol"] = symbol_scope
                    evaluated_frames.append(evaluated_df)
        if evaluated_frames:
            df = pd.concat(evaluated_frames, ignore_index=True)

    if df.empty:
        return pd.DataFrame(columns=TESTED_REGION_COLUMNS)

    expanded_hypotheses_path = None
    experiment_config_path = (
        manifest.get("config_resolution", {}) if isinstance(manifest.get("config_resolution"), dict) else {}
    ).get("experiment_config_path")
    if experiment_config_path:
        experiment_path = Path(str(experiment_config_path))
        candidate = experiment_path.parent / "expanded_hypotheses.parquet"
        if candidate.exists():
            expanded_hypotheses_path = candidate
    if expanded_hypotheses_path is None:
        legacy_candidate = (
            resolved_data_root
            / "artifacts"
            / "experiments"
            / program_id
            / run_id
            / "expanded_hypotheses.parquet"
        )
        if legacy_candidate.exists():
            expanded_hypotheses_path = legacy_candidate

    trigger_payload_by_hypothesis: dict[str, dict[str, Any]] = {}
    if expanded_hypotheses_path is not None and expanded_hypotheses_path.exists():
        try:
            expanded_hypotheses = pd.read_parquet(expanded_hypotheses_path)
            for row in expanded_hypotheses.to_dict(orient="records"):
                hypothesis_id = str(row.get("hypothesis_id", "")).strip()
                if not hypothesis_id:
                    continue
                trigger_payload = _parse_json_payload(row.get("trigger_payload"))
                if trigger_payload:
                    trigger_payload_by_hypothesis[hypothesis_id] = trigger_payload
        except Exception as exc:
            raise DataIntegrityError(
                f"Failed to read expanded hypotheses from {expanded_hypotheses_path}: {exc}"
            ) from exc

    records: list[dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        hypothesis_id = str(row.get("hypothesis_id", row.get("plan_row_id", ""))).strip()
        trigger_payload = _parse_json_payload(row.get("trigger_payload"))
        if not trigger_payload and hypothesis_id:
            trigger_payload = trigger_payload_by_hypothesis.get(hypothesis_id, {})
        trigger_key = str(row.get("trigger_key", "")).strip()
        if not trigger_key and trigger_payload:
            trigger_key = _label_from_trigger_payload(trigger_payload)
        trigger_type = (
            str(row.get("trigger_type", trigger_payload.get("trigger_type", "EVENT"))).strip().upper()
            or "EVENT"
        )
        context_raw = row.get("context_json", row.get("contexts", row.get("context", {})))
        if context_raw is None or context_raw == "":
            context_raw = {}
        context_blob = canonical_json(context_raw)
        state_id = str(trigger_payload.get("state_id", row.get("state_id", ""))).strip()
        from_state = str(trigger_payload.get("from_state", row.get("from_state", ""))).strip()
        to_state = str(trigger_payload.get("to_state", row.get("to_state", ""))).strip()
        feature = str(trigger_payload.get("feature", row.get("feature", ""))).strip()
        operator = str(trigger_payload.get("operator", row.get("operator", ""))).strip()
        threshold = trigger_payload.get("threshold", row.get("threshold"))
        event_type = _canonical_event_type_from_trigger(
            trigger_type=trigger_type,
            trigger_key=trigger_key,
            trigger_payload=trigger_payload,
            existing_event_type=str(row.get("event_type", row.get("event", ""))).strip(),
        )
        payload = {
            "program_id": program_id,
            "symbol_scope": str(
                row.get("symbol", row.get("symbols", manifest.get("normalized_symbols", "")))
            ).strip(),
            "event_type": event_type,
            "trigger_type": trigger_type,
            "template_id": str(row.get("template_id", row.get("template", ""))).strip(),
            "direction": str(row.get("direction", "")).strip(),
            "horizon": str(row.get("horizon", row.get("timeframe", ""))).strip(),
            "entry_lag": int(row.get("entry_lag", row.get("entry_lag_bars", 0)) or 0),
            "context_hash": str(row.get("context_hash", "")) or stable_hash((context_blob,)),
            "context_json": context_blob,
        }
        records.append(
            {
                "region_key": region_key(payload),
                "program_id": program_id,
                "run_id": run_id,
                "hypothesis_id": hypothesis_id,
                "candidate_id": str(row.get("candidate_id", "")).strip(),
                "symbol_scope": payload["symbol_scope"],
                "event_type": payload["event_type"],
                "trigger_type": payload["trigger_type"],
                "trigger_key": trigger_key,
                "trigger_payload_json": canonical_json(trigger_payload) if trigger_payload else "{}",
                "state_id": state_id,
                "from_state": from_state,
                "to_state": to_state,
                "feature": feature,
                "operator": operator,
                "threshold": pd.to_numeric(threshold, errors="coerce")
                if threshold not in (None, "")
                else None,
                "template_id": payload["template_id"],
                "direction": payload["direction"],
                "horizon": payload["horizon"],
                "entry_lag": payload["entry_lag"],
                "context_hash": payload["context_hash"],
                "context_json": payload["context_json"],
                "eval_status": str(
                    row.get("promotion_decision", row.get("eval_status", "evaluated"))
                ).strip()
                or "evaluated",
                "train_n_obs": int(
                    row.get("train_n_obs", row.get("n", row.get("sample_size", 0))) or 0
                ),
                "validation_n_obs": int(
                    row.get("validation_n_obs", row.get("validation_samples", 0)) or 0
                ),
                "test_n_obs": int(row.get("test_n_obs", row.get("test_samples", 0)) or 0),
                "q_value": pd.to_numeric(
                    row.get(
                        "q_value",
                        row.get(
                            "p_value_adj",
                            row.get(
                                "p_value_for_fdr",
                                row.get("p_value_raw"),
                            ),
                        ),
                    ),
                    errors="coerce",
                ),
                "mean_return_bps": pd.to_numeric(
                    row.get(
                        "mean_return_bps",
                        row.get(
                            "cost_adjusted_return_bps",
                            row.get("bridge_validation_after_cost_bps", row.get("net_expectancy_bps")),
                        ),
                    ),
                    errors="coerce",
                ),
                "after_cost_expectancy": pd.to_numeric(
                    row.get(
                        "after_cost_expectancy",
                        row.get(
                            "cost_adjusted_return_bps",
                            row.get("after_cost_expectancy_per_trade", row.get("net_expectancy_bps")),
                        ),
                    ),
                    errors="coerce",
                ),
                "stressed_after_cost_expectancy": pd.to_numeric(
                    row.get(
                        "stressed_after_cost_expectancy",
                        row.get(
                            "stressed_after_cost_expectancy_per_trade",
                            row.get("bridge_validation_stressed_after_cost_bps"),
                        ),
                    ),
                    errors="coerce",
                ),
                "robustness_score": pd.to_numeric(
                    row.get("robustness_score", row.get("stability_score")),
                    errors="coerce",
                ),
                "gate_bridge_tradable": bool(row.get("gate_bridge_tradable") == "pass"),
                "gate_promo_statistical": bool(row.get("gate_promo_statistical") == "pass"),
                "gate_promo_retail_net_expectancy": bool(
                    row.get("gate_promo_retail_net_expectancy") == "pass"
                    or row.get("gate_promo_retail_net_expectancy") is True
                ),
                "mechanical_status": "ok",
                "primary_fail_gate": str(
                    row.get("promotion_fail_gate_primary", row.get("primary_fail_gate", ""))
                ).strip(),
                "warning_count": int(row.get("warning_count", 0) or 0),
                "updated_at": str(row.get("updated_at", "")),
                # Phase 1.3 — failure metadata for probabilistic avoidance
                "failure_confidence": None,   # populated by update_campaign_memory after reflection join
                "failure_cause_class": classify_failure_cause(
                    primary_fail_gate=str(
                        row.get("promotion_fail_gate_primary", row.get("primary_fail_gate", ""))
                    ).strip(),
                    train_n_obs=int(row.get("train_n_obs", row.get("sample_size", 0)) or 0),
                ),
                "failure_sample_size": int(row.get("train_n_obs", row.get("sample_size", 0)) or 0),
            }
        )
    return pd.DataFrame(records).reindex(columns=TESTED_REGION_COLUMNS)


def build_failures_snapshot(
    *,
    run_id: str,
    program_id: str,
    data_root: Path | None = None,
) -> pd.DataFrame:
    resolved_data_root = Path(data_root) if data_root is not None else get_data_root()
    manifest_path = resolved_data_root / "runs" / run_id / "run_manifest.json"
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            manifest = {}
    else:
        manifest = load_run_manifest(run_id)
    rows: list[dict[str, Any]] = []
    raw_failed_stage = manifest.get("failed_stage", "")
    failed_stage = str(raw_failed_stage).strip() if raw_failed_stage is not None else ""
    if failed_stage.lower() in {"none", "null"}:
        failed_stage = ""
    if failed_stage:
        rows.append(
            {
                "run_id": run_id,
                "program_id": program_id,
                "stage": failed_stage,
                "failure_class": "run_failed_stage",
                "failure_detail": str(manifest.get("error_message", "")).strip(),
                "artifact_path": str(resolved_data_root / "runs" / run_id / f"{failed_stage}.json"),
                "is_mechanical": True,
                "is_repeated": False,
                "superseded_by_run_id": "",
            }
        )
    stage_dir = resolved_data_root / "runs" / run_id
    if stage_dir.exists():
        for path in sorted(stage_dir.glob("*.json")):
            if path.name == "run_manifest.json":
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if str(payload.get("status", "")).strip().lower() != "failed":
                continue
            rows.append(
                {
                    "run_id": run_id,
                    "program_id": program_id,
                    "stage": str(payload.get("stage", path.stem)),
                    "failure_class": "stage_failed",
                    "failure_detail": str(
                        payload.get("error_message", payload.get("traceback", ""))
                    ).strip(),
                    "artifact_path": str(path),
                    "is_mechanical": True,
                    "is_repeated": False,
                    "superseded_by_run_id": "",
                }
            )
    return pd.DataFrame(rows).reindex(columns=FAILURE_COLUMNS)


def _dominant_fail_gate(series: pd.Series) -> str:
    cleaned = series.dropna().astype(str).str.strip()
    cleaned = cleaned[cleaned != ""]
    if cleaned.empty:
        return ""
    return str(cleaned.value_counts().idxmax())


def _truthy_rate(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    normalized = (
        series.astype("object")
        .where(series.notna(), "")
        .astype(str)
        .str.strip()
        .str.lower()
    )
    mapped = normalized.map(
        {
            "pass": True,
            "true": True,
            "1": True,
            "1.0": True,
            "fail": False,
            "false": False,
            "0": False,
            "0.0": False,
        }
    )
    bool_values = mapped.apply(lambda value: bool(value) if pd.notna(value) else False)
    if bool_values.empty:
        return 0.0
    return float(bool_values.mean())


def _positive_rate(series: pd.Series) -> float:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return 0.0
    return float((numeric > 0.0).mean())


def _recent_metric(group: pd.DataFrame, column: str, *, tail_count: int = 3) -> float:
    if column not in group.columns:
        return float("nan")
    work = group.copy()
    if "updated_at" in work.columns:
        work["__updated_at"] = pd.to_datetime(work["updated_at"], errors="coerce", utc=True)
    else:
        work["__updated_at"] = pd.NaT
    work["__row_order"] = range(len(work))
    work = work.sort_values(["__updated_at", "__row_order"], ascending=[True, True], na_position="last")
    numeric = pd.to_numeric(work[column], errors="coerce").dropna()
    if numeric.empty:
        return float("nan")
    return float(numeric.tail(max(int(tail_count), 1)).mean())


def _stats_row(group: pd.DataFrame) -> dict[str, Any]:
    times_evaluated = len(group)
    times_promoted = int((group.get("eval_status", pd.Series(dtype=object)).astype(str) == "promoted").sum())
    runs_tested = int(group.get("run_id", pd.Series(dtype=object)).nunique()) if "run_id" in group.columns else times_evaluated
    after_cost = pd.to_numeric(group.get("after_cost_expectancy", pd.Series(dtype=float)), errors="coerce")
    stressed = pd.to_numeric(group.get("stressed_after_cost_expectancy", pd.Series(dtype=float)), errors="coerce")
    robustness = pd.to_numeric(group.get("robustness_score", pd.Series(dtype=float)), errors="coerce")
    q_value = pd.to_numeric(group.get("q_value", pd.Series(dtype=float)), errors="coerce")
    return {
        "runs_tested": runs_tested,
        "times_evaluated": times_evaluated,
        "times_promoted": times_promoted,
        "promotion_rate": float(times_promoted / max(times_evaluated, 1)),
        "avg_q_value": float(q_value.mean()) if not q_value.dropna().empty else float("nan"),
        "avg_after_cost_expectancy": float(after_cost.mean()) if not after_cost.dropna().empty else float("nan"),
        "median_after_cost_expectancy": float(after_cost.median()) if not after_cost.dropna().empty else float("nan"),
        "avg_stressed_after_cost_expectancy": float(stressed.mean()) if not stressed.dropna().empty else float("nan"),
        "median_stressed_after_cost_expectancy": float(stressed.median()) if not stressed.dropna().empty else float("nan"),
        "recent_after_cost_expectancy": _recent_metric(group, "after_cost_expectancy"),
        "recent_stressed_after_cost_expectancy": _recent_metric(group, "stressed_after_cost_expectancy"),
        "positive_after_cost_rate": _positive_rate(after_cost),
        "positive_stressed_after_cost_rate": _positive_rate(stressed),
        "tradable_rate": _truthy_rate(group.get("gate_bridge_tradable", pd.Series(dtype=object))),
        "statistical_pass_rate": _truthy_rate(group.get("gate_promo_statistical", pd.Series(dtype=object))),
        "avg_robustness_score": float(robustness.mean()) if not robustness.dropna().empty else float("nan"),
        "dominant_fail_gate": _dominant_fail_gate(group.get("primary_fail_gate", pd.Series(dtype=object))),
        "last_tested_at": str(group.get("updated_at", pd.Series(dtype=object)).dropna().astype(str).max()) if "updated_at" in group.columns and not group.get("updated_at", pd.Series(dtype=object)).dropna().empty else "",
    }


def _build_dimension_statistics(
    tested_regions: pd.DataFrame,
    group_columns: list[str],
    output_columns: list[str],
) -> pd.DataFrame:
    if tested_regions.empty:
        return pd.DataFrame(columns=output_columns)
    records: list[dict[str, Any]] = []
    grouped = tested_regions.groupby(group_columns, dropna=False)
    for key, group in grouped:
        row: dict[str, Any] = {}
        if isinstance(key, tuple):
            for column, value in zip(group_columns, key):
                row[column] = value
        else:
            row[group_columns[0]] = key
        row.update(_stats_row(group))
        if "eval_rate" in output_columns:
            row["eval_rate"] = float(row["times_evaluated"] / max(row["runs_tested"], 1))
        records.append(row)
    return pd.DataFrame(records).reindex(columns=output_columns)


def compute_region_statistics(tested_regions: pd.DataFrame) -> pd.DataFrame:
    return _build_dimension_statistics(tested_regions, ["region_key"], _TABLES["region_statistics"])


def _aggregate_dimension(
    tested_regions: pd.DataFrame, column: str, output_columns: list[str]
) -> pd.DataFrame:
    return _build_dimension_statistics(tested_regions, [column], output_columns)


def compute_event_statistics(tested_regions: pd.DataFrame) -> pd.DataFrame:
    return _aggregate_dimension(tested_regions, "event_type", _TABLES["event_statistics"])


def compute_template_statistics(tested_regions: pd.DataFrame) -> pd.DataFrame:
    return _aggregate_dimension(tested_regions, "template_id", _TABLES["template_statistics"])


def compute_context_statistics(tested_regions: pd.DataFrame) -> pd.DataFrame:
    return _build_dimension_statistics(
        tested_regions,
        ["context_hash", "context_json"],
        _TABLES["context_statistics"],
    )
