from __future__ import annotations

import json
import math
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

import pandas as pd
import yaml

from project import PROJECT_ROOT
from project.artifacts import phase2_candidates_path
from project.core.config import get_data_root
from project.domain.compiled_registry import get_domain_registry
from project.events.config import compose_event_config
from project.io.utils import ensure_dir, read_parquet
from project.research.agent_io.execute_proposal import build_run_all_command
from project.research.agent_io.proposal_schema import _proposal_settable_knobs, load_agent_proposal
from project.research.agent_io.proposal_to_experiment import translate_and_validate_proposal
from project.research.regime_routing import executable_regime_event_fanout, routing_entry_for_regime
from project.research.services.run_comparison_service import (
    compare_run_ids,
    research_diagnostics_paths,
)


REQUIRED_MATRIX_KEYS = ("matrix_id", "symbols", "windows", "regimes")
REQUIRED_WINDOW_KEYS = ("label", "start", "end")
NEW_REGIME_FIELDS = (
    "canonical_regime",
    "subtype",
    "phase",
    "evidence_mode",
    "recommended_bucket",
    "regime_bucket",
    "routing_profile_id",
)
DEFAULT_AUDIT_THRESHOLDS: Dict[str, float] = {
    "min_metadata_field_coverage": 0.99,
    "max_unknown_regime_rate": 0.01,
    "max_routing_profile_candidate_share": 0.90,
}

_PROPOSAL_SETTABLE_KNOBS = _proposal_settable_knobs()
_SEARCH_LIMITS_PATH = PROJECT_ROOT / "configs" / "registries" / "search_limits.yaml"


@dataclass(frozen=True)
class ShakeoutSlice:
    pair_id: str
    run_id: str
    slice_type: str
    canonical_regime: str
    symbol: str
    window_label: str
    start: str
    end: str
    templates: tuple[str, ...]
    raw_control_events: tuple[str, ...]
    baseline_event_type: str
    subtypes: tuple[str, ...]
    phases: tuple[str, ...]
    evidence_modes: tuple[str, ...]
    contexts: Dict[str, list[str]]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _slug(text: Any) -> str:
    cleaned = "".join(ch.lower() if str(ch).isalnum() else "_" for ch in str(text or "").strip())
    compact = "_".join(token for token in cleaned.split("_") if token)
    return compact or "slice"


def _as_str_list(values: Any) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        token = values.strip()
        return [token] if token else []
    if not isinstance(values, (list, tuple, set)):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        token = str(value).strip()
        if token and token not in seen:
            out.append(token)
            seen.add(token)
    return out


def _normalize_contexts(raw: Any) -> Dict[str, list[str]]:
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise ValueError("contexts must be a mapping of dimension -> allowed values")
    out: Dict[str, list[str]] = {}
    for key, value in raw.items():
        name = str(key).strip()
        if not name:
            continue
        out[name] = _as_str_list(value)
    return out


def _split_knobs(raw: Any) -> tuple[Dict[str, Any], Dict[str, Any]]:
    if not isinstance(raw, Mapping):
        return {}, {}
    proposal_knobs: Dict[str, Any] = {}
    runtime_overrides: Dict[str, Any] = {}
    for key, value in raw.items():
        name = str(key).strip()
        if not name:
            continue
        if name in _PROPOSAL_SETTABLE_KNOBS:
            proposal_knobs[name] = value
        else:
            runtime_overrides[name] = value
    return proposal_knobs, runtime_overrides


def load_regime_shakeout_matrix(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Regime shakeout matrix not found: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Regime shakeout matrix must be a YAML mapping.")
    for key in REQUIRED_MATRIX_KEYS:
        if key not in payload:
            raise ValueError(f"Regime shakeout matrix missing required key '{key}'.")

    symbols = _as_str_list(payload.get("symbols"))
    if len(symbols) < 1:
        raise ValueError("Regime shakeout matrix must define at least one symbol.")
    payload["symbols"] = symbols

    windows = payload.get("windows", [])
    if not isinstance(windows, list) or not windows:
        raise ValueError("Regime shakeout matrix must define a non-empty 'windows' list.")
    normalized_windows: list[dict[str, str]] = []
    for idx, row in enumerate(windows):
        if not isinstance(row, Mapping):
            raise ValueError(f"Window at index {idx} must be a mapping.")
        normalized: dict[str, str] = {}
        for key in REQUIRED_WINDOW_KEYS:
            value = str(row.get(key, "")).strip()
            if not value:
                raise ValueError(f"Window at index {idx} missing required key '{key}'.")
            normalized[key] = value
        normalized_windows.append(normalized)
    payload["windows"] = normalized_windows

    regimes = payload.get("regimes", [])
    if not isinstance(regimes, list) or not regimes:
        raise ValueError("Regime shakeout matrix must define a non-empty 'regimes' list.")
    for idx, row in enumerate(regimes):
        if not isinstance(row, Mapping):
            raise ValueError(f"Regime at index {idx} must be a mapping.")
        regime = str(row.get("canonical_regime", "")).strip().upper()
        if not regime:
            raise ValueError(f"Regime at index {idx} missing canonical_regime.")
    defaults = payload.get("defaults", {})
    payload["defaults"] = dict(defaults) if isinstance(defaults, Mapping) else {}
    return payload


def _routing_templates_for_regime(canonical_regime: str) -> list[str]:
    entry = routing_entry_for_regime(canonical_regime)
    if entry is not None and entry.eligible_templates:
        return list(entry.eligible_templates)
    registry = get_domain_registry()
    event_ids = registry.get_event_ids_for_regime(canonical_regime, executable_only=True)
    families = {
        (registry.get_event(event_id).research_family or registry.get_event(event_id).canonical_family)
        for event_id in event_ids
        if registry.get_event(event_id) is not None
    }
    templates: list[str] = []
    seen: set[str] = set()
    for family in sorted(families):
        for template in registry.family_execution_templates(family):
            if template not in seen:
                templates.append(template)
                seen.add(template)
    return templates or list(registry.default_hypothesis_templates() or ("mean_reversion",))


def _max_templates_per_run() -> int:
    if not _SEARCH_LIMITS_PATH.exists():
        return 6
    try:
        payload = yaml.safe_load(_SEARCH_LIMITS_PATH.read_text(encoding="utf-8")) or {}
    except Exception:
        return 6
    limits = payload.get("limits", {}) if isinstance(payload, Mapping) else {}
    try:
        return max(int(limits.get("max_templates_per_run", 6)), 1)
    except (TypeError, ValueError):
        return 6


def _raw_control_events_for_regime(canonical_regime: str, raw: Mapping[str, Any]) -> list[str]:
    explicit = _as_str_list(raw.get("raw_control_events"))
    if explicit:
        return explicit
    auto = executable_regime_event_fanout([canonical_regime]).get(canonical_regime, [])
    if auto:
        return list(auto)
    raise ValueError(f"No executable raw control events found for regime {canonical_regime}.")


def _native_templates_for_event(event_type: str) -> list[str]:
    registry = get_domain_registry()
    allowed_templates = set(registry.template_operator_definitions.keys())
    max_templates = _max_templates_per_run()
    cfg = compose_event_config(str(event_type).strip().upper())
    templates = [template for template in cfg.templates if template in allowed_templates]
    if templates:
        return templates[:max_templates]
    family = str(cfg.family or cfg.canonical_family or "").strip().upper()
    family_templates = [
        template
        for template in (list(registry.family_execution_templates(family)) if family else [])
        if template in allowed_templates
    ]
    if family_templates:
        return family_templates[:max_templates]
    return [
        template
        for template in (registry.default_hypothesis_templates() or ("mean_reversion",))
        if template in allowed_templates
    ][:max_templates]


def materialize_regime_shakeout_slices(matrix: Mapping[str, Any]) -> list[ShakeoutSlice]:
    defaults = dict(matrix.get("defaults", {}))
    default_contexts = _normalize_contexts(defaults.get("contexts", {}))
    symbols = _as_str_list(matrix.get("symbols"))
    windows = list(matrix.get("windows", []))

    slices: list[ShakeoutSlice] = []
    for regime_row in matrix.get("regimes", []):
        canonical_regime = str(regime_row.get("canonical_regime", "")).strip().upper()
        templates = _as_str_list(regime_row.get("templates")) or _routing_templates_for_regime(
            canonical_regime
        )
        raw_control_events = _raw_control_events_for_regime(canonical_regime, regime_row)
        contexts = default_contexts
        if "contexts" in regime_row:
            contexts = _normalize_contexts(regime_row.get("contexts"))
        subtypes = _as_str_list(regime_row.get("subtypes"))
        phases = _as_str_list(regime_row.get("phases"))
        evidence_modes = _as_str_list(regime_row.get("evidence_modes"))
        regime_slug = _slug(canonical_regime)
        for symbol in symbols:
            symbol_slug = _slug(symbol)
            for window in windows:
                window_label = str(window["label"]).strip()
                pair_id = f"{regime_slug}__{symbol_slug}__{_slug(window_label)}"
                common = dict(
                    pair_id=pair_id,
                    canonical_regime=canonical_regime,
                    symbol=symbol,
                    window_label=window_label,
                    start=str(window["start"]),
                    end=str(window["end"]),
                    templates=tuple(templates),
                    raw_control_events=tuple(raw_control_events),
                    baseline_event_type="",
                    subtypes=tuple(subtypes),
                    phases=tuple(phases),
                    evidence_modes=tuple(evidence_modes),
                    contexts=dict(contexts),
                )
                slices.append(
                    ShakeoutSlice(
                        run_id=f"shakeout_{regime_slug}_{symbol_slug}_{_slug(window_label)}_regime",
                        slice_type="regime_first",
                        **common,
                    )
                )
                for event_type in raw_control_events:
                    slices.append(
                        ShakeoutSlice(
                            run_id=(
                                f"shakeout_{regime_slug}_{symbol_slug}_{_slug(window_label)}_raw_"
                                f"{_slug(event_type)}"
                            ),
                            slice_type="raw_control",
                            pair_id=pair_id,
                            canonical_regime=canonical_regime,
                            symbol=symbol,
                            window_label=window_label,
                            start=str(window["start"]),
                            end=str(window["end"]),
                            templates=tuple(_native_templates_for_event(event_type)),
                            raw_control_events=(str(event_type).strip().upper(),),
                            baseline_event_type=str(event_type).strip().upper(),
                            subtypes=(),
                            phases=(),
                            evidence_modes=(),
                            contexts=dict(contexts),
                        )
                    )
    return slices


def build_shakeout_proposal_payload(
    *,
    matrix: Mapping[str, Any],
    slice_def: ShakeoutSlice,
) -> Dict[str, Any]:
    defaults = dict(matrix.get("defaults", {}))
    proposal_knobs, _runtime_overrides = _split_knobs(defaults.get("knobs", {}))
    base_program_id = str(
        defaults.get("program_id", matrix.get("matrix_id", "regime_shakeout"))
    ).strip()
    program_id = f"{_slug(base_program_id)}__{slice_def.run_id}"
    payload: Dict[str, Any] = {
        "program_id": program_id,
        "objective_name": str(defaults.get("objective_name", "retail_profitability")).strip()
        or "retail_profitability",
        "description": (
            f"{slice_def.slice_type} shakeout for {slice_def.canonical_regime} "
            f"on {slice_def.symbol} during {slice_def.window_label}"
        ),
        "run_mode": str(defaults.get("run_mode", "research")).strip() or "research",
        "promotion_profile": str(defaults.get("promotion_profile", "disabled")).strip()
        or "disabled",
        "symbols": [slice_def.symbol],
        "timeframe": str(defaults.get("timeframe", "5m")).strip() or "5m",
        "start": slice_def.start,
        "end": slice_def.end,
        "trigger_space": {
            "allowed_trigger_types": ["EVENT"],
            "events": {},
            "canonical_regimes": [],
            "subtypes": list(slice_def.subtypes),
            "phases": list(slice_def.phases),
            "evidence_modes": list(slice_def.evidence_modes),
        },
        "templates": list(slice_def.templates),
        "horizons_bars": list(defaults.get("horizons_bars", [12, 24])),
        "directions": list(defaults.get("directions", ["long", "short"])),
        "entry_lags": list(defaults.get("entry_lags", [1])),
        "contexts": dict(slice_def.contexts),
        "search_control": dict(defaults.get("search_control", {})),
        "artifacts": dict(defaults.get("artifacts", {})),
        "knobs": proposal_knobs,
    }
    if slice_def.slice_type == "regime_first":
        payload["trigger_space"]["canonical_regimes"] = [slice_def.canonical_regime]
    else:
        include = (
            [slice_def.baseline_event_type]
            if str(slice_def.baseline_event_type).strip()
            else list(slice_def.raw_control_events)
        )
        payload["trigger_space"]["events"] = {"include": include}
        payload["trigger_space"]["subtypes"] = []
        payload["trigger_space"]["phases"] = []
        payload["trigger_space"]["evidence_modes"] = []
    load_agent_proposal(payload)
    return payload


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(yaml.safe_dump(dict(payload), sort_keys=False), encoding="utf-8")


def _merged_run_all_overrides(
    *,
    matrix: Mapping[str, Any],
    translation: Mapping[str, Any],
) -> Dict[str, Any]:
    defaults = dict(matrix.get("defaults", {}))
    _proposal_knobs, runtime_from_knobs = _split_knobs(defaults.get("knobs", {}))
    explicit_runtime = defaults.get("run_all_overrides", {})
    runtime_overrides = (
        dict(explicit_runtime)
        if isinstance(explicit_runtime, Mapping)
        else {}
    )
    runtime_overrides.update(runtime_from_knobs)
    merged = dict(translation.get("run_all_overrides", {}))
    merged.update(runtime_overrides)
    return merged


def _run_env(*, data_root: Path) -> Dict[str, str]:
    env = os.environ.copy()
    repo_root = str(PROJECT_ROOT.parent)
    existing_pythonpath = str(env.get("PYTHONPATH", "")).strip()
    env["PYTHONPATH"] = f"{repo_root}:{existing_pythonpath}" if existing_pythonpath else repo_root
    env["BACKTEST_DATA_ROOT"] = str(data_root)
    return env


def _execute_command(
    *,
    command: list[str],
    data_root: Path,
    check: bool,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT.parent),
        env=_run_env(data_root=data_root),
        check=check,
    )


def _safe_read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _safe_read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return read_parquet(path)
    except FileNotFoundError:
        return pd.DataFrame()


def _candidate_surface_frame(data_root: Path, run_id: str) -> tuple[pd.DataFrame, str]:
    edge_candidates_path = _edge_candidates_path(data_root, run_id)
    if edge_candidates_path.exists():
        return _safe_read_parquet(edge_candidates_path), "edge_candidates"
    return _safe_read_parquet(_phase2_path(data_root, run_id)), "phase2_search_engine"


def _phase2_path(data_root: Path, run_id: str) -> Path:
    return phase2_candidates_path(run_id, root=data_root)


def _edge_candidates_path(data_root: Path, run_id: str) -> Path:
    return data_root / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.parquet"


def _promoted_path(data_root: Path, run_id: str) -> Path:
    return data_root / "reports" / "promotions" / run_id / "promoted_candidates.parquet"


def _promotion_decisions_path(data_root: Path, run_id: str) -> Path:
    return data_root / "reports" / "promotions" / run_id / "promotion_decisions.parquet"


def _entropy(values: Iterable[str]) -> float:
    counts: Dict[str, int] = {}
    total = 0
    for raw in values:
        token = str(raw).strip()
        if not token:
            continue
        counts[token] = counts.get(token, 0) + 1
        total += 1
    if total <= 1 or len(counts) <= 1:
        return 0.0
    probs = [count / total for count in counts.values()]
    entropy = -sum(prob * math.log(prob, 2) for prob in probs if prob > 0.0)
    max_entropy = math.log(len(counts), 2)
    return float(entropy / max_entropy) if max_entropy > 0.0 else 0.0


def _distribution(series: pd.Series) -> Dict[str, int]:
    if series.empty:
        return {}
    counts = series.astype(str).str.strip()
    counts = counts[counts != ""].value_counts().sort_index()
    return {str(key): int(value) for key, value in counts.items()}


def _max_share(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    counts = series.astype(str).str.strip()
    counts = counts[counts != ""].value_counts()
    if counts.empty:
        return 0.0
    return float(counts.iloc[0] / max(int(counts.sum()), 1))


def _field_non_null_rates(frame: pd.DataFrame) -> Dict[str, float]:
    if frame.empty:
        return {field: 0.0 for field in NEW_REGIME_FIELDS}
    rates: Dict[str, float] = {}
    for field in NEW_REGIME_FIELDS:
        if field not in frame.columns:
            rates[field] = 0.0
            continue
        values = frame[field]
        present = values.notna() & values.astype(str).str.strip().ne("")
        rates[field] = float(present.mean())
    return rates


def _topk_after_cost_mean(frame: pd.DataFrame, *, k: int = 10) -> float:
    if frame.empty or "after_cost_expectancy" not in frame.columns:
        return 0.0
    ranked = frame.copy()
    ranked["_metric"] = pd.to_numeric(ranked["after_cost_expectancy"], errors="coerce")
    ranked = ranked.dropna(subset=["_metric"])
    if ranked.empty:
        return 0.0
    if "q_value" in ranked.columns:
        ranked["_q"] = pd.to_numeric(ranked["q_value"], errors="coerce").fillna(1.0)
        ranked = ranked.sort_values(["_q", "_metric"], ascending=[True, False], kind="stable")
    else:
        ranked = ranked.sort_values(["_metric"], ascending=[False], kind="stable")
    return float(ranked.head(int(k))["_metric"].mean())


def summarize_shakeout_run(
    *,
    data_root: Path,
    run_id: str,
    thresholds: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    phase2, candidate_surface = _candidate_surface_frame(data_root, run_id)
    promoted = _safe_read_parquet(_promoted_path(data_root, run_id))
    promotion_decisions = _safe_read_parquet(_promotion_decisions_path(data_root, run_id))
    regime_summary = _safe_read_json(
        research_diagnostics_paths(data_root=data_root, run_id=run_id)["regime_effectiveness"]
    )
    candidate_rates = _field_non_null_rates(phase2)
    promoted_rates = _field_non_null_rates(promoted)
    candidate_count = int(len(phase2))
    promoted_count = int(len(promoted))
    represented_regimes = (
        phase2.get("canonical_regime", pd.Series(dtype="object")).astype(str).str.strip()
    )
    represented_regimes = represented_regimes[represented_regimes != ""]
    represented_events = phase2.get("event_type", pd.Series(dtype="object")).astype(str).str.strip()
    represented_events = represented_events[represented_events != ""]
    represented_subtypes = phase2.get("subtype", pd.Series(dtype="object")).astype(str).str.strip()
    represented_subtypes = represented_subtypes[represented_subtypes != ""]
    represented_phases = phase2.get("phase", pd.Series(dtype="object")).astype(str).str.strip()
    represented_phases = represented_phases[represented_phases != ""]
    represented_modes = phase2.get("evidence_mode", pd.Series(dtype="object")).astype(str).str.strip()
    represented_modes = represented_modes[represented_modes != ""]
    bucket_agreement_rate = 0.0
    if not phase2.empty and {"recommended_bucket", "regime_bucket"}.issubset(phase2.columns):
        lhs = phase2["recommended_bucket"].astype(str).str.strip()
        rhs = phase2["regime_bucket"].astype(str).str.strip()
        comparable = (lhs != "") & (rhs != "")
        if comparable.any():
            bucket_agreement_rate = float((lhs[comparable] == rhs[comparable]).mean())
    summary = {
        "run_id": run_id,
        "candidate_surface": candidate_surface,
        "candidate_count": candidate_count,
        "promoted_count": promoted_count,
        "promotion_rate": float(promoted_count / max(candidate_count, 1)),
        "unique_raw_events_represented": int(represented_events.nunique()),
        "unique_canonical_regimes_represented": int(represented_regimes.nunique()),
        "unique_subtypes_represented": int(represented_subtypes.nunique()),
        "unique_phases_represented": int(represented_phases.nunique()),
        "unique_evidence_modes_represented": int(represented_modes.nunique()),
        "candidate_regime_distribution": _distribution(
            phase2.get("canonical_regime", pd.Series(dtype="object"))
        ),
        "routing_profile_usage_distribution": _distribution(
            phase2.get("routing_profile_id", pd.Series(dtype="object"))
        ),
        "promoted_routing_profile_usage_distribution": _distribution(
            promoted.get("routing_profile_id", pd.Series(dtype="object"))
        ),
        "candidate_regime_max_share": _max_share(
            phase2.get("canonical_regime", pd.Series(dtype="object"))
        ),
        "candidate_routing_profile_max_share": _max_share(
            phase2.get("routing_profile_id", pd.Series(dtype="object"))
        ),
        "promoted_routing_profile_max_share": _max_share(
            promoted.get("routing_profile_id", pd.Series(dtype="object"))
        ),
        "subtype_entropy": _entropy(represented_subtypes.tolist()),
        "evidence_mode_entropy": _entropy(represented_modes.tolist()),
        "candidate_field_coverage": candidate_rates,
        "promoted_field_coverage": promoted_rates,
        "unknown_regime_rate": (
            float(1.0 - candidate_rates.get("canonical_regime", 0.0)) if candidate_count > 0 else 0.0
        ),
        "raw_event_to_canonical_collapse_ratio": float(
            int(represented_events.nunique()) / max(int(represented_regimes.nunique()), 1)
        )
        if candidate_count > 0
        else 0.0,
        "bucket_agreement_rate": bucket_agreement_rate,
        "recommended_bucket_distribution": _distribution(
            phase2.get("recommended_bucket", pd.Series(dtype="object"))
        ),
        "regime_bucket_distribution": _distribution(
            phase2.get("regime_bucket", pd.Series(dtype="object"))
        ),
        "topk_after_cost_expectancy_mean": _topk_after_cost_mean(phase2, k=10),
        "topk_promoted_after_cost_expectancy_mean": _topk_after_cost_mean(promoted, k=10),
        "regime_effectiveness_status": str(regime_summary.get("status", "")).strip(),
        "regime_effectiveness_rows": int(regime_summary.get("scorecard_rows", 0) or 0),
        "promotion_decision_rows": int(len(promotion_decisions)),
    }
    resolved_thresholds = dict(DEFAULT_AUDIT_THRESHOLDS)
    if isinstance(thresholds, Mapping):
        for key, value in thresholds.items():
            try:
                resolved_thresholds[str(key)] = float(value)
            except (TypeError, ValueError):
                continue
    min_cov = float(resolved_thresholds["min_metadata_field_coverage"])
    max_unknown = float(resolved_thresholds["max_unknown_regime_rate"])
    max_routing = float(resolved_thresholds["max_routing_profile_candidate_share"])
    issues: list[str] = []
    if candidate_count > 0:
        for field, rate in candidate_rates.items():
            if (
                field in {"canonical_regime", "recommended_bucket", "regime_bucket", "routing_profile_id"}
                and rate < min_cov
            ):
                issues.append(f"candidate field coverage for {field}={rate:.3f} < {min_cov:.3f}")
    if candidate_count > 0 and summary["unknown_regime_rate"] > max_unknown:
        issues.append(
            f"unknown regime rate={summary['unknown_regime_rate']:.3f} > {max_unknown:.3f}"
        )
    if (
        candidate_count > 0
        and summary["unique_canonical_regimes_represented"] > 1
        and summary["candidate_routing_profile_max_share"] > max_routing
    ):
        issues.append(
            "candidate routing profile max share="
            f"{summary['candidate_routing_profile_max_share']:.3f} > {max_routing:.3f}"
        )
    summary["contract_health"] = {
        "passed": not issues,
        "issue_count": len(issues),
        "issues": issues,
        "thresholds": resolved_thresholds,
    }
    return summary


def summarize_shakeout_run_group(
    *,
    data_root: Path,
    run_ids: Iterable[str],
    thresholds: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    run_id_list = [str(run_id).strip() for run_id in run_ids if str(run_id).strip()]
    candidate_frames: list[pd.DataFrame] = []
    candidate_surfaces: list[str] = []
    promoted_frames: list[pd.DataFrame] = []
    promotion_frames: list[pd.DataFrame] = []
    regime_summaries: list[Dict[str, Any]] = []
    for run_id in run_id_list:
        frame, surface = _candidate_surface_frame(data_root, run_id)
        candidate_frames.append(frame)
        candidate_surfaces.append(surface)
        promoted_frames.append(_safe_read_parquet(_promoted_path(data_root, run_id)))
        promotion_frames.append(_safe_read_parquet(_promotion_decisions_path(data_root, run_id)))
        regime_summaries.append(
            _safe_read_json(research_diagnostics_paths(data_root=data_root, run_id=run_id)["regime_effectiveness"])
        )
    phase2 = pd.concat(candidate_frames, ignore_index=True) if candidate_frames else pd.DataFrame()
    if not phase2.empty and "candidate_id" in phase2.columns:
        phase2 = phase2.drop_duplicates(subset=["candidate_id"], keep="first")
    promoted = pd.concat(promoted_frames, ignore_index=True) if promoted_frames else pd.DataFrame()
    if not promoted.empty and "candidate_id" in promoted.columns:
        promoted = promoted.drop_duplicates(subset=["candidate_id"], keep="first")
    promotion_decisions = (
        pd.concat(promotion_frames, ignore_index=True) if promotion_frames else pd.DataFrame()
    )
    candidate_surface = (
        candidate_surfaces[0]
        if candidate_surfaces and len(set(candidate_surfaces)) == 1
        else "grouped_mixed"
    )
    scorecard_rows = sum(int(summary.get("scorecard_rows", 0) or 0) for summary in regime_summaries)
    representative_summary = next((summary for summary in regime_summaries if summary), {})
    candidate_rates = _field_non_null_rates(phase2)
    promoted_rates = _field_non_null_rates(promoted)
    candidate_count = int(len(phase2))
    promoted_count = int(len(promoted))
    represented_regimes = (
        phase2.get("canonical_regime", pd.Series(dtype="object")).astype(str).str.strip()
    )
    represented_regimes = represented_regimes[represented_regimes != ""]
    represented_events = phase2.get("event_type", pd.Series(dtype="object")).astype(str).str.strip()
    represented_events = represented_events[represented_events != ""]
    represented_subtypes = phase2.get("subtype", pd.Series(dtype="object")).astype(str).str.strip()
    represented_subtypes = represented_subtypes[represented_subtypes != ""]
    represented_phases = phase2.get("phase", pd.Series(dtype="object")).astype(str).str.strip()
    represented_phases = represented_phases[represented_phases != ""]
    represented_modes = phase2.get("evidence_mode", pd.Series(dtype="object")).astype(str).str.strip()
    represented_modes = represented_modes[represented_modes != ""]
    bucket_agreement_rate = 0.0
    if not phase2.empty and {"recommended_bucket", "regime_bucket"}.issubset(phase2.columns):
        lhs = phase2["recommended_bucket"].astype(str).str.strip()
        rhs = phase2["regime_bucket"].astype(str).str.strip()
        comparable = (lhs != "") & (rhs != "")
        if comparable.any():
            bucket_agreement_rate = float((lhs[comparable] == rhs[comparable]).mean())
    summary = {
        "run_id": "+".join(run_id_list),
        "candidate_surface": candidate_surface,
        "candidate_count": candidate_count,
        "promoted_count": promoted_count,
        "promotion_rate": float(promoted_count / max(candidate_count, 1)),
        "unique_raw_events_represented": int(represented_events.nunique()),
        "unique_canonical_regimes_represented": int(represented_regimes.nunique()),
        "unique_subtypes_represented": int(represented_subtypes.nunique()),
        "unique_phases_represented": int(represented_phases.nunique()),
        "unique_evidence_modes_represented": int(represented_modes.nunique()),
        "candidate_regime_distribution": _distribution(
            phase2.get("canonical_regime", pd.Series(dtype="object"))
        ),
        "routing_profile_usage_distribution": _distribution(
            phase2.get("routing_profile_id", pd.Series(dtype="object"))
        ),
        "promoted_routing_profile_usage_distribution": _distribution(
            promoted.get("routing_profile_id", pd.Series(dtype="object"))
        ),
        "candidate_regime_max_share": _max_share(
            phase2.get("canonical_regime", pd.Series(dtype="object"))
        ),
        "candidate_routing_profile_max_share": _max_share(
            phase2.get("routing_profile_id", pd.Series(dtype="object"))
        ),
        "promoted_routing_profile_max_share": _max_share(
            promoted.get("routing_profile_id", pd.Series(dtype="object"))
        ),
        "subtype_entropy": _entropy(represented_subtypes.tolist()),
        "evidence_mode_entropy": _entropy(represented_modes.tolist()),
        "candidate_field_coverage": candidate_rates,
        "promoted_field_coverage": promoted_rates,
        "unknown_regime_rate": (
            float(1.0 - candidate_rates.get("canonical_regime", 0.0)) if candidate_count > 0 else 0.0
        ),
        "raw_event_to_canonical_collapse_ratio": float(
            int(represented_events.nunique()) / max(int(represented_regimes.nunique()), 1)
        )
        if candidate_count > 0
        else 0.0,
        "bucket_agreement_rate": bucket_agreement_rate,
        "recommended_bucket_distribution": _distribution(
            phase2.get("recommended_bucket", pd.Series(dtype="object"))
        ),
        "regime_bucket_distribution": _distribution(
            phase2.get("regime_bucket", pd.Series(dtype="object"))
        ),
        "topk_after_cost_expectancy_mean": _topk_after_cost_mean(phase2, k=10),
        "topk_promoted_after_cost_expectancy_mean": _topk_after_cost_mean(promoted, k=10),
        "regime_effectiveness_status": str(representative_summary.get("status", "")).strip(),
        "regime_effectiveness_rows": int(scorecard_rows),
        "promotion_decision_rows": int(len(promotion_decisions)),
        "grouped_run_ids": run_id_list,
    }
    resolved_thresholds = dict(DEFAULT_AUDIT_THRESHOLDS)
    if isinstance(thresholds, Mapping):
        for key, value in thresholds.items():
            try:
                resolved_thresholds[str(key)] = float(value)
            except (TypeError, ValueError):
                continue
    min_cov = float(resolved_thresholds["min_metadata_field_coverage"])
    max_unknown = float(resolved_thresholds["max_unknown_regime_rate"])
    max_routing = float(resolved_thresholds["max_routing_profile_candidate_share"])
    issues: list[str] = []
    if candidate_count > 0:
        for field, rate in candidate_rates.items():
            if (
                field in {"canonical_regime", "recommended_bucket", "regime_bucket", "routing_profile_id"}
                and rate < min_cov
            ):
                issues.append(f"candidate field coverage for {field}={rate:.3f} < {min_cov:.3f}")
    if candidate_count > 0 and summary["unknown_regime_rate"] > max_unknown:
        issues.append(
            f"unknown regime rate={summary['unknown_regime_rate']:.3f} > {max_unknown:.3f}"
        )
    if (
        candidate_count > 0
        and summary["unique_canonical_regimes_represented"] > 1
        and summary["candidate_routing_profile_max_share"] > max_routing
    ):
        issues.append(
            "candidate routing profile max share="
            f"{summary['candidate_routing_profile_max_share']:.3f} > {max_routing:.3f}"
        )
    summary["contract_health"] = {
        "passed": not issues,
        "issue_count": len(issues),
        "issues": issues,
        "thresholds": resolved_thresholds,
    }
    return summary


def build_shakeout_audit(
    *,
    matrix: Mapping[str, Any],
    slices: Iterable[ShakeoutSlice],
    data_root: Path,
) -> Dict[str, Any]:
    thresholds = dict(matrix.get("defaults", {})).get("audit_thresholds", {})
    slices_by_run = {slice_def.run_id: slice_def for slice_def in slices}
    run_summaries: Dict[str, Any] = {}
    for run_id in sorted(slices_by_run):
        edge_path = _edge_candidates_path(data_root, run_id)
        phase2_path = _phase2_path(data_root, run_id)
        if not edge_path.exists() and not phase2_path.exists():
            continue
        run_summaries[run_id] = summarize_shakeout_run(
            data_root=data_root,
            run_id=run_id,
            thresholds=thresholds if isinstance(thresholds, Mapping) else None,
        )

    pair_reports: list[dict[str, Any]] = []
    grouped: Dict[str, Dict[str, Any]] = {}
    for slice_def in slices:
        bucket = grouped.setdefault(slice_def.pair_id, {"regime_first": "", "raw_control": []})
        if slice_def.slice_type == "regime_first":
            bucket["regime_first"] = slice_def.run_id
        elif slice_def.slice_type == "raw_control":
            bucket.setdefault("raw_control", []).append(slice_def.run_id)
    for pair_id, run_map in sorted(grouped.items()):
        regime_run_id = str(run_map.get("regime_first", "")).strip()
        raw_run_ids = [str(run_id).strip() for run_id in run_map.get("raw_control", []) if str(run_id).strip()]
        if regime_run_id not in run_summaries or not raw_run_ids:
            continue
        regime_summary = run_summaries[regime_run_id]
        raw_summary = summarize_shakeout_run_group(
            data_root=data_root,
            run_ids=raw_run_ids,
            thresholds=thresholds if isinstance(thresholds, Mapping) else None,
        )
        pair_reports.append(
            {
                "pair_id": pair_id,
                "regime_run_id": regime_run_id,
                "raw_control_run_ids": raw_run_ids,
                "canonical_regime": slices_by_run[regime_run_id].canonical_regime,
                "regime_summary": regime_summary,
                "raw_control_summary": raw_summary,
                "delta": {
                    "candidate_count": regime_summary["candidate_count"] - raw_summary["candidate_count"],
                    "promoted_count": regime_summary["promoted_count"] - raw_summary["promoted_count"],
                    "promotion_rate": regime_summary["promotion_rate"] - raw_summary["promotion_rate"],
                    "topk_after_cost_expectancy_mean": regime_summary["topk_after_cost_expectancy_mean"]
                    - raw_summary["topk_after_cost_expectancy_mean"],
                    "topk_promoted_after_cost_expectancy_mean": regime_summary["topk_promoted_after_cost_expectancy_mean"]
                    - raw_summary["topk_promoted_after_cost_expectancy_mean"],
                    "unique_raw_events_represented": regime_summary["unique_raw_events_represented"]
                    - raw_summary["unique_raw_events_represented"],
                    "unique_subtypes_represented": regime_summary["unique_subtypes_represented"]
                    - raw_summary["unique_subtypes_represented"],
                    "unique_evidence_modes_represented": regime_summary["unique_evidence_modes_represented"]
                    - raw_summary["unique_evidence_modes_represented"],
                    "raw_event_to_canonical_collapse_ratio": regime_summary["raw_event_to_canonical_collapse_ratio"]
                    - raw_summary["raw_event_to_canonical_collapse_ratio"],
                    "bucket_agreement_rate": regime_summary["bucket_agreement_rate"]
                    - raw_summary["bucket_agreement_rate"],
                },
                "research_run_comparisons": [
                    compare_run_ids(
                        data_root=data_root,
                        baseline_run_id=raw_run_id,
                        candidate_run_id=regime_run_id,
                    )
                    for raw_run_id in raw_run_ids
                ],
            }
        )
    return {
        "created_at_utc": _utc_now_iso(),
        "matrix_id": str(matrix.get("matrix_id", "")).strip(),
        "run_count": len(run_summaries),
        "runs": run_summaries,
        "pairs": pair_reports,
    }


def render_shakeout_audit_markdown(audit: Mapping[str, Any]) -> str:
    lines = [
        "# Regime Shakeout Audit",
        "",
        f"- `matrix_id`: `{audit.get('matrix_id', '')}`",
        f"- `run_count`: `{audit.get('run_count', 0)}`",
        "",
        "## Runs",
        "",
        "| Run | Candidates | Promoted | Promotion Rate | Collapse Ratio | Routing Max Share | Contract Health |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for run_id, summary in sorted(dict(audit.get("runs", {})).items()):
        lines.append(
            f"| `{run_id}` | `{summary.get('candidate_count', 0)}` | "
            f"`{summary.get('promoted_count', 0)}` | `{summary.get('promotion_rate', 0.0):.3f}` | "
            f"`{summary.get('raw_event_to_canonical_collapse_ratio', 0.0):.3f}` | "
            f"`{summary.get('candidate_routing_profile_max_share', 0.0):.3f}` | "
            f"`{'pass' if summary.get('contract_health', {}).get('passed') else 'fail'}` |"
        )
    lines.extend(
        [
            "",
            "## Pairs",
            "",
            "| Pair | Regime Run | Raw Control | Candidate Delta | Promoted Delta | Top-k After-Cost Delta |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    for pair in audit.get("pairs", []):
        delta = dict(pair.get("delta", {}))
        lines.append(
            f"| `{pair.get('pair_id', '')}` | `{pair.get('regime_run_id', '')}` | "
            f"`{len(pair.get('raw_control_run_ids', []))} raw runs` | `{delta.get('candidate_count', 0)}` | "
            f"`{delta.get('promoted_count', 0)}` | `{delta.get('topk_after_cost_expectancy_mean', 0.0):.3f}` |"
        )
    lines.append("")
    return "\n".join(lines)


def write_shakeout_audit(*, out_dir: Path, audit: Mapping[str, Any]) -> Dict[str, Path]:
    ensure_dir(out_dir)
    json_path = out_dir / "regime_shakeout_audit.json"
    md_path = out_dir / "regime_shakeout_audit.md"
    json_path.write_text(json.dumps(dict(audit), indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(render_shakeout_audit_markdown(audit), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}


def _load_existing_manifest_results(out_dir: Path) -> Dict[str, Dict[str, Any]]:
    manifest_path = out_dir / "regime_shakeout_manifest.json"
    if not manifest_path.exists():
        return {}
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    rows = payload.get("results", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        run_id = str(row.get("run_id", "")).strip()
        if run_id:
            out[run_id] = dict(row)
    return out


def _write_shakeout_manifest(
    *,
    manifest_path: Path,
    matrix: Mapping[str, Any],
    matrix_path: Path,
    registry_root: Path,
    data_root: Path,
    execute: bool,
    plan_only: bool,
    dry_run: bool,
    check: bool,
    planned_runs: int,
    failures: int,
    results: list[dict[str, Any]],
    audit_paths: Mapping[str, str] | None = None,
) -> None:
    manifest = {
        "created_at_utc": _utc_now_iso(),
        "matrix_id": str(matrix.get("matrix_id", "")).strip(),
        "matrix_path": str(matrix_path),
        "registry_root": str(registry_root),
        "data_root": str(data_root),
        "execute": bool(execute),
        "plan_only": bool(plan_only),
        "dry_run": bool(dry_run),
        "check": bool(check),
        "planned_runs": int(planned_runs),
        "completed_rows": len(results),
        "failures": int(failures),
        "results": results,
    }
    if audit_paths:
        manifest["audit_json"] = str(audit_paths.get("json", ""))
        manifest["audit_markdown"] = str(audit_paths.get("markdown", ""))
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")


def run_regime_shakeout_matrix(
    *,
    matrix_path: Path,
    out_dir: Path,
    registry_root: Path,
    data_root: Path,
    execute: bool,
    plan_only: bool,
    dry_run: bool,
    check: bool,
) -> Dict[str, Any]:
    matrix = load_regime_shakeout_matrix(matrix_path)
    slices = materialize_regime_shakeout_slices(matrix)
    ensure_dir(out_dir)
    manifest_path = out_dir / "regime_shakeout_manifest.json"
    prior_results = _load_existing_manifest_results(out_dir)
    manifest_rows: list[dict[str, Any]] = []
    failures = 0
    for slice_def in slices:
        slice_dir = out_dir / "runs" / slice_def.run_id
        proposal_path = slice_dir / "proposal.yaml"
        payload = build_shakeout_proposal_payload(matrix=matrix, slice_def=slice_def)
        _write_yaml(proposal_path, payload)
        translation = translate_and_validate_proposal(
            proposal_path,
            registry_root=registry_root,
            out_dir=slice_dir,
            config_path=slice_dir / "experiment.yaml",
        )
        merged_overrides = _merged_run_all_overrides(matrix=matrix, translation=translation)
        (slice_dir / "run_all_overrides.json").write_text(
            json.dumps(merged_overrides, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        command = build_run_all_command(
            run_id=slice_def.run_id,
            registry_root=registry_root,
            experiment_config_path=slice_dir / "experiment.yaml",
            run_all_overrides=merged_overrides,
            symbols=list(translation["proposal"]["symbols"]),
            start=str(translation["proposal"]["start"]),
            end=str(translation["proposal"]["end"]),
            plan_only=bool(plan_only),
            dry_run=bool(dry_run),
        )
        row: Dict[str, Any] = {
            "pair_id": slice_def.pair_id,
            "run_id": slice_def.run_id,
            "slice_type": slice_def.slice_type,
            "canonical_regime": slice_def.canonical_regime,
            "symbol": slice_def.symbol,
            "window_label": slice_def.window_label,
            "start": slice_def.start,
            "end": slice_def.end,
            "proposal_path": str(proposal_path),
            "experiment_config_path": str(slice_dir / "experiment.yaml"),
            "run_all_overrides_path": str(slice_dir / "run_all_overrides.json"),
            "command": command,
            "status": "planned",
            "returncode": None,
            "duration_sec": None,
            "validated_plan": translation["validated_plan"],
        }
        prior = prior_results.get(slice_def.run_id)
        if (
            execute
            and isinstance(prior, dict)
            and str(prior.get("status", "")).strip().lower() == "success"
            and int(prior.get("returncode", 1)) == 0
        ):
            reused = dict(prior)
            reused["reused_result"] = True
            manifest_rows.append(reused)
            continue
        if execute:
            t0 = time.perf_counter()
            result = _execute_command(command=command, data_root=data_root, check=bool(check))
            row["duration_sec"] = round(time.perf_counter() - t0, 3)
            row["returncode"] = int(result.returncode)
            row["status"] = "success" if int(result.returncode) == 0 else "failed"
            if int(result.returncode) != 0:
                failures += 1
        manifest_rows.append(row)
        _write_shakeout_manifest(
            manifest_path=manifest_path,
            matrix=matrix,
            matrix_path=matrix_path,
            registry_root=registry_root,
            data_root=data_root,
            execute=execute,
            plan_only=plan_only,
            dry_run=dry_run,
            check=check,
            planned_runs=len(slices),
            failures=failures,
            results=manifest_rows,
        )

    audit_paths: Dict[str, str] = {}
    if execute and not plan_only and not dry_run:
        audit = build_shakeout_audit(matrix=matrix, slices=slices, data_root=data_root)
        written = write_shakeout_audit(out_dir=out_dir, audit=audit)
        audit_paths = {key: str(value) for key, value in written.items()}
        _write_shakeout_manifest(
            manifest_path=manifest_path,
            matrix=matrix,
            matrix_path=matrix_path,
            registry_root=registry_root,
            data_root=data_root,
            execute=execute,
            plan_only=plan_only,
            dry_run=dry_run,
            check=check,
            planned_runs=len(slices),
            failures=failures,
            results=manifest_rows,
            audit_paths=audit_paths,
        )

    return {
        "matrix_id": str(matrix.get("matrix_id", "")).strip(),
        "manifest_path": str(manifest_path),
        "planned_runs": len(slices),
        "failures": failures,
        "audit_paths": audit_paths,
    }


def default_shakeout_out_dir(*, matrix_id: str, data_root: Path | None = None) -> Path:
    root = Path(data_root) if data_root is not None else get_data_root()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return root / "reports" / "regime_shakeout" / f"{_slug(matrix_id)}_{stamp}"
