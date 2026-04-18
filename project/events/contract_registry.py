from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

from project import PROJECT_ROOT
from project.domain.compiled_registry import get_domain_registry
from project.events.event_aliases import EVENT_ALIASES, EXECUTABLE_EVENT_ALIASES
from project.spec_registry import load_yaml_path

REPO_ROOT = PROJECT_ROOT.parent
RUNTIME_SPEC_DIR = REPO_ROOT / "spec" / "events"
INTERACTION_SPEC_DIR = RUNTIME_SPEC_DIR / "interaction"

REQUIRED_CONTRACT_FIELDS: tuple[str, ...] = (
    "description",
    "causal_mechanism",
    "observable_formula",
    "required_features",
    "threshold_method",
    "calibration_method",
    "failure_modes",
    "regime_applicability",
    "disabled_regimes",
    "expected_overlap",
    "invalidation_rule",
    "synthetic_coverage",
    "deployment_disposition",
    "operational_role",
    "tier",
)

_TIER_BASELINE_SCORES: dict[str, dict[str, int]] = {
    "A": {
        "ontology_maturity": 5,
        "implementation_maturity": 5,
        "spec_richness": 5,
        "spec_code_fidelity": 5,
        "calibration_maturity": 4,
        "synthetic_coverage": 4,
        "research_evidence_strength": 4,
        "deployment_suitability": 5,
    },
    "B": {
        "ontology_maturity": 4,
        "implementation_maturity": 4,
        "spec_richness": 4,
        "spec_code_fidelity": 4,
        "calibration_maturity": 3,
        "synthetic_coverage": 3,
        "research_evidence_strength": 3,
        "deployment_suitability": 3,
    },
    "C": {
        "ontology_maturity": 3,
        "implementation_maturity": 3,
        "spec_richness": 3,
        "spec_code_fidelity": 3,
        "calibration_maturity": 2,
        "synthetic_coverage": 2,
        "research_evidence_strength": 2,
        "deployment_suitability": 1,
    },
    "D": {
        "ontology_maturity": 2,
        "implementation_maturity": 2,
        "spec_richness": 2,
        "spec_code_fidelity": 2,
        "calibration_maturity": 1,
        "synthetic_coverage": 1,
        "research_evidence_strength": 1,
        "deployment_suitability": 0,
    },
    "X": {
        "ontology_maturity": 0,
        "implementation_maturity": 0,
        "spec_richness": 0,
        "spec_code_fidelity": 0,
        "calibration_maturity": 0,
        "synthetic_coverage": 0,
        "research_evidence_strength": 0,
        "deployment_suitability": 0,
    },
}


def allowed_runtime_aliases() -> tuple[str, ...]:
    aliases = set(EVENT_ALIASES.keys()) | set(EXECUTABLE_EVENT_ALIASES.keys())
    return tuple(sorted(str(alias).strip().upper() for alias in aliases if str(alias).strip()))


def _load_runtime_spec(event_type: str) -> dict[str, Any]:
    path = RUNTIME_SPEC_DIR / f"{event_type}.yaml"
    if not path.exists():
        return {}
    payload = load_yaml_path(path)
    return dict(payload) if isinstance(payload, dict) else {}


def _coalesce_text(*values: Any, default: str = "") -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return default


def _coalesce_list(*values: Any) -> list[Any]:
    for value in values:
        if isinstance(value, list) and value:
            return list(value)
        if isinstance(value, tuple) and value:
            return list(value)
    return []


def _flatten_disabled_regimes(values: Any) -> list[str]:
    out: list[str] = []
    if isinstance(values, (list, tuple)):
        for row in values:
            if isinstance(row, str):
                token = row.strip()
                if token:
                    out.append(token)
            elif isinstance(row, Mapping):
                for key, val in row.items():
                    token = str(key).strip()
                    detail = str(val).strip()
                    if token and detail:
                        out.append(f"{token}: {detail}")
                    elif token:
                        out.append(token)
    return out


def _flatten_failure_modes(values: Any) -> list[str]:
    return _flatten_disabled_regimes(values)


def _default_description(event_type: str, row: Mapping[str, Any]) -> str:
    regime = _coalesce_text(
        row.get("canonical_regime"),
        row.get("research_family"),
        row.get("canonical_family"),
        default="UNSPECIFIED",
    )
    phase = _coalesce_text(row.get("phase"), default="event")
    evidence = _coalesce_text(row.get("evidence_mode"), default="unspecified evidence")
    return f"{event_type} is a {phase.replace('_', ' ')} contract in the {regime.replace('_', ' ')} regime using {evidence.replace('_', ' ')} evidence."


def _default_causal_mechanism(event_type: str, row: Mapping[str, Any]) -> str:
    regime = _coalesce_text(
        row.get("canonical_regime"),
        row.get("research_family"),
        row.get("canonical_family"),
        default="market microstructure",
    )
    phase = _coalesce_text(row.get("phase"), default="state transition")
    return f"The event is treated as a {phase.replace('_', ' ')} in {regime.replace('_', ' ')}, where the detector seeks an observable footprint large enough to matter for downstream research while suppressing mechanical lookalikes."


def _parameter_keys(parameters: Mapping[str, Any]) -> set[str]:
    if not isinstance(parameters, Mapping):
        return set()
    return {
        str(key).strip()
        for key, value in parameters.items()
        if str(key).strip() and value not in (None, "", [], {})
    }


def _normalize_policy_token(*parts: str) -> str:
    tokens: list[str] = []
    for part in parts:
        token = str(part).strip().lower().replace("-", "_").replace(" ", "_")
        token = "_".join(chunk for chunk in token.split("_") if chunk)
        if token:
            tokens.append(token)
    return "_".join(tokens) or "parameterized_detector_policy"


def _salient_threshold_keys(parameters: Mapping[str, Any]) -> list[str]:
    keys = _parameter_keys(parameters)
    priority_order = [
        "shock_threshold_mode",
        "threshold_method",
        "anchor_mode",
        "threshold_z",
        "z_threshold",
        "band_z_threshold",
        "breakout_z_threshold",
        "transition_z_threshold",
        "desync_z_threshold",
        "oi_change_z_threshold",
        "spread_z_threshold",
        "slippage_z_threshold",
        "funding_extreme_quantile",
        "imbalance_abs_quantile",
        "spread_quantile",
        "rv_quantile",
        "liquidation_quantile",
        "oi_drop_quantile",
        "return_quantile",
        "reversal_quantile",
        "compression_quantile",
        "expansion_quantile",
        "session_range_quantile",
        "funding_pct_window",
        "extreme_pct",
        "accel_pct",
        "persistence_pct",
        "normalization_pct",
        "min_basis_bps",
        "threshold_bps",
        "min_flip_abs",
        "oi_drop_abs_min",
        "liquidation_abs_min",
        "return_abs_min",
        "hours_utc",
        "minute_open",
        "minute_close_start",
    ]
    out = [key for key in priority_order if key in keys]
    extras = sorted(
        key
        for key in keys
        if key not in out
        and any(
            token in key for token in ("threshold", "quantile", "pct", "abs_min", "minute", "hour")
        )
    )
    out.extend(extras)
    return out


def _infer_threshold_method(parameters: Mapping[str, Any], source_spec: Mapping[str, Any]) -> str:
    detector = source_spec.get("detector", {}) if isinstance(source_spec, Mapping) else {}
    calibration = source_spec.get("calibration", {}) if isinstance(source_spec, Mapping) else {}
    keys = _parameter_keys(parameters)
    if isinstance(parameters, Mapping):
        mode = str(parameters.get("shock_threshold_mode", "")).strip()
        if mode:
            return mode
        for key in ("threshold_method", "anchor_mode"):
            token = str(parameters.get(key, "")).strip()
            if token:
                return token
    if isinstance(calibration, Mapping):
        if calibration.get("search_range"):
            return "calibrated_search_range"
        if calibration.get("default_threshold") is not None:
            return "fixed_threshold"
    formula = str(detector.get("formula", "")).lower()
    if "quantile" in formula:
        return "rolling_quantile"
    if "z[" in formula or "zscore" in formula:
        return "rolling_zscore"

    if {"hours_utc", "minute_open", "minute_close_start"} & keys or "event_spacing_bars" in keys:
        return "scheduled_window_gate"
    if any("quantile" in key for key in keys) and any(
        "z_threshold" in key or key == "threshold_z" for key in keys
    ):
        return "quantile_plus_zscore_gate"
    if any(key.endswith("_pct") or key.endswith("_pct_window") for key in keys):
        return "rolling_percentile_gate"
    if any("quantile" in key for key in keys):
        if any(key.endswith("_abs_min") for key in keys):
            return "quantile_plus_abs_floor"
        return "rolling_quantile_gate"
    if any("z_threshold" in key or key == "threshold_z" for key in keys):
        if {"min_basis_bps", "threshold_bps"} & keys:
            return "zscore_plus_bps_floor"
        return "rolling_zscore_gate"
    if {"min_basis_bps", "threshold_bps"} & keys:
        return "fixed_bps_floor"
    if any(key.endswith("_abs_min") for key in keys) or "min_flip_abs" in keys:
        return "absolute_move_floor"

    family = (
        str(
            source_spec.get("research_family", source_spec.get("canonical_family", "detector"))
        ).strip()
        or "detector"
    )
    phase = str(source_spec.get("phase", "event")).strip() or "event"
    evidence = str(source_spec.get("evidence_mode", "observed")).strip() or "observed"
    return _normalize_policy_token(family, phase, evidence, "gate")


def _calibration_objective(source_spec: Mapping[str, Any]) -> tuple[str, str]:
    family = (
        str(source_spec.get("research_family", source_spec.get("canonical_family", "")))
        .strip()
        .upper()
    )
    phase = str(source_spec.get("phase", "")).strip().lower()
    objective_by_phase = {
        "breakout": "breakout follow-through versus false-break separation",
        "shock": "post-shock response separation versus normal conditions",
        "persistence": "persistent-state separation versus transient noise",
        "window": "window-effect separation versus adjacent bars",
        "transition": "regime-transition separation with stable onset timing",
        "flip": "flip confirmation versus whipsaw separation",
        "unwind": "unwind and rebound separation",
        "reversal": "reversal separation versus continuation",
        "relaxation": "relaxation follow-through versus renewed stress",
    }
    default_objective = objective_by_phase.get(phase, "forward response separation")
    objective_by_family = {
        "LIQUIDITY_DISLOCATION": (
            default_objective,
            "require stability across liquidity buckets, venues, and rolling windows while preserving minimum event counts",
        ),
        "POSITIONING_EXTREMES": (
            "post-extreme unwind or normalization separation",
            "require stability across assets, funding regimes, and rolling windows while preserving minimum event counts",
        ),
        "FORCED_FLOW_AND_EXHAUSTION": (
            "exhaustion-to-reversal separation after forced-flow shocks",
            "require stability across volatility regimes and rolling splits while preserving minimum event counts",
        ),
        "STATISTICAL_DISLOCATION": (
            "mean-reversion or continuation separation after statistical stretch",
            "require stability across assets and rolling windows while preserving minimum event counts",
        ),
        "VOLATILITY_TRANSITION": (
            "post-transition volatility and return separation",
            "require stability across volatility regimes and rolling windows while preserving minimum event counts",
        ),
        "TREND_STRUCTURE": (
            "continuation-versus-failure separation after trend signals",
            "require stability across volatility regimes and rolling windows while preserving minimum event counts",
        ),
        "REGIME_TRANSITION": (
            "persistent regime-state separation after transition onset",
            "require stable change-point timing across rolling windows while preserving minimum event counts",
        ),
        "INFORMATION_DESYNC": (
            "convergence or repricing separation after desynchronization",
            "require stability across assets, venues, and sessions while preserving minimum event counts",
        ),
        "TEMPORAL_STRUCTURE": (
            "session-specific response separation around scheduled windows",
            "require stability across calendar partitions and rolling windows while preserving minimum event counts",
        ),
        "EXECUTION_FRICTION": (
            "execution-friction separation across spread or slippage stress",
            "require stability across venues and liquidity buckets while preserving minimum event counts",
        ),
        "CROSS_ASSET_DESYNCHRONIZATION": (
            "cross-asset convergence separation after desynchronization shocks",
            "require stability across major pairs and rolling windows while preserving minimum event counts",
        ),
    }
    return objective_by_family.get(
        family,
        (
            default_objective,
            "require stability across rolling windows while preserving minimum event counts",
        ),
    )


def _infer_calibration_method(parameters: Mapping[str, Any], source_spec: Mapping[str, Any]) -> str:
    calibration = source_spec.get("calibration", {}) if isinstance(source_spec, Mapping) else {}
    if isinstance(parameters, Mapping):
        token = str(parameters.get("calibration_method", "")).strip()
        if token:
            return token
    if isinstance(calibration, Mapping):
        text = _coalesce_text(
            calibration.get("calibration_target"), calibration.get("stability_requirement")
        )
        if text:
            return text
    event_type = str(source_spec.get("event_type", "event")).strip().upper() or "EVENT"
    threshold_keys = _salient_threshold_keys(parameters)
    policy_keys = threshold_keys[:3] or ["detector thresholds"]
    objective, stability = _calibration_objective(source_spec)
    return (
        f"Calibrate {event_type} by tuning {', '.join(policy_keys)} to maximize {objective}; "
        f"{stability}."
    )


def _infer_regime_applicability(event_type: str, row: Mapping[str, Any]) -> str:
    role = _coalesce_text(row.get("operational_role"), default="trigger")
    regime = _coalesce_text(
        row.get("canonical_regime"),
        row.get("research_family"),
        row.get("canonical_family"),
        default="UNSPECIFIED",
    )
    return f"Primary use is as a {role.replace('_', ' ')} inside {regime.replace('_', ' ')} research and promotion flows."


def _infer_maturity_scores(contract: Mapping[str, Any]) -> dict[str, int]:
    tier = str(contract.get("tier", "D")).strip().upper() or "D"
    base = dict(_TIER_BASELINE_SCORES.get(tier, _TIER_BASELINE_SCORES["D"]))
    if contract.get("synthetic_coverage") == "covered":
        base["synthetic_coverage"] = min(5, base["synthetic_coverage"] + 1)
    if str(contract.get("evidence_mode", "")).strip().lower() == "direct":
        base["research_evidence_strength"] = min(5, base["research_evidence_strength"] + 1)
    if str(contract.get("runtime_category", "")).strip() == "research_only":
        base["deployment_suitability"] = min(base["deployment_suitability"], 1)
    return base


def _merged_row(event_type: str) -> dict[str, Any]:
    registry = get_domain_registry()
    event_def = registry.get_event(event_type)
    if event_def is None:
        raise KeyError(f"Unknown event_type: {event_type}")
    row = dict(event_def.raw)
    runtime_spec = _load_runtime_spec(event_type)

    for source in (runtime_spec,):
        for key, value in source.items():
            if key == "parameters":
                continue
            if value not in (None, "", [], {}):
                row[key] = value

    parameters: dict[str, Any] = {}
    if isinstance(row.get("parameters"), Mapping):
        parameters.update(dict(row["parameters"]))
    if isinstance(runtime_spec.get("parameters"), Mapping):
        parameters.update(dict(runtime_spec["parameters"]))
    row["parameters"] = parameters
    row.setdefault("event_type", event_type)
    row.setdefault("research_family", event_def.research_family)
    row.setdefault("canonical_family", event_def.canonical_family)
    row.setdefault("canonical_regime", event_def.canonical_regime)
    row.setdefault("research_only", event_def.research_only)
    row.setdefault("is_context_tag", event_def.is_context_tag)
    row.setdefault("is_strategy_construct", event_def.is_strategy_construct)
    row.setdefault("evidence_mode", event_def.evidence_mode)
    row.setdefault("phase", event_def.phase)
    row.setdefault("asset_scope", event_def.asset_scope)
    row.setdefault("venue_scope", event_def.venue_scope)
    row.setdefault("notes", event_def.notes)
    row.setdefault("tier", event_def.tier)
    row.setdefault("operational_role", event_def.operational_role)
    row.setdefault("deployment_disposition", event_def.deployment_disposition)
    row.setdefault("runtime_category", event_def.runtime_category)
    return row


def build_event_contract(event_type: str) -> dict[str, Any]:
    token = str(event_type).strip().upper()
    row = _merged_row(token)
    params = row.get("parameters", {}) if isinstance(row.get("parameters"), Mapping) else {}
    detector = row.get("detector", {}) if isinstance(row.get("detector"), Mapping) else {}
    calibration = row.get("calibration", {}) if isinstance(row.get("calibration"), Mapping) else {}
    expected_behavior = (
        row.get("expected_behavior", {})
        if isinstance(row.get("expected_behavior"), Mapping)
        else {}
    )
    semantics = row.get("semantics", {}) if isinstance(row.get("semantics"), Mapping) else {}
    detector_contract = None
    try:
        from project.events.registry import get_detector_contract

        detector_contract = get_detector_contract(token)
    except Exception:
        detector_contract = None

    runtime_category = str(row.get("runtime_category", "")).strip()
    if not runtime_category:
        if bool(row.get("research_only", False)):
            runtime_category = "research_only"
        elif bool(row.get("is_context_tag", False)):
            runtime_category = "active_runtime_event"
        elif bool(row.get("is_strategy_construct", False)):
            runtime_category = "research_only"
        else:
            runtime_category = "active_runtime_event"

    contract = {
        "event_type": token,
        "research_family": _coalesce_text(
            row.get("research_family"),
            row.get("canonical_family"),
            row.get("canonical_regime"),
        ),
        "canonical_family": _coalesce_text(
            row.get("research_family"),
            row.get("canonical_family"),
            row.get("canonical_regime"),
        ),
        "canonical_regime": _coalesce_text(
            row.get("canonical_regime"), row.get("canonical_family")
        ),
        "phase": _coalesce_text(row.get("phase")),
        "evidence_mode": _coalesce_text(row.get("evidence_mode"), default="unspecified"),
        "asset_scope": _coalesce_text(row.get("asset_scope"), default="single_asset"),
        "venue_scope": _coalesce_text(row.get("venue_scope"), default="single_venue"),
        "runtime_category": runtime_category,
        "description": _coalesce_text(
            row.get("description"),
            semantics.get("summary"),
            detector.get("signal_definition"),
            row.get("trigger"),
            row.get("notes"),
            default=_default_description(token, row),
        ),
        "causal_mechanism": _coalesce_text(
            row.get("causal_mechanism"),
            params.get("causal_mechanism"),
            default=_default_causal_mechanism(token, row),
        ),
        "observable_formula": _coalesce_text(
            row.get("observable_formula"),
            params.get("observable_formula"),
            detector.get("formula"),
            default="See detector contract and event parameters for the observable declaration.",
        ),
        "required_features": _coalesce_list(
            row.get("required_features"),
            params.get("required_features"),
            detector.get("required_columns"),
            detector_contract.required_columns if detector_contract is not None else (),
        )
        or ["timestamp"],
        "threshold_method": _infer_threshold_method(params, row),
        "calibration_method": _infer_calibration_method(params, row),
        "failure_modes": _flatten_failure_modes(
            row.get("failure_modes")
            or params.get("failure_modes")
            or calibration.get("failure_modes")
            or expected_behavior.get("false_positive_scenarios")
        )
        or [
            "Detector-specific false positives are controlled through thresholding, cooldown, and regime suppression."
        ],
        "regime_applicability": _coalesce_text(
            row.get("regime_applicability"),
            params.get("regime_applicability"),
            default=_infer_regime_applicability(token, row),
        ),
        "disabled_regimes": _flatten_disabled_regimes(
            row.get("disabled_regimes")
            or params.get("disabled_regimes")
            or expected_behavior.get("disabled_regimes")
        ),
        "expected_overlap": _flatten_disabled_regimes(
            row.get("expected_overlap")
            or params.get("expected_overlap")
            or expected_behavior.get("expected_overlap")
        )
        or [
            f"Can overlap with nearby {_coalesce_text(row.get('canonical_regime'), row.get('research_family'), row.get('canonical_family'), default='adjacent').replace('_', ' ').lower()} events when the same mechanism cascades across bars."
        ],
        "invalidation_rule": _coalesce_text(
            row.get("invalidation_rule"),
            params.get("invalidation_rule"),
            "; ".join(_flatten_disabled_regimes(expected_behavior.get("invalidation_conditions"))),
            default="Explicit invalidation is detector specific and documented in expected behavior.",
        ),
        "synthetic_coverage": _coalesce_text(
            row.get("synthetic_coverage"),
            params.get("synthetic_coverage"),
            default="uncovered",
        ),
        "deployment_disposition": _coalesce_text(
            row.get("deployment_disposition"),
            params.get("deployment_disposition"),
            row.get("disposition"),
            default="review_required",
        ),
        "operational_role": _coalesce_text(
            row.get("operational_role"),
            params.get("operational_role"),
            default="trigger",
        ),
        "tier": _coalesce_text(row.get("tier"), params.get("tier"), default="D").upper(),
        "source_spec_exists": bool(_load_runtime_spec(token)),
    }
    contract["maturity_scores"] = _infer_maturity_scores(contract)
    contract["raw"] = row
    return contract


@lru_cache(maxsize=1)
def load_active_event_contracts() -> dict[str, dict[str, Any]]:
    registry = get_domain_registry()
    return {event_type: build_event_contract(event_type) for event_type in registry.event_ids}


def load_research_motif_specs() -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not INTERACTION_SPEC_DIR.exists():
        return out
    for path in sorted(INTERACTION_SPEC_DIR.glob("*.yaml")):
        payload = load_yaml_path(path)
        if not isinstance(payload, Mapping):
            continue
        event_type = str(payload.get("event_type", path.stem)).strip().upper()
        if event_type:
            out[event_type] = dict(payload)
    return out


def validate_contract_completeness(
    contracts: Mapping[str, Mapping[str, Any]],
) -> dict[str, list[str]]:
    missing: dict[str, list[str]] = {}
    for event_type, contract in contracts.items():
        absent = []
        for field in REQUIRED_CONTRACT_FIELDS:
            value = contract.get(field)
            if value in (None, "", [], {}):
                absent.append(field)
        if absent:
            missing[str(event_type)] = absent
    return missing


def active_runtime_event_ids() -> tuple[str, ...]:
    contracts = load_active_event_contracts()
    return tuple(
        sorted(
            k for k, v in contracts.items() if v.get("runtime_category") == "active_runtime_event"
        )
    )


def filter_event_ids(
    *, tiers: Iterable[str] | None = None, roles: Iterable[str] | None = None
) -> tuple[str, ...]:
    tier_set = {str(t).strip().upper() for t in (tiers or []) if str(t).strip()}
    role_set = {str(r).strip().lower() for r in (roles or []) if str(r).strip()}
    out: list[str] = []
    for event_type, contract in load_active_event_contracts().items():
        if tier_set and str(contract.get("tier", "")).upper() not in tier_set:
            continue
        if role_set and str(contract.get("operational_role", "")).lower() not in role_set:
            continue
        out.append(event_type)
    return tuple(sorted(out))
