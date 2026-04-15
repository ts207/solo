from __future__ import annotations

from project.core.coercion import safe_float, safe_int, as_bool

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from project.specs.loader import (
    DEFAULT_OBJECTIVE_NAME_ENV_VAR,
    DEFAULT_OBJECTIVE_SPEC_ENV_VAR,
    DEFAULT_RETAIL_PROFILE_NAME_ENV_VAR,
    DEFAULT_RETAIL_PROFILES_SPEC_ENV_VAR,
)
from project.spec_registry import load_objective_spec, load_retail_profile

LOW_CAPITAL_REQUIRED_FIELDS = (
    "account_equity_usd",
    "max_position_notional_usd",
    "min_position_notional_usd",
    "max_leverage",
    "max_trades_per_day",
    "max_turnover_per_day",
    "fee_tier",
    "slippage_model_baseline_bps",
    "stress_cost_multiplier_2x",
    "stress_cost_multiplier_3x",
    "spread_model",
    "entry_delay_bars_default",
    "entry_delay_bars_stress",
    "max_drawdown_pct",
    "max_daily_loss_pct",
    "stop_trading_rule",
    "bar_timestamp_semantics",
    "signal_snap_side",
    "active_range_semantics",
)

LOW_CAPITAL_POSITIVE_FIELDS = (
    "account_equity_usd",
    "max_position_notional_usd",
    "min_position_notional_usd",
    "max_leverage",
    "max_trades_per_day",
    "max_turnover_per_day",
    "slippage_model_baseline_bps",
    "stress_cost_multiplier_2x",
    "stress_cost_multiplier_3x",
    "entry_delay_bars_default",
    "entry_delay_bars_stress",
)

LOW_CAPITAL_STRICT_ENUMS = {
    "bar_timestamp_semantics": "open_time",
    "signal_snap_side": "left",
    "active_range_semantics": "[start,end)",
}


@dataclass(frozen=True)
class ObjectiveProfileContract:
    objective_name: str
    objective_id: str
    objective_spec_path: str
    objective_spec_hash: str
    objective_hard_gates: Dict[str, Any]
    objective_constraints: Dict[str, Any]
    retail_profile_name: str
    retail_profile_spec_path: str
    retail_profile_spec_hash: str
    retail_profile_config: Dict[str, Any]
    min_trade_count: int
    min_oos_sign_consistency: float
    min_tob_coverage: float
    min_net_expectancy_bps: float
    max_fee_plus_slippage_bps: Optional[float]
    max_daily_turnover_multiple: Optional[float]
    max_concurrent_positions: Optional[int]
    target_account_size_usd: Optional[float]
    max_initial_margin_pct: Optional[float]
    max_leverage: Optional[float]
    max_position_notional_usd: Optional[float]
    capital_budget_usd: Optional[float]
    effective_per_position_notional_cap_usd: Optional[float]
    require_retail_viability: bool
    forbid_fallback_in_deploy_mode: bool
    require_low_capital_contract: bool
    low_capital_contract: Dict[str, Any]

    def as_dict(self) -> Dict[str, Any]:
        return {
            "objective_name": self.objective_name,
            "objective_id": self.objective_id,
            "objective_spec_path": self.objective_spec_path,
            "objective_spec_hash": self.objective_spec_hash,
            "objective_hard_gates": dict(self.objective_hard_gates),
            "objective_constraints": dict(self.objective_constraints),
            "retail_profile_name": self.retail_profile_name,
            "retail_profile_spec_path": self.retail_profile_spec_path,
            "retail_profile_spec_hash": self.retail_profile_spec_hash,
            "retail_profile_config": dict(self.retail_profile_config),
            "min_trade_count": int(self.min_trade_count),
            "min_oos_sign_consistency": float(self.min_oos_sign_consistency),
            "min_tob_coverage": float(self.min_tob_coverage),
            "min_net_expectancy_bps": float(self.min_net_expectancy_bps),
            "max_fee_plus_slippage_bps": self.max_fee_plus_slippage_bps,
            "max_daily_turnover_multiple": self.max_daily_turnover_multiple,
            "max_concurrent_positions": self.max_concurrent_positions,
            "target_account_size_usd": self.target_account_size_usd,
            "max_initial_margin_pct": self.max_initial_margin_pct,
            "max_leverage": self.max_leverage,
            "max_position_notional_usd": self.max_position_notional_usd,
            "capital_budget_usd": self.capital_budget_usd,
            "effective_per_position_notional_cap_usd": self.effective_per_position_notional_cap_usd,
            "require_retail_viability": bool(self.require_retail_viability),
            "forbid_fallback_in_deploy_mode": bool(self.forbid_fallback_in_deploy_mode),
            "require_low_capital_contract": bool(self.require_low_capital_contract),
            "low_capital_contract": dict(self.low_capital_contract),
        }


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _safe_positive_float(value: Any) -> Optional[float]:
    out = safe_float(value)
    if out is None or out <= 0.0:
        return None
    return float(out)


def _safe_positive_int(value: Any) -> Optional[int]:
    out = safe_float(value)
    if out is None:
        return None
    val = int(out)
    if val <= 0:
        return None
    return val


def _is_missing_scalar(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def _extract_low_capital_contract(profile_cfg: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(profile_cfg, dict):
        return {}
    out = {}
    for key in LOW_CAPITAL_REQUIRED_FIELDS:
        if key in profile_cfg:
            out[key] = profile_cfg.get(key)
    # Optional low-capital hints if present.
    for key in (
        "max_holding_bars",
        "min_notional_safety_margin",
        "liquidity_adv_min_usd",
        "spread_ceiling_bps",
        "require_top_book_coverage",
    ):
        if key in profile_cfg:
            out[key] = profile_cfg.get(key)
    return out


def assert_low_capital_contract(
    contract: ObjectiveProfileContract,
    *,
    stage_name: str,
) -> Dict[str, Any]:
    required = bool(getattr(contract, "require_low_capital_contract", False))
    profile_name = str(getattr(contract, "retail_profile_name", "unknown")).strip() or "unknown"
    if not required:
        return {}

    raw_cfg = getattr(contract, "low_capital_contract", None)
    if not isinstance(raw_cfg, dict) or not raw_cfg:
        retail_cfg = getattr(contract, "retail_profile_config", {})
        raw_cfg = _extract_low_capital_contract(retail_cfg if isinstance(retail_cfg, dict) else {})
    cfg: Dict[str, Any] = dict(raw_cfg or {})

    missing = [k for k in LOW_CAPITAL_REQUIRED_FIELDS if _is_missing_scalar(cfg.get(k))]
    if missing:
        raise ValueError(
            f"{stage_name}: low-capital contract missing required fields for profile={profile_name}: "
            + ", ".join(sorted(missing))
        )

    invalid_numeric: list[str] = []
    for key in LOW_CAPITAL_POSITIVE_FIELDS:
        val = _safe_positive_float(cfg.get(key))
        if val is None:
            invalid_numeric.append(key)
    if invalid_numeric:
        raise ValueError(
            f"{stage_name}: low-capital contract has non-positive/invalid numeric fields for profile={profile_name}: "
            + ", ".join(sorted(invalid_numeric))
        )

    semantic_errors: list[str] = []
    for key, expected in LOW_CAPITAL_STRICT_ENUMS.items():
        actual = str(cfg.get(key, "")).strip()
        if actual != expected:
            semantic_errors.append(f"{key}={actual!r} expected={expected!r}")
    if semantic_errors:
        raise ValueError(
            f"{stage_name}: low-capital bar semantics contract violation for profile={profile_name}: "
            + "; ".join(semantic_errors)
        )

    return cfg


def _load_run_manifest(data_root: Path, run_id: str) -> Dict[str, Any]:
    path = data_root / "runs" / str(run_id) / "run_manifest.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve_source_path(
    *,
    raw_path: str,
    base_root: Path,
    field_name: str,
    source_name: str,
    require_exists: bool,
    require_within_repo: bool,
) -> str:
    candidate = Path(str(raw_path).strip())
    if not candidate.is_absolute():
        candidate = (base_root / candidate).resolve()
    else:
        candidate = candidate.resolve()

    repo_root = base_root.resolve()
    if require_within_repo:
        try:
            candidate.relative_to(repo_root)
        except ValueError as exc:
            raise ValueError(
                f"{source_name} {field_name} must stay within active repo root {repo_root}: {candidate}"
            ) from exc

    if require_exists and not candidate.exists():
        raise FileNotFoundError(f"{source_name} {field_name} missing: {candidate}")

    return str(candidate)


def _resolve_objective_name(*, explicit: str | None, run_manifest: Dict[str, Any]) -> str:
    name = str(explicit or "").strip()
    if name:
        return name
    from_manifest = str(run_manifest.get("objective_name", "")).strip()
    if from_manifest:
        return from_manifest
    from_env = str(os.getenv(DEFAULT_OBJECTIVE_NAME_ENV_VAR, "")).strip()
    return from_env or "retail_profitability"


def _resolve_retail_profile_name(*, explicit: str | None, run_manifest: Dict[str, Any]) -> str:
    name = str(explicit or "").strip()
    if name:
        return name
    from_manifest = str(run_manifest.get("retail_profile_name", "")).strip()
    if from_manifest:
        return from_manifest
    from_env = str(os.getenv(DEFAULT_RETAIL_PROFILE_NAME_ENV_VAR, "")).strip()
    return from_env or "capital_constrained"


def _resolve_objective_spec_path(
    *,
    project_root: Path,
    objective_name: str,
    explicit: str | None,
    run_manifest: Dict[str, Any],
) -> str:
    repo_root = project_root.parent
    if explicit and str(explicit).strip():
        return _resolve_source_path(
            raw_path=str(explicit).strip(),
            base_root=repo_root,
            field_name="objective_spec_path",
            source_name="explicit",
            require_exists=False,
            require_within_repo=False,
        )
    from_manifest = str(run_manifest.get("objective_spec_path", "")).strip()
    if from_manifest:
        return _resolve_source_path(
            raw_path=from_manifest,
            base_root=repo_root,
            field_name="objective_spec_path",
            source_name="run manifest",
            require_exists=True,
            require_within_repo=True,
        )
    from_env = str(os.getenv(DEFAULT_OBJECTIVE_SPEC_ENV_VAR, "")).strip()
    if from_env:
        return _resolve_source_path(
            raw_path=from_env,
            base_root=repo_root,
            field_name="objective_spec_path",
            source_name=DEFAULT_OBJECTIVE_SPEC_ENV_VAR,
            require_exists=True,
            require_within_repo=False,
        )
    return str((repo_root / "spec" / "objectives" / f"{objective_name}.yaml").resolve())


def _resolve_retail_profiles_spec_path(
    *,
    project_root: Path,
    explicit: str | None,
    run_manifest: Dict[str, Any],
) -> str:
    repo_root = project_root.parent
    if explicit and str(explicit).strip():
        return _resolve_source_path(
            raw_path=str(explicit).strip(),
            base_root=repo_root,
            field_name="retail_profile_spec_path",
            source_name="explicit",
            require_exists=False,
            require_within_repo=False,
        )
    from_manifest = str(run_manifest.get("retail_profile_spec_path", "")).strip()
    if from_manifest:
        return _resolve_source_path(
            raw_path=from_manifest,
            base_root=repo_root,
            field_name="retail_profile_spec_path",
            source_name="run manifest",
            require_exists=True,
            require_within_repo=True,
        )
    from_env = str(os.getenv(DEFAULT_RETAIL_PROFILES_SPEC_ENV_VAR, "")).strip()
    if from_env:
        return _resolve_source_path(
            raw_path=from_env,
            base_root=repo_root,
            field_name="retail_profile_spec_path",
            source_name=DEFAULT_RETAIL_PROFILES_SPEC_ENV_VAR,
            require_exists=True,
            require_within_repo=False,
        )
    return str((project_root / "configs" / "retail_profiles.yaml").resolve())


def resolve_objective_profile_contract(
    *,
    project_root: Path,
    data_root: Path,
    run_id: str,
    objective_name: str | None = None,
    objective_spec_path: str | None = None,
    retail_profile_name: str | None = None,
    retail_profiles_spec_path: str | None = None,
    required: bool = True,
) -> ObjectiveProfileContract:
    run_manifest = _load_run_manifest(data_root, run_id)
    resolved_objective_name = _resolve_objective_name(
        explicit=objective_name, run_manifest=run_manifest
    )
    resolved_retail_profile_name = _resolve_retail_profile_name(
        explicit=retail_profile_name, run_manifest=run_manifest
    )
    resolved_objective_spec_path = _resolve_objective_spec_path(
        project_root=project_root,
        objective_name=resolved_objective_name,
        explicit=objective_spec_path,
        run_manifest=run_manifest,
    )
    resolved_retail_profiles_spec_path = _resolve_retail_profiles_spec_path(
        project_root=project_root,
        explicit=retail_profiles_spec_path,
        run_manifest=run_manifest,
    )

    objective_spec = load_objective_spec(
        objective_name=resolved_objective_name,
        explicit_path=resolved_objective_spec_path,
        required=required,
    )
    retail_profile = load_retail_profile(
        profile_name=resolved_retail_profile_name,
        explicit_path=resolved_retail_profiles_spec_path,
        required=required,
    )

    objective_hard_gates = dict(objective_spec.get("hard_gates", {}))
    objective_constraints = dict(objective_spec.get("constraints", {}))
    retail_profile_config = dict(retail_profile)

    min_trade_count = int(safe_float(objective_hard_gates.get("min_trade_count")) or 0)
    min_oos_sign_consistency = float(
        safe_float(objective_hard_gates.get("min_oos_sign_consistency")) or 0.0
    )
    min_tob_coverage = float(
        safe_float(retail_profile_config.get("require_top_book_coverage")) or 0.0
    )
    min_net_expectancy_bps = float(
        safe_float(retail_profile_config.get("min_net_expectancy_bps")) or 0.0
    )
    max_fee_plus_slippage_bps = _safe_positive_float(
        retail_profile_config.get("max_fee_plus_slippage_bps")
    )
    max_daily_turnover_multiple = _safe_positive_float(
        retail_profile_config.get("max_daily_turnover_multiple")
    )
    max_concurrent_positions = _safe_positive_int(
        retail_profile_config.get("max_concurrent_positions")
    )
    target_account_size_usd = _safe_positive_float(
        retail_profile_config.get("target_account_size_usd")
    )
    max_initial_margin_pct = _safe_positive_float(
        retail_profile_config.get("max_initial_margin_pct")
    )
    max_leverage = _safe_positive_float(retail_profile_config.get("max_leverage"))
    max_position_notional_usd = _safe_positive_float(
        retail_profile_config.get("max_position_notional_usd")
    )

    capital_budget_usd: Optional[float] = None
    if (
        target_account_size_usd is not None
        and max_initial_margin_pct is not None
        and max_leverage is not None
    ):
        capital_budget_usd = float(target_account_size_usd * max_initial_margin_pct * max_leverage)

    effective_per_position_notional_cap_usd: Optional[float] = max_position_notional_usd
    if capital_budget_usd is not None and max_concurrent_positions:
        per_slot_budget = float(capital_budget_usd) / float(max_concurrent_positions)
        if effective_per_position_notional_cap_usd is None:
            effective_per_position_notional_cap_usd = per_slot_budget
        else:
            effective_per_position_notional_cap_usd = min(
                float(effective_per_position_notional_cap_usd), per_slot_budget
            )

    require_retail_viability = bool(objective_constraints.get("require_retail_viability", False))
    forbid_fallback_in_deploy_mode = bool(
        objective_constraints.get("forbid_fallback_in_deploy_mode", False)
    )
    require_low_capital_contract = bool(
        objective_constraints.get("require_low_capital_contract", False)
    )
    low_capital_contract = _extract_low_capital_contract(retail_profile_config)
    objective_hash = _sha256_text(json.dumps(objective_spec, sort_keys=True))
    profile_hash = _sha256_text(json.dumps(retail_profile, sort_keys=True))

    return ObjectiveProfileContract(
        objective_name=resolved_objective_name,
        objective_id=str(objective_spec.get("id", resolved_objective_name)),
        objective_spec_path=resolved_objective_spec_path,
        objective_spec_hash=objective_hash,
        objective_hard_gates=objective_hard_gates,
        objective_constraints=objective_constraints,
        retail_profile_name=resolved_retail_profile_name,
        retail_profile_spec_path=resolved_retail_profiles_spec_path,
        retail_profile_spec_hash=profile_hash,
        retail_profile_config=retail_profile_config,
        min_trade_count=min_trade_count,
        min_oos_sign_consistency=min_oos_sign_consistency,
        min_tob_coverage=min_tob_coverage,
        min_net_expectancy_bps=min_net_expectancy_bps,
        max_fee_plus_slippage_bps=max_fee_plus_slippage_bps,
        max_daily_turnover_multiple=max_daily_turnover_multiple,
        max_concurrent_positions=max_concurrent_positions,
        target_account_size_usd=target_account_size_usd,
        max_initial_margin_pct=max_initial_margin_pct,
        max_leverage=max_leverage,
        max_position_notional_usd=max_position_notional_usd,
        capital_budget_usd=capital_budget_usd,
        effective_per_position_notional_cap_usd=effective_per_position_notional_cap_usd,
        require_retail_viability=require_retail_viability,
        forbid_fallback_in_deploy_mode=forbid_fallback_in_deploy_mode,
        require_low_capital_contract=require_low_capital_contract,
        low_capital_contract=low_capital_contract,
    )
