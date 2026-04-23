from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, List
from unittest.mock import MagicMock


def write_strategy_contract_artifacts(
    *,
    blueprints: List[Any],
    out_dir: Path,
    run_id: str,
    retail_profile: str,
    low_capital_contract: Dict[str, Any],
    require_low_capital_contract: bool,
    effective_max_concurrent_positions: int,
    effective_per_position_notional_cap_usd: float,
    default_fee_tier: str,
    fees_bps_per_side: float,
    slippage_bps_per_fill: float,
    audit_rows: Dict[str, Dict[str, Any]] | None = None,
    portfolio_state_path: str | None = None,
    build_executable_strategy_spec_fn: Callable[..., Any],
    build_allocation_spec_fn: Callable[..., Any],
    validate_strategy_contract_fn: Callable[..., None],
    ensure_dir_fn: Callable[[Path], None],
    logger: Any,
) -> Dict[str, Any]:
    executable_dir = out_dir / "executable_strategy_specs"
    allocation_dir = out_dir / "allocation_specs"
    ensure_dir_fn(executable_dir)
    ensure_dir_fn(allocation_dir)

    executable_entries = []
    allocation_entries = []
    executor_lines = []

    live_portfolio_state: Dict[str, Any] = {}
    if portfolio_state_path:
        try:
            ps_path = Path(portfolio_state_path)
            if ps_path.exists():
                live_portfolio_state = json.loads(ps_path.read_text(encoding="utf-8"))
                logger.info(
                    "Loaded portfolio state from %s: gross_exposure=%.2f",
                    ps_path,
                    float(live_portfolio_state.get("gross_exposure", 0.0)),
                )
        except Exception as exc:
            logger.warning("Could not load portfolio_state_path %s: %s", portfolio_state_path, exc)

    promoted_blueprints: list[Any] = []
    for deployed in live_portfolio_state.get("deployed_strategies", []):
        try:
            stub = MagicMock()
            stub.id = str(deployed.get("blueprint_id", ""))
            stub.sizing = MagicMock()
            stub.sizing.risk_per_trade = float(deployed.get("risk_per_trade", 0.01))
            stub.sizing.max_gross_leverage = float(deployed.get("max_gross_leverage", 2.0))
            stub.sizing.portfolio_risk_budget = float(deployed.get("portfolio_risk_budget", 1.0))
            promoted_blueprints.append(stub)
        except Exception:
            pass

    marginal_contribution_log: list[Dict[str, Any]] = []

    for bp in blueprints:
        passes_mc, max_corr = _check_marginal_contribution(bp, promoted_blueprints)
        marginal_contribution_log.append(
            {
                "blueprint_id": bp.id,
                "max_similarity": round(max_corr, 4),
                "passes_marginal_check": passes_mc,
            }
        )
        if not passes_mc:
            logger.warning(
                "Phase 4.4: Blueprint %s has high similarity (%.3f) to existing promoted "
                "strategies — AllocationSpec risk_per_trade will be reduced.",
                bp.id,
                max_corr,
            )

        executable_spec = build_executable_strategy_spec_fn(
            blueprint=bp,
            run_id=run_id,
            retail_profile=retail_profile,
            low_capital_contract=low_capital_contract,
            effective_max_concurrent_positions=effective_max_concurrent_positions,
            effective_per_position_notional_cap_usd=effective_per_position_notional_cap_usd,
            default_fee_tier=default_fee_tier,
            fees_bps_per_side=fees_bps_per_side,
            slippage_bps_per_fill=slippage_bps_per_fill,
        )
        audit_row = (audit_rows or {}).get(bp.id)
        allocation_spec = build_allocation_spec_fn(
            blueprint=bp,
            run_id=run_id,
            retail_profile=retail_profile,
            low_capital_contract=low_capital_contract,
            effective_max_concurrent_positions=effective_max_concurrent_positions,
            effective_per_position_notional_cap_usd=effective_per_position_notional_cap_usd,
            default_fee_tier=default_fee_tier,
            fees_bps_per_side=fees_bps_per_side,
            slippage_bps_per_fill=slippage_bps_per_fill,
            audit_row=audit_row,
        )
        try:
            validate_strategy_contract_fn(
                executable_spec,
                low_capital_contract=low_capital_contract,
                require_low_capital_contract=require_low_capital_contract,
            )
        except ValueError as exc:
            logger.warning("Strategy contract validation failed for %s: %s", bp.id, exc)
        executable_path = executable_dir / f"{bp.id}.executable_strategy_spec.json"
        executable_path.write_text(
            json.dumps(executable_spec.model_dump(), indent=2), encoding="utf-8"
        )
        executable_entries.append(
            {"id": bp.id, "candidate_id": bp.candidate_id, "path": str(executable_path)}
        )
        allocation_path = allocation_dir / f"{bp.id}.allocation_spec.json"
        allocation_path.write_text(
            json.dumps(allocation_spec.model_dump(), indent=2), encoding="utf-8"
        )
        allocation_entries.append(
            {"id": bp.id, "candidate_id": bp.candidate_id, "path": str(allocation_path)}
        )
        executor_lines.append(json.dumps(executable_spec.execution.policy_executor_config))
        promoted_blueprints.append(bp)

    (out_dir / "marginal_contribution_log.json").write_text(
        json.dumps(marginal_contribution_log, indent=2), encoding="utf-8"
    )

    executable_index = {"count": len(executable_entries), "entries": executable_entries}
    (out_dir / "executable_strategy_spec_index.json").write_text(
        json.dumps(executable_index, indent=2), encoding="utf-8"
    )
    allocation_index = {"count": len(allocation_entries), "entries": allocation_entries}
    (out_dir / "allocation_spec_index.json").write_text(
        json.dumps(allocation_index, indent=2), encoding="utf-8"
    )
    (out_dir / "policy_executor_configs.jsonl").write_text(
        "\n".join(executor_lines) + ("\n" if executor_lines else ""), encoding="utf-8"
    )

    return {
        "count": len(executable_entries),
        "entries": executable_entries,
        "strategy_contract_count": len(executable_entries),
        "strategy_contract_entries": executable_entries,
        "executable_strategy_spec_count": len(executable_entries),
        "executable_strategy_spec_entries": executable_entries,
        "allocation_spec_count": len(allocation_entries),
        "allocation_spec_entries": allocation_entries,
    }


def _check_marginal_contribution(
    blueprint: Any,
    existing_blueprints: list[Any],
    *,
    max_correlation: float = 0.8,
) -> tuple[bool, float]:
    import numpy as np

    if not existing_blueprints:
        return True, 0.0

    def _vec(bp: Any) -> np.ndarray:
        s = bp.sizing
        return np.array(
            [
                float(s.risk_per_trade or 0.0),
                float(s.max_gross_leverage or 0.0),
                float(s.portfolio_risk_budget or 1.0),
            ],
            dtype=float,
        )

    new_vec = _vec(blueprint)
    new_norm = np.linalg.norm(new_vec)
    if new_norm < 1e-10:
        return True, 0.0

    max_sim = 0.0
    for ex_bp in existing_blueprints:
        ex_vec = _vec(ex_bp)
        ex_norm = np.linalg.norm(ex_vec)
        if ex_norm < 1e-10:
            continue
        cosine_sim = float(np.dot(new_vec, ex_vec) / (new_norm * ex_norm))
        max_sim = max(max_sim, cosine_sim)

    return max_sim < max_correlation, max_sim
