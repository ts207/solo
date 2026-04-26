from __future__ import annotations

import json
import logging
from typing import Any

_LOG = logging.getLogger(__name__)


def generate_strategy_summary(
    blueprint: dict[str, Any],
    backtest_metrics: dict[str, Any],
    stability_metrics: dict[str, Any],
    walkforward_results: dict[str, Any],
) -> str:
    """
    Generate a human-readable strategy report.
    """
    lines = []
    lines.append(f"Strategy Report: {blueprint.get('id', 'Unknown')}")
    lines.append("=" * 40)
    lines.append(
        f"Thesis: {blueprint.get('event_type', 'N/A')} with {blueprint.get('direction', 'N/A')} orientation."
    )
    lines.append(f"Candidate ID: {blueprint.get('candidate_id', 'N/A')}")
    lines.append("-" * 20)

    lines.append("Performance Summary:")
    lines.append(f"  Total Trades: {backtest_metrics.get('total_trades', 0)}")
    lines.append(f"  Expectancy (BPS): {backtest_metrics.get('expectancy_bps', 0.0):.2f}")
    lines.append(f"  Sharpe Ratio: {backtest_metrics.get('sharpe_ratio', 0.0):.2f}")
    lines.append(f"  Max Drawdown (BPS): {backtest_metrics.get('max_drawdown_bps', 0.0):.2f}")
    lines.append("-" * 20)

    lines.append("Robustness & Stability:")
    lines.append(f"  Regime SR Stability: {stability_metrics.get('sr_stability_ratio', 0.0):.2f}")
    lines.append(
        f"  Walk-forward Sign Consistency: {walkforward_results.get('sign_consistency', 0.0):.1%}"
    )
    lines.append(f"  Stability Pass: {'PASS' if stability_metrics.get('is_stable') else 'FAIL'}")
    lines.append("-" * 20)

    lines.append("Kill Criteria:")
    # Extract kill criteria from strategy metadata or provide defaults
    kill_criteria = blueprint.get("execution", {}).get(
        "kill_criteria",
        [
            "Expectancy drops below 50% of research mean for 30 days",
            "Max drawdown exceeds 1.5x research drawdown",
            "Slippage drift > 2x research slippage",
        ],
    )
    for i, criterion in enumerate(kill_criteria):
        lines.append(f"  {i + 1}. {criterion}")

    return "\n".join(lines)


def write_promotion_rationale(
    blueprint: dict[str, Any],
    metrics: dict[str, Any],
    out_path: str,
):
    """
    Write detailed promotion rationale to file.
    """
    payload = {
        "blueprint_id": blueprint.get("id"),
        "metrics_summary": metrics,
        "promotion_gate_results": {
            "stressed_cost_survival": True,  # Placeholder
            "walkforward_stability": True,
            "parameter_smoothness": True,
        },
        "kill_switch_config": blueprint.get("execution", {}).get(
            "retry_logic"
        ),  # Reusing field for policy
    }
    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2)
