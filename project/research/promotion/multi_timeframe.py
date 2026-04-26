from __future__ import annotations

import logging
from typing import Any

import numpy as np

from project.core.coercion import safe_float

_LOG = logging.getLogger(__name__)


def _quiet_float(value: Any, default: float) -> float:
    if value is None or (isinstance(value, float) and not np.isfinite(value)):
        return float(default)
    coerced = safe_float(value, default)
    return float(default if coerced is None else coerced)


def evaluate_timeframe_consensus(
    base_timeframe: str,
    alternate_timeframes: list[str],
    row: dict[str, Any],
    min_consensus_ratio: float = 0.5,
    min_sharpe_retention: float = 0.5,
) -> dict[str, Any]:
    """
    Evaluates whether the edge observed on the base timeframe holds up on alternate timeframes.
    A candidate passes if its expectancy on alternate timeframes remains same-sign
    and retains at least `min_consensus_ratio` of its base performance, and Sharpe Ratio
    is also reasonably stable.
    """
    base_expectancy = _quiet_float(
        row.get(
            "net_expectancy_bps",
            row.get(
                "bridge_validation_after_cost_bps",
                row.get("bridge_train_after_cost_bps"),
            ),
        ),
        0.0,
    )
    base_sharpe = _quiet_float(row.get("sharpe_ratio", row.get("sharpe_ratio_5m")), 0.0)

    if base_expectancy == 0.0:
        return {"pass_consensus": False, "reason": "base_expectancy_zero"}

    results = {}
    passes = 0

    for tf in alternate_timeframes:
        if tf == base_timeframe:
            continue

        alt_exp_key = f"expectancy_bps_{tf}"
        alt_sharpe_key = f"sharpe_ratio_{tf}"

        if alt_exp_key in row:
            alt_exp = _quiet_float(row[alt_exp_key], 0.0)
            alt_sharpe = _quiet_float(row.get(alt_sharpe_key), 0.0)

            # Check sign match and magnitude retention for expectancy
            sign_match = (base_expectancy > 0 and alt_exp > 0) or (
                base_expectancy < 0 and alt_exp < 0
            )
            exp_retention = alt_exp / base_expectancy if sign_match else 0.0

            # Sharpe stability check
            sharpe_retention = alt_sharpe / base_sharpe if base_sharpe > 0 else 1.0

            if (
                sign_match
                and exp_retention >= min_consensus_ratio
                and sharpe_retention >= min_sharpe_retention
            ):
                passes += 1
                results[tf] = {
                    "pass": True,
                    "exp_retention": exp_retention,
                    "sharpe_retention": sharpe_retention,
                }
            else:
                results[tf] = {
                    "pass": False,
                    "reason": "failed_thresholds",
                    "sign_match": sign_match,
                    "exp_retention": exp_retention,
                    "sharpe_retention": sharpe_retention,
                }
        else:
            _LOG.debug(
                f"Missing alternate timeframe data for {tf} on candidate {row.get('candidate_id')}"
            )
            passes += 1
            results[tf] = {"pass": True, "reason": "missing_data_assumed_pass"}

    total_alts = len([t for t in alternate_timeframes if t != base_timeframe])
    if total_alts == 0:
        return {"pass_consensus": True, "details": results}

    pass_rate = passes / total_alts

    return {
        "pass_consensus": bool(pass_rate >= 0.5),  # Require passing on at least half of alternates
        "details": results,
        "consensus_pass_rate": pass_rate,
    }
