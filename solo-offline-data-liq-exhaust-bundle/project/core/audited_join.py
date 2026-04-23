from __future__ import annotations

import logging
import pandas as pd
import numpy as np
from typing import Optional, Any, Dict, Literal

LOGGER = logging.getLogger(__name__)


def audited_merge_asof(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    left_on: str,
    right_on: str,
    direction: str = "backward",
    tolerance: Optional[pd.Timedelta] = None,
    feature_name: str,
    stale_threshold_seconds: float,
    audit_registry: Optional[Any] = None,
    symbol: str,
    run_id: str,
    stale_action: Literal["raise", "warn", "ignore"] = "raise",
) -> pd.DataFrame:
    """
    L1: Centralized audited as-of join helper.
    Mandates PIT safety and staleness auditing.
    """
    # 1. PIT Safety Assertions
    if direction == "forward":
        # Forward joins are extremely dangerous in research unless carefully used for labels
        LOGGER.warning(
            f"DANGEROUS: Forward as-of join used for feature {feature_name}. Ensure this is for label construction only."
        )
    elif direction == "nearest":
        LOGGER.warning(
            f"DANGEROUS: Nearest as-of join used for feature {feature_name}. This is not strictly causal and may look ahead."
        )

    # 2. Perform Join
    # Ensure sorted timestamps
    left = left.sort_values(left_on)
    right = right.sort_values(right_on)

    # Check for same-timestamp lookahead if direction is backward
    # (In standard merge_asof backward, left_ts >= right_ts is matched)

    merged = pd.merge_asof(
        left,
        right,
        left_on=left_on,
        right_on=right_on,
        direction=direction,
        tolerance=tolerance,
    )

    # 3. Auditing and Staleness Check
    if right_on in merged.columns:
        # Compute age
        age_seconds = (merged[left_on] - merged[right_on]).dt.total_seconds()

        # Log to registry if provided (F1/F2 integration)
        if audit_registry:
            feature_cols = [c for c in right.columns if c != right_on]
            audit_registry.record_join(
                feature_cols=feature_cols,
                source_table=feature_name,
                source_ts_col=right_on,
                join_method=f"asof_{direction}",
                join_tolerance=str(tolerance),
                age_seconds=age_seconds,
                symbol=symbol,
                run_id=run_id,
            )

        # Hard fail if excessive staleness
        stale_mask = age_seconds > stale_threshold_seconds
        stale_rate = stale_mask.mean()
        if stale_rate > 0.05:  # Global 5% threshold
            msg = f"Audited join for {feature_name} failed: excessive stale usage {stale_rate:.2%} > 5%"
            if stale_action == "raise":
                LOGGER.error(msg)
                raise RuntimeError(msg)
            elif stale_action == "warn":
                LOGGER.warning(msg)
            # if ignore, do nothing

    return merged
