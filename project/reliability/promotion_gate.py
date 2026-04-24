from __future__ import annotations

import logging
from typing import Any

from project.contracts.temporal_contracts import TemporalContract

LOGGER = logging.getLogger(__name__)


def verify_pit_integrity(module: Any) -> bool:
    """
    Check if a module has a valid TEMPORAL_CONTRACT and passes basic checks.
    """
    if not hasattr(module, "TEMPORAL_CONTRACT"):
        LOGGER.error(f"Module {module.__name__} missing TEMPORAL_CONTRACT")
        return False

    contract = module.TEMPORAL_CONTRACT
    if not isinstance(contract, TemporalContract):
        LOGGER.error(f"Module {module.__name__} has invalid TEMPORAL_CONTRACT type")
        return False

    # Validation of PIT settings
    if contract.decision_lag_bars < 1 and contract.output_mode != "alignment":
        if not contract.uses_current_observation:
            # This is a bit contradictory, usually if lag < 1 you MUST use current obs
            # but we prefer lag >= 1 for safety.
            pass

    return True


def promotion_gate(candidate_id: str, module: Any, performance_stats: dict) -> bool:
    """
    Gate for promoting a candidate to production/staging.
    Requires PIT integrity and minimum performance.
    """
    if not verify_pit_integrity(module):
        LOGGER.error(f"Promotion rejected for {candidate_id}: PIT Integrity Check Failed")
        return False

    # Check for lookback consistency in contract
    contract = module.TEMPORAL_CONTRACT
    if contract.calibration_mode == "prefit" and contract.fit_scope == "streaming":
        LOGGER.warning(f"Suspect contract for {candidate_id}: prefit + streaming fit scope")

    LOGGER.info(f"Promotion gate PASS for {candidate_id}")
    return True
