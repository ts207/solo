from __future__ import annotations

import pytest

from project.core.datasets import resolve_dataset_id
from project.core.exceptions import ContractViolationError


def test_dataset_resolver_maps_ohlcv_contracts() -> None:
    perp_1m = resolve_dataset_id("perp_ohlcv_1m")
    perp_5m = resolve_dataset_id("perp_ohlcv_5m")
    spot_15m = resolve_dataset_id("spot_ohlcv_15m")

    assert perp_1m.artifact_token == "raw.perp.ohlcv_1m"
    assert perp_1m.dataset_name == "bars_1m"
    assert perp_5m.artifact_token == "raw.perp.ohlcv_5m"
    assert spot_15m.artifact_token == "raw.spot.ohlcv_15m"
    assert spot_15m.dataset_name == "bars_15m"


def test_dataset_resolver_rejects_invalid_identifiers() -> None:
    with pytest.raises(ContractViolationError):
        resolve_dataset_id("perp_ohlcv_2m")
    with pytest.raises(ContractViolationError):
        resolve_dataset_id("unknown_dataset")
