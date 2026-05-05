from datetime import datetime, timezone

import pytest

from project.core.run_id import new_run_id, normalize_run_id_prefix


def test_normalize_run_id_prefix() -> None:
    assert normalize_run_id_prefix("Custom Shock / BTC") == "custom_shock_btc"


def test_new_run_id_contains_prefix_and_timestamp() -> None:
    now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
    rid = new_run_id(prefix="Custom Shock", now=now, entropy_bytes=1)
    assert rid.startswith("custom_shock_20260501_120000_")


def test_empty_prefix_rejected() -> None:
    with pytest.raises(ValueError):
        normalize_run_id_prefix("!!!")
