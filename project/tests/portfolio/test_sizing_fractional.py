from __future__ import annotations

from project.portfolio.sizing import calculate_target_notional


def _base_kwargs() -> dict:
    return {
        "event_score": 1.0,
        "expected_return_bps": 100.0,
        "expected_adverse_bps": 10.0,
        "vol_regime": 0.10,
        "liquidity_usd": 10_000_000.0,
        "portfolio_state": {
            "portfolio_value": 1_000_000.0,
            "gross_exposure": 0.0,
            "max_gross_leverage": 1.0,
            "target_vol": 0.10,
            "current_vol": 0.10,
            "bucket_exposures": {},
        },
        "symbol": "BTCUSDT",
    }


def test_default_fractional_kelly_caps_multiplier() -> None:
    result = calculate_target_notional(**_base_kwargs())

    assert result["max_kelly_multiplier"] == 0.5
    assert result["confidence_multiplier"] <= 0.5


def test_explicit_fuller_kelly_override_remains_available_for_research() -> None:
    base = calculate_target_notional(**_base_kwargs())
    override = calculate_target_notional(**_base_kwargs(), max_kelly_multiplier=5.0)

    assert override["confidence_multiplier"] > base["confidence_multiplier"]
    assert override["target_notional"] > base["target_notional"]


def test_portfolio_state_can_configure_kelly_cap() -> None:
    kwargs = _base_kwargs()
    kwargs["portfolio_state"] = dict(kwargs["portfolio_state"], max_kelly_multiplier=0.25)

    result = calculate_target_notional(**kwargs)

    assert result["max_kelly_multiplier"] == 0.25
    assert result["confidence_multiplier"] <= 0.25
