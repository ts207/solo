from __future__ import annotations

from project.live.risk import RuntimeRiskCaps


def test_runtime_risk_caps_are_safe_by_default() -> None:
    caps = RuntimeRiskCaps()
    assert caps.max_gross_exposure == 0.0
    assert caps.max_symbol_exposure == 0.0
    assert caps.max_family_exposure == 0.0
    assert caps.max_order_notional == 0.0
    assert caps.max_active_theses == 0
