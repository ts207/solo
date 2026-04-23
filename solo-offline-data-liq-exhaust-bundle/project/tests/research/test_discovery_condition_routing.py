from __future__ import annotations

from project.research.discovery import condition_routing


def test_vol_regime_routes_to_runtime():
    cond, source = condition_routing("vol_regime_low")
    assert cond == "vol_regime_low"
    assert source == "runtime"


def test_carry_pos_routes_to_runtime():
    cond, source = condition_routing("carry_pos")
    assert cond == "carry_pos"
    assert source == "runtime"


def test_ms_trend_routes_to_runtime():
    cond, source = condition_routing("ms_trend_state_0.0")
    assert cond == "ms_trend_state_0.0"
    assert source == "runtime"


def test_severity_bucket_routes_to_all():
    cond, source = condition_routing("severity_bucket_top_20pct")
    assert cond == "all"
    assert source == "bucket_non_runtime"


def test_all_routes_to_unconditional():
    cond, source = condition_routing("all")
    assert cond == "all"
    assert source == "unconditional"


def test_empty_routes_to_unconditional():
    cond, source = condition_routing("")
    assert cond == "all"
    assert source == "unconditional"


def test_unknown_strict_blocks():
    cond, source = condition_routing("unknown_xyz_abc", strict=True)
    assert cond == "__BLOCKED__"
    assert source == "blocked"


def test_unknown_non_strict_permissive():
    cond, source = condition_routing("unknown_xyz_abc", strict=False)
    assert cond == "all"
    assert source == "permissive_fallback"
