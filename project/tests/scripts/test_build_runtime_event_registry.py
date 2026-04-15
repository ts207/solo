from __future__ import annotations

from project.scripts.build_runtime_event_registry import _runtime_payload


def test_runtime_payload_uses_compiled_event_metadata() -> None:
    payload = _runtime_payload()
    events = payload["events"]

    depth_collapse = events["DEPTH_COLLAPSE"]
    assert depth_collapse["detector"] == "DepthCollapseDetector"
    assert depth_collapse["enabled"] is True
    assert depth_collapse["family"] == "LIQUIDITY_DISLOCATION"
    assert depth_collapse["instrument_classes"] == ["crypto", "futures"]
    assert depth_collapse["requires_features"] == []
    assert depth_collapse["sequence_eligible"] is True
    assert depth_collapse["tags"] == ["liquidity_stress"]

    absorption_proxy = events["ABSORPTION_PROXY"]
    assert absorption_proxy["detector"] == "AbsorptionProxyDetector"
