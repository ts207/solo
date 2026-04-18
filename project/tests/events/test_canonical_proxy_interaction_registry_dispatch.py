from __future__ import annotations

import pytest

from project.events.detectors.registry import get_detector_class
from project.events.registries.canonical_proxy import (
    CANONICAL_PROXY_EVENT_TYPES,
    get_canonical_proxy_detectors,
)
from project.events.registries.interaction import (
    STATIC_INTERACTION_EVENT_TYPES,
    get_static_interaction_detectors,
)
from project.events.families.canonical_proxy import (
    AbsorptionProxyDetector,
    DepthCollapseDetector,
    DepthStressProxyDetector,
    OrderflowImbalanceShockDetector,
    PriceVolImbalanceProxyDetector,
    SweepStopRunDetector,
    WickReversalProxyDetector,
)


def test_canonical_proxy_family_uses_registry_module() -> None:
    detectors = get_canonical_proxy_detectors()
    expected = {
        "PRICE_VOL_IMBALANCE_PROXY",
        "WICK_REVERSAL_PROXY",
        "ABSORPTION_PROXY",
        "DEPTH_STRESS_PROXY",
        "ORDERFLOW_IMBALANCE_SHOCK",
        "SWEEP_STOPRUN",
        "DEPTH_COLLAPSE",
    }
    assert expected == set(CANONICAL_PROXY_EVENT_TYPES)
    assert expected <= set(detectors.keys())


def test_canonical_proxy_registry_dispatch_resolves_specific_detector_classes() -> None:
    detectors = get_canonical_proxy_detectors()
    assert detectors["PRICE_VOL_IMBALANCE_PROXY"] is PriceVolImbalanceProxyDetector
    assert detectors["WICK_REVERSAL_PROXY"] is WickReversalProxyDetector
    assert detectors["ABSORPTION_PROXY"] is AbsorptionProxyDetector
    assert detectors["DEPTH_STRESS_PROXY"] is DepthStressProxyDetector
    assert detectors["ORDERFLOW_IMBALANCE_SHOCK"] is OrderflowImbalanceShockDetector
    assert detectors["SWEEP_STOPRUN"] is SweepStopRunDetector
    assert detectors["DEPTH_COLLAPSE"] is DepthCollapseDetector


def test_canonical_proxy_registry_dispatch_matches_global_registry() -> None:
    for et in CANONICAL_PROXY_EVENT_TYPES:
        cls = get_detector_class(et)
        assert cls is not None, f"{et} not in global registry"


def test_interaction_static_detectors_are_accessible() -> None:
    static = get_static_interaction_detectors()
    assert "CROSS_ASSET_INTERACTION" in STATIC_INTERACTION_EVENT_TYPES
    assert "CROSS_ASSET_INTERACTION" in static


def test_interaction_family_no_register_detector_drift() -> None:
    from project.events.families.interaction import _DETECTORS

    assert isinstance(_DETECTORS, dict)
    registry = get_static_interaction_detectors()
    for et, cls in registry.items():
        if et in _DETECTORS:
            assert _DETECTORS[et] is cls, f"{et} class mismatch between family and registry"


def test_canonical_proxy_no_inline_register_detector_call() -> None:
    import inspect
    from project.events.families import canonical_proxy
    source = inspect.getsource(canonical_proxy)
    assert "register_detector(et, cls)" not in source
    assert "for et, cls in _DETECTORS.items()" not in source


def test_interaction_no_inline_register_detector_call() -> None:
    import inspect
    from project.events.families import interaction
    source = inspect.getsource(interaction)
    assert "register_detector(et, cls)" not in source
    assert "for et, cls in _DETECTORS.items()" not in source


def test_regime_no_inline_register_detector_call() -> None:
    import inspect
    from project.events.families import regime
    source = inspect.getsource(regime)
    assert "register_detector(et, cls)" not in source
    assert "for et, cls in _DETECTORS.items()" not in source


def test_all_families_have_no_inline_register_detector() -> None:
    import inspect
    import project.events.families as fam_pkg
    import pkgutil
    family_modules = [
        name
        for _, name, _ in pkgutil.iter_modules(fam_pkg.__path__)
        if name not in ("__pycache__",)
    ]
    for mod_name in family_modules:
        mod = __import__(f"project.events.families.{mod_name}", fromlist=[mod_name])
        source = inspect.getsource(mod)
        assert "register_detector(" not in source, (
            f"project.events.families.{mod_name} still has inline register_detector call"
        )
        assert "for et, cls in _DETECTORS.items()" not in source, (
            f"project.events.families.{mod_name} still has _DETECTORS iteration loop"
        )
        assert "register_family_detectors(" not in source, (
            f"project.events.families.{mod_name} still has register_family_detectors call"
        )