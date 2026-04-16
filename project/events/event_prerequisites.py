from __future__ import annotations

from pathlib import Path
from typing import Dict

from project.events.event_specs import EVENT_REGISTRY_SPECS
from project.core.feature_capabilities import has_feature_family, resolve_feature_loader


def check_event_prerequisites(
    data_root: Path,
    run_id: str,
    symbol: str,
    event_type: str,
) -> Dict[str, object]:
    evt_spec = EVENT_REGISTRY_SPECS.get(event_type)
    if evt_spec is None:
        return {
            "present": False,
            "missing": [f"Unknown event type: {event_type}"],
            "coverage": {},
        }

    required_features = []
    feature_family = ""
    if "funding" in event_type.lower():
        required_features = ["funding_rate", "funding_rate_scaled"]
        feature_family = "funding"
    elif "oi" in event_type.lower() or "open_interest" in event_type.lower():
        required_features = ["oi_notional", "oi_delta_1h"]
        feature_family = "oi"
    elif "liquidation" in event_type.lower():
        required_features = ["liquidation_notional", "liquidation_count"]
        feature_family = "liquidation"

    if not required_features:
        return {"present": True, "missing": [], "coverage": {}}

    if not has_feature_family(feature_family):
        return {
            "present": False,
            "missing": required_features,
            "coverage": {feat: 0.0 for feat in required_features},
            "reason": f"Feature family '{feature_family}' is not supported or module is missing.",
        }

    loader = resolve_feature_loader(feature_family)
    coverage = {}
    missing = []

    try:
        df = loader(data_root, run_id, symbol)
        if df is None or df.empty:
            missing.extend(required_features)
            for feat in required_features:
                coverage[feat] = 0.0
        else:
            for feat in required_features:
                if feat in df.columns:
                    coverage[feat] = float((~df[feat].isna()).mean() * 100)
                    if coverage[feat] == 0:
                        missing.append(feat)
                else:
                    missing.append(feat)
                    coverage[feat] = 0.0
    except Exception as e:
        missing.extend(required_features)
        for feat in required_features:
            coverage[feat] = 0.0

    return {
        "present": len(missing) == 0,
        "missing": missing,
        "coverage": coverage,
    }
