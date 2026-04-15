import os
import glob
from pathlib import Path
import yaml

covered_events = {
    "CROSS_VENUE_DESYNC",
    "BASIS_DISLOC",
    "SPOT_PERP_BASIS_SHOCK",
    "FND_DISLOC",
    "FUNDING_FLIP",
    "TREND_ACCELERATION",
    "TREND_EXHAUSTION_TRIGGER",
    "MOMENTUM_DIVERGENCE_TRIGGER",
    "FALSE_BREAKOUT",
    "BREAKOUT_TRIGGER",
    "FAILED_CONTINUATION",
    "LIQUIDITY_STRESS_DIRECT",
    "LIQUIDITY_STRESS_PROXY",
    "ABSORPTION_PROXY",
    "DEPTH_STRESS_PROXY",
    "PRICE_VOL_IMBALANCE_PROXY",
    "SPREAD_REGIME_WIDENING_EVENT",
    "DELEVERAGING_WAVE",
    "CLIMAX_VOLUME_BAR",
    "LIQUIDATION_EXHAUSTION_REVERSAL",
}

for yaml_file in glob.glob("spec/events/*.yaml"):
    if os.path.basename(yaml_file).startswith("_"):
        continue

    with open(yaml_file, "r") as f:
        content = f.read()

    try:
        data = yaml.safe_load(content)
        if not isinstance(data, dict):
            continue
        if data.get("kind") in [
            "canonical_event_registry",
            "event_config_defaults",
            "event_family_defaults",
            "event_unified_registry",
        ]:
            continue
    except:
        continue

    event_type = data.get("event_type")
    if not event_type:
        continue

    if event_type in covered_events:
        coverage = "covered"
    elif data.get("is_descriptive", False) or not data.get("is_trade_trigger", True):
        coverage = "synthetic-unvalidatable"
    else:
        coverage = "uncovered"

    if "synthetic_coverage:" not in content:
        content = content.replace(
            f"event_type: {event_type}", f"event_type: {event_type}\nsynthetic_coverage: {coverage}"
        )
        with open(yaml_file, "w") as f:
            f.write(content)
