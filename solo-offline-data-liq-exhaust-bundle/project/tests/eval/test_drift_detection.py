import json
import numpy as np
import pandas as pd
import tempfile
import os

from project.eval.drift_detection import detect_parameter_drift, detect_feature_drift


def test_detect_parameter_drift():
    old_manifest = {
        "blueprints": {"bp1": {"stop_value": 0.05, "target_value": 0.1, "cooldown_bars": 10}}
    }

    new_manifest = {
        "blueprints": {
            "bp1": {
                "stop_value": 0.06,  # 20% drift
                "target_value": 0.105,  # 5% drift, should not be flagged if threshold is 0.1
                "cooldown_bars": 10,  # 0% drift
            }
        }
    }

    flags = detect_parameter_drift(old_manifest, new_manifest, threshold=0.1)

    assert len(flags) == 1
    assert flags[0]["blueprint_id"] == "bp1"
    assert flags[0]["parameter"] == "stop_value"
    # Allow small float arithmetic differences
    assert abs(flags[0]["pct_change"] - 0.2) < 1e-6


def test_detect_feature_drift():
    np.random.seed(42)
    # Setup current features (needs >= 100 rows to bypass early exit)
    # First half: N(0, 1), Second half: feature_1 = N(0, 1), feature_2 = N(10, 1) (massive drift)
    df = pd.DataFrame(
        {
            "timestamp": range(150),
            "open": range(150),
            "feature_1": np.random.normal(0, 1, 150),
            "feature_2": np.concatenate([np.random.normal(0, 1, 75), np.random.normal(10, 1, 75)]),
        }
    )

    flags = detect_feature_drift(df, p_value_threshold=0.05)

    feature_2_flags = [f for f in flags if f["feature"] == "feature_2"]
    assert len(feature_2_flags) == 1
    assert feature_2_flags[0]["p_value"] < 0.05

    feature_1_flags = [f for f in flags if f["feature"] == "feature_1"]
    assert len(feature_1_flags) == 0
