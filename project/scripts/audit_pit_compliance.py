import numpy as np
import pandas as pd

from project.features.liquidity_vacuum import LiquidityVacuumConfig, detect_liquidity_vacuum_events
from project.features.vol_regime import calculate_rv_percentile_24h
from project.reliability.audit_utils import verify_pit_compliance


def audit_features():
    print("Running PIT Compliance Audit...")

    # Setup dummy data
    n = 1000
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "close": 100.0 * np.exp(np.random.normal(0, 0.001, n).cumsum()),
            "high": 100.1,
            "low": 99.9,
            "volume": 150.0,
        }
    )

    # 1. Audit vol_regime
    print("\nAuditing: vol_regime.calculate_rv_percentile_24h")

    # Wrap series-only function
    def wrap_rv(d):
        return calculate_rv_percentile_24h(d["close"])

    res_rv = verify_pit_compliance(wrap_rv, df)
    print(f"Result: {'PASS' if res_rv['is_compliant'] else 'FAIL'}")
    if not res_rv["is_compliant"]:
        print(f"  First mismatch: {res_rv['mismatches'][0]}")

    # 2. Audit liquidity_vacuum
    print("\nAuditing: liquidity_vacuum.detect_liquidity_vacuum_events")
    cfg = LiquidityVacuumConfig(volume_window=50, range_window=50)

    # Wrap to match signature
    def wrap_lv(d):
        return detect_liquidity_vacuum_events(d, "TEST", cfg=cfg)

    res_lv = verify_pit_compliance(wrap_lv, df)
    print(f"Result: {'PASS' if res_lv['is_compliant'] else 'FAIL'}")
    if not res_lv["is_compliant"]:
        print(
            "  Leak detected! Future data affects past event evaluation via global thresholding."
        )
        print(f"  First mismatch at index: {res_lv['mismatches'][0]['first_mismatch_at']}")


if __name__ == "__main__":
    audit_features()
