import pandas as pd

from project.reliability.audit_utils import verify_pit_compliance


def test_verify_pit_compliance_safe():
    # A simple moving average is PIT-safe
    def sma_feat(df):
        return df["val"].rolling(2).mean()

    df = pd.DataFrame({"val": [1.0, 2.0, 3.0, 4.0]})
    res = verify_pit_compliance(sma_feat, df)
    assert res["is_compliant"] is True


def test_verify_pit_compliance_leaky():
    # A feature that uses future data is NOT PIT-safe
    def leaky_feat(df):
        # uses the mean of the WHOLE series (look-ahead!)
        return df["val"] / df["val"].mean()

    df = pd.DataFrame({"val": [1.0, 2.0, 3.0, 4.0]})
    res = verify_pit_compliance(leaky_feat, df)
    assert res["is_compliant"] is False
    assert len(res["mismatches"]) > 0
