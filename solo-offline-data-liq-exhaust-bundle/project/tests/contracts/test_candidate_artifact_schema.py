from __future__ import annotations

import pandas as pd

from project.reliability.contracts import validate_candidate_table


def test_validate_candidate_table_requires_multiplicity_fields():
    df = pd.DataFrame(
        {
            "candidate_id": ["c1"],
            "event_type": ["VOL_SHOCK"],
            "symbol": ["BTCUSDT"],
            "run_id": ["r1"],
            "split_scheme_id": ["smoke_tvt"],
            "estimate_bps": [12.0],
            "p_value_raw": [0.01],
            "p_value_adj": [0.02],
            "correction_family_id": ["r1::VOL::24"],
            "correction_method": ["bh"],
        }
    )
    out = validate_candidate_table(df)
    assert out.iloc[0]["correction_method"] == "bh"
