from __future__ import annotations

import json

import pandas as pd

from project.scripts.update_regime_event_inventory import main


def test_update_regime_event_inventory_writes_outputs(tmp_path):
    out_dir = tmp_path / "inventory"

    assert main(["--output-dir", str(out_dir)]) == 0

    expected = [
        "context_dimensions.json",
        "state_inventory.json",
        "event_inventory.json",
        "mechanism_inventory.json",
        "regime_event_inventory.parquet",
    ]
    for name in expected:
        assert (out_dir / name).exists()

    event_payload = json.loads((out_dir / "event_inventory.json").read_text(encoding="utf-8"))
    rows = {row["id"]: row for row in event_payload["rows"]}
    assert rows["FUNDING_EXTREME"]["classification"] == "invalid_unregistered"
    assert rows["FUNDING_EXTREME"]["active_candidate_event"] is False
    assert rows["FUNDING_EXTREME"]["draft_event"] is True

    mechanism_payload = json.loads(
        (out_dir / "mechanism_inventory.json").read_text(encoding="utf-8")
    )
    mechanisms = {row["id"]: row for row in mechanism_payload["rows"]}
    assert mechanisms["funding_squeeze"]["active_invalid_event_count"] == 0
    assert (
        mechanisms["funding_squeeze"]["recommended_action"]
        == "baseline_and_event_lift_before_proposal"
    )

    df = pd.read_parquet(out_dir / "regime_event_inventory.parquet")
    assert not df.empty
    assert "classification" in df.columns
