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

    df = pd.read_parquet(out_dir / "regime_event_inventory.parquet")
    assert not df.empty
    assert "classification" in df.columns
