from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.scripts.generate_synthetic_crypto_regimes import (
    PROFILE_SETTINGS,
    build_regime_schedule,
    generate_symbol_frames,
    generate_synthetic_dataset_suite,
)


def test_profiles_are_exposed_and_manifested():
    assert {"default", "2021_bull", "range_chop", "stress_crash", "alt_rotation"}.issubset(
        PROFILE_SETTINGS
    )

    payload = generate_symbol_frames(
        symbol="BTCUSDT",
        start_ts=pd.Timestamp("2026-01-01T00:00:00Z"),
        end_exclusive=pd.Timestamp("2026-01-15T00:00:00Z"),
        seed=7,
        volatility_profile="stress_crash",
    )

    assert not payload["perp"].empty
    assert payload["perp"]["spread_bps"].median() > 3.5


def test_profile_changes_schedule_density():
    index = pd.date_range(
        start="2026-01-01", end="2026-04-01", freq="5min", tz="UTC", inclusive="left"
    )
    stress = build_regime_schedule("BTCUSDT", index, volatility_profile="stress_crash")
    default = build_regime_schedule("BTCUSDT", index, volatility_profile="default")

    assert len(stress) > len(default)


def test_generate_synthetic_dataset_suite_writes_suite_manifest(tmp_path: Path):
    suite_config = tmp_path / "suite.yaml"
    suite_config.write_text(
        """
        suite_name: test_suite
        datasets:
          - run_id: ds_one
            start_date: 2026-01-01
            end_date: 2026-01-15
            symbols: [BTCUSDT, ETHUSDT]
            volatility_profile: default
            noise_scale: 1.0
          - run_id: ds_two
            start_date: 2026-02-01
            end_date: 2026-02-15
            symbols: [BTCUSDT, SOLUSDT]
            volatility_profile: range_chop
            noise_scale: 0.9
        """,
        encoding="utf-8",
    )

    manifest = generate_synthetic_dataset_suite(suite_config_path=suite_config, data_root=tmp_path)
    out_path = tmp_path / "synthetic" / "test_suite" / "synthetic_dataset_suite_manifest.json"

    assert out_path.exists()
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert manifest["dataset_count"] == 2
    assert payload["suite_name"] == "test_suite"
    assert len(payload["datasets"]) == 2
    assert Path(payload["datasets"][0]["truth_map_path"]).exists()
