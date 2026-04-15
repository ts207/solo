from __future__ import annotations

import json

import pandas as pd

from project.events.ontology_deconfliction import (
    attach_canonical_event_bundle,
    deconflict_event_episodes,
)


def test_attach_canonical_event_bundle_canonicalizes_aliases_before_lookup():
    df = pd.DataFrame(
        {
            "event_type": ["ABSORPTION_EVENT", "ABSORPTION_PROXY"],
            "timestamp": pd.to_datetime(
                ["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"], utc=True
            ),
            "symbol": ["BTCUSDT", "BTCUSDT"],
        }
    )

    out = attach_canonical_event_bundle(df)
    assert out["canonical_regime"].notna().all()
    assert set(out["raw_event_type"]) == {"ABSORPTION_PROXY"}
    assert set(out["canonical_regime"]) == {"LIQUIDITY_STRESS"}


def test_deconflict_event_episodes_collapses_alias_variants_to_one_canonical_bundle():
    df = pd.DataFrame(
        {
            "event_type": ["LIQUIDITY_STRESS_DIRECT", "LIQUIDITY_STRESS_PROXY", "SESSION_OPEN_EVENT"],
            "timestamp": pd.to_datetime(
                ["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"],
                utc=True,
            ),
            "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT"],
            "event_id": ["a", "b", "c"],
        }
    )

    out = deconflict_event_episodes(df)
    liquidity = out[out["canonical_regime"] == "LIQUIDITY_STRESS"]
    assert len(liquidity) == 1
    raw_event_types = set(json.loads(liquidity.iloc[0]["raw_event_types"]))
    assert raw_event_types == {"LIQUIDITY_STRESS_DIRECT", "LIQUIDITY_STRESS_PROXY"}
    assert liquidity.iloc[0]["raw_event_type"] == "LIQUIDITY_STRESS_DIRECT"


def test_deconflict_event_episodes_penalizes_context_tags_against_canonical_events():
    df = pd.DataFrame(
        {
            "event_type": ["SESSION_OPEN_EVENT", "VOL_SPIKE"],
            "timestamp": pd.to_datetime(
                ["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"],
                utc=True,
            ),
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "event_id": ["ctx", "evt"],
        }
    )
    out = deconflict_event_episodes(df)
    assert "VOL_SPIKE" in set(out["event_type"])
