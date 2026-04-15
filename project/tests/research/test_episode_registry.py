from __future__ import annotations

from project.episodes import build_episode_artifacts, infer_live_episode_matches, load_episode_registry


def test_episode_registry_loads_expected_contracts() -> None:
    registry = load_episode_registry()

    assert "EP_LIQUIDITY_SHOCK" in registry
    assert registry["EP_LIQUIDITY_SHOCK"].runtime_hint == "wide_spread_and_thin_depth"
    assert "VOL_SHOCK" in registry["EP_VOLATILITY_BREAKOUT"].required_events


def test_episode_registry_returns_fresh_mapping_from_cached_registry() -> None:
    first = load_episode_registry()
    second = load_episode_registry()

    assert first is not second
    assert first["EP_LIQUIDITY_SHOCK"] is second["EP_LIQUIDITY_SHOCK"]


def test_episode_runtime_inference_derives_liquidity_shock() -> None:
    matches = infer_live_episode_matches(
        ["VOL_SHOCK"],
        regime_snapshot={"canonical_regime": "VOLATILITY"},
        live_features={"spread_bps": 8.0, "depth_usd": 10_000.0, "move_bps": 65.0, "volume": 100_000.0},
    )

    episode_ids = {match.episode_id for match in matches}
    assert "EP_LIQUIDITY_SHOCK" in episode_ids


def test_episode_artifacts_builder_writes_catalog(tmp_path) -> None:
    paths = build_episode_artifacts(tmp_path)

    assert paths["catalog_path"].exists()
    assert paths["matrix_path"].exists()
