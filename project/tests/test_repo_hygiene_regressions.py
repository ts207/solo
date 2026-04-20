from __future__ import annotations

import pytest
from pathlib import Path


def _has_plugins() -> bool:
    return (Path(__file__).parents[2] / "plugins").exists()


def test_binance_um_ingest_has_no_raw_prompts():
    path = (
        Path(__file__).parents[2]
        / "project"
        / "pipelines"
        / "ingest"
        / "ingest_binance_um_ohlcv.py"
    )
    text = path.read_text(encoding="utf-8")
    assert "【" not in text


@pytest.mark.skipif(not _has_plugins(), reason="plugins directory not present")
def test_plugin_metadata_has_no_placeholder_author_email():
    path = Path(__file__).parents[2] / "plugins" / "edge-plugins" / ".codex-plugin" / "plugin.json"
    if not path.exists():
        pytest.skip("edge-plugins metadata not present")
    text = path.read_text(encoding="utf-8")
    assert "author@example.com" not in text


def test_core_concept_specs_reference_canonical_search_stage():
    root = Path(__file__).parents[2] / "spec" / "concepts"
    files = [
        root / "C_VALIDATION.yaml",
        root / "C_ML_TRADING_MODELS.yaml",
        root / "C_CANDIDATE_SEARCH.yaml",
        root / "C_STRATEGY_SYNTHESIS.yaml",
    ]
    for path in files:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        assert "phase2_search_engine" in text
        assert "phase2_v1" not in text
