from pathlib import Path


def test_ingest_docstring_has_no_inline_citation_artifacts():
    path = (
        Path(__file__).parents[2]
        / "project"
        / "pipelines"
        / "ingest"
        / "ingest_binance_um_ohlcv.py"
    )
    text = path.read_text(encoding="utf-8")
    assert "【" not in text
    assert "†" not in text


def test_plugin_metadata_has_no_placeholder_author_email():
    path = Path(__file__).parents[2] / "plugins" / "edge-plugins" / ".codex-plugin" / "plugin.json"
    text = path.read_text(encoding="utf-8")
    assert "you@example.com" not in text


def test_core_concept_specs_reference_canonical_search_stage():
    root = Path(__file__).parents[2] / "spec" / "concepts"
    files = [
        root / "C_VALIDATION.yaml",
        root / "C_ML_TRADING_MODELS.yaml",
        root / "C_CONTEXT_INTERACTIONS.yaml",
    ]
    for path in files:
        text = path.read_text(encoding="utf-8")
        assert "project/research/phase2_search_engine.py" in text
        assert "phase2_candidate_discovery.py" not in text
