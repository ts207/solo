from __future__ import annotations

from project.scripts.baseline import _common as baseline_common


def test_build_baseline_creates_metadata_and_manifests(monkeypatch, tmp_path):
    monkeypatch.setattr(baseline_common, "BASELINE_ROOT", tmp_path / "baseline")

    result = baseline_common.build_baseline(strict=False)
    assert "metadata" in result
    assert "events" in result
    assert "analyzers" in result
