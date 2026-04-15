from __future__ import annotations

from project.experiments.config_loader import resolve_experiment_config


def test_resolve_experiment_config_with_inheritance(tmp_path):
    base = tmp_path / "base.yaml"
    child = tmp_path / "child.yaml"
    base.write_text("a: 1\nnested:\n  x: 10\n", encoding="utf-8")
    child.write_text(f"inherits: {base.name}\nnested:\n  y: 20\n", encoding="utf-8")
    out = resolve_experiment_config(child)
    assert out["a"] == 1
    assert out["nested"]["x"] == 10
    assert out["nested"]["y"] == 20
