from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

import project.spec_registry.loaders as loaders


def test_loader_utility_functions_cover_merge_and_safe_parsers(tmp_path: Path, monkeypatch):
    base = {"a": 1, "nested": {"x": 1, "y": 2}}
    override = {"nested": {"y": 9, "z": 10}, "b": 2}
    merged = loaders._deep_merge(base, override)
    assert merged == {"a": 1, "nested": {"x": 1, "y": 9, "z": 10}, "b": 2}

    assert loaders._safe_objective({"objective": {"score_weights": {"a": 1}}})["score_weights"]["a"] == 1
    assert loaders._safe_objective({"objective": {"constraints": "bad"}})["constraints"] == {}
    assert loaders._safe_profiles({"profiles": {"p1": {"x": 1}, "": {"x": 2}}}) == {"p1": {"x": 1}}

    file_path = tmp_path / "sample.yaml"
    file_path.write_text("a: 1\nb:\n  c: 2\n", encoding="utf-8")

    assert loaders._read_yaml(file_path) == {"a": 1, "b": {"c": 2}}
    assert loaders.load_yaml_path(file_path) == {"a": 1, "b": {"c": 2}}

    monkeypatch.setattr(loaders, "resolve_relative_spec_path", lambda relative_path, repo_root=None: file_path)
    loaders.load_yaml_relative.cache_clear()
    assert loaders.load_yaml_relative("anything.yaml") == {"a": 1, "b": {"c": 2}}
    loaders.clear_caches()


def test_spec_loader_path_helpers_and_digests(tmp_path: Path, monkeypatch):
    repo_root = tmp_path / "repo"
    spec_dir = repo_root / "spec" / "sub"
    spec_dir.mkdir(parents=True)
    yaml_file = spec_dir / "one.yaml"
    yaml_file.write_text("name: one\n", encoding="utf-8")
    assert loaders.iter_spec_yaml_files(repo_root=repo_root) == [yaml_file.resolve()]

    monkeypatch.setattr(loaders, "resolve_relative_spec_path", lambda rel, repo_root=None: (repo_root / rel).resolve() if repo_root else (repo_root := tmp_path / "repo" / rel).resolve())
    # explicit patched paths for digest
    real_a = tmp_path / "a.yaml"
    real_a.write_text("k: 1\n", encoding="utf-8")
    real_b = tmp_path / "b.yaml"
    real_b.write_text("k: 2\n", encoding="utf-8")
    mapping = {"spec/a.yaml": real_a, "spec/b.yaml": real_b}
    monkeypatch.setattr(loaders, "resolve_relative_spec_path", lambda rel, repo_root=None: mapping.get(str(rel), tmp_path / "missing"))
    digest = loaders.compute_spec_digest(["spec/a.yaml", "spec/missing.yaml"])
    payload = json.loads(digest)
    assert payload[0]["exists"] is True and payload[1]["exists"] is False

    assert isinstance(loaders.ontology_spec_paths(repo_root=repo_root), dict)
    assert isinstance(loaders.runtime_spec_paths(repo_root=repo_root), dict)


def test_registry_loaders_for_objective_profiles_and_features(tmp_path: Path, monkeypatch):
    obj = tmp_path / "objective.yaml"
    obj.write_text("objective:\n  score_weights:\n    alpha: 1\n", encoding="utf-8")
    profiles = tmp_path / "profiles.yaml"
    profiles.write_text("profiles:\n  capital_constrained:\n    x: 1\n", encoding="utf-8")
    feature = tmp_path / "feature_schema_v2.json"
    feature.write_text(json.dumps({"version": "v2", "fields": []}), encoding="utf-8")

    assert loaders.load_objective_spec(explicit_path=obj, required=True)["id"] == "retail_profitability"
    assert loaders.load_retail_profiles_spec(explicit_path=profiles, required=True)["capital_constrained"]["x"] == 1
    assert loaders.load_retail_profile(explicit_path=profiles, required=True)["id"] == "capital_constrained"

    monkeypatch.setattr(loaders, "feature_schema_registry_path", lambda version=None: feature)
    assert loaders.load_feature_schema_registry()["version"] == "v2"

    assert loaders.canonical_yaml_hash(obj).startswith("{")
    loaders.clear_caches()


def test_loaders_can_load_spec_files_and_handle_missing_or_invalid_values(tmp_path: Path, monkeypatch):
    # load_yaml_relative and load_yaml_path missing/invalid behavior
    missing = tmp_path / "missing.yaml"
    with pytest.raises(FileNotFoundError):
        loaders._read_yaml(missing, required=True)
    assert loaders._read_yaml(missing, required=False) == {}

    bad_json = tmp_path / "feature.json"
    bad_json.write_text("[]", encoding="utf-8")
    monkeypatch.setattr(loaders, "feature_schema_registry_path", lambda version=None: bad_json)
    with pytest.raises(ValueError):
        loaders.load_feature_schema_registry()

    bad_yaml = tmp_path / "bad.yaml"
    bad_yaml.write_text("[1,2,3]\n", encoding="utf-8")
    monkeypatch.setattr(loaders, "resolve_relative_spec_path", lambda relative_path, repo_root=None: bad_yaml)
    loaders.load_yaml_relative.cache_clear()
    with pytest.raises(ValueError):
        loaders.load_yaml_relative("x.yaml")
