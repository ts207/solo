from project.pipelines.pipeline_provenance import objective_spec_metadata, retail_profile_metadata


def test_objective_spec_metadata_uses_registry_backed_loading():
    spec, spec_hash, path = objective_spec_metadata("retail_profitability", None)
    assert isinstance(spec, dict)
    assert spec.get("id") == "retail_profitability"
    assert spec_hash not in {"unknown_hash", "error_hash"}
    assert path.endswith("spec/objectives/retail_profitability.yaml")


def test_retail_profile_metadata_uses_registry_backed_loading():
    profile, registry_hash, path = retail_profile_metadata("capital_constrained", None)
    assert isinstance(profile, dict)
    assert profile.get("id") == "capital_constrained"
    assert registry_hash not in {"unknown_hash", "error_hash"}
    assert path.endswith("project/configs/retail_profiles.yaml")
