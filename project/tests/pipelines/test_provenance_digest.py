from project.pipelines.pipeline_provenance import data_fingerprint


def test_data_fingerprint_changes_with_spec_edit(tmp_path):
    repo = tmp_path / "repo"
    project_root = repo / "project"
    spec_root = repo / "spec"
    (project_root / "configs").mkdir(parents=True)
    (spec_root / "events").mkdir(parents=True)
    (spec_root / "features").mkdir(parents=True)
    (spec_root / "runtime").mkdir(parents=True)
    (spec_root / "states").mkdir(parents=True)
    (spec_root / "objectives").mkdir(parents=True)
    (spec_root / "events" / "e.yaml").write_text("a: 1\n", encoding="utf-8")
    (spec_root / "gates.yaml").write_text("g: 1\n", encoding="utf-8")
    (project_root / "configs" / "retail_profiles.yaml").write_text(
        "profiles: {}\n", encoding="utf-8"
    )
    data_root = tmp_path / "data"
    raw = data_root / "lake" / "raw" / "binance" / "spot" / "BTCUSDT"
    raw.mkdir(parents=True)
    (raw / "bars.csv").write_text("x\n1\n", encoding="utf-8")

    d1, m1 = data_fingerprint(["BTCUSDT"], "r1", project_root=project_root, data_root=data_root)
    (spec_root / "events" / "e.yaml").write_text("a: 2\n", encoding="utf-8")
    d2, m2 = data_fingerprint(["BTCUSDT"], "r1", project_root=project_root, data_root=data_root)

    assert d1 != d2
    assert m1["spec_component_hashes"] != m2["spec_component_hashes"]
