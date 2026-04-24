from __future__ import annotations

from project.specs import invariants as runtime_invariants
from project.tests.conftest import PROJECT_ROOT


def test_runtime_invariants_specs_validate_for_repo_contract():
    repo_root = PROJECT_ROOT.parent
    issues = runtime_invariants.validate_runtime_invariants_specs(repo_root)
    assert issues == []

    fields = runtime_invariants.runtime_component_hash_fields(
        runtime_invariants.runtime_component_hashes(repo_root)
    )
    for key in ("runtime_lanes_hash", "runtime_firewall_hash", "runtime_hashing_hash"):
        value = fields.get(key)
        assert isinstance(value, str) and value.startswith("sha256:")

    spec_hash = runtime_invariants.runtime_spec_hash(repo_root)
    assert spec_hash.startswith("sha256:")


def test_runtime_invariants_validation_reports_duplicate_lane_ids(tmp_path):
    runtime_dir = tmp_path / "spec" / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    (runtime_dir / "lanes.yaml").write_text(
        "\n".join(
            [
                "schema_version: 1",
                "tick_time_unit: us",
                "lanes:",
                "  - lane_id: alpha_5s",
                "    cadence_us: 5000000",
                "    watermark:",
                "      policy: bounded_out_of_orderness",
                "      max_lateness_us: 5000000",
                "      idle_source_policy: stall",
                "      idle_timeout_us: 0",
                "    processing_time_gate:",
                "      require_recv_time_leq_decision_time: true",
                "    inputs:",
                "      normalized_event_types: [book_l1]",
                "  - lane_id: alpha_5s",
                "    cadence_us: 1000000",
                "    watermark:",
                "      policy: bounded_out_of_orderness",
                "      max_lateness_us: 1000000",
                "      idle_source_policy: allow_advance",
                "      idle_timeout_us: 0",
                "    processing_time_gate:",
                "      require_recv_time_leq_decision_time: true",
                "    inputs:",
                "      normalized_event_types: [trades]",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (runtime_dir / "firewall.yaml").write_text(
        "\n".join(
            [
                "schema_version: 1",
                "roles:",
                "  alpha:",
                "    allowed_provenance: [market, quality]",
                "    allow_exec_state: false",
                "  events:",
                "    allowed_provenance: [market, quality]",
                "    allow_exec_state: false",
                "  execution:",
                "    allowed_provenance: [execution, market]",
                "    allow_exec_state: true",
                "    allowed_market_state_fields: [mu]",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (runtime_dir / "hashing.yaml").write_text(
        "\n".join(
            [
                "schema_version: 1",
                "algorithm: blake2b_256",
                "require_version_fields: [schema_version, config_version, model_version]",
                "domains: [feature_frame, state, event_set]",
                "canonicalization:",
                "  json_sort_keys: true",
                "  ensure_ascii: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    issues = runtime_invariants.validate_runtime_invariants_specs(tmp_path)
    assert any("duplicate lane_id" in issue for issue in issues)
