from __future__ import annotations

from project.runtime.hashing import compute_run_hash, load_hashing_spec
from project.runtime.replay import determinism_replay_check
from project.tests.conftest import PROJECT_ROOT


def test_compute_run_hash_stable_for_artifact_key_order():
    hashing_spec = load_hashing_spec(PROJECT_ROOT.parent)
    manifest = {
        "git_commit": "abc123",
        "data_hash": "sha256:data",
        "spec_hashes": {"gates.yaml": "111"},
        "ontology_spec_hash": "sha256:ont",
        "feature_schema_hash": "sha256:feat",
        "objective_spec_hash": "sha256:obj",
        "retail_profile_spec_hash": "sha256:retail",
        "runtime_invariants_spec_hash": "sha256:runtime",
        "runtime_lanes_hash": "sha256:lanes",
        "runtime_firewall_hash": "sha256:firewall",
        "runtime_hashing_hash": "sha256:hashing",
        "runtime_postflight_status": "pass",
        "runtime_watermark_violation_count": 0,
        "runtime_normalization_issue_count": 0,
        "determinism_status": "pass",
        "replay_digest": "blake2b_256:replay",
    }
    artifact_hashes_a = {
        "/tmp/a.parquet": "sha256:a",
        "/tmp/b.parquet": "sha256:b",
    }
    artifact_hashes_b = {
        "/tmp/b.parquet": "sha256:b",
        "/tmp/a.parquet": "sha256:a",
    }
    h1 = compute_run_hash(
        manifest=manifest,
        artifact_hashes=artifact_hashes_a,
        hashing_spec=hashing_spec,
    )
    h2 = compute_run_hash(
        manifest=manifest,
        artifact_hashes=artifact_hashes_b,
        hashing_spec=hashing_spec,
    )
    assert h1 == h2
    assert h1.startswith("blake2b_256:")


def test_determinism_replay_check_is_order_invariant():
    hashing_spec = load_hashing_spec(PROJECT_ROOT.parent)
    ticks = [
        {
            "tick_time": 2,
            "lane_id": "alpha_5s",
            "role": "alpha",
            "instrument_id": "BTCUSDT",
            "venue_id": "binance",
            "event_id": "e2",
            "source_seq": 2,
        },
        {
            "tick_time": 1,
            "lane_id": "alpha_5s",
            "role": "alpha",
            "instrument_id": "BTCUSDT",
            "venue_id": "binance",
            "event_id": "e1",
            "source_seq": 1,
        },
    ]
    out = determinism_replay_check(ticks, hashing_spec=hashing_spec)
    assert out["status"] == "pass"
    assert str(out["replay_digest"]).startswith("blake2b_256:")
    variants = dict(out["variant_digests"])
    assert variants["canonical"] == variants["reverse"] == variants["source_seq_sorted"]
