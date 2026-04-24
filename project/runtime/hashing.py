from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from project.specs.invariants import load_runtime_invariants_specs

DEFAULT_HASH_SCHEMA_VERSION = "runtime_hash_v1"


def load_hashing_spec(repo_root: Path) -> Dict[str, Any]:
    specs = load_runtime_invariants_specs(Path(repo_root))
    raw = specs.get("hashing", {})
    if not isinstance(raw, dict):
        raw = {}
    return dict(raw)


def _sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize(x) for x in value]
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return float(f"{value:.17g}")
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def _canonical_json_bytes(payload: Any, *, ensure_ascii: bool = True) -> bytes:
    return json.dumps(
        _sanitize(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=bool(ensure_ascii),
    ).encode("utf-8")


def _hash_bytes(payload: bytes, *, algorithm: str) -> str:
    algo = str(algorithm).strip().lower()
    if algo == "blake2b_256":
        return "blake2b_256:" + hashlib.blake2b(payload, digest_size=32).hexdigest()
    if algo == "sha256":
        return "sha256:" + hashlib.sha256(payload).hexdigest()
    raise ValueError(f"unsupported hash algorithm: {algorithm}")


def _record_sort_key(record: Mapping[str, Any], sort_keys: Iterable[str]) -> tuple:
    return tuple(_sanitize(record.get(k)) for k in sort_keys)


def hash_record(
    record: Mapping[str, Any],
    *,
    hashing_spec: Mapping[str, Any],
) -> str:
    algo = str(hashing_spec.get("algorithm", "blake2b_256"))
    ensure_ascii = bool(hashing_spec.get("canonicalization", {}).get("ensure_ascii", True))
    payload = _canonical_json_bytes(record, ensure_ascii=ensure_ascii)
    return _hash_bytes(payload, algorithm=algo)


def hash_records(
    records: Iterable[Mapping[str, Any]],
    *,
    hashing_spec: Mapping[str, Any],
) -> str:
    sort_keys = hashing_spec.get("record_sort_keys")
    ordered: List[Mapping[str, Any]]
    records_list = list(records)
    if isinstance(sort_keys, list) and sort_keys:
        ordered = sorted(records_list, key=lambda rec: _record_sort_key(rec, sort_keys))
    else:
        ordered = sorted(records_list, key=lambda rec: _canonical_json_bytes(rec))
    algo = str(hashing_spec.get("algorithm", "blake2b_256"))
    ensure_ascii = bool(hashing_spec.get("canonicalization", {}).get("ensure_ascii", True))
    payload = _canonical_json_bytes(ordered, ensure_ascii=ensure_ascii)
    return _hash_bytes(payload, algorithm=algo)


def hash_file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return "sha256:" + hasher.hexdigest()


def compute_artifact_hashes(paths: Iterable[Path]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for path in paths:
        p = Path(path)
        if not p.exists() or not p.is_file():
            continue
        try:
            out[str(p)] = hash_file_sha256(p)
        except OSError:
            continue
    return out


def compute_run_hash(
    *,
    manifest: Mapping[str, Any],
    artifact_hashes: Mapping[str, str],
    hashing_spec: Mapping[str, Any],
    schema_version: str = DEFAULT_HASH_SCHEMA_VERSION,
) -> str:
    stable_payload = {
        "hash_schema_version": str(schema_version),
        "git_commit": manifest.get("git_commit"),
        "data_hash": manifest.get("data_hash"),
        "spec_hashes": manifest.get("spec_hashes"),
        "ontology_spec_hash": manifest.get("ontology_spec_hash"),
        "feature_schema_hash": manifest.get("feature_schema_hash"),
        "objective_spec_hash": manifest.get("objective_spec_hash"),
        "retail_profile_spec_hash": manifest.get("retail_profile_spec_hash"),
        "runtime_invariants_spec_hash": manifest.get("runtime_invariants_spec_hash"),
        "runtime_lanes_hash": manifest.get("runtime_lanes_hash"),
        "runtime_firewall_hash": manifest.get("runtime_firewall_hash"),
        "runtime_hashing_hash": manifest.get("runtime_hashing_hash"),
        "runtime_postflight_status": manifest.get("runtime_postflight_status"),
        "runtime_watermark_violation_count": manifest.get("runtime_watermark_violation_count"),
        "runtime_normalization_issue_count": manifest.get("runtime_normalization_issue_count"),
        "runtime_firewall_violation_count": manifest.get("runtime_firewall_violation_count"),
        "determinism_status": manifest.get("determinism_status"),
        "replay_digest": manifest.get("replay_digest"),
        "oms_replay_status": manifest.get("oms_replay_status"),
        "oms_replay_violation_count": manifest.get("oms_replay_violation_count"),
        "oms_replay_digest": manifest.get("oms_replay_digest"),
        "artifact_hashes": dict(sorted((str(k), str(v)) for k, v in artifact_hashes.items())),
    }
    return hash_record(stable_payload, hashing_spec=hashing_spec)
