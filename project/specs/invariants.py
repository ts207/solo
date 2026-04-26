from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from project.spec_registry import (
    RUNTIME_SPEC_RELATIVE_PATHS,
    load_runtime_spec,
)
from project.spec_registry import (
    runtime_spec_paths as _registry_runtime_spec_paths,
)

_REQUIRED_FIREWALL_ROLES = ("alpha", "events", "execution")
_ALLOWED_WATERMARK_POLICIES = {"bounded_out_of_orderness"}
_ALLOWED_IDLE_SOURCE_POLICIES = {"stall", "allow_advance"}
_REQUIRED_HASH_VERSION_FIELDS = {"schema_version", "config_version", "model_version"}
_SUPPORTED_HASH_ALGOS = {"blake2b_256"}


def runtime_spec_paths(repo_root: Path) -> dict[str, Path]:
    return dict(_registry_runtime_spec_paths(repo_root))


def _sha256_bytes(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _canonical_spec_bytes(path: Path) -> bytes:
    if not path.exists():
        return b""
    try:
        suffix = path.suffix.lower()
        if suffix in {".yaml", ".yml"}:
            payload = _load_yaml_mapping(path)
        elif suffix == ".json":
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        else:
            return path.read_bytes()
        return json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
    except Exception:
        return path.read_bytes()


def runtime_component_hashes(repo_root: Path) -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for key, path in runtime_spec_paths(repo_root).items():
        if not path.exists():
            out[key] = None
            continue
        out[key] = _sha256_bytes(_canonical_spec_bytes(path))
    return out


def runtime_component_hash_fields(
    component_hashes: Mapping[str, str | None],
) -> dict[str, str | None]:
    return {
        "runtime_lanes_hash": component_hashes.get("lanes"),
        "runtime_firewall_hash": component_hashes.get("firewall"),
        "runtime_hashing_hash": component_hashes.get("hashing"),
    }


def runtime_spec_hash(repo_root: Path) -> str:
    hasher = hashlib.sha256()
    paths = runtime_spec_paths(repo_root)
    for key in sorted(paths):
        rel_path = RUNTIME_SPEC_RELATIVE_PATHS[key]
        hasher.update(key.encode("utf-8"))
        hasher.update(rel_path.encode("utf-8"))
        hasher.update(_canonical_spec_bytes(paths[key]))
    return "sha256:" + hasher.hexdigest()


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing runtime invariants spec: {path}")
    payload = (
        load_runtime_spec(path.stem)
        if Path(path).resolve().parent
        == _registry_runtime_spec_paths().get(path.stem, Path()).resolve().parent
        else None
    )
    if payload is None:
        import yaml

        with Path(path).open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Runtime invariants spec must be a mapping: {path}")
    return dict(payload)


def load_runtime_invariants_specs(repo_root: Path) -> dict[str, dict[str, Any]]:
    paths = runtime_spec_paths(repo_root)
    return {
        "lanes": _load_yaml_mapping(paths["lanes"]),
        "firewall": _load_yaml_mapping(paths["firewall"]),
        "hashing": _load_yaml_mapping(paths["hashing"]),
    }


def _as_positive_int(value: Any) -> int | None:
    try:
        ivalue = int(value)
    except Exception:
        return None
    return ivalue if ivalue > 0 else None


def _as_non_negative_int(value: Any) -> int | None:
    try:
        ivalue = int(value)
    except Exception:
        return None
    return ivalue if ivalue >= 0 else None


def _as_str_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if value is None:
        return []
    token = str(value).strip()
    return [token] if token else []


def validate_runtime_invariants_specs(repo_root: Path) -> list[str]:
    issues: list[str] = []
    paths = runtime_spec_paths(repo_root)

    for key, path in paths.items():
        if not path.exists():
            issues.append(f"missing runtime spec '{key}': {path}")

    if issues:
        return issues

    try:
        specs = load_runtime_invariants_specs(repo_root)
    except Exception as exc:
        return [f"failed to load runtime invariants specs: {exc}"]

    issues.extend(_validate_lanes_spec(specs["lanes"]))
    issues.extend(_validate_firewall_spec(specs["firewall"]))
    issues.extend(_validate_hashing_spec(specs["hashing"]))
    return issues


def _validate_lanes_spec(spec: Mapping[str, Any]) -> list[str]:
    issues: list[str] = []
    schema_version = _as_positive_int(spec.get("schema_version"))
    if schema_version is None:
        issues.append("runtime lanes spec: schema_version must be a positive integer")

    tick_time_unit = str(spec.get("tick_time_unit", "")).strip().lower()
    if tick_time_unit != "us":
        issues.append("runtime lanes spec: tick_time_unit must be 'us'")

    lanes = spec.get("lanes")
    if not isinstance(lanes, list) or not lanes:
        issues.append("runtime lanes spec: lanes must be a non-empty list")
        return issues

    seen_lane_ids: set[str] = set()
    for idx, lane in enumerate(lanes):
        prefix = f"runtime lanes spec lane[{idx}]"
        if not isinstance(lane, dict):
            issues.append(f"{prefix}: lane entry must be a mapping")
            continue

        lane_id = str(lane.get("lane_id", "")).strip()
        if not lane_id:
            issues.append(f"{prefix}: lane_id is required")
        elif lane_id in seen_lane_ids:
            issues.append(f"{prefix}: duplicate lane_id '{lane_id}'")
        else:
            seen_lane_ids.add(lane_id)

        cadence_us = _as_positive_int(lane.get("cadence_us"))
        if cadence_us is None:
            issues.append(f"{prefix}: cadence_us must be a positive integer")

        watermark = lane.get("watermark")
        if not isinstance(watermark, dict):
            issues.append(f"{prefix}: watermark must be a mapping")
        else:
            policy = str(watermark.get("policy", "")).strip().lower()
            if policy not in _ALLOWED_WATERMARK_POLICIES:
                issues.append(
                    f"{prefix}: watermark.policy '{policy}' not in "
                    f"{sorted(_ALLOWED_WATERMARK_POLICIES)}"
                )
            if _as_non_negative_int(watermark.get("max_lateness_us")) is None:
                issues.append(f"{prefix}: watermark.max_lateness_us must be a non-negative integer")
            idle_policy = str(watermark.get("idle_source_policy", "")).strip().lower()
            if idle_policy not in _ALLOWED_IDLE_SOURCE_POLICIES:
                issues.append(
                    f"{prefix}: watermark.idle_source_policy '{idle_policy}' not in "
                    f"{sorted(_ALLOWED_IDLE_SOURCE_POLICIES)}"
                )
            if _as_non_negative_int(watermark.get("idle_timeout_us")) is None:
                issues.append(f"{prefix}: watermark.idle_timeout_us must be a non-negative integer")

        processing_time_gate = lane.get("processing_time_gate")
        if not isinstance(processing_time_gate, dict):
            issues.append(f"{prefix}: processing_time_gate must be a mapping")
        elif not isinstance(processing_time_gate.get("require_recv_time_leq_decision_time"), bool):
            issues.append(
                f"{prefix}: processing_time_gate.require_recv_time_leq_decision_time must be a bool"
            )

        inputs = lane.get("inputs")
        if not isinstance(inputs, dict):
            issues.append(f"{prefix}: inputs must be a mapping")
        else:
            normalized_event_types = inputs.get("normalized_event_types")
            if not isinstance(normalized_event_types, list) or not normalized_event_types:
                issues.append(f"{prefix}: inputs.normalized_event_types must be a non-empty list")

    return issues


def _validate_firewall_spec(spec: Mapping[str, Any]) -> list[str]:
    issues: list[str] = []
    schema_version = _as_positive_int(spec.get("schema_version"))
    if schema_version is None:
        issues.append("runtime firewall spec: schema_version must be a positive integer")

    roles = spec.get("roles")
    if not isinstance(roles, dict):
        issues.append("runtime firewall spec: roles must be a mapping")
        return issues

    for role in _REQUIRED_FIREWALL_ROLES:
        if role not in roles:
            issues.append(f"runtime firewall spec: missing required role '{role}'")

    for role_name, role_cfg in roles.items():
        if not isinstance(role_cfg, dict):
            issues.append(
                f"runtime firewall spec role '{role_name}': role config must be a mapping"
            )
            continue
        provenance = _as_str_list(role_cfg.get("allowed_provenance"))
        if not provenance:
            issues.append(
                f"runtime firewall spec role '{role_name}': allowed_provenance must be non-empty"
            )
        allow_exec_state = role_cfg.get("allow_exec_state")
        if not isinstance(allow_exec_state, bool):
            issues.append(
                f"runtime firewall spec role '{role_name}': allow_exec_state must be bool"
            )
        elif role_name == "execution" and not allow_exec_state:
            issues.append("runtime firewall spec role 'execution': allow_exec_state must be true")
        elif role_name in {"alpha", "events"} and allow_exec_state:
            issues.append(
                f"runtime firewall spec role '{role_name}': allow_exec_state must be false"
            )

        if role_name == "execution":
            allowed_market_fields = _as_str_list(role_cfg.get("allowed_market_state_fields"))
            if not allowed_market_fields:
                issues.append(
                    "runtime firewall spec role 'execution': allowed_market_state_fields must be non-empty"
                )

    alpha_provenance = _as_str_list((roles.get("alpha") or {}).get("allowed_provenance"))
    if "posttrade" in {item.lower() for item in alpha_provenance}:
        issues.append("runtime firewall spec role 'alpha': posttrade provenance is forbidden")
    return issues


def _validate_hashing_spec(spec: Mapping[str, Any]) -> list[str]:
    issues: list[str] = []
    schema_version = _as_positive_int(spec.get("schema_version"))
    if schema_version is None:
        issues.append("runtime hashing spec: schema_version must be a positive integer")

    algorithm = str(spec.get("algorithm", "")).strip().lower()
    if algorithm not in _SUPPORTED_HASH_ALGOS:
        issues.append(
            f"runtime hashing spec: algorithm '{algorithm}' not in {sorted(_SUPPORTED_HASH_ALGOS)}"
        )

    required_fields = set(_as_str_list(spec.get("require_version_fields")))
    missing_required = sorted(_REQUIRED_HASH_VERSION_FIELDS - required_fields)
    if missing_required:
        issues.append(
            "runtime hashing spec: require_version_fields missing " + ", ".join(missing_required)
        )

    domains = _as_str_list(spec.get("domains"))
    if not domains:
        issues.append("runtime hashing spec: domains must be a non-empty list")

    canonicalization = spec.get("canonicalization")
    if not isinstance(canonicalization, dict):
        issues.append("runtime hashing spec: canonicalization must be a mapping")
    else:
        for bool_field in ("json_sort_keys", "ensure_ascii"):
            if not isinstance(canonicalization.get(bool_field), bool):
                issues.append(f"runtime hashing spec: canonicalization.{bool_field} must be a bool")
    return issues
