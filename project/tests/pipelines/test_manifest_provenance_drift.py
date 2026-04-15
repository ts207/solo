"""
Regression tests for manifest drift detection via data_fingerprint().

Verifies that:
1. Modifying a lake file changes lake_digest and the overall hash.
2. Modifying a spec YAML changes spec_component_hashes and the overall hash.
3. Identical inputs produce identical hashes (stability).
4. Adding a new lake file for a new symbol changes lake_digest and overall hash.

Path conventions (discovered from _lake_fingerprint / _spec_component_digests):
- Lake files:  {data_root}/lake/raw/binance/{perp|spot}/{SYMBOL}/*.{csv,parquet}
- Spec files:  {project_root.parent}/spec/**/*.{yaml,yml,json,csv}
- project_root itself only needs to exist (configs sub-dir is optional).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from project.pipelines.pipeline_provenance import data_fingerprint


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_minimal_lake(data_root: Path, symbol: str, content: str = "ts,price\n1,100\n") -> Path:
    """Create a minimal perp CSV file for *symbol* under the expected lake path."""
    symbol_dir = data_root / "lake" / "raw" / "binance" / "perp" / symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)
    csv_file = symbol_dir / "ohlcv.csv"
    csv_file.write_text(content, encoding="utf-8")
    return csv_file


def _create_minimal_spec(project_root: Path, content: str = "key: value\n") -> Path:
    """Create a minimal spec YAML under {project_root.parent}/spec/."""
    spec_dir = project_root.parent / "spec"
    spec_dir.mkdir(parents=True, exist_ok=True)
    yaml_file = spec_dir / "test_spec.yaml"
    yaml_file.write_text(content, encoding="utf-8")
    return yaml_file


def _fingerprint(symbols, data_root: Path, project_root: Path):
    """Thin wrapper; project_root must exist before calling."""
    project_root.mkdir(parents=True, exist_ok=True)
    return data_fingerprint(symbols, "test-run", project_root=project_root, data_root=data_root)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestLakeFileDriftDetection:
    """Lake-file mutation must be reflected in lake_digest and overall hash."""

    def test_lake_file_modification_changes_data_hash(self, tmp_path):
        """Modifying an existing lake file changes lake_digest independently and overall hash."""
        data_root = tmp_path / "data"
        project_root = tmp_path / "proj"

        csv_file = _create_minimal_lake(data_root, "BTCUSDT")
        _create_minimal_spec(project_root)

        hash_before, prov_before = _fingerprint(["BTCUSDT"], data_root, project_root)
        lake_digest_before = prov_before["lake"]["digest"]

        # Mutate the lake file
        csv_file.write_text("ts,price\n1,999\n", encoding="utf-8")

        hash_after, prov_after = _fingerprint(["BTCUSDT"], data_root, project_root)
        lake_digest_after = prov_after["lake"]["digest"]

        # Overall hash must change
        assert hash_before != hash_after, "Overall hash must change after lake file mutation"

        # lake_digest specifically must change (independent check)
        assert lake_digest_before != lake_digest_after, (
            "lake_digest must change when a lake file is modified"
        )

    def test_adding_new_lake_file_changes_data_hash(self, tmp_path):
        """Adding a new symbol's lake files changes lake_digest and overall hash."""
        data_root = tmp_path / "data"
        project_root = tmp_path / "proj"

        _create_minimal_lake(data_root, "BTCUSDT")
        _create_minimal_spec(project_root)

        hash_before, prov_before = _fingerprint(["BTCUSDT"], data_root, project_root)
        lake_digest_before = prov_before["lake"]["digest"]

        # Add a second symbol
        _create_minimal_lake(data_root, "ETHUSDT")

        hash_after, prov_after = _fingerprint(["BTCUSDT", "ETHUSDT"], data_root, project_root)
        lake_digest_after = prov_after["lake"]["digest"]

        assert hash_before != hash_after, "Overall hash must change when a new lake symbol is added"
        assert lake_digest_before != lake_digest_after, (
            "lake_digest must change when a new lake file is added"
        )


class TestSpecFileDriftDetection:
    """Spec YAML mutation must be reflected in spec_component_hashes and overall hash."""

    def test_spec_file_modification_changes_spec_hash(self, tmp_path):
        """Modifying a spec YAML changes spec_component_hashes and the overall hash."""
        data_root = tmp_path / "data"
        project_root = tmp_path / "proj"

        _create_minimal_lake(data_root, "BTCUSDT")
        spec_file = _create_minimal_spec(project_root)

        hash_before, prov_before = _fingerprint(["BTCUSDT"], data_root, project_root)
        spec_hashes_before = dict(prov_before["spec_component_hashes"])

        # Mutate the spec YAML
        spec_file.write_text("key: changed_value\n", encoding="utf-8")

        hash_after, prov_after = _fingerprint(["BTCUSDT"], data_root, project_root)
        spec_hashes_after = dict(prov_after["spec_component_hashes"])

        assert hash_before != hash_after, "Overall hash must change after spec file mutation"

        # At least one component hash must differ
        changed_components = [
            k for k in spec_hashes_before if spec_hashes_before[k] != spec_hashes_after.get(k)
        ]
        assert changed_components, (
            "At least one spec_component_hashes entry must change when a spec file is modified; "
            f"before={spec_hashes_before}, after={spec_hashes_after}"
        )


class TestHashStability:
    """Identical inputs must produce identical hashes (determinism guard)."""

    def test_identical_inputs_produce_identical_hash(self, tmp_path):
        """Two successive calls with no changes must return the same hash."""
        data_root = tmp_path / "data"
        project_root = tmp_path / "proj"

        _create_minimal_lake(data_root, "BTCUSDT")
        _create_minimal_spec(project_root)

        hash_first, _ = _fingerprint(["BTCUSDT"], data_root, project_root)
        hash_second, _ = _fingerprint(["BTCUSDT"], data_root, project_root)

        assert hash_first == hash_second, (
            "data_fingerprint must be deterministic: identical inputs must produce identical hashes"
        )
