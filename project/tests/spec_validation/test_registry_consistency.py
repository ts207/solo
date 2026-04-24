"""Template-family registry consistency test.

Asserts that spec/grammar/family_registry.yaml and
spec/templates/registry.yaml agree on family template lists.
This prevents drift between the canonical authored template surface and the
legacy compatibility family registry.
"""
from __future__ import annotations

from pathlib import Path

import yaml

# ── Paths ─────────────────────────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).parent.parent.parent.parent  # project/ -> EDGEE-main/
_FAMILY_REGISTRY = _REPO_ROOT / "spec" / "grammar" / "family_registry.yaml"
_TEMPLATE_REGISTRY = _REPO_ROOT / "spec" / "templates" / "registry.yaml"


def _load(path: Path) -> dict:
    assert path.exists(), f"Registry file not found: {path}"
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _allowed(registry_dict: dict, family: str) -> set[str]:
    row = registry_dict.get(family, {})
    if not isinstance(row, dict):
        return set()
    return set(row.get("allowed_templates", row.get("templates", [])))


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestRegistryConsistency:
    """Both registry files must agree on allowed_templates for every family."""

    def setup_method(self):
        family_raw = _load(_FAMILY_REGISTRY)
        template_raw = _load(_TEMPLATE_REGISTRY)
        self.family_families: dict = family_raw.get("event_families", {})
        self.template_families: dict = template_raw.get("families", {})

    def test_files_exist(self):
        assert _FAMILY_REGISTRY.exists(), f"Missing: {_FAMILY_REGISTRY}"
        assert _TEMPLATE_REGISTRY.exists(), f"Missing: {_TEMPLATE_REGISTRY}"

    def test_no_families_missing_from_family_registry(self):
        """Every family in template_registry must also appear in family_registry."""
        missing = set(self.template_families) - set(self.family_families)
        assert not missing, (
            f"Families present in template_registry but missing from family_registry: {missing}\n"
            f"Add them to {_FAMILY_REGISTRY}"
        )

    def test_no_families_missing_from_template_registry(self):
        """Every family in family_registry must also appear in template_registry."""
        missing = set(self.family_families) - set(self.template_families)
        assert not missing, (
            f"Families present in family_registry but missing from template_registry: {missing}\n"
            f"Add them to {_TEMPLATE_REGISTRY}"
        )

    def test_allowed_templates_match_for_all_families(self):
        """allowed_templates must be identical (same set) across both files."""
        mismatches = []
        all_families = set(self.family_families) | set(self.template_families)
        for family in sorted(all_families):
            t = _allowed(self.template_families, family)
            f = _allowed(self.family_families, family)
            if t != f:
                only_t = sorted(t - f)
                only_f = sorted(f - t)
                parts = []
                if only_t:
                    parts.append(f"only in template_registry: {only_t}")
                if only_f:
                    parts.append(f"only in family_registry: {only_f}")
                mismatches.append(f"  {family}: {'; '.join(parts)}")

        assert not mismatches, (
            "Registry mismatch — family_registry.yaml and templates/registry.yaml disagree:\n"
            + "\n".join(mismatches)
            + "\n\nFix: update spec/grammar/family_registry.yaml to match "
            "spec/templates/registry.yaml (authoritative source)."
        )

    def test_no_empty_template_lists(self):
        """Every family in template_registry must have at least one allowed template."""
        empties = [f for f, v in self.template_families.items()
                   if not v.get("allowed_templates", v.get("templates", []))]
        assert not empties, (
            f"Families with empty allowed_templates in template_registry: {empties}"
        )

    def test_no_duplicate_templates_within_family(self):
        """No family should list the same template twice in either file."""
        duplicates = []
        for source_name, families in [
            ("template_registry", self.template_families),
            ("family_registry", self.family_families),
        ]:
            for family, meta in families.items():
                templates = meta.get("allowed_templates", meta.get("templates", []))
                seen = set()
                for t in templates:
                    if t in seen:
                        duplicates.append(f"{source_name}/{family}: '{t}' listed twice")
                    seen.add(t)
        assert not duplicates, "Duplicate template entries found:\n" + "\n".join(duplicates)
