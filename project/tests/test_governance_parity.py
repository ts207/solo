from __future__ import annotations

from pathlib import Path


DOCS_ROOT = Path(__file__).parents[2] / "docs"
GENERATED_ROOT = DOCS_ROOT / "generated"


class TestPolicyDomainParity:
    def test_deployable_core_policy_matches_compiled_domain(self):
        from project.events.policy import assert_policy_domain_parity

        issues = assert_policy_domain_parity()
        assert issues == [], (
            "DEPLOYABLE_CORE_EVENT_TYPES diverges from compiled domain runtime_eligible set:\n"
            + "\n".join(f"  - {i}" for i in issues)
        )

    def test_runtime_eligible_ids_are_nonempty(self):
        from project.events.policy import runtime_eligible_event_ids_from_domain

        ids = runtime_eligible_event_ids_from_domain()
        assert len(ids) > 0

    def test_deployable_core_policy_is_domain_derived(self):
        import inspect

        import project.events.policy as policy

        source = inspect.getsource(policy)
        hardcoded = [
            event_type
            for event_type in policy.runtime_eligible_event_ids_from_domain()
            if f'"{event_type}"' in source or f"'{event_type}'" in source
        ]
        assert hardcoded == []


class TestDocLinks:
    """Verify that key file paths referenced in lifecycle docs resolve on disk."""

    def _collect_py_refs(self, text: str) -> list[str]:
        """Extract project/*.py paths referenced in a markdown doc."""
        import re
        return re.findall(r"project/[^\s`\"')]+\.py", text)

    def test_lifecycle_overview_script_refs_exist(self):
        doc = (DOCS_ROOT / "lifecycle" / "overview.md").read_text(encoding="utf-8")
        root = Path(__file__).parents[2]
        missing = [p for p in self._collect_py_refs(doc) if not (root / p).exists()]
        assert missing == [], f"lifecycle/overview.md references missing files: {missing}"

    def test_reference_commands_script_refs_exist(self):
        doc = (DOCS_ROOT / "reference" / "commands.md").read_text(encoding="utf-8")
        root = Path(__file__).parents[2]
        missing = [p for p in self._collect_py_refs(doc) if not (root / p).exists()]
        assert missing == [], f"reference/commands.md references missing files: {missing}"


class TestGeneratedDocStaleness:
    """Generated docs under docs/generated/ must exist (stale content is caught by separate tests)."""

    def test_key_generated_docs_exist(self):
        required = [
            "detector_eligibility_matrix.md",
            "detector_role_inventory.md",
            "event_ontology_mapping.md",
        ]
        missing = [f for f in required if not (GENERATED_ROOT / f).exists()]
        assert missing == [], f"Missing generated docs: {missing}"

    def test_detector_governance_docs_are_nonempty(self):
        for name in ["detector_eligibility_matrix.md", "detector_role_inventory.md"]:
            path = GENERATED_ROOT / name
            content = path.read_text(encoding="utf-8")
            assert len(content.strip()) > 100, f"{name} appears to be empty or trivial"
