from __future__ import annotations


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
