from pathlib import Path

from project.scripts.ontology_consistency_audit import run_audit


def test_ontology_audit_has_no_unregistered_materialized_states():
    """TICKET-017: all materialized state IDs must be present in state_registry.yaml."""
    report = run_audit(Path("."))
    unregistered = report["states"]["materialized_not_in_registry"]
    assert not unregistered, (
        f"Found {len(unregistered)} materialized states missing from registry: {unregistered}"
    )


def test_ontology_audit_dead_registry_entries_are_advisory_only():
    """TICKET-017: registry entries that are not yet materialized are acceptable planned states.
    The critical invariant is one-way: every MATERIALIZED state must be registered.
    Registered-but-not-materialized entries are valid future/planned states (e.g. LOW_LIQUIDITY_STATE).
    """
    report = run_audit(Path("."))
    not_materialized = report["states"]["state_registry_not_materialized"]
    assert isinstance(not_materialized, list)  # advisory only — may be non-empty
