"""Tests for _validate_phase2_event_chain detector coverage check."""



class TestValidatePhase2EventChainDetectorCoverage:
    def test_no_issues_when_all_detectors_registered(self):
        """When all event types have registered detectors, validation returns empty list."""
        from project.pipelines.run_all import _validate_phase2_event_chain

        issues = _validate_phase2_event_chain()
        # Filter to only detector-related issues
        detector_issues = [i for i in issues if "No registered detector" in i]
        assert detector_issues == [], "These event types lack registered detectors:\n" + "\n".join(
            detector_issues
        )

    def test_function_returns_list(self):
        """_validate_phase2_event_chain must return a list."""
        from project.pipelines.run_all import _validate_phase2_event_chain

        result = _validate_phase2_event_chain()
        assert isinstance(result, list)

    def test_get_detector_none_reported(self, monkeypatch):
        """A missing detector must produce a 'No registered detector' issue."""
        from project.events.phase2 import PHASE2_EVENT_CHAIN
        from project.pipelines import run_all

        # Get first event type from the chain
        first_entry = PHASE2_EVENT_CHAIN[0]
        first_etype = first_entry[0]

        # Patch get_detector to return None for the first event type
        import project.events.detectors.registry as reg_mod

        original_get = reg_mod.get_detector

        def patched_get(etype):
            if etype == first_etype:
                return None
            return original_get(etype)

        monkeypatch.setattr(reg_mod, "get_detector", patched_get)

        issues = run_all._validate_phase2_event_chain()
        detector_issues = [i for i in issues if "No registered detector" in i and first_etype in i]
        assert len(detector_issues) >= 1, (
            f"Expected 'No registered detector for {first_etype}' in issues, got: {issues}"
        )
