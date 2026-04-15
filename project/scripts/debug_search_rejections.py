from project.research.search.generator import generate_hypotheses_with_audit
from project.domain.hypotheses import HypothesisSpec
from project.research.search.validation import validate_hypothesis_spec
import logging
import sys

# Configure logging to see rejections
logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)


def debug_rejections():
    print("Generating hypotheses from 'synthetic_truth' with debug logging...")
    # This will trigger log.debug("Rejecting invalid spec %s: %s", ...)
    specs, audit = generate_hypotheses_with_audit("synthetic_truth")

    print("\nSummary of Rejections:")
    print(f"Total Attempted: {audit['counts']['generated']}")
    print(f"Total Rejected:  {audit['counts']['rejected']}")
    print(f"Rejection Reasons: {audit['rejection_reason_counts']}")

    # Analyze the rejected rows to find specific errors
    errors_by_event = {}
    for row in audit["rejected_rows"]:
        # row comes from FeasibilityCheckedHypothesis.to_record()
        # It should have feasibility.details['errors'] if it was a validation_error
        # But wait, looking at generator.py line 226:
        # _record_rejection("validation_error", {"errors": list(errors)})

        # Let's just manually re-validate to be sure
        spec_dict = row.get("spec")  # Not in the record directly usually?
        # Actually FeasibilityCheckedHypothesis.to_record() includes spec?
        # Let's just use the audit data if possible.
        pass

    # Alternative: just iterate through events and templates and validate
    from project.spec_validation.loaders import load_search_spec
    from project.spec_validation.search import expand_triggers
    from project.domain.hypotheses import TriggerSpec, TriggerType
    from itertools import product

    doc = load_search_spec("synthetic_truth")
    expanded = expand_triggers(doc)
    events = expanded.get("events", [])
    templates = doc.get("templates", ["base"])
    horizons = [str(h) for h in doc.get("horizons", ["15m"])]
    directions = [str(d) for d in doc.get("directions", ["long", "short"])]

    print("\nDetailed Rejection Audit:")
    for event_id in events:
        trigger = TriggerSpec.event(event_id)
        for horizon, direction, template in product(horizons, directions, templates):
            spec = HypothesisSpec(
                trigger=trigger,
                direction=direction,
                horizon=horizon,
                template_id=template,
                entry_lag=1,
            )
            errors = validate_hypothesis_spec(spec)
            if errors:
                print(f"REJECTED: {event_id} | {template} | {horizon} | {direction} -> {errors}")


if __name__ == "__main__":
    debug_rejections()
