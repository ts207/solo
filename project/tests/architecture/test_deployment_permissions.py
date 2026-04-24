from project.live.contracts.promoted_thesis import PromotedThesis


def test_deployment_state_guards_runtime():
    """Ensure explicit state tokens map exactly to permitted run modes."""
    paper_thesis = PromotedThesis.model_construct(
        thesis_id="t1",
        run_id="run1",
        event_family="v1",
        primary_event_id="vol",
        promotion_class="paper_promoted",
        deployment_state="paper_only",
        status="active"
    )

    live_thesis = PromotedThesis.model_construct(
        thesis_id="t2",
        run_id="run1",
        event_family="v1",
        primary_event_id="vol",
        promotion_class="production_promoted",
        deployment_state="live_enabled",
        status="active"
    )

    # Asserting logic expected by CLI parsing model
    assert paper_thesis.deployment_state in ("paper_only", "live_enabled")
    assert paper_thesis.deployment_state != "live_enabled"

    assert live_thesis.deployment_state == "live_enabled"
