from types import SimpleNamespace

from project.pipelines.run_all_support import evaluate_startup_guards


def test_evaluate_startup_guards_rejects_blueprint_fallback_with_actionable_message():
    message = evaluate_startup_guards(
        args=SimpleNamespace(
            strategy_blueprint_allow_fallback=1,
            mode="research",
            ci_fail_on_non_production_overrides=0,
        ),
        non_production_overrides=[],
    )

    assert (
        message == "INV_NO_FALLBACK_IN_MEASUREMENT: "
        "strategy blueprint fallback cannot be enabled for measured runs"
    )
