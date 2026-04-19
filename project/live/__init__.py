"""Live data ingestion module."""

__all__ = [
    "LiveEngineRunner",
    "DataHealthMonitor",
    "build_runtime_certification_manifest",
    "LiveStateStore",
    "check_kill_switch_triggers",
    "RuntimeTrace",
    "ThesisArbitrationResult",
    "ReconciliationStateTransition",
]


def __dir__():
    return __all__


def __getattr__(name: str):
    if name == "LiveEngineRunner":
        from project.live.runner import LiveEngineRunner

        return LiveEngineRunner
    if name in {"DataHealthMonitor", "build_runtime_certification_manifest"}:
        from project.live.health_checks import (
            DataHealthMonitor,
            build_runtime_certification_manifest,
        )

        return locals()[name]
    if name == "LiveStateStore":
        from project.live.state import LiveStateStore

        return LiveStateStore
    if name == "check_kill_switch_triggers":
        from project.live.health_checks import check_kill_switch_triggers

        return check_kill_switch_triggers
    if name in {"RuntimeTrace", "ThesisArbitrationResult", "ReconciliationStateTransition"}:
        from project.live.runtime_trace import (
            ReconciliationStateTransition,
            RuntimeTrace,
            ThesisArbitrationResult,
        )
        return locals()[name]
    raise AttributeError(name)
