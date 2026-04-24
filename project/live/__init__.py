"""Live runtime package public surface."""

__all__ = [
    "DataHealthMonitor",
    "LiveEngineRunner",
    "LiveStateStore",
    "build_runtime_certification_manifest",
    "check_kill_switch_triggers",
]


def __getattr__(name: str):
    if name in {
        "DataHealthMonitor",
        "build_runtime_certification_manifest",
        "check_kill_switch_triggers",
    }:
        from project.live import health_checks

        return getattr(health_checks, name)
    if name == "LiveEngineRunner":
        from project.live.runner import LiveEngineRunner

        return LiveEngineRunner
    if name == "LiveStateStore":
        from project.live.state import LiveStateStore

        return LiveStateStore
    raise AttributeError(f"module 'project.live' has no attribute {name!r}")
