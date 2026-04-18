"""Live data ingestion module."""

__all__ = [
    "LiveEngineRunner",
    "check_kill_switch_triggers",
]


def __getattr__(name: str):
    if name == "LiveEngineRunner":
        from project.live.runner import LiveEngineRunner

        return LiveEngineRunner
    if name == "check_kill_switch_triggers":
        from project.live.health_checks import check_kill_switch_triggers

        return check_kill_switch_triggers
    raise AttributeError(name)
