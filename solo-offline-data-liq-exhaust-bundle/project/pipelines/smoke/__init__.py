"""Smoke pipeline entrypoints."""

from project.pipelines.smoke.smoke_offline import main as smoke_offline_main

__all__ = ["smoke_offline_main"]
