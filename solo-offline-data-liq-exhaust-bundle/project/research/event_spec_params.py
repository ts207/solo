from __future__ import annotations
import argparse
from typing import Any, Dict, TypeVar, Type
from project.events.config import compose_event_config

T = TypeVar("T")


class EventSpecParams:
    """Utility to manage event parameters with hierarchical precedence:
    1. CLI Argument (Explicitly provided)
    2. Event Spec (compose_event_config)
    3. Hardcoded Default
    """

    def __init__(self, event_type: str, args: argparse.Namespace):
        self.event_type = event_type
        self.args = args
        try:
            self.config = compose_event_config(event_type)
            self.spec = self.config.parameters
        except Exception:
            self.config = None
            self.spec = {}

    def get(self, name: str, default: T, type_func: Type[T] = str) -> T:
        # 1. Check CLI args
        cli_val = getattr(self.args, name, None)
        if cli_val is not None:
            return type_func(cli_val)

        # 2. Check Event Spec
        spec_val = self.spec.get(name)
        if spec_val is not None:
            return type_func(spec_val)

        # 3. Fallback to Hardcoded Default
        return type_func(default)
