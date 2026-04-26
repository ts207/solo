from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class StageSpec:
    """Specification for a single pipeline stage."""

    name: str
    script: str | Path
    args: list[str] = field(default_factory=list)
    depends_on: list[str] = field(default_factory=list)
    is_template: bool = False

    def expand(self, tf: str, event: str | None = None) -> StageSpec:
        """Expand placeholders in name and args."""
        new_name = self.name.replace("{tf}", tf)
        if event:
            new_name = new_name.replace("{event}", event)

        new_args = [a.replace("{tf}", tf) for a in self.args]
        if event:
            new_args = [a.replace("{event}", event) for a in new_args]

        return StageSpec(
            name=new_name,
            script=self.script,
            args=new_args,
            depends_on=[
                d.replace("{tf}", tf).replace("{event}", event or "") for d in self.depends_on
            ],
        )


@dataclass
class PipelineSpec:
    """Specification for an entire pipeline."""

    name: str
    stages: list[StageSpec] = field(default_factory=list)

    def build_dag(self) -> dict[str, Any]:
        """Convert specs into a runnable DAG."""
        # This will eventually replace the manual building in stages/
        pass
