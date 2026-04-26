import json
from dataclasses import asdict, dataclass, field
from pathlib import Path


@dataclass
class SearchCoverageReport:
    search_spec_name: str
    event_families: list[str] = field(default_factory=list)
    state_families: list[str] = field(default_factory=list)
    num_events: int = 0
    num_states: int = 0
    num_hypotheses: int = 0
    context_cardinalities: dict[str, int] = field(default_factory=dict)


class CoverageReporter:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def save_report(self, report: SearchCoverageReport):
        path = self.output_dir / f"coverage_{report.search_spec_name}.json"
        with open(path, "w") as f:
            json.dump(asdict(report), f, indent=2)
        print(f"Coverage report saved to {path}")
