from __future__ import annotations

from typing import Optional

from .engine import ValidationResult


def format_validation_result(result: ValidationResult, verbose: bool = False) -> str:
    lines = []

    status = "PASS" if result.passed else "FAIL"
    lines.append(f"[{status}] {result.scenario_name}")
    lines.append(f"  Event: {result.event_type} ({result.polarity})")
    lines.append(f"  Detected: {result.events_detected or 'none'}")
    lines.append(f"  Time: {result.execution_time_ms:.2f}ms")

    if verbose and result.errors:
        lines.append("  Errors:")
        for err in result.errors:
            lines.append(f"    - [{err.error_type}] {err.message}")
            if err.details:
                for k, v in err.details.items():
                    lines.append(f"        {k}: {v}")

    return "\n".join(lines)


class TruthReporter:
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.results: list[ValidationResult] = []

    def add_result(self, result: ValidationResult) -> None:
        self.results.append(result)

    def add_results(self, results: list[ValidationResult]) -> None:
        self.results.extend(results)

    def summary(self) -> str:
        if not self.results:
            return "No results to report."

        passed = sum(1 for r in self.results if r.passed)
        failed = sum(1 for r in self.results if not r.passed)
        total = len(self.results)

        lines = [
            f"Truth Validation Summary",
            f"=" * 50,
            f"Total: {total} | Passed: {passed} | Failed: {failed}",
            f"Success Rate: {100 * passed / total:.1f}%",
            "",
        ]

        if failed > 0:
            lines.append("Failures:")
            for result in self.results:
                if not result.passed:
                    lines.append(f"  - {result.scenario_name}")
                    for err in result.errors:
                        lines.append(f"      [{err.error_type}] {err.message}")

        if self.verbose:
            lines.append("")
            lines.append("Details:")
            for result in self.results:
                lines.append(format_validation_result(result, verbose=True))

        return "\n".join(lines)

    def failed_results(self) -> list[ValidationResult]:
        return [r for r in self.results if not r.passed]

    def passed_results(self) -> list[ValidationResult]:
        return [r for r in self.results if r.passed]
