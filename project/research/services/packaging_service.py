from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PackagingConfig:
    run_id: str
    symbols: str
    execute: bool = False


@dataclass
class PackagingResult:
    run_id: str
    success: bool
    error: str | None = None


class PackagingService:
    def prepare_package(
        self, run_id: str, config: PackagingConfig | None = None
    ) -> PackagingResult:
        return PackagingResult(run_id, True)
