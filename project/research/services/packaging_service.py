from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Dict


@dataclass
class PackagingConfig:
    run_id: str
    symbols: str
    execute: bool = False


@dataclass
class PackagingResult:
    run_id: str
    success: bool
    error: Optional[str] = None


class PackagingService:
    def prepare_package(
        self, run_id: str, config: Optional[PackagingConfig] = None
    ) -> PackagingResult:
        return PackagingResult(run_id, True)
