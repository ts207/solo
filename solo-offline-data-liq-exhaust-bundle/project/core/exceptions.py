from __future__ import annotations


class EdgeError(Exception):
    """Base exception for all project-specific errors."""

    pass


class ContractViolationError(EdgeError):
    """Raised when a data contract or structural invariant is violated."""

    pass


class StageExecutionError(EdgeError):
    """Raised when a major pipeline stage or service fails during execution."""

    pass


class ConfigurationError(EdgeError):
    """Raised when configuration or specifications are invalid or missing."""

    pass


class DataIntegrityError(EdgeError):
    """Raised when data source or artifact integrity checks fail."""

    pass


class MissingArtifactError(DataIntegrityError):
    """Raised when a required artifact is missing on a canonical control-plane path."""

    pass


class MalformedArtifactError(DataIntegrityError):
    """Raised when an artifact exists but cannot be parsed as the expected payload type."""

    pass


class SchemaMismatchError(ContractViolationError):
    """Raised when an artifact payload shape or schema version violates contract."""

    pass


class IncompleteLineageError(ContractViolationError):
    """Raised when required validation, promotion, or evidence lineage is incomplete."""

    pass


class CompatibilityRequiredError(ContractViolationError):
    """Raised when a legacy fallback requires explicit compatibility mode."""

    pass


class AmbiguousLatestResolutionError(ContractViolationError):
    """Raised when latest thesis resolution is incomplete or ambiguous."""

    pass


class MalformedReconciliationMetadataError(DataIntegrityError):
    """Raised when reconciliation metadata exists but is malformed or inconsistent."""

    pass


class PromotionDecisionError(EdgeError):
    """Raised when promotion logic cannot reach a valid decision due to data or logic issues."""

    pass


class ArtifactWriteError(EdgeError):
    """Raised when generated artifacts cannot be written or verified."""

    pass


class ArtifactReadError(DataIntegrityError):
    """Raised when an artifact cannot be read from disk in the expected format."""

    pass


class ArtifactPersistenceError(ArtifactWriteError):
    """Raised when an artifact cannot be durably persisted to disk."""

    pass
