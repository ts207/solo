"""
Specification schema definitions and validation utilities.

This module defines Pydantic models for the declarative specification files
found under the ``spec/`` directory.  The goal of these models is to
validate the structure and types of YAML specs at load time, catching
missing keys or incorrect field types early rather than allowing them to
propagate silently through the pipeline.  While the current models are
minimal, they can be extended over time as new spec fields are added.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

__all__ = ["ObjectiveSpec", "validate_spec"]


class ObjectiveHardGates(BaseModel):
    """Schema for the ``hard_gates`` section of an objective spec."""

    min_trade_count: int | None = Field(None, ge=0)
    max_drawdown_pct: float | None = Field(None, ge=0.0)
    min_oos_sign_consistency: float | None = Field(None, ge=0.0, le=1.0)
    min_p95_cost_survival_bps: float | None = Field(None)
    max_capacity_utilization: float | None = Field(None, ge=0.0, le=1.0)


class ObjectiveScoreWeights(BaseModel):
    """Schema for the ``score_weights`` section of an objective spec."""

    net_return_after_cost: float | None = None
    max_drawdown: float | None = None
    turnover_penalty: float | None = None
    fragility_penalty: float | None = None
    complexity_penalty: float | None = None


class ObjectiveConstraints(BaseModel):
    """Schema for the ``constraints`` section of an objective spec."""

    require_retail_viability: bool | None = None
    forbid_fallback_in_deploy_mode: bool | None = None
    require_low_capital_contract: bool | None = None


class ObjectiveSpec(BaseModel):
    """
    Pydantic model for an objective specification.

    Fields not present in the YAML will default to empty dictionaries where
    appropriate.  Unknown fields are allowed to preserve forward compatibility.
    """

    id: str
    description: str | None = None
    score_weights: ObjectiveScoreWeights = Field(default_factory=ObjectiveScoreWeights)
    hard_gates: ObjectiveHardGates = Field(default_factory=ObjectiveHardGates)
    constraints: ObjectiveConstraints = Field(default_factory=ObjectiveConstraints)

    model_config = ConfigDict(extra="allow")


def validate_spec(spec: dict[str, Any], model: type[BaseModel]) -> BaseModel:
    """
    Validate a specification dictionary against a Pydantic model.

    Parameters
    ----------
    spec : Dict[str, Any]
        Parsed YAML dictionary containing the spec data.  The expected
        structure depends on the supplied ``model``.
    model : type[BaseModel]
        Pydantic model class to validate against.

    Returns
    -------
    BaseModel
        An instance of ``model`` populated with validated data.

    Raises
    ------
    ValueError
        If the specification does not conform to the schema.  The error
        message will include details from Pydantic.
    """
    try:
        return model.model_validate(spec)
    except ValidationError as exc:
        raise ValueError(f"Spec validation failed: {exc}") from exc
