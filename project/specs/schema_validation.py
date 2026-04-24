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

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError

__all__ = ["ObjectiveSpec", "validate_spec"]


class ObjectiveHardGates(BaseModel):
    """Schema for the ``hard_gates`` section of an objective spec."""

    min_trade_count: Optional[int] = Field(None, ge=0)
    max_drawdown_pct: Optional[float] = Field(None, ge=0.0)
    min_oos_sign_consistency: Optional[float] = Field(None, ge=0.0, le=1.0)
    min_p95_cost_survival_bps: Optional[float] = Field(None)
    max_capacity_utilization: Optional[float] = Field(None, ge=0.0, le=1.0)


class ObjectiveScoreWeights(BaseModel):
    """Schema for the ``score_weights`` section of an objective spec."""

    net_return_after_cost: Optional[float] = None
    max_drawdown: Optional[float] = None
    turnover_penalty: Optional[float] = None
    fragility_penalty: Optional[float] = None
    complexity_penalty: Optional[float] = None


class ObjectiveConstraints(BaseModel):
    """Schema for the ``constraints`` section of an objective spec."""

    require_retail_viability: Optional[bool] = None
    forbid_fallback_in_deploy_mode: Optional[bool] = None
    require_low_capital_contract: Optional[bool] = None


class ObjectiveSpec(BaseModel):
    """
    Pydantic model for an objective specification.

    Fields not present in the YAML will default to empty dictionaries where
    appropriate.  Unknown fields are allowed to preserve forward compatibility.
    """

    id: str
    description: Optional[str] = None
    score_weights: ObjectiveScoreWeights = Field(default_factory=ObjectiveScoreWeights)
    hard_gates: ObjectiveHardGates = Field(default_factory=ObjectiveHardGates)
    constraints: ObjectiveConstraints = Field(default_factory=ObjectiveConstraints)

    model_config = ConfigDict(extra="allow")


def validate_spec(spec: Dict[str, Any], model: type[BaseModel]) -> BaseModel:
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
