from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class ExperimentConfig(BaseModel):
    """
    Pydantic model for experiment configurations.
    Allows extra fields to accommodate various pipeline flags.
    """

    model_config = ConfigDict(extra="allow")

    name: str | None = Field(None, description="Experiment name")
    description: str | None = Field(None, description="Experiment description")
    inherits: str | list[str] | None = Field(
        None, description="List of base configs to inherit from"
    )
