from __future__ import annotations

from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, ConfigDict, Field


class ExperimentConfig(BaseModel):
    """
    Pydantic model for experiment configurations.
    Allows extra fields to accommodate various pipeline flags.
    """

    model_config = ConfigDict(extra="allow")

    name: Optional[str] = Field(None, description="Experiment name")
    description: Optional[str] = Field(None, description="Experiment description")
    inherits: Optional[Union[str, List[str]]] = Field(
        None, description="List of base configs to inherit from"
    )
