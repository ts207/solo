from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from project.core.exceptions import ConfigurationError
from project.domain.compiled_registry import get_domain_registry


class TemplateParameter(BaseModel):
    name: str
    type: str
    default: Any
    description: str = ""


class TemplateSpec(BaseModel):
    name: str
    family: str
    parameters: list[TemplateParameter] = Field(default_factory=list)
    condition_logic: str = "all"
    default_exit_bars: int = 48
    default_stop_bps: float = 50.0
    default_target_bps: float = 100.0


class TemplateRegistry:
    """
    Registry for strategy templates defined in YAML.
    Enables declarative strategy building from event concepts.
    """

    _TEMPLATES: ClassVar[dict[str, TemplateSpec]] = {}

    @classmethod
    def load_from_yaml(cls, path: Path | None = None) -> None:
        del path
        try:
            registry = get_domain_registry()
            cls._TEMPLATES = {}
            for family_name in (
                registry.searchable_event_families
                or registry.unified_payload.get("families", {}).keys()
            ):
                family = str(family_name).strip().upper()
                if not family:
                    continue
                for template_id in registry.family_templates(family):
                    spec_name = f"{family.lower()}_{template_id}"
                    cls._TEMPLATES[spec_name] = TemplateSpec(name=template_id, family=family)
        except Exception as e:
            cls._TEMPLATES = {}
            raise ConfigurationError(f"Failed to load templates from registry: {e}") from e

    @classmethod
    def get_template(cls, family: str, name: str) -> TemplateSpec | None:
        if not cls._TEMPLATES:
            cls.load_from_yaml()
        return cls._TEMPLATES.get(f"{family.lower()}_{name}")

    @classmethod
    def list_templates(cls) -> list[str]:
        if not cls._TEMPLATES:
            cls.load_from_yaml()
        return sorted(cls._TEMPLATES.keys())
