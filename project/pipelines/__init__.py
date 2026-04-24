from project.pipelines.stages.core import build_core_stages
from project.pipelines.stages.evaluation import build_evaluation_stages
from project.pipelines.stages.ingest import build_ingest_stages
from project.pipelines.stages.research import build_research_stages

__all__ = [
    "build_ingest_stages",
    "build_core_stages",
    "build_research_stages",
    "build_evaluation_stages",
]
