import pandas as pd

from src.pipeline.models.enums import DatasetType
from src.pipeline.models.pipeline import Pipeline
from src.patterns.builder.pipeline_stage_builder import PipelineStageBuilder


class PipelineBuilder:
    def __init__(self, name: str,
                 dataset_type: DatasetType,
                 df: pd.DataFrame | None = None):
        self.name: str = name
        self.dataset_type: DatasetType = dataset_type
        self.stage_builders: list[PipelineStageBuilder] = []
        self.df: pd.DataFrame | None = df

    def add_stage(self, stage_builder: 'PipelineStageBuilder') -> 'PipelineBuilder':
        self.stage_builders.append(stage_builder)
        return self

    def build(self) -> Pipeline:
        stages = [stage_builder.build() for stage_builder in self.stage_builders]
        return Pipeline(name=self.name, dataset_type=self.dataset_type, data=self.df, stages=stages)
