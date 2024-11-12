from src.pipeline.models.enums import StageType
from src.pipeline.models.stage import PipelineStage
from src.pipeline.models.step import PipelineStep


class PipelineStageBuilder:
    def __init__(self, name: str, stage_type: StageType):
        self.name: str = name
        self.stage_type = stage_type
        self.steps = []

    def add_step(self, step: PipelineStep) -> 'PipelineStageBuilder':
        self.steps.append(step)
        return self

    def build(self) -> PipelineStage:
        return PipelineStage(name=self.name, stage_type=self.stage_type, steps=self.steps)