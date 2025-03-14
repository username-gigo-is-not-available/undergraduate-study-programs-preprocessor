import logging

import pandas as pd

from src.pipeline.models.enums import StageType
from src.patterns.builder.step import PipelineStep


class PipelineStage:
    def __init__(self,
                 name: str,
                 stage_type: StageType,
                 steps: list[PipelineStep] | None = None):
        self.name: str = name
        self.stage_type: StageType = stage_type
        self.steps: list[PipelineStep] | None = steps if steps is not None else []

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        logging.info(f"Stage: {repr(self)} started...")
        for step in self.steps:
            data = step.run(data)
        logging.info(f"Stage: {repr(self)} finished.")
        return data

    def add_step(self, step: PipelineStep) -> 'PipelineStage':
        self.steps.append(step)
        return self

    def build(self) -> 'PipelineStage':
        return PipelineStage(name=self.name, stage_type=self.stage_type, steps=self.steps)

    def __repr__(self):
        return f"PipelineStage(name={self.name}, steps={self.steps})"

    def __str__(self):
        return f"{self.name}"
