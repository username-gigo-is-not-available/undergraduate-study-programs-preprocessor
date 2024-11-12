import logging

import pandas as pd

from src.pipeline.models.enums import DatasetType
from src.pipeline.models.stage import PipelineStage


class Pipeline:
    def __init__(self,
                 name: str,
                 dataset_type: DatasetType,
                 stages: list[PipelineStage] | None = None,
                 data: pd.DataFrame | None = None,
                 ):
        self.name: str = name
        self.dataset_type: DatasetType = dataset_type
        self.stages: list[PipelineStage] = stages if stages is not None else []
        self.data: pd.DataFrame | None = data.copy() if data is not None else None

    def run(self) -> pd.DataFrame:
        logging.info(f"Pipeline: {repr(self)} started...")
        for stage in self.stages:
            self.data = stage.run(self.data)
        logging.info(f"Pipeline: {repr(self)} finished.")
        return self.data

    def add_stage(self, stage: PipelineStage) -> None:
        self.stages.append(stage)

    def __repr__(self):
        return f"Pipeline(name={self.name}, stages={self.stages})"

    def __str__(self):
        return f"{self.name}"
