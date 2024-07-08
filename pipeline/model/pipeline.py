import logging

import pandas as pd

from pipeline.model.steps import PipelineStep


class Pipeline:
    def __init__(self, name: str, steps: list[PipelineStep]):
        self.name = name
        self.steps = steps

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        logging.info(f"Running pipeline: {self.__repr__()}")
        for step in self.steps:
            data = step.run(data)
        return data

    def __repr__(self):
        return f"Pipeline(name={self.name})"
