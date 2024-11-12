import logging

import pandas as pd

from src.pipeline.models.mixin import StorageMixin, DataTransformationMixin


class PipelineStep(StorageMixin, DataTransformationMixin):
    def __init__(self, name: str, function: callable, *args, **kwargs):
        super().__init__()
        self.name: str = name
        self.function: callable = function
        self.args = args
        self.kwargs = kwargs

    def run(self, data: pd.DataFrame | None = None) -> pd.DataFrame:
        logging.info(f"Executing step: {repr(self)}...")
        if data is None:
            data = self.function(self, *self.args, **self.kwargs)
        else:
            data = self.function(self, df=data, *self.args, **self.kwargs)
        logging.info(f"Finished executing step: {repr(self)}.")
        return data

    def __repr__(self):
        return (f"PipelineStep(name={self.name!r}, function={self.function!r}, "
                f"args={self.args!r}, kwargs={self.kwargs!r})")

    def __str__(self):
        return f"{self.name}"
