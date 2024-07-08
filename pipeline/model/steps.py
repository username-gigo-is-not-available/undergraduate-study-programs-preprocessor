import logging

import pandas as pd


class PipelineStep:
    def __init__(self, name: str, function: callable, source_columns: list[str], destination_columns: list[str], *args, **kwargs):
        self.name = name
        self.function = function
        self.source_columns = source_columns
        self.destination_columns = destination_columns
        self.args = args
        self.kwargs = kwargs

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        logging.info(f"Running step: {self.__repr__()}")
        data[self.destination_columns] = data[self.source_columns].apply(
            lambda x: pd.Series(self.function(*x)), axis=1
        )
        return data

    def __repr__(self):
        return f"PipelineStep(name={self.name}, function={self.function.__name__}, source_columns={self.source_columns}, destination_columns={self.destination_columns})"

    def __str__(self):
        return f"PipelineStep: {self.name}"


