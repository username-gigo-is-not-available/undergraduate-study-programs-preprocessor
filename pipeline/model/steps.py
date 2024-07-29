import logging

import pandas as pd


class PipelineStep:
    def __init__(self, name: str, function: callable, source_columns: list[str], destination_columns: list[str], *args, **kwargs):
        self.name: str = name
        self.function: callable = function
        self.source_columns: list[str] = source_columns
        self.destination_columns: list[str] = destination_columns
        self.args = args
        self.kwargs = kwargs

    def run(self, destination_dataframe: pd.DataFrame) -> pd.DataFrame:
        logging.info(f"Running step: {self.__repr__()}")
        if self.function is None:

            destination_dataframe[self.destination_columns.pop()] = destination_dataframe[self.source_columns.pop()].index

        else:
            destination_dataframe[self.destination_columns] = destination_dataframe[self.source_columns].apply(
                lambda x: pd.Series(self.function(*x)), axis=1
            )

        return destination_dataframe

    def __repr__(self):
        return f"PipelineStep(name={self.name}, function={self.function.__name__ if self.function is not None else 'index'}, source_columns={self.source_columns}, destination_columns={self.destination_columns})"

    def __str__(self):
        return f"PipelineStep: {self.name}"
