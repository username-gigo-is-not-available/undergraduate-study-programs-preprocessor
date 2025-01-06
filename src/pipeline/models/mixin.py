from functools import wraps
from pathlib import Path
from typing import Literal, Any

import pandas as pd

from src.config import Config
from src.patterns.strategy.storage_strategy import LocalStorage, MinioStorage


class StorageMixin:
    def __init__(self):
        if Config.STORAGE_TYPE == 'LOCAL':
            if not Config.OUTPUT_DIRECTORY_PATH.exists():
                Config.OUTPUT_DIRECTORY_PATH.mkdir(parents=True)
            self.storage_strategy = LocalStorage()
        elif Config.STORAGE_TYPE == 'MINIO':
            if not Config.MINIO_CLIENT.bucket_exists(Config.MINIO_DESTINATION_BUCKET_NAME):
                Config.MINIO_CLIENT.make_bucket(Config.MINIO_DESTINATION_BUCKET_NAME)
            self.storage_strategy = MinioStorage()
        else:
            raise ValueError(f"Unsupported storage type: {Config.STORAGE_TYPE}")

    def read_data(self, input_file_name: Path) -> pd.DataFrame:
        return self.storage_strategy.read_data(input_file_name)

    def save_data(self, df: pd.DataFrame, output_file_name: Path, column_order: list[str], drop_duplicates: bool = False) -> pd.DataFrame:
        column_order = [col for col in column_order if col in df.columns]
        df = df[column_order]
        if drop_duplicates:
            df = df.drop_duplicates()
        return self.storage_strategy.save_data(df, output_file_name)


class DataTransformationMixin:

    @staticmethod
    def _to_list(value: str | list[str]) -> list[str]:
        if value is not None:
            return [value] if isinstance(value, str) else value
        return []

    @staticmethod
    def _func_args_factory(**kwargs) -> dict[str, Any]:
        for key in ['source_columns', 'destination_columns', 'truth_columns']:
            if key in kwargs:
                kwargs[key] = DataTransformationMixin._to_list(kwargs.get(key))
        return kwargs

    @staticmethod
    def wrap_columns(func: callable) -> callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> pd.DataFrame:
            kwargs = DataTransformationMixin._func_args_factory(**kwargs)
            return func(*args, **kwargs)

        return wrapper

    @wrap_columns
    def explode_column(self, df: pd.DataFrame, source_columns: str | list[str], destination_columns: str | list[str]) -> pd.DataFrame:
        df[destination_columns] = df[source_columns].map(lambda x: x.split("|"))
        df = df.explode(source_columns)
        return df

    @wrap_columns
    def apply_function(self, df: pd.DataFrame,
                       source_columns: str | list[str],
                       destination_columns: str | list[str],
                       mapping_function: callable
                       ) -> pd.DataFrame:
        df[destination_columns] = df[source_columns].apply(
            lambda row: pd.Series(mapping_function(*row)), axis=1
        )
        return df

    @wrap_columns
    def apply_matching(self, df: pd.DataFrame,
                       source_columns: str | list[str],
                       destination_columns: str | list[str],
                       mapping_function: callable,
                       truth_columns: str | list[str]
                       ) -> pd.DataFrame:
        truth_data = tuple([row.pop() for row in df[truth_columns].values.tolist()])

        df[destination_columns] = df[source_columns].apply(
            lambda row: pd.Series(mapping_function(*row, truth_data)), axis=1
        )
        return df

    def generate_indeces(self, df: pd.DataFrame, source_columns: str | list[str], destination_columns: str) -> pd.DataFrame:
        df = df.sort_values(by=source_columns)
        df[destination_columns] = df.groupby(source_columns).ngroup().values
        return df

    def map_values_to_indeces(self, df: pd.DataFrame, value_column: str, reference_column: str, merge_column: str, destination_column: str,
                              how: Literal["left", "right", "inner", "outer", "cross"]) -> pd.DataFrame:
        values_subset = df[value_column].drop_duplicates()
        reference_subset = df[[reference_column, merge_column]].drop_duplicates().rename(columns={merge_column: value_column})
        result = pd.merge(reference_subset, values_subset, on=value_column, how=how).rename(columns={reference_column: destination_column})

        return pd.merge(df, result, on=value_column, how=how).fillna({destination_column: -1}).astype({destination_column: int})

    def merge_dataframes(self, df: pd.DataFrame, merge_df: pd.DataFrame, on: str | list[str],
                         how: Literal["left", "right", "inner", "outer", "cross"]) -> pd.DataFrame:
        return pd.merge(df, merge_df, on=on, how=how)
