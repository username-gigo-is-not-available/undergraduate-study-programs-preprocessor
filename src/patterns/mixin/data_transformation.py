import uuid
from functools import wraps
from typing import Any, Literal

import pandas as pd

from src.patterns.strategy.data_frame import DataFrameStrategy


class DataTransformationMixin:

    @staticmethod
    def _to_list(value: str | list[str]) -> list[str]:
        if value is not None:
            return [value] if isinstance(value, str) else value
        return []

    @staticmethod
    def _normalize_column_args(**kwargs) -> dict[str, Any]:
        for key in ['input_columns', 'output_columns', 'truth_columns', 'on']:
            if key in kwargs:
                kwargs[key] = DataTransformationMixin._to_list(kwargs.get(key))
        return kwargs

    @staticmethod
    def normalize_column_args(func: callable) -> callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> pd.DataFrame:
            kwargs = DataTransformationMixin._normalize_column_args(**kwargs)
            return func(*args, **kwargs)

        return wrapper

    @normalize_column_args
    def explode(self, df: pd.DataFrame, input_columns: str | list[str], output_columns: str | list[str],
                delimiter: str,
                drop_duplicates: bool = False) -> pd.DataFrame:
        df[output_columns] = df[input_columns].map(lambda x: str(x).split(delimiter))
        df = df.explode(input_columns)
        if drop_duplicates:
            df = df.drop_duplicates()
        return df


    def apply(self, df: pd.DataFrame, strategy: DataFrameStrategy) -> pd.DataFrame:
        return strategy.run(df)

    @normalize_column_args
    def uuid(self, df: pd.DataFrame,
             input_columns: str | list[str],
             output_columns: str | list[str]) -> pd.DataFrame:

        df_unique = df[input_columns].dropna().drop_duplicates()

        df_unique[output_columns] = df_unique.apply(
            lambda row: pd.Series(uuid.uuid5(uuid.NAMESPACE_DNS, "_".join(str(val) for val in row))), axis=1
        )

        df = df.merge(df_unique, on=input_columns, how='left')

        return df

    def link(self, df: pd.DataFrame,
            value_column: str,
            reference_column: str,
            merge_column: str,
            output_column: str,
            how: Literal["left", "right", "inner", "outer", "cross"]) -> pd.DataFrame:
        values_subset = df[value_column].drop_duplicates()
        reference_subset = df[[reference_column, merge_column]].drop_duplicates().rename(
            columns={merge_column: value_column})
        result = pd.merge(reference_subset, values_subset, on=value_column, how=how).rename(
            columns={reference_column: output_column})

        return pd.merge(df, result, on=value_column, how=how)

    @normalize_column_args
    def merge(self, df: pd.DataFrame, merge_df: pd.DataFrame, on: str | list[str],
              how: Literal["left", "right", "inner", "outer", "cross"]) -> pd.DataFrame:
        return pd.merge(df, merge_df, on=on, how=how)
