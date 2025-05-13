import uuid
from functools import wraps
from typing import Any, Literal

import pandas as pd


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
    def explode(self, df: pd.DataFrame, source_columns: str | list[str], destination_columns: str | list[str],
                drop_duplicates: bool = False) -> pd.DataFrame:
        df[destination_columns] = df[source_columns].map(lambda x: str(x).split("|"))
        df = df.explode(source_columns)
        if drop_duplicates:
            df = df.drop_duplicates()
        return df

    @wrap_columns
    def apply(self, df: pd.DataFrame,
              source_columns: str | list[str],
              destination_columns: str | list[str],
              mapping_function: callable
              ) -> pd.DataFrame:
        df[destination_columns] = df[source_columns].apply(
            lambda row: pd.Series(mapping_function(*row)), axis=1
        )
        return df


    @wrap_columns
    def match(self, df: pd.DataFrame,
              source_columns: str | list[str],
              destination_columns: str | list[str],
              matching_function: callable,
              truth_columns: str | list[str],
              ) -> pd.DataFrame:

        truth_data: list[str] = [row.pop() for row in df[truth_columns].values.tolist()]
        df[destination_columns] = df[source_columns].apply(
            lambda row: pd.Series(matching_function(*row[source_columns], tuple(truth_data))), axis=1
        )
        return df

    @wrap_columns
    def uuid(self, df: pd.DataFrame,
             source_columns: str | list[str],
             destination_columns: str | list[str]) -> pd.DataFrame:

        df_unique = df[source_columns].dropna().drop_duplicates()

        df_unique[destination_columns] = df_unique.apply(
            lambda row: pd.Series(uuid.uuid5(uuid.NAMESPACE_DNS, "_".join(str(val) for val in row))), axis=1
        )

        df = df.merge(df_unique, on=source_columns, how='left')

        return df

    def map(self, df: pd.DataFrame,
            value_column: str,
            reference_column: str,
            merge_column: str,
            destination_column: str,
            how: Literal["left", "right", "inner", "outer", "cross"]) -> pd.DataFrame:
        values_subset = df[value_column].drop_duplicates()
        reference_subset = df[[reference_column, merge_column]].drop_duplicates().rename(
            columns={merge_column: value_column})
        result = pd.merge(reference_subset, values_subset, on=value_column, how=how).rename(
            columns={reference_column: destination_column})

        return pd.merge(df, result, on=value_column, how=how)

    def merge(self, df: pd.DataFrame, merge_df: pd.DataFrame, on: str | list[str],
              how: Literal["left", "right", "inner", "outer", "cross"]) -> pd.DataFrame:
        return pd.merge(df, merge_df, on=on, how=how)
