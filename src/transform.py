import uuid
from functools import wraps
from typing import Any, Literal

import pandas as pd

from src.patterns.strategy.data_frame import DataFrameStrategy
from src.patterns.strategy.filtering import FilteringStrategy


class DataTransformationMixin:

    @staticmethod
    def _to_list(value: str | list[str]) -> list[str]:
        if value is not None:
            return [value] if isinstance(value, str) else value
        return []

    @staticmethod
    def _normalize_column_args(**kwargs) -> dict[str, Any]:
        for key in ['columns', 'input_columns', 'output_columns', 'on', 'left_on', 'right_on']:
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

    @staticmethod
    def _apply_prefix(
            df: pd.DataFrame,
            merge_df: pd.DataFrame,
            on: list[str] | None,
            left_on: list[str] | None,
            right_on: list[str] | None,
            prefix: str | None
    ) -> tuple[pd.DataFrame, list[str] | None]:

        if not prefix:
            return merge_df, right_on

        duplicate_columns: set[str] = set(df.columns) & set(merge_df.columns)

        new_merge_df: pd.DataFrame = merge_df.rename(columns={
            col: f"{prefix}{col}" for col in duplicate_columns
        })
        new_right_on: list[str] = [f"{prefix}{col}" if col in duplicate_columns else col for col in
                                   right_on] if right_on else None

        return new_merge_df, new_right_on

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

    def filter(self, df: pd.DataFrame,
               strategy: FilteringStrategy,
               ) -> pd.DataFrame:
        df: pd.DataFrame = strategy.filter(df)
        return df

    @normalize_column_args
    def select(self, df: pd.DataFrame,
               columns: str | list[str],
               drop_duplicates: bool = False):
        df: pd.DataFrame = df[columns]
        if drop_duplicates:
            df: pd.DataFrame = df.drop_duplicates()
        return df

    @normalize_column_args
    def uuid(self, df: pd.DataFrame,
             input_columns: str | list[str],
             output_columns: str | list[str]) -> pd.DataFrame:

        df_unique = df[input_columns].dropna().drop_duplicates()

        df_unique[output_columns] = df_unique.apply(
            lambda row: pd.Series(str(uuid.uuid5(uuid.NAMESPACE_DNS, "_".join(str(val) for val in row)))), axis=1
        )

        df = df.merge(df_unique, on=input_columns, how='left')

        return df

    @normalize_column_args
    def merge(
            self,
            df: pd.DataFrame,
            merge_df: pd.DataFrame | None = None,
            on: str | list[str] = None,
            left_on: str | list[str] = None,
            right_on: str | list[str] = None,
            prefix: str | None = None,
            how: Literal["left", "right", "inner", "outer", "cross"] = "inner"
    ) -> pd.DataFrame:

        if not on and not left_on and not right_on:
            raise ValueError("One of 'on' or 'left_on' or 'right_on' must be specified")
        if on and left_on or on and right_on:
            raise ValueError("You must specify either `on`, or both `left_on` and `right_on`.")

        merge_df, right_on = self._apply_prefix(df, merge_df, on, left_on, right_on, prefix)

        return pd.merge(
            df,
            merge_df,
            how=how,
            on=on if on else None,
            left_on=None if on else left_on,
            right_on=None if on else right_on,
        )

    @normalize_column_args
    def self_merge(self, df: pd.DataFrame,
                   columns: str | list[str],
                   on: str | list[str] = None,
                   left_on: str | list[str] = None,
                   right_on: str | list[str] = None,
                   prefix: str | None = None,
                   how: Literal["left", "right", "inner", "outer", "cross"] = "inner"):
        lookup: pd.DataFrame = pd.DataFrame(df[columns]).drop_duplicates()
        lookup, right_on = self._apply_prefix(df, lookup, on, left_on, right_on, prefix)
        return pd.merge(df, lookup, left_on=left_on, right_on=right_on, how=how)

