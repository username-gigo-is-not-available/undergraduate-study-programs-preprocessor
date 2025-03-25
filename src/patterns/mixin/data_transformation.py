from concurrent.futures import ProcessPoolExecutor
from functools import wraps, partial
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
    def explode_column(self, df: pd.DataFrame, source_columns: str | list[str], destination_columns: str | list[str],
                       drop_duplicates: bool = False) -> pd.DataFrame:
        df[destination_columns] = df[source_columns].map(lambda x: x.split("|"))
        df = df.explode(source_columns)
        if drop_duplicates:
            df = df.drop_duplicates()
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

    def _match_group(self, group: pd.DataFrame, source_columns: str | list[str],
                     destination_columns: str | list[str],
                     mapping_function: callable,
                     truth_columns: str | list[str] ) -> pd.DataFrame:
        truth_data = [row.pop() for row in group[truth_columns].values.tolist()]
        new_group = group.copy()
        is_invalid = True
        while is_invalid:
            new_group[destination_columns] = new_group[source_columns].apply(
                lambda row: pd.Series(mapping_function(*row[source_columns], tuple(truth_data))), axis=1
            )
            invalid_records = [
                item for items in new_group[new_group.isna().any(axis=1)][truth_columns].to_dict(orient='records')
                for item in items.values()
            ]
            is_invalid = bool(invalid_records)
            truth_data = [item for item in truth_data if item not in invalid_records]
            new_group = new_group.dropna()
        return new_group.dropna()

    @wrap_columns
    def apply_matching(self, df: pd.DataFrame,
                       source_columns: str | list[str],
                       destination_columns: str | list[str],
                       mapping_function: callable,
                       group_by_columns: str | list[str],
                       truth_columns: str | list[str],
                       sort_columns: str | list[str],
                       sort_order: dict[str, int]
                       ) -> pd.DataFrame:

        updated_rows = []

        df_grouped = df.sort_values(by=[sort_columns], key=lambda x: x.map(sort_order)).groupby(group_by_columns)
        groups = [group for _, group in df_grouped]
        with ProcessPoolExecutor() as executor:
            partial_match_group = partial(
                self._match_group,
                source_columns=source_columns,
                destination_columns=destination_columns,
                mapping_function=mapping_function,
                truth_columns=truth_columns
            )
            for group in executor.map(partial_match_group, groups):
                updated_rows.append(group)

        return pd.concat(updated_rows, ignore_index=True)

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
