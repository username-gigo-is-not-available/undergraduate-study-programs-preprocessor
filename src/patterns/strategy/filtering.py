from typing import Any

import pandas as pd


class FilteringStrategy:

    def __and__(self, other: 'FilteringStrategy') -> 'FilteringStrategy':
        return AndFilteringStrategy(self, other)

    def __or__(self, other: 'FilteringStrategy') -> 'FilteringStrategy':
        return OrFilteringStrategy(self, other)

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError("Subclasses must implement the filter method.")


class AndFilteringStrategy(FilteringStrategy):
    def __init__(self, left: FilteringStrategy, right: FilteringStrategy):
        self.left = left
        self.right = right

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        left_result: pd.DataFrame = self.left.filter(df)
        right_result: pd.DataFrame = self.right.filter(df)
        mask: pd.Series = df.index.isin(left_result.index) & df.index.isin(right_result.index)
        return df.loc[mask]


class OrFilteringStrategy(FilteringStrategy):
    def __init__(self, left: FilteringStrategy, right: FilteringStrategy):
        self.left = left
        self.right = right

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        left_result: pd.DataFrame = self.left.filter(df)
        right_result: pd.DataFrame = self.right.filter(df)
        mask: pd.Series = df.index.isin(left_result.index) | df.index.isin(right_result.index)
        return df.loc[mask]


class ColumnValueFilteringStrategy(FilteringStrategy):
    def __init__(self, column: str, value: Any):
        self.column = column
        self.value = value


class ColumnReferenceFilteringStrategy(FilteringStrategy):
    def __init__(self, column: str):
        self.column = column


class NotNullFilteringStrategy(ColumnReferenceFilteringStrategy):
    def __init__(self, column: str):
        super().__init__(column)

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df[self.column].notnull()]


class NotEqualFilteringStrategy(ColumnValueFilteringStrategy):
    def __init__(self, column: str, value: Any):
        super().__init__(column, value)

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df[self.column] != self.value]


class GroupFilteringStrategy(FilteringStrategy):
    def __init__(self, group_by_columns: str | list[str], evaluated_columns: str | list[str]):
        self.group_by_columns = group_by_columns
        self.evaluated_columns = evaluated_columns


class GroupExistsFilteringStrategy(GroupFilteringStrategy):
    def __init__(self, group_by_columns: str | list[str], evaluated_columns: str | list[str]):
        super().__init__(group_by_columns, evaluated_columns)

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        mask: pd.DataFrame = df.groupby(self.group_by_columns)[self.evaluated_columns].transform(
            lambda x: x.notnull().any())
        return df[mask]


class GroupHasAtLeastNMembersFilteringStrategy(GroupFilteringStrategy):
    def __init__(self, group_by_columns: str | list[str], evaluated_columns: str | list[str],
                 threshold_columns: str | list[str]):
        super().__init__(group_by_columns, evaluated_columns)
        self.threshold_columns = threshold_columns

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        mask: pd.Series = (  # type: ignore
                df.groupby(self.group_by_columns)[self.evaluated_columns].transform('count')
                >=
                df.groupby(self.group_by_columns)[self.threshold_columns].transform('first')
        )
        return df[mask]
