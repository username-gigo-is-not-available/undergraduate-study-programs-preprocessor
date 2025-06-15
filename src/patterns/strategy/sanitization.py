import re
from abc import abstractmethod
from functools import cache

import pandas as pd

from src.patterns.strategy.data_frame import DataFrameStrategy


class SanitizationStrategy(DataFrameStrategy):
    def __init__(self, column: str) -> None:
        super().__init__()
        self.column = column

    @abstractmethod
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError("Subclasses must implement the apply method.")

class RemoveExtraDelimitersStrategy(SanitizationStrategy):
    def __init__(self, column: str, delimiter: str) -> None:
        super().__init__(column)
        self.delimiter = delimiter

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        @cache
        def remove(row: str) -> str:
            return self.delimiter.join(part.strip() for part in str(row).split(self.delimiter) if part.strip())
        df[self.column] = df[self.column].map(remove)
        return df

class PreserveAcronymsSentenceCaseStrategy(SanitizationStrategy):
    def __init__(self, column: str) -> None:
        super().__init__(column)

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        @cache
        def sentence_case(row: str) -> str:
            parts: list[str] = re.split(r'(\s+)', row)
            result = []
            first_word = True
            for part in parts:
                if part.isspace():
                    result.append(part)
                elif first_word:
                    result.append(part.capitalize())
                    first_word = False
                else:
                    if not all(char.isupper() for char in part):
                        part = part.lower()
                    result.append(part)
            return ''.join(result)
        df[self.column] = df[self.column].map(sentence_case)
        return df

class ReplaceValuesStrategy(SanitizationStrategy):
    def __init__(self, column: str, values: str | list[str], replacement: str) -> None:
        super().__init__(column)
        if isinstance(values, str):
            values = [values]
        self.values = values
        self.replacement = replacement

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        @cache
        def remove(row: str) -> str:
            for value in self.values:
                row = str(row).replace(value, self.replacement).strip()
            return row.strip()
        df[self.column] = df[self.column].map(remove)
        return df

