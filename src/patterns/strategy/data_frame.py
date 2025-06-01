from abc import abstractmethod, ABC

import pandas as pd


class DataFrameStrategy(ABC):
    def __init__(self):
        self._next = None

    def then(self, next_strategy: 'DataFrameStrategy') -> 'DataFrameStrategy':
        current = self
        while current._next is not None:
            current = current._next

        current._next = next_strategy
        return self

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        current: DataFrameStrategy = self
        while current:
            df: pd.DataFrame = current.apply(df)
            current = current._next
        return df

    @abstractmethod
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError("Subclasses must implement the apply method.")



