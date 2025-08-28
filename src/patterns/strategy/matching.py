from difflib import SequenceMatcher
from functools import cache

import pandas as pd

from src.configurations import ApplicationConfiguration
from src.patterns.strategy.data_frame import DataFrameStrategy
from src.pipeline.models.enums import CoursePrerequisiteType


class MatchingStrategy(DataFrameStrategy):

    def __init__(self, column: str, values: list[str]) -> None:
        super().__init__()
        self.column = column
        self.values = values

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        @cache
        def match(row: str | None, values: list[str]) -> str | None:
            if row:
                return MatchingStrategy.get_most_similar_string(row, values)
            return None

        df[self.column] = df.apply(
            lambda row: match(row[self.column], tuple(self.values)), axis='columns')
        return df

    @staticmethod
    def get_most_similar_string(arg: str, values: list[str]) -> str:
        similarity = {}
        for value in values:
            ratio: float = SequenceMatcher(None, arg, value).ratio()
            if ratio == ApplicationConfiguration.MAXIMUM_SIMILARITY_RATIO:
                return value
            if ratio >= ApplicationConfiguration.MINIMUM_SIMILARITY_RATIO:
                similarity[value] = ratio

        return max(similarity, key=lambda k: similarity[k]) if similarity else None


class CoursePrerequisiteMatchingStrategy(MatchingStrategy):
    def __init__(self, column: str, course_name_mk_column: str, prerequisite_type_column: str, values: list[str],
                 delimiter: str) -> None:
        super().__init__(column, values)
        self.course_name_mk_column = course_name_mk_column
        self.prerequisite_type_column = prerequisite_type_column
        self.delimiter = delimiter

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        @cache
        def match(prerequisite_course_name_mk: str, course_name_mk: str, prerequisite_type: CoursePrerequisiteType,
                  course_names: list[str]) -> str:

            if prerequisite_type == CoursePrerequisiteType.ONE:

                course_prerequisite = MatchingStrategy.get_most_similar_string(
                    prerequisite_course_name_mk.strip(),
                    course_names
                )

            elif prerequisite_type == CoursePrerequisiteType.ANY:
                course_prerequisites_list = [
                    MatchingStrategy.get_most_similar_string(course.strip(), course_names)
                    for course in prerequisite_course_name_mk.split(self.delimiter)
                ]

                course_prerequisite = self.delimiter.join(
                    course_prerequisites_list) if course_prerequisites_list else None

            elif prerequisite_type == CoursePrerequisiteType.TOTAL:
                course_prerequisite = self.delimiter.join(
                    [course_name for course_name in course_names if course_name != course_name_mk])

            elif prerequisite_type == CoursePrerequisiteType.NONE:
                course_prerequisite = None
            else:
                raise ValueError(f"Invalid course prerequisite type: {prerequisite_type}")
            return course_prerequisite

        df[self.column] = df.apply(
            lambda row: match(row[self.column], row[self.course_name_mk_column], row[self.prerequisite_type_column],
                              tuple(self.values)), axis='columns')
        return df
